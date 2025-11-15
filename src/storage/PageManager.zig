const std = @import("std");
const muscle = @import("../muscle.zig");
const page_types = @import("page_types.zig");
const constants = @import("constants.zig");
const BufferPoolManager = @import("BufferPoolManager.zig");
const Journal = @import("Journal.zig");
const IO = @import("io.zig").IO;

const assert = std.debug.assert;
const print = std.debug.print;
const PageNumber = muscle.PageNumber;
const PAGE_SIZE = muscle.PAGE_SIZE;
const DBMetadataPage = page_types.DBMetadataPage;
const OverflowPage = page_types.OverflowPage;
const FreePage = page_types.FreePage;
const Page = page_types.Page;
const MAX_DIRTY_COUNT_BEFORE_COMMIT = constants.MAX_DIRTY_COUNT_BEFORE_COMMIT;
const serde = muscle.common.serde;

// PageManager is responsible to manage pages of database file.
// It's job is to read and interpret the page by page number
// It's job is to interpret and write page by page number
// It maintains list of dirty pages that are not yet written to the disk
// Depending upon some stratergy it will try to write the pages to database file
pub const PageManager = @This();

allocator: std.mem.Allocator,
io: IO,
cache: BufferPoolManager,
dirty_pages: std.BoundedArray(PageNumber, MAX_DIRTY_COUNT_BEFORE_COMMIT),
journal: Journal,

pub fn init(database_file_path: []const u8, allocator: std.mem.Allocator) !PageManager {
    print("Opening Database file.\n", .{});
    var io = try IO.init(database_file_path);
    // try to read metadata page
    // if we can't read metadata page that means it does not exists (we need to create new metadata page)
    var metadata_buffer = [_]u8{0} ** PAGE_SIZE;
    const bytes_read = try io.read(0, &metadata_buffer);

    if (bytes_read == 0) {
        const initial_metadata = DBMetadataPage.init();
        const bytes = try initial_metadata.to_bytes();
        for (&bytes, 0..bytes.len) |*item, i| metadata_buffer[i] = item.*;
        _ = try io.write(0, &metadata_buffer);
    }

    var dirty_pages = try std.BoundedArray(u32, MAX_DIRTY_COUNT_BEFORE_COMMIT).init(0);
    var cache = try BufferPoolManager.init(allocator);
    // insert metadata page into cache
    try cache.put(0, metadata_buffer, dirty_pages.constSlice());

    // allocator for arrays
    return PageManager{
        .allocator = allocator,
        .io = io,
        .dirty_pages = dirty_pages,
        .cache = cache,
        .journal = try Journal.init(database_file_path),
    };
}

pub fn deinit(self: *PageManager) void {
    self.cache.deinit();
}

// commit gets called when we reach `max_dirty_pages` OR before returning the results to client
// guaranteeing that their updates are persisted.
// If commit fails for any reason rollback will be called
// execution_completed tells us that this is the call from execution engine, in which case we need to
// clear our journal file.
pub fn commit(self: *PageManager, execution_completed: bool) !void {
    // for update queries even if query did not update any rows, commit is still
    // called by execution engine.
    if (self.dirty_pages.len == 0) {
        print("Nothing to commit.\n", .{});
        return;
    }

    // persist journal
    try self.journal.persist();

    // sort for sequential io
    std.mem.sort(u32, self.dirty_pages.slice(), {}, comptime std.sort.asc(u32));
    for (self.dirty_pages.slice()) |page_number| {
        _ = self.io.write(
            page_number,
            // dirty pages will always be inside the cache
            std.mem.asBytes(self.cache.get(page_number).?),
        ) catch |io_err| {
            print("Failed to save page {} during commit.\n", .{page_number});
            return io_err;
        };
    }

    // clear dirty pages
    self.dirty_pages.clear();

    if (execution_completed) try self.journal.clear();
}

// Moves the original page copies from the journal file back to the database file.
pub fn rollback(self: *PageManager) !void {
    var n_reverted: u32 = 0;
    var offset: u32 = 0;
    while (true) {
        // @Todo sequential io // ran into simd error when trying earlier
        // https://chatgpt.com/share/67f2613a-19e0-8000-b8a8-af6aa0fa5725
        var results = try self.journal.batch_get_original_pages(offset);
        if (results.n_read == 0) break;
        for (results.pages[0..results.n_read]) |*item| _ = try self.io.write(item.page_number, &item.page);
        n_reverted += results.n_read;
        if (results.n_read < results.pages.len) break;
        offset += results.n_read;
    }

    // need to truncate file until first_newly_alloced
    if (self.journal.get_first_newly_alloced_page()) |first_newly_alloced| {
        std.debug.print("Truncated file to page number: {}\n", .{first_newly_alloced});
        try self.io.truncate(first_newly_alloced);
    }

    self.dirty_pages.clear();
    try self.journal.clear();
    print("Rollback Completed. Reverted {} pages.\n", .{n_reverted});
}

fn mark_dirty(self: *PageManager, page_number: PageNumber) !void {
    // commit if we reach max dirty pages
    if (self.dirty_pages.len == self.dirty_pages.capacity()) try self.commit(false);
    // mark dirty
    try self.dirty_pages.append(page_number);
}

// update page inside the cache recording it's original state and
// when we reach max dirty pages capacity then call commit
pub fn update_page(self: *PageManager, page_number: u32, page_ptr: anytype) !void {
    // A following scenario can happen: we've given a page reference to someone but they did not
    // call update page quick enough and in between their page gets evicted.
    // Hence, we can't always expect the page which we are going to update to be inside cache.
    const original: *const [4096]u8 = try self.get_page_bytes_from_cache_or_disk(page_number);
    var is_dirty = false;
    for (self.dirty_pages.constSlice()) |item| {
        if (item == page_number) {
            is_dirty = true;
            break;
        }
    }

    if (!is_dirty) {
        try self.mark_dirty(page_number);

        // if some page already dirty we know for sure that it's part of journal
        // if page is not dirty it still can be part of journal.record because we might have
        // updated it during previous partial commit(commit called from PageManager).
        // So it is the job of journal to record conditionally
        try self.journal.record(page_number, original.*);
    }

    // update cache with latest version
    const serialized_bytes: [PAGE_SIZE]u8 = try page_ptr.to_bytes();
    try self.cache.put(page_number, serialized_bytes, self.dirty_pages.constSlice());
}

fn get_page_bytes_from_cache_or_disk(self: *PageManager, page_number: u32) !*const [PAGE_SIZE]u8 {
    const entry = self.cache.get(page_number);
    if (entry) |e| return e;

    // load from disk and cache it
    {
        var buffer = [_]u8{0} ** PAGE_SIZE;
        const bytes_read = self.io.read(page_number, &buffer) catch {
            @panic("Page is not present. The call to fetch invalid page should not have been made.");
        };

        if (bytes_read == 0) {
            @panic("Page does not exist on database file.");
        }

        if (bytes_read != PAGE_SIZE) @panic("Page is corrupted.");
        try self.cache.put(page_number, buffer, self.dirty_pages.constSlice());
    }

    return self.cache.get(page_number).?;
}

pub fn get_page(self: *PageManager, comptime T: type, page_number: u32) !T {
    // deserialize the page inside the cache and return it.
    // @Perf: This returns a Copy
    const page_struct = serde.deserialize_page(T, try self.get_page_bytes_from_cache_or_disk(page_number));
    return page_struct;
}

// find the available free page or allocate new page on disk and update passed in metadata
pub fn alloc_free_page(self: *PageManager, metadata: *DBMetadataPage) !u32 {
    var free_page_number: u32 = undefined;

    if (metadata.first_free_page == 0) {
        //
        // the idea is to return a free page number from here. later caller will call get_[page_type]
        // which internally calls `get_page_bytes_from_cache_or_disk` to get the page.
        // We need to take care of following:
        // 1. Add this to cache Because the page is totally new we don't have it on disk yet and
        // `get_page_bytes_from_cache_or_disk` will fail
        // 2. Mark dirty so it won't get evicted.
        // 3. consider a scenario where we have allocated a new page at the end.
        // first cycle of partial commit gets completed.
        // final cycle fails. During rollback we need to release those new
        // pages as free to use. For this we need our journal to also tell us
        // wheather we allocated some new pages at the end of the database file
        // or used existing pages, and mark those free to use again.

        free_page_number = metadata.total_pages;
        // mark dirty
        try self.mark_dirty(free_page_number);
        // put inside the cache
        try self.cache.put(free_page_number, try FreePage.init().to_bytes(), self.dirty_pages.constSlice());
        // save to journal
        self.journal.maybe_set_first_newly_alloced_page(free_page_number);
        metadata.total_pages += 1;
        return free_page_number;
    } else {
        free_page_number = metadata.first_free_page;
        const free_page = try self.get_page(FreePage, free_page_number);
        metadata.first_free_page = free_page.next;
        metadata.free_pages -= 1;
        return free_page_number;
    }
}

pub fn free(self: *PageManager, metadata: *DBMetadataPage, page_number: PageNumber) !void {
    // We should never attempt to free metadata page
    assert(page_number > 0);

    var free_page = FreePage.init();
    free_page.next = metadata.first_free_page;
    metadata.first_free_page = page_number;
    metadata.free_pages += 1;

    // record the original state inside the journal and mark dirty
    try self.update_page(page_number, &free_page);
}

test {
    std.testing.refAllDecls(@This());
}
