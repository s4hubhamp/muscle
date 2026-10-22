const std = @import("std");
const muscle = @import("../muscle.zig");
const page_types = @import("page_types.zig");
const Journal = @import("Journal.zig");
const IO = @import("io.zig").IO;

const assert = std.debug.assert;
const PageNumber = muscle.PageNumber;
const PAGE_SIZE = muscle.PAGE_SIZE;
const DBMetadataPage = page_types.DBMetadataPage;
const OverflowPage = page_types.OverflowPage;
const FreePage = page_types.FreePage;
const Page = page_types.Page;
const serde = muscle.common.serde;
const BoundedArray = muscle.common.BoundedArray;

const logger = std.log.scoped(.page_manager);

// Number of pages to cache
// pub const MAX_CACHE_SIZE = 16384; // 64MB
pub const MAX_CACHE_SIZE = 128; //

comptime {
    // Journal holds some pages in it's cache. Max dirty pages is max capacity journal can hold
    assert(Journal.MAX_JOURNAL_UNSAVED_ENTRIES <= MAX_CACHE_SIZE);
}

// PageManager is responsible to manage pages of database file.
// It's job is to read and interpret the page by page number
// It's job is to interpret and write page by page number
// It maintains list of dirty pages that are not yet written to the disk
// Depending upon some strategy it will try to write the pages to database file
const PageManager = @This();

allocator: std.mem.Allocator,

io: IO,

journal: Journal,

clock_position: usize,
clock_reference_bits: []?u1,
page_table: std.AutoHashMap(muscle.PageNumber, struct { index: usize, pinned: bool = false, dirty: bool = false }),
pages: std.ArrayList(?Frame),
n_pinned: usize, // both dirty and pinned pages

const Frame = struct {
    page_number: PageNumber,
    page: muscle.Page,
};

pub fn init(database_file_path: []const u8, allocator: std.mem.Allocator) !PageManager {
    logger.debug("Opening Database file.", .{});
    var io = try IO.init(allocator, database_file_path);
    errdefer io.deinit();

    const clock_reference_bits = try allocator.alloc(?u1, MAX_CACHE_SIZE);
    errdefer allocator.free(clock_reference_bits);
    for (clock_reference_bits) |*bit| {
        bit.* = null;
    }

    var page_table: @FieldType(PageManager, "page_table") = .init(allocator);
    errdefer page_table.deinit();
    var pages = try std.ArrayList(?Frame).initCapacity(allocator, MAX_CACHE_SIZE);
    errdefer pages.deinit(allocator);

    var journal = try Journal.init(allocator, database_file_path);
    errdefer journal.deinit();

    var self = PageManager{
        .allocator = allocator,
        .io = io,
        .journal = journal,
        .clock_position = 0,
        .clock_reference_bits = clock_reference_bits,
        .page_table = page_table,
        .pages = pages,
        .n_pinned = 0,
    };

    try self.load_metadata();
    return self;
}

pub fn deinit(self: *PageManager) void {
    self.allocator.free(self.clock_reference_bits);
    self.page_table.deinit();
    self.pages.deinit(self.allocator);
    self.journal.deinit();
    self.io.deinit();
}

fn load_metadata(self: *PageManager) !void {
    // try to read metadata page
    // if we can't read metadata page that means it does not exists (we need to create new metadata page)
    var buf = [_]u8{0} ** PAGE_SIZE;
    const bytes_read = try self.io.read(0, &buf);

    if (bytes_read == 0) {
        logger.debug("Database file is empty. Initializing metadata for the first time.", .{});
        const initial_metadata = DBMetadataPage.init();
        const bytes = try initial_metadata.to_bytes();
        for (&bytes, 0..bytes.len) |*item, i| buf[i] = item.*;
        _ = try self.io.write(0, &buf);
    } else if (bytes_read != PAGE_SIZE) @panic("Page is corrupted.");

    assert(self.pages.items.len == 0); // Should only get called from init()
    self.pages.appendAssumeCapacity(.{
        .page_number = 0,
        .page = .{ .metadata_page = try serde.deserialize_page(DBMetadataPage, &buf) },
    });
    try self.page_table.put(0, .{ .index = 0 });
    self.clock_reference_bits[0] = 1;
}

// commit gets called when we reach `max_dirty_pages` OR before returning the results to client
// guaranteeing that their updates are persisted.
// If commit fails for any reason rollback will be called
// execution_completed tells us that this is the call from execution engine, in which case we need to
// clear our journal file.
pub fn commit(self: *PageManager, execution_completed: bool) !void {
    // persist journal
    try self.journal.persist();

    for (self.pages.items) |frame| {
        if (frame == null) continue;

        const page_number = frame.?.page_number;
        const info = self.page_table.getPtr(page_number) orelse unreachable;
        if (info.dirty) {
            // @todo sequential IO to disk. Should be handled during file write
            _ = self.io.write(
                page_number,
                // dirty pages will always be inside the cache
                &frame.?.page.to_serialized_bytes(),
            ) catch |io_err| {
                logger.err(
                    "Failed to save a page during commit. page number: {d} error: {any}\n",
                    .{ page_number, io_err },
                );
                return io_err;
            };

            info.dirty = false;
        }
    }

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
        for (results.pages[0..results.n_read]) |*item| {
            _ = try self.io.write(item.page_number, &item.page);

            // Cached pages may still contain the changes that were just rolled back
            //
            // @todo if we knew the type of page we are rolling back we can simply sync the
            // page state with cache
            const removed = self.page_table.fetchRemove(item.page_number);
            if (removed) |kv| {
                assert(self.pages.items[kv.value.index] != null);
                self.pages.items[kv.value.index] = null;
                self.clock_reference_bits[kv.value.index] = null;
            }
        }
        n_reverted += results.n_read;
        if (results.n_read < results.pages.len) break;
        offset += results.n_read;
    }

    // need to truncate file until first_newly_alloced
    if (self.journal.get_first_newly_alloced_page()) |first_newly_alloced| {
        logger.debug("Truncated file to page number: {}\n", .{first_newly_alloced});
        try self.io.truncate(first_newly_alloced);
        for (self.pages.items, 0..) |*frame, index| {
            if (frame.*) |cached| {
                if (cached.page_number >= first_newly_alloced) {
                    _ = self.page_table.remove(cached.page_number);
                    frame.* = null;
                    self.clock_reference_bits[index] = null;
                }
            }
        }
    }

    try self.journal.clear();
    logger.debug("Rollback Completed. Reverted {} pages.", .{n_reverted});
}

// update page inside the cache recording it's original state and
// when we reach max dirty pages capacity then call commit
pub fn update_page(self: *PageManager, page_number: muscle.PageNumber, page: anytype) !void {
    // A following scenario can happen: we've given a page reference to someone but they did not
    // call update page quick enough and in between their page gets evicted.
    // Hence, we can't always expect the page which we are going to update to be inside our cache.
    const PageT = comptime switch (@typeInfo(@TypeOf(page))) {
        .pointer => |p| if (p.size == .one) p.child else @TypeOf(page),
        else => @TypeOf(page),
    };
    const value: PageT = if (comptime @typeInfo(@TypeOf(page)) == .pointer) page.* else page;
    if (!self.page_table.contains(page_number)) {
        try self.load(PageT, page_number);
    }

    const page_info = self.page_table.getPtr(page_number).?;
    if (!page_info.dirty) {
        const original_bytes = self.pages.items[page_info.index].?.page.to_serialized_bytes();
        // if some page already dirty we know for sure that it's part of journal
        // if page is not dirty it still can be part of journal.record because we might have
        // updated it during previous partial commit(commit called from PageManager).
        // So it is the job of journal to record conditionally
        try self.journal.record(page_number, original_bytes);
        page_info.dirty = true;
    }
    self.pages.items[page_info.index].?.page = @unionInit(muscle.Page, @tagName(tag_for(muscle.Page, PageT)), value);
    self.clock_reference_bits[page_info.index] = 1;
}

fn tag_for(comptime U: type, comptime T: type) std.meta.Tag(U) {
    inline for (@typeInfo(U).@"union".fields) |f| {
        if (f.type == T) return @field(std.meta.Tag(U), f.name);
    }
    @compileError("type " ++ @typeName(T) ++ " is not a member of " ++ @typeName(U));
}

pub fn get_page(self: *PageManager, comptime T: type, page_number: muscle.PageNumber) !T {
    if (!self.page_table.contains(page_number)) {
        try self.load(T, page_number);
    }

    const info = self.page_table.get(page_number).?;
    self.clock_reference_bits[info.index] = 1;
    const page = self.pages.items[info.index].?.page;
    assert(std.meta.activeTag(page) == tag_for(muscle.Page, T));

    return @field(page, @tagName(tag_for(muscle.Page, T)));
}

// if page is not inside the cache, loads it and stores inside the cache
fn load(self: *PageManager, comptime T: type, page_number: muscle.PageNumber) !void {
    if (self.page_table.get(page_number)) |info| {
        self.clock_reference_bits[info.index] = 1;
        return;
    }

    var buf = [_]u8{0} ** PAGE_SIZE;
    const bytes_read = self.io.read(page_number, &buf) catch {
        @panic("Page is not present. The call to fetch invalid page should not have been made.");
    };

    if (bytes_read == 0) {
        @panic("Page does not exist on database file.");
    }

    if (bytes_read != PAGE_SIZE) @panic("Page is corrupted.");

    const frame = Frame{
        .page_number = page_number,
        .page = @unionInit(muscle.Page, @tagName(tag_for(muscle.Page, T)), try serde.deserialize_page(T, &buf)),
    };
    try self.cache_frame(frame, false);
}

fn cache_frame(self: *PageManager, frame: Frame, mark_dirty: bool) !void {
    // Allocate before choosing a victim so failure preserves all cached mappings.
    try self.page_table.ensureUnusedCapacity(1);
    if (self.pages.items.len < MAX_CACHE_SIZE) {
        try self.pages.ensureUnusedCapacity(self.allocator, 1);
    }
    const index = self.next_free_slot() orelse blk: {
        try self.commit(false);
        break :blk self.next_free_slot() orelse return error.CacheFull;
    };

    if (index == self.pages.items.len) {
        self.pages.appendAssumeCapacity(frame);
    } else {
        if (self.pages.items[index]) |victim| {
            const removed = self.page_table.remove(victim.page_number);
            assert(removed);
        }
        self.pages.items[index] = frame;
    }

    self.page_table.putAssumeCapacity(frame.page_number, .{
        .index = index,
        .dirty = mark_dirty,
    });

    self.clock_reference_bits[index] = 1;
}

fn next_free_slot(self: *PageManager) ?usize {
    var idx: ?usize = null;
    for (self.pages.items, 0..) |page, i| {
        if (page == null) {
            idx = i;
            break;
        }
    }

    if (idx == null) {
        if (self.pages.items.len < MAX_CACHE_SIZE) {
            idx = self.pages.items.len;
        } else {
            // Two passes give every referenced frame one second chance.
            var scanned: usize = 0;
            while (scanned < 2 * self.pages.items.len) : (scanned += 1) {
                const index = self.clock_position;
                self.clock_position = (index + 1) % self.pages.items.len;
                const victim = self.pages.items[index].?;
                const info = self.page_table.get(victim.page_number).?;
                if (info.pinned or info.dirty) continue;

                if (self.clock_reference_bits[index] == 1) {
                    self.clock_reference_bits[index] = 0;
                    continue;
                }

                idx = index;
                break;
            }
        }
    }

    return idx orelse null;
}

pub fn alloc_free_page(self: *PageManager, comptime tag: muscle.PageType, metadata: *DBMetadataPage) !muscle.PageNumber {
    if (metadata.first_free_page == 0) {
        //
        // Because the page is not on the disk we need to make sure it's pinned / marked dirty in cache
        // to make sure it's not evicted.
        //

        const free_page_number = metadata.total_pages;
        try self.cache_frame(.{ .page_number = free_page_number, .page = muscle.Page.init(tag) }, true);
        // Save to journal so we can revert in case of rollback
        self.journal.maybe_set_first_newly_alloced_page(free_page_number);
        metadata.total_pages += 1;
        return free_page_number;
    } else {
        const free_page_number = metadata.first_free_page;
        const free_page = try self.get_page(FreePage, free_page_number);
        metadata.first_free_page = free_page.next;
        metadata.free_pages -= 1;
        var new_page = muscle.Page.init(tag);
        switch (new_page) {
            inline else => |*payload| try self.update_page(free_page_number, payload),
        }
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

    try self.update_page(page_number, &free_page);
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("../testing/page_manager.zig");
}
