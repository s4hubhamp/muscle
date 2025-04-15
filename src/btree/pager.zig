const std = @import("std");
const muscle = @import("muscle");
const page = @import("./page.zig");
const IO = @import("../io.zig").IO;
const serde = @import("../serialize_deserialize.zig");

const assert = std.debug.assert;
const print = std.debug.print;
const DBMetadataPage = page.DBMetadataPage;
const OverflowPage = page.OverflowPage;
const FreePage = page.FreePage;
const Page = page.Page;

// number of pages we can have at a time inside our cache
const MAX_CACHE_SIZE = 69;

// When this count is hit, we will commit before proceeding to next query
// various data structures below are optmized based on this number. Since this is a
// small and well known number we've choosen Arrays instead of HashMaps since for smaller
// number of elements arrays perform better or same as hash maps and we can have
// *static* allocations.
const MAX_DIRTY_COUNT_BEFORE_COMMIT = 64;

comptime {
    // This is important.
    // cache.put should always be able to evict non dirty pages.
    // Hence we need to make sure that number of dirty pages are always less than max cached capacity.
    assert(MAX_DIRTY_COUNT_BEFORE_COMMIT < MAX_CACHE_SIZE);
}

// Pager is responsible to manage pages of database file.
// It's job is to read and interpret the page by page number
// It's job is to interpret and write page by page number
// It maintains list of dirty pages that are not yet written to the disk
// Depending upon some stratergy it will try to write the pages to database file
pub const Pager = struct {
    allocator: std.mem.Allocator,
    io: IO,
    cache: PagerCache,
    dirty_pages: std.BoundedArray(u32, MAX_DIRTY_COUNT_BEFORE_COMMIT),
    journal: Journal,

    pub fn init(database_file_path: []const u8, allocator: std.mem.Allocator) !Pager {
        print("Opening Database file.\n", .{});
        var io = try IO.init(database_file_path);
        // try to read metadata page
        // if we can't read metadata page that means it does not exists (we need to create new metadata page)
        var metadata_buffer = [_]u8{0} ** muscle.PAGE_SIZE;
        const bytes_read = try io.read(0, &metadata_buffer);

        if (bytes_read == 0) {
            const initial_metadata = DBMetadataPage.init();
            const bytes = try initial_metadata.to_bytes();
            for (&bytes, 0..bytes.len) |*item, i| metadata_buffer[i] = item.*;
            _ = try io.write(0, &metadata_buffer);
        }

        var dirty_pages = try std.BoundedArray(u32, MAX_DIRTY_COUNT_BEFORE_COMMIT).init(0);
        var cache = try PagerCache.init();
        // insert metadata page into cache
        try cache.put(0, metadata_buffer, dirty_pages.constSlice());

        // allocator for arrays
        return Pager{
            .allocator = allocator,
            .io = io,
            .dirty_pages = dirty_pages,
            .cache = cache,
            .journal = try Journal.init(database_file_path),
        };
    }

    pub fn deinit(self: *Pager) void {
        // @cleanup
        _ = self;
    }

    // commit gets called when we are in the middle of update and we reach dirty pages capacity
    // this is detected when we are trying to cache a new page and cache is full with all dirty pages.
    // since we can't evict anymore we have to commit.
    //
    // 1. write dirty pages to the journal
    // 2. write dirty pages to the file
    // 3. clear journal file
    // 4. mark dirty pages as clean
    // If commit fails for any reason rollback will be called TODO verify all different combinations
    // execution_completed tells us that this is the call from execution engine, which means that need to
    // clear our journal file after updating database file.
    pub fn commit(self: *Pager, execution_completed: bool) !void {
        // for update queries even if query did not update any rows, commit is still
        // called by execution engine.
        if (self.dirty_pages.len == 0) {
            print("Nothing to commit returning\n", .{});
            return;
        }

        // persist journal
        self.journal.persist() catch {
            return error.JournalError;
        };

        // sort for sequential io
        std.mem.sort(u32, self.dirty_pages.slice(), {}, comptime std.sort.asc(u32));
        for (self.dirty_pages.slice()) |page_number| {
            //print("commiting page: {} content: {any}\n", .{
            //    page_number,
            //    std.mem.asBytes(self.cache.get(page_number).?),
            //});
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

        if (execution_completed) self.journal.clear() catch {
            return error.JournalError;
        };

        print("Commit successfull.\n", .{});
    }

    // Moves the original page copies from the journal file back to the database file.
    pub fn rollback(self: *Pager) !void {
        var n_reverted: u32 = 0;
        var offset: u32 = 0;
        while (true) {
            // TODO sequential io // ran into simd error when trying earlier
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
            try self.io.truncate(first_newly_alloced);
        }

        self.dirty_pages.clear();
        try self.journal.clear();
        print("Rollback Completed. Reverted {} pages.\n", .{n_reverted});
    }

    // check if we reached the max dirty pages capacity, if yes we need to commit
    // existing changes.
    // Record the page's original state inside of the journal file.
    pub fn update_page(self: *Pager, page_number: u32, page_ptr: anytype) !void {
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
            // commit if we reach max dirty pages
            if (self.dirty_pages.len == self.dirty_pages.capacity()) try self.commit(false);
            // mark dirty
            try self.dirty_pages.append(page_number);
        }

        // if some page already dirty we know for sure that it's part of journal
        // if page is not dirty it still can be part of journal.record because we might have
        // updated it during previous partial commit(commit called from Pager).
        // So it is the job of journal to check for duplicates
        try self.journal.record(page_number, original.*);

        // update cache with latest version
        const serialized_bytes: [muscle.PAGE_SIZE]u8 = try page_ptr.to_bytes();
        try self.cache.put(page_number, serialized_bytes, self.dirty_pages.constSlice());
    }

    fn get_page_bytes_from_cache_or_disk(self: *Pager, page_number: u32) !*const [muscle.PAGE_SIZE]u8 {
        const entry = self.cache.get(page_number);
        if (entry) |e| return e;

        // load from disk and cache it
        {
            var buffer = [_]u8{0} ** muscle.PAGE_SIZE;
            const bytes_read = self.io.read(page_number, &buffer) catch {
                @panic("Page is not present. The call to fetch invalid page should not have been made.");
            };
            if (bytes_read != muscle.PAGE_SIZE) @panic("Page is corrupted.");
            try self.cache.put(page_number, buffer, self.dirty_pages.constSlice());
        }

        return self.cache.get(page_number).?;
    }

    pub fn get_page(self: *Pager, comptime T: type, page_number: u32) !T {
        // deserialize the page inside the cache and return it.
        // @speed: This returns a Copy
        const page_struct = serde.deserialize_page(T, try self.get_page_bytes_from_cache_or_disk(page_number));
        return page_struct;
    }

    pub fn alloc_free_page(self: *Pager) !u32 {
        var metadata = try self.get_page(DBMetadataPage, 0);
        var free_page_number: u32 = undefined;

        if (metadata.first_free_page == 0) {
            //
            // the idea is to return a free page number from here. later caller will call get_[page_type]
            // which internally calls `get_page_bytes_from_cache_or_disk` to get the page.
            // We need to take care of following:
            // 1. Add this to cache Because the page is totally new we can't have it on disk yet and
            // `get_page_bytes_from_cache_or_disk` will fail
            // 2. Mark dirty so it won't get evicted.
            // 3. consider a scenario where we have allocated a new page at the end.
            // first cycle of partial commit gets completed.
            // final cycle fails. During rollback we need to release those new
            // pages as free to use. For this we need our journal to also tell us
            // wheather we allocated some new pages at the end of the database file
            // or used existing pages, and mark those free to use again.

            free_page_number = metadata.total_pages;
            // put inside the cache
            try self.cache.put(free_page_number, try FreePage.init().to_bytes(), self.dirty_pages.constSlice());
            // mark dirty
            try self.dirty_pages.append(free_page_number);
            // save to journal
            self.journal.maybe_set_first_newly_alloced_page(free_page_number);
            metadata.total_pages += 1;
            try self.update_page(0, &metadata);
            return free_page_number;
        } else {
            free_page_number = metadata.first_free_page;
            const free_page = try self.get_page(FreePage, free_page_number);
            metadata.first_free_page = free_page.next;
            try self.update_page(0, &metadata);
            return free_page_number;
        }
    }
};

// @multithreading: When we have many threads we have to make sure that there are no more than one
// mutable references to the same page
const PagerCache = struct {
    cache: std.BoundedArray(CacheItem, MAX_CACHE_SIZE),

    const CacheItem = struct {
        page_number: u32,
        page: [muscle.PAGE_SIZE]u8,
    };

    pub fn init() !PagerCache {
        return PagerCache{ .cache = try std.BoundedArray(CacheItem, MAX_CACHE_SIZE).init(0) };
    }

    pub fn is_full(self: *PagerCache) bool {
        return self.cache.len == self.cache.capacity();
    }

    // evict some page non dirty page to make space
    pub fn evict(self: *PagerCache, dirty_pages: []const u32) void {
        // we should be always able to evict
        assert(dirty_pages.len < self.cache.len);

        for (self.cache.constSlice(), 0..) |*item, item_index| {
            const page_number = item.page_number;
            // check if this page is dirty
            var is_dirty = false;
            for (dirty_pages) |n| {
                if (n == page_number) is_dirty = true;
            }

            if (!is_dirty) {
                // evict this one
                _ = self.cache.swapRemove(item_index);
                return;
            }
        }

        unreachable;
    }

    pub fn get(self: *PagerCache, page_number: u32) ?*const [muscle.PAGE_SIZE]u8 {
        for (self.cache.slice()) |*item| {
            if (item.page_number == page_number) return &item.page;
        }

        return null;
    }

    // put gets called for existing item or the newer item
    pub fn put(self: *PagerCache, page_number: u32, buffer: [muscle.PAGE_SIZE]u8, dirty_pages: []const u32) !void {
        if (self.is_full()) self.evict(dirty_pages);

        for (self.cache.slice()) |*item| {
            if (item.page_number == page_number) {
                item.page = buffer;
                return;
            }
        }

        try self.cache.append(CacheItem{ .page_number = page_number, .page = buffer });
    }
};

// Journal stores the original state of pages before update/delete operations
// If update/delete fails in the middle we can rollback to original state
// If update/delete complets we clear the journal
// Note: create is just a update for some page
//
const Journal = struct {
    io: IO,
    // unsaved entries
    entries: std.BoundedArray(JournalEntry, MAX_DIRTY_COUNT_BEFORE_COMMIT),
    metadata: JournalMetadataPage,

    const JournalEntry = struct {
        page_number: u32,
        entry: [muscle.PAGE_SIZE]u8,
    };

    const JournalMetadataPage = extern struct {
        first_new_alloced_page: u32,
        n_pages: u32,
        pages: [1022]u32,

        comptime {
            assert(@alignOf(JournalMetadataPage) == 4);
            assert(@sizeOf(JournalMetadataPage) == muscle.PAGE_SIZE);
        }
    };

    fn init(database_file_path: []const u8) !Journal {
        var journal_file_path = [_]u8{0} ** 128;

        for (database_file_path, 0..) |char, index| journal_file_path[index] = char;
        const JOURNAL_FILE_EXTENSION = ".journal";
        for (JOURNAL_FILE_EXTENSION, database_file_path.len..) |char, index| journal_file_path[index] = char;

        // in posix path should not be null terminated
        print("Opening Journal file.\n", .{});
        var io = try IO.init(journal_file_path[0 .. database_file_path.len + JOURNAL_FILE_EXTENSION.len]);

        // load metadata
        var metadata: JournalMetadataPage = undefined;
        var buffer = [_]u8{0} ** muscle.PAGE_SIZE;
        const bytes_read = try io.read(0, &buffer);

        if (bytes_read > 0) {
            assert(bytes_read == muscle.PAGE_SIZE);
            metadata = @as(*JournalMetadataPage, @ptrCast(@alignCast(&buffer))).*;
        } else {
            metadata = JournalMetadataPage{
                // zero indicates no allocation
                .first_new_alloced_page = 0,
                .n_pages = 0,
                .pages = [_]u32{0} ** 1022,
            };
        }

        return Journal{
            .io = io,
            .entries = try std.BoundedArray(JournalEntry, MAX_DIRTY_COUNT_BEFORE_COMMIT).init(0),
            .metadata = metadata,
        };
    }

    const OriginalPage = struct { page_number: u32, page: [muscle.PAGE_SIZE]u8 };
    const BATCH_GET_SIZE = 16;
    comptime {
        assert(BATCH_GET_SIZE <= MAX_DIRTY_COUNT_BEFORE_COMMIT);
    }

    // checks for functions which are supposed to get called after rollback
    fn assert_no_unsaved_entries(self: *Journal) void {
        assert(self.entries.len == 0);
    }

    fn get_first_newly_alloced_page(self: *const Journal) ?u32 {
        if (self.metadata.first_new_alloced_page == 0) {
            return null;
        }
        return self.metadata.first_new_alloced_page;
    }

    fn maybe_set_first_newly_alloced_page(self: *Journal, page_number: u32) void {
        if (self.metadata.first_new_alloced_page == 0) {
            self.metadata.first_new_alloced_page = page_number;
        }
    }

    // return <= BATCH_GET_SIZE of original pages at a time
    // this always reads from the disk
    pub fn batch_get_original_pages(self: *Journal, offset: u32) !struct {
        pages: [BATCH_GET_SIZE]OriginalPage,
        n_read: u32,
    } {
        self.assert_no_unsaved_entries();

        const metadata = &self.metadata;
        var pages = [_]OriginalPage{OriginalPage{ .page_number = 0, .page = undefined }} ** BATCH_GET_SIZE;

        var i: u32 = offset;
        var j: u8 = 0;
        while (i < metadata.n_pages and j < BATCH_GET_SIZE) {
            const original_page_number = self.metadata.pages[i];
            pages[j].page_number = original_page_number;
            _ = try self.io.read(original_page_number, &pages[j].page);
            i += 1;
            j += 1;
        }

        return .{ .pages = pages, .n_read = j };
    }

    // record original state of the page
    // record always called for non recorded pages
    fn record(self: *Journal, page_number: u32, entry: [muscle.PAGE_SIZE]u8) !void {
        // check if record already exists
        for (0..self.metadata.n_pages) |i| {
            if (self.metadata.pages[i] == page_number) {
                return;
            }
        }

        try self.entries.append(JournalEntry{ .page_number = page_number, .entry = entry });
    }

    // Save all the original unsaved pages to the journal file.
    // After we write all the pages then we update the header page saving the
    // metadata of journal file.
    fn persist(self: *Journal) !void {
        if (self.entries.len == 0) return;

        var metadata = &self.metadata;

        // write unsaved pages
        for (self.entries.constSlice()) |*entry| {
            const page_number = metadata.n_pages + 1;
            _ = try self.io.write(page_number, &entry.entry);
            metadata.pages[metadata.n_pages] = entry.page_number;
            metadata.n_pages += 1;
        }

        // write metadata page
        // NOTE: It's important that we write metadata page only after saving
        // all the entries.
        _ = try self.io.write(0, std.mem.asBytes(metadata));

        // reset
        self.entries.clear();
        print("Persisted journal.\n", .{});
        var buf = [_]u8{0} ** muscle.PAGE_SIZE;
        _ = try self.io.read(0, &buf);
    }

    // clear the journal file
    // this gets called only when whole query execution is completed
    fn clear(self: *Journal) !void {
        self.assert_no_unsaved_entries();

        try self.io.truncate(null);
        self.entries.clear();
    }
};
