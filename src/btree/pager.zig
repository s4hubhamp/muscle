const std = @import("std");
const assert = std.debug.assert;
const print = std.debug.print;
const page = @import("./page.zig");

const DBMetadataPage = page.DBMetadataPage;
const OverflowPage = page.OverflowPage;
const Page = page.Page;
const PageType = page.PageType;

const IO = @import("../io.zig").IO;

pub const PageNumber = u32;
pub const SlotIndex = u16;

// constants
pub const PAGE_SIZE: u16 = 4096;
// number of pages we can have at a time inside our cache
const MAX_CACHE_SIZE = 100;

// Pager is responsible to manage pages of database file.
// It's job is to read and interpret the page by page number
// It's job is to interpret and write page by page number
// It maintains list of dirty pages that are not yet written to the disk
// Depending upon some stratergy it will try to write the pages to database file
pub const Pager = struct {
    allocator: std.mem.Allocator,
    io: IO,
    cache: PagerCache,
    // when this gets full we will write to disk
    // It's important that the capacity of this array is always equals to max pages in cache
    dirty_pages: [MAX_CACHE_SIZE]u32 = undefined,
    n_dirty: usize = 0,
    journal: Journal,

    pub fn init(database_file_path: []const u8, allocator: std.mem.Allocator) !Pager {
        print("Opening Database file\n", .{});
        var io = try IO.init(database_file_path);
        // try to read metadata page
        // if we can't read metadata page that means it does not exists (we need to create new metadata page)
        var metadata_buffer = [_]u8{0} ** PAGE_SIZE;
        const bytes_read = try io.read(0, &metadata_buffer);

        if (bytes_read == 0) {
            const initial_metadata = DBMetadataPage.init();
            const bytes = std.mem.asBytes(&initial_metadata);
            for (bytes, 0..bytes.len) |*item, i| metadata_buffer[i] = item.*;
            _ = try io.write(0, &metadata_buffer);
        }

        var cache = PagerCache.init();
        // insert metadata page into cache
        try cache.put(0, metadata_buffer);

        return Pager{
            .allocator = allocator,
            .io = io,
            .cache = cache,
            .journal = try Journal.init(database_file_path),
        };
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
        if (self.n_dirty == 0) {
            print("Nothing to commit returning\n", .{});
            return;
        }

        // persist journal
        try self.journal.persist();

        // sort for sequential io
        std.mem.sort(u32, self.dirty_pages[0..self.n_dirty], {}, comptime std.sort.asc(u32));
        for (self.dirty_pages[0..self.n_dirty]) |page_number| {
            _ = self.io.write(
                page_number,
                std.mem.asBytes(self.cache.get(page_number).?),
            ) catch |io_err| {
                print("Failed to save page {} calling rollback.\n", .{page_number});
                try self.rollback();
                // stop the process
                return io_err;
            };
        }

        self.dirty_pages = undefined;
        self.n_dirty = 0;

        if (execution_completed) try self.journal.clear();

        print("Commit successfull\n", .{});
    }

    // Moves the original page copies from the journal file back to the database file.
    pub fn rollback(self: *Pager) !void {
        var offset: usize = 0;
        while (true) {
            // TODO sequential io // ran into simd error when trying earlier
            // https://chatgpt.com/share/67f2613a-19e0-8000-b8a8-af6aa0fa5725
            var results = try self.journal.get_original_pages(offset);
            if (results.n_read == 0) break;
            for (results.pages[0..results.n_read]) |*item| _ = try self.io.write(item.page_number, &item.page);
            if (results.n_read < results.pages.len) break;
            offset += 10;
        }

        print("Rollback Completed.\n", .{});
    }

    // 1. check if we reached the max dirty pages capacity, if yes we need to commit
    // existing changes.
    // 2. Record the page's original state inside of the journal file.
    pub fn update_page(self: *Pager, page_number: u32, bytes: [PAGE_SIZE]u8) !void {
        var is_first_time = true;

        for (self.dirty_pages[0..self.n_dirty]) |item| {
            if (item == page_number) {
                // we've already recorded the original version
                // just need to update cache
                is_first_time = false;
                break;
            }
        }

        const original = self.cache.get(page_number).?; // unwrap is safe here
        if (is_first_time) {
            // record original state and also need to mark dirty
            self.journal.record(page_number, original.*);
            self.dirty_pages[self.n_dirty] = page_number;
            self.n_dirty += 1;
        }

        // update cache with latest version
        original.* = bytes;
    }

    // this should be always called if and only if the entry is not present inside the cache
    // 1. try to make space by removing non-dirty pages
    // 2. if we can't make space then we will commit. Mark all pages as clean pages.
    // 3. make space by removing some page
    fn cache_page(self: *Pager, page_number: u32, bytes: [PAGE_SIZE]u8) !void {
        // if we are full
        if (self.cache.is_full()) {
            var commit_and_clean = false;
            // if we can't evict
            if (self.dirty_pages.len == self.n_dirty) {
                commit_and_clean = true;
            } else {
                // try to evict
                const made_space = self.cache.evict(&self.dirty_pages);
                if (!made_space) commit_and_clean = true;
            }

            if (commit_and_clean) {
                try self.commit(false);
                // mark everything as clean
                self.dirty_pages = undefined;
                self.n_dirty = 0;
            }
        }

        // we may fail due to allocation related errors but should never fail with CacheIsFull
        // failure here suggests the bug in above code
        try self.cache.put(page_number, bytes);
    }

    // return a const references to different type of pages
    pub fn get_metadata_page(self: *Pager) *const DBMetadataPage {
        const entry = self.cache.get(0).?; // safe to unwrap
        return @as(*const DBMetadataPage, @ptrCast(@alignCast(entry)));
    }

    pub fn get_overflow_page(self: *Pager, page_number: u32) !*const OverflowPage {
        var entry = self.cache.get(page_number);
        if (entry == null) {
            // load from disk
            var buffer = [_]u8{0} ** PAGE_SIZE;
            const bytes_read = self.io.read(page_number, &buffer) catch {
                @panic("Page is not present. The call to fetch invalid page should not have been made.");
            };
            if (bytes_read != PAGE_SIZE) @panic("Page is corrupted.");
            // put the page inside cache
            try self.cache_page(page_number, buffer);
            entry = &buffer;
        }

        return @as(*const OverflowPage, @ptrCast(@alignCast(entry)));
    }

    pub fn get_free_page(self: *Pager) !u32 {
        // Note that cache can't ever evict the PageZero
        const metadata = self.get_metadata_page();
        if (metadata.first_free_page == 0) {
            print("No free pages available. New page will be: {}\n", .{metadata.total_pages});
            return metadata.total_pages;
        } else {
            return metadata.first_free_page;
        }
    }
};

// @multithreading: When we have many threads we have to make sure that there are no more than one
// mutable references to the same page
const PagerCache = struct {
    cache: [MAX_CACHE_SIZE]CacheItem = undefined,
    n_cached: usize = 0,

    const CacheItem = struct {
        page_number: u32,
        page: [PAGE_SIZE]u8,
    };

    pub fn init() PagerCache {
        return PagerCache{};
    }

    pub fn is_full(self: *PagerCache) bool {
        return self.cache.len == self.n_cached;
    }

    // evict some page to make space. we will evict a non dirty page
    pub fn evict(self: *PagerCache, dirty_pages: []u32) bool {
        var evicted = false;
        for (0..self.n_cached) |i| {
            const item = &self.cache[i];
            const page_number = item.page_number;
            // check if this page is dirty
            var is_dirty = false;
            for (dirty_pages) |n| {
                if (n == page_number) is_dirty = true;
            }

            if (!is_dirty) {
                // evict this one
                for (i..self.n_cached - 1) |j| std.mem.swap(CacheItem, &self.cache[j], &self.cache[j + 1]);
                evicted = true;
                self.n_cached -= 1;
                break;
            }
        }

        return evicted;
    }

    pub fn get(self: *PagerCache, page_number: u32) ?*[PAGE_SIZE]u8 {
        for (0..self.n_cached) |i| {
            const item = &self.cache[i];
            if (item.page_number == page_number) {
                return &item.page;
            }
        }

        return null;
    }

    pub fn put(self: *PagerCache, page_number: u32, buffer: [PAGE_SIZE]u8) !void {
        if (self.cache.len == self.n_cached) {
            return error.CacheIsFull;
        }

        self.cache[self.n_cached] = CacheItem{ .page_number = page_number, .page = buffer };
        self.n_cached += 1;
    }
};

// Journal stores the original state of pages before update/delete operations
// If update/delete fails in the middle we can rollback to original state
// If update/delete complets we clear the journal
// Note: create is just a update for some page
//
const Journal = struct {
    io: IO,
    // TODO rename to records
    pages: [MAX_CACHE_SIZE]JournalEntry,
    n_recorded: usize,
    metadata: JournalMetadataPage,

    // TODO rename to JournalRecord
    const JournalEntry = struct {
        page_number: u32,
        entry: [PAGE_SIZE]u8,
    };

    const JournalMetadataPage = extern struct {
        n_pages: u32,
        pages: [1023]u32,

        comptime {
            assert(@alignOf(JournalMetadataPage) == 4);
            assert(@sizeOf(JournalMetadataPage) == 4096);
        }
    };

    fn init(database_file_path: []const u8) !Journal {
        var journal_file_path = [_]u8{0} ** 128;

        for (database_file_path, 0..) |char, index| journal_file_path[index] = char;
        const JOURNAL_FILE_EXTENSION = ".journal";
        for (JOURNAL_FILE_EXTENSION, database_file_path.len..) |char, index| journal_file_path[index] = char;

        // in posix path should not be null terminated
        print("Opening Journal file\n", .{});
        var io = try IO.init(journal_file_path[0 .. database_file_path.len + JOURNAL_FILE_EXTENSION.len]);

        // load metadata
        var metadata: JournalMetadataPage = undefined;
        var buffer = [_]u8{0} ** PAGE_SIZE;
        const bytes_read = try io.read(0, &buffer);

        if (bytes_read > 0) {
            assert(bytes_read == PAGE_SIZE);
            metadata = @as(*JournalMetadataPage, @ptrCast(@alignCast(&buffer))).*;
        } else {
            metadata = JournalMetadataPage{
                .n_pages = 0,
                .pages = [_]u32{0} ** 1023,
            };
        }

        return Journal{
            .io = io,
            .pages = undefined,
            .n_recorded = 0,
            .metadata = metadata,
        };
    }

    const OriginalPage = struct { page_number: u32, page: [PAGE_SIZE]u8 };
    // we always return a fixed number of pages
    pub fn get_original_pages(self: *Journal, offset: usize) !struct {
        pages: [10]OriginalPage,
        n_read: usize,
    } {
        var pages: [10]OriginalPage = [_]OriginalPage{OriginalPage{ .page_number = 0, .page = undefined }} ** 10;
        // when we are done
        if (offset == self.n_recorded) return .{ .pages = pages, .n_read = 0 };

        var i = offset; // index inside the metadata.pages
        var j: usize = 0; // index inside the pages array
        while (j < pages.len and i < self.n_recorded) {
            const original_page_number = self.metadata.pages[i];
            pages[j].page_number = original_page_number;
            _ = try self.io.read(original_page_number, &pages[j].page);
            i += 1;
            j += 1;
        }

        return .{ .pages = pages, .n_read = j };
    }

    // record original state of the page
    fn record(self: *Journal, page_number: u32, entry: [PAGE_SIZE]u8) void {
        // asserts
        assert(self.n_recorded < self.pages.len);
        for (self.pages[0..self.n_recorded]) |item| {
            // record should not be called for already recorded pages
            assert(item.page_number != page_number);
        }

        // if not found then insert it
        assert(self.n_recorded < self.pages.len);
        self.pages[self.n_recorded] = JournalEntry{ .page_number = page_number, .entry = entry };
        self.n_recorded += 1;
    }

    // Save all the original pages that we currently have in journal to the
    // journal file.
    // After we persist all the pages then we update the header page saving the
    // metadata of journal file.
    // Later when we recover we exactly read those many pages from journal
    // which journal header has (because if we write some pages and crashed before
    // updating the header, later when we recover we know the page numbers.
    fn persist(self: *Journal) !void {
        // persist will get called only when we ran out of cache
        var metadata = &self.metadata;

        for (&self.pages) |*journal_entry| {
            _ = try self.io.write(metadata.n_pages, &journal_entry.entry);
            metadata.pages[metadata.n_pages] = journal_entry.page_number;
            metadata.n_pages += 1;
        }

        // save the updated metadata page
        _ = try self.io.write(0, std.mem.asBytes(&metadata));

        // reset
        self.n_recorded = 0;
        self.pages = undefined;
    }

    fn clear(self: *Journal) !void {
        _ = try self.io.write(0, @as([]const u8, ""));
        self.n_recorded = 0;
        self.pages = undefined;
    }
};
