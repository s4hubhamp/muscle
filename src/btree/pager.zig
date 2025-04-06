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

const PagerCacheEntry = union(PageType) {
    DBMetadataPage: DBMetadataPage,
    Page: Page,
    OverflowPage: OverflowPage,
};

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
    dirty_pages: [MAX_CACHE_SIZE]u32 = undefined,
    n_dirty: usize = 0,

    journal: Journal,

    pub fn init(database_file_path: []const u8, allocator: std.mem.Allocator) !Pager {
        var io = try IO.init(database_file_path);
        var metadata: DBMetadataPage = undefined;

        // try to read metadata page
        // if we can't read metadata page that means it does not exists (we need to create new metadata page)
        var metadata_buffer = [_]u8{0} ** PAGE_SIZE;
        const bytes_read = try io.read(0, &metadata_buffer);

        if (bytes_read == 0) {
            print("creating metadata page.\n", .{});
            const initial_metadata = DBMetadataPage.init();
            const bytes = std.mem.asBytes(&initial_metadata);
            for (bytes, 0..bytes.len) |*item, i| metadata_buffer[i] = item.*;
            _ = try io.write(0, &metadata_buffer);
        }

        const ptr: *DBMetadataPage = @ptrCast(@alignCast(&metadata_buffer));
        metadata = ptr.*;

        var cache = PagerCache.init();
        // insert metadata page into cache
        try cache.put(0, PagerCacheEntry{ .DBMetadataPage = metadata });

        return Pager{
            .allocator = allocator,
            .io = io,
            .cache = cache,
            .journal = try Journal.init(database_file_path),
        };
    }

    // 1. write dirty pages to the journal
    // 2. write dirty pages to the file
    // 3. clear journal file
    // If commit fails for any reason rollback will be called
    pub fn commit(self: *Pager) !void {
        if (self.n_dirty == 0) return;

        // sort for sequential io
        std.mem.sort(u32, self.dirty_pages[0..self.n_dirty], {}, comptime std.sort.asc(u32));
        for (self.dirty_pages[0..self.n_dirty]) |page_number| {
            _ = self.io.write(
                page_number,
                std.mem.asBytes(self.cache.get(page_number).?),
            ) catch {
                print("Failed to save page {} calling rollback.\n", .{page_number});
                try self.rollback();
                // stop the process
                return;
            };
        }

        print("Commit successfull\n", .{});
    }

    // Moves the page copies from the journal file back to the database file.
    pub fn rollback(self: *Pager) !void {
        _ = self;
    }

    fn mark_dirty(self: *Pager, page_number: u32, entry: *PagerCacheEntry) !void {
        // if we fill all the cache
        if (self.dirty_pages.len == self.n_dirty) {
            print("Reached max dirty pages. Calling commit()\n", .{});
            try self.commit();
        }

        for (self.dirty_pages[0..self.n_dirty]) |item| {
            if (item == page_number) {
                // already marked dirty and recorded the original state
                return;
            }
        }

        // 1. capture original state of the page into journal if not already captured
        // 2. mark dirty
        self.journal.record(page_number, entry.*);
        self.dirty_pages[self.n_dirty] = page_number;
        self.n_dirty += 1;
    }

    // return a reference to metadata page
    pub fn get_metadata_page(self: *Pager) !*DBMetadataPage {
        const entry = self.cache.get(0).?;
        switch (entry.*) {
            PagerCacheEntry.DBMetadataPage => |*value| {
                try self.mark_dirty(0, entry);
                return value;
            },
            else => unreachable,
        }
    }

    pub fn get_page(self: *Pager, page_number: u32) *Page {
        const entry = self.cache.get(page_number).?;
        switch (entry.*) {
            PagerCacheEntry.Page => |*value| {
                try self.mark_dirty(page_number, entry);
                return value;
            },
            else => unreachable,
        }
    }

    pub fn get_free_page(self: *Pager) !u32 {
        // Note that cache can't ever evict the PageZero
        const metadata = try self.get_metadata_page();
        if (metadata.first_free_page == 0) {
            print("No free pages available. New page will be: {}\n", .{metadata.total_pages});
            return metadata.total_pages;
        } else {
            return metadata.first_free_page;
        }
    }

    fn get_pages() void {}
    fn free_page() void {}

    //// write pages to disk
    //// 1. This is write all or fail all operation.
    //// 2. Update the journal when the transaction succeeds
    fn write_dirty_pages() void {}
};

// @multithreading: When we have many threads we have to make sure that there are no more than one
// mutable references to the same page
const PagerCache = struct {
    cache: [MAX_CACHE_SIZE]CacheItem = undefined,
    n_cached: usize = 0,

    const CacheItem = struct {
        page_number: u32,
        page: PagerCacheEntry,
    };

    pub fn init() PagerCache {
        return PagerCache{};
    }

    pub fn get(self: *PagerCache, page_number: u32) ?*PagerCacheEntry {
        for (0..self.n_cached) |i| {
            const item = &self.cache[i];
            if (item.page_number == page_number) {
                return &item.page;
            }
        }

        return null;
    }

    pub fn put(self: *PagerCache, page_number: u32, value: PagerCacheEntry) !void {
        if (self.cache.len == self.n_cached) {
            return error.CacheIsFull;
        }

        self.cache[self.n_cached] = CacheItem{ .page_number = page_number, .page = value };
        self.n_cached += 1;
    }
};

// Journal stores the original state of pages before update/delete operations
// If update/delete fails in the middle we can rollback to original state
// If update/delete complets we clear the journal
// Note: create is just a update for some page
const Journal = struct {
    io: IO,
    pages: [MAX_CACHE_SIZE]JournalEntry,
    n_recorded: usize,

    const JournalEntry = struct {
        page_number: u32,
        entry: PagerCacheEntry,
    };

    fn init(database_file_path: []const u8) !Journal {
        var journal_file_path = [_]u8{0} ** 128;

        for (database_file_path, 0..) |char, index| journal_file_path[index] = char;
        const JOURNAL_FILE_EXTENSION = ".journal";
        for (JOURNAL_FILE_EXTENSION, database_file_path.len..) |char, index| journal_file_path[index] = char;

        // in posix path should not be null terminated
        const io = try IO.init(journal_file_path[0 .. database_file_path.len + JOURNAL_FILE_EXTENSION.len]);

        return Journal{ .io = io, .pages = undefined, .n_recorded = 0 };
    }

    // record original state of the page
    fn record(self: *Journal, page_number: u32, entry: PagerCacheEntry) void {
        // check if we have already recorded it
        for (self.pages[0..self.n_recorded]) |item| {
            if (item.page_number == page_number) {
                @panic("record should not be called for already recorded pages");
            }
        }

        // if not found then insert it
        assert(self.n_recorded < self.pages.len);
        self.pages[self.n_recorded] = JournalEntry{ .page_number = page_number, .entry = entry };
        self.n_recorded += 1;
    }
};
