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
        try cache.put(0, PageType{ .DBMetadataPage = metadata });

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
    pub fn commit(self: *Pager) !void {
        _ = self;
    }
    // Moves the page copies from the journal file back to the database file.
    pub fn rollback(self: *Pager) !void {
        _ = self;
    }

    // return a reference to metadata page
    pub fn get_metadata_page(self: *Pager) *DBMetadataPage {
        // Note that cache should not evict page 0
        const entry = self.cache.get(0).?;
        switch (entry.*) {
            .DBMetadataPage => |*metadata| {
                return metadata;
            },
            else => unreachable,
        }
    }

    pub fn get_free_page(self: *Pager) u32 {
        // Note that cache can't ever evict the PageZero
        const metadata = self.get_metadata_page();
        if (metadata.first_free_page == 0) {
            print("No free pages available. New page will be: {}\n", .{metadata.total_pages});
            return metadata.total_pages;
        } else {
            return metadata.first_free_page;
        }
    }

    // see if the page is inside the cache or else load page from disk, cache it
    // and return pointer
    fn get_page() void {}
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
        page_type: PageType,
    };

    pub fn init() PagerCache {
        return PagerCache{};
    }

    pub fn get(self: *PagerCache, page_number: u32) ?*PageType {
        for (0..self.n_cached) |i| {
            const item = &self.cache[i];
            if (item.page_number == page_number) {
                return &item.page_type;
            }
        }

        return null;
    }

    pub fn put(self: *PagerCache, page_number: u32, page_type: PageType) !void {
        if (self.cache.len == self.n_cached) {
            return error.CacheIsFull;
        }

        self.cache[self.n_cached] = CacheItem{ .page_number = page_number, .page_type = page_type };
        self.n_cached += 1;
    }
};

// Journal stores the original state of pages before update/delete operations
// If update/delete fails in the middle we can rollback to original state
// If update/delete complets we clear the journal
// Note: create is just a update for some page
const Journal = struct {
    io: IO,

    fn init(database_file_path: []const u8) !Journal {
        var journal_file_path = [_]u8{0} ** 128;

        for (database_file_path, 0..) |char, index| journal_file_path[index] = char;
        const JOURNAL_FILE_EXTENSION = ".journal";
        for (JOURNAL_FILE_EXTENSION, database_file_path.len..) |char, index| journal_file_path[index] = char;

        // in posix path should not be null terminated
        const io = try IO.init(journal_file_path[0 .. database_file_path.len + JOURNAL_FILE_EXTENSION.len]);

        return Journal{
            .io = io,
        };
    }
};

const std = @import("std");
const assert = std.debug.assert;
const print = std.debug.print;
const page = @import("./page.zig");
const DBMetadataPage = page.DBMetadataPage;
const PageType = page.PageType;
const IO = @import("../io.zig").IO;
