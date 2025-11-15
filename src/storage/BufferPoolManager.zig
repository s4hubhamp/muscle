const std = @import("std");
const muscle = @import("../muscle.zig");
const constants = @import("constants.zig");

const assert = std.debug.assert;
const PAGE_SIZE = muscle.PAGE_SIZE;
const MAX_CACHE_SIZE = constants.MAX_CACHE_SIZE;

const BufferPoolManager = @This();

cache: std.ArrayList(CacheItem),
allocator: std.mem.Allocator,

const CacheItem = struct {
    page_number: u32,
    page: [PAGE_SIZE]u8,
};

pub fn init(allocator: std.mem.Allocator) !BufferPoolManager {
    return BufferPoolManager{
        .cache = try std.ArrayList(CacheItem).initCapacity(allocator, MAX_CACHE_SIZE),
        .allocator = allocator,
    };
}

pub fn deinit(self: *BufferPoolManager) void {
    self.cache.deinit();
}

pub fn is_full(self: *BufferPoolManager) bool {
    return self.cache.items.len == self.cache.capacity;
}

// evict some non dirty page to make space
pub fn evict(self: *BufferPoolManager, dirty_pages: []const u32) void {
    // we should be always able to evict
    assert(dirty_pages.len < self.cache.items.len);

    for (self.cache.items, 0..) |*item, item_index| {
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

pub fn get(self: *BufferPoolManager, page_number: u32) ?*const [PAGE_SIZE]u8 {
    for (self.cache.items) |*item| {
        if (item.page_number == page_number) return &item.page;
    }

    return null;
}

// put gets called for existing item or the newer item
pub fn put(self: *BufferPoolManager, page_number: u32, buffer: [PAGE_SIZE]u8, dirty_pages: []const u32) !void {
    if (self.is_full()) self.evict(dirty_pages);

    for (self.cache.items) |*item| {
        if (item.page_number == page_number) {
            item.page = buffer;
            return;
        }
    }

    // new item
    try self.cache.append(CacheItem{ .page_number = page_number, .page = buffer });
}
