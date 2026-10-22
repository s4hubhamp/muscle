const std = @import("std");
const muscle = @import("../muscle.zig");
const Fixture = @import("helpers.zig").PageManagerFixture;
const PageManager = muscle.PageManager;
const Metadata = muscle.page_types.DBMetadataPage;
const Page = muscle.page_types.Page;
const FreePage = muscle.page_types.FreePage;

fn page_with_marker(marker: u8) Page {
    var page = Page.init();
    @memset(&page.content, marker);
    return page;
}

fn allocate(pager: *PageManager, marker: u8) !u32 {
    var metadata = try pager.get_page(Metadata, 0);
    const number = try pager.alloc_free_page(.data_page, &metadata);
    const page = page_with_marker(marker);
    try pager.update_page(number, &page);
    // Allocation and free update caller-owned metadata, which the caller saves.
    try pager.update_page(0, &metadata);
    return number;
}

fn expect_page(pager: *PageManager, number: u32, marker: u8) !void {
    const actual = try pager.get_page(Page, number);
    const expected_bytes = try page_with_marker(marker).to_bytes();
    const actual_bytes = try actual.to_bytes();
    try std.testing.expectEqualSlices(u8, &expected_bytes, &actual_bytes);
}

fn expect_disk_page(pager: *PageManager, number: u32, marker: u8) !void {
    var bytes: [muscle.PAGE_SIZE]u8 = undefined;
    try std.testing.expectEqual(@as(usize, muscle.PAGE_SIZE), try pager.io.read(number, &bytes));
    const expected = try page_with_marker(marker).to_bytes();
    try std.testing.expectEqualSlices(u8, &expected, &bytes);
}

fn expect_eof(pager: *PageManager, number: u32) !void {
    var bytes: [muscle.PAGE_SIZE]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try pager.io.read(number, &bytes));
}

fn dirty_count(pager: *PageManager) usize {
    var count: usize = 0;
    var entries = pager.page_table.valueIterator();
    while (entries.next()) |info| {
        if (info.dirty) count += 1;
    }
    return count;
}

// Both directions must agree, including frames that have been evicted or invalidated.
fn expect_cache_consistent(pager: *PageManager) !void {
    try std.testing.expect(pager.pages.items.len <= PageManager.MAX_CACHE_SIZE);
    var occupied: usize = 0;
    for (pager.pages.items, 0..) |maybe_frame, index| {
        if (maybe_frame) |frame| {
            occupied += 1;
            const info = pager.page_table.get(frame.page_number);
            try std.testing.expect(info != null);
            try std.testing.expectEqual(index, info.?.index);
            try std.testing.expect(pager.clock_reference_bits[index] != null);
        }
    }
    try std.testing.expectEqual(occupied, pager.page_table.count());
}

fn write_disk_page(pager: *PageManager, number: u32, marker: u8) !void {
    const bytes = try page_with_marker(marker).to_bytes();
    try std.testing.expectEqual(@as(usize, muscle.PAGE_SIZE), try pager.io.write(number, &bytes));
}

fn fill_clean_cache(pager: *PageManager) !void {
    for (1..PageManager.MAX_CACHE_SIZE) |i| {
        const number: u32 = @intCast(i);
        try write_disk_page(pager, number, @truncate(i));
        try expect_page(pager, number, @truncate(i));
    }
    try expect_cache_consistent(pager);
}

fn expect_empty_journal(pager: *PageManager) !void {
    var bytes: [muscle.PAGE_SIZE]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try pager.journal.io.read(0, &bytes));
    try std.testing.expectEqual(@as(u32, 0), (try pager.journal.batch_get_original_pages(0)).n_read);
    try std.testing.expectEqual(@as(?u32, null), pager.journal.get_first_newly_alloced_page());
}

test "PageManager initializes metadata and returns independent page copies" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try std.testing.expectEqualDeep(Metadata.init(), try pager.get_page(Metadata, 0));
    var copy = try pager.get_page(Metadata, 0);
    copy.total_pages = 99;
    try std.testing.expectEqualDeep(Metadata.init(), try pager.get_page(Metadata, 0));
    try std.testing.expectEqual(@as(usize, 0), dirty_count(pager));
    try expect_eof(pager, 1);
    try pager.commit(true);
    try fixture.reopen();
    try std.testing.expectEqualDeep(Metadata.init(), try fixture.pager.?.get_page(Metadata, 0));
}

test "PageManager cache loads disk pages and refreshes reference bits on hits" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try write_disk_page(pager, 1, 42);
    try expect_page(pager, 1, 42);
    const index = pager.page_table.get(1).?.index;
    try std.testing.expectEqual(@as(usize, 2), pager.pages.items.len);
    try std.testing.expectEqual(@as(?u1, 1), pager.clock_reference_bits[index]);

    // A cache hit must return the cached version even if the file changes.
    try write_disk_page(pager, 1, 99);
    pager.clock_reference_bits[index] = 0;
    try expect_page(pager, 1, 42);
    try std.testing.expectEqual(@as(?u1, 1), pager.clock_reference_bits[index]);
    try std.testing.expectEqual(@as(usize, 2), pager.pages.items.len);
    try expect_cache_consistent(pager);
    try fixture.reopen();
    try expect_page(&fixture.pager.?, 1, 99);
}

test "PageManager cache deserializes every page type" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    const types = .{ Metadata, Page, FreePage, muscle.page_types.OverflowPage };
    const tags = [_]muscle.PageType{ .metadata_page, .data_page, .free_page, .overflow_page };
    inline for (types, tags, 0..) |T, tag, i| {
        const expected = @field(muscle.Page.init(tag), @tagName(tag));
        const bytes = muscle.common.serde.serialize_page(expected);
        const number: u32 = @intCast(i + 1);
        _ = try pager.io.write(number, &bytes);
        try std.testing.expectEqualDeep(expected, try pager.get_page(T, number));
    }
    try expect_cache_consistent(pager);
}

test "PageManager cache evicts exactly one frame and keeps mappings consistent" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try fill_clean_cache(pager);
    const incoming: u32 = PageManager.MAX_CACHE_SIZE;
    try write_disk_page(pager, incoming, 77);
    try expect_page(pager, incoming, 77);
    try expect_cache_consistent(pager);
    try std.testing.expectEqual(@as(usize, PageManager.MAX_CACHE_SIZE), pager.pages.items.len);
    try std.testing.expectEqual(@as(usize, PageManager.MAX_CACHE_SIZE), pager.page_table.count());

    var evicted: usize = 0;
    for (0..PageManager.MAX_CACHE_SIZE) |i| {
        if (!pager.page_table.contains(@intCast(i))) evicted += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), evicted);
}

test "PageManager cache clock gives referenced frames a second chance" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try fill_clean_cache(pager);
    pager.clock_position = 0;
    pager.clock_reference_bits[0] = 1;
    pager.clock_reference_bits[1] = 0;
    const incoming: u32 = PageManager.MAX_CACHE_SIZE;
    try write_disk_page(pager, incoming, 77);
    try expect_page(pager, incoming, 77);
    try std.testing.expect(pager.page_table.contains(0));
    try std.testing.expect(!pager.page_table.contains(1));
    try std.testing.expectEqual(@as(?u1, 0), pager.clock_reference_bits[0]);
    try expect_cache_consistent(pager);
}

test "PageManager cache preserves pinned and dirty frames during eviction" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try fill_clean_cache(pager);
    pager.page_table.getPtr(0).?.pinned = true;
    pager.page_table.getPtr(1).?.dirty = true;
    pager.clock_position = 0;
    const incoming: u32 = PageManager.MAX_CACHE_SIZE;
    try write_disk_page(pager, incoming, 77);
    try expect_page(pager, incoming, 77);
    try std.testing.expect(pager.page_table.get(0).?.pinned);
    try std.testing.expect(pager.page_table.get(1).?.dirty);
    try std.testing.expectEqual(@as(u32, 0), pager.pages.items[0].?.page_number);
    try std.testing.expectEqual(@as(u32, 1), pager.pages.items[1].?.page_number);
    try expect_cache_consistent(pager);
}

test "PageManager cache reuses slots invalidated by rollback" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try write_disk_page(pager, 1, 11);
    try expect_page(pager, 1, 11);
    const index = pager.page_table.get(1).?.index;
    const changed = page_with_marker(99);
    try pager.update_page(1, &changed);
    try pager.rollback();
    try std.testing.expect(!pager.page_table.contains(1));
    try std.testing.expect(pager.pages.items[index] == null);
    try write_disk_page(pager, 2, 22);
    try expect_page(pager, 2, 22);
    try std.testing.expectEqual(index, pager.page_table.get(2).?.index);
    try std.testing.expectEqual(@as(usize, 2), pager.pages.items.len);
    try expect_cache_consistent(pager);
    try expect_disk_page(pager, 1, 11);
}

test "PageManager update writes the supplied page on commit" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try write_disk_page(pager, 1, 11);
    try expect_page(pager, 1, 11);
    const changed = page_with_marker(99);
    try pager.update_page(1, &changed);
    try std.testing.expect(pager.page_table.get(1).?.dirty);
    try expect_disk_page(pager, 1, 11);
    try pager.commit(true);
    try std.testing.expectEqual(@as(usize, 0), dirty_count(pager));
    try expect_disk_page(pager, 1, 99);
}

test "PageManager cache returns CacheFull without losing mappings when every frame is pinned" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try fill_clean_cache(pager);
    var entries = pager.page_table.valueIterator();
    while (entries.next()) |info| info.pinned = true;
    const incoming: u32 = PageManager.MAX_CACHE_SIZE;
    try write_disk_page(pager, incoming, 77);
    try std.testing.expectError(error.CacheFull, pager.get_page(Page, incoming));
    try std.testing.expect(!pager.page_table.contains(incoming));
    try expect_cache_consistent(pager);
}

test "PageManager commits allocations without journal originals and rollback invalidates them" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    var metadata = try pager.get_page(Metadata, 0);
    const number = try pager.alloc_free_page(.data_page, &metadata);
    // Brand-new pages have no original bytes to record in the journal.
    try std.testing.expectEqual(@as(usize, 0), pager.journal.entries.len);
    try pager.commit(false);
    try std.testing.expectEqualDeep(Page.init(), try pager.get_page(Page, number));
    try expect_disk_page(pager, number, 0);
    try pager.rollback();
    try std.testing.expect(!pager.page_table.contains(number));
    try expect_eof(pager, number);
    try expect_cache_consistent(pager);

    // Recovery must retain the allocation boundary across a close and reopen too.
    metadata = try pager.get_page(Metadata, 0);
    _ = try pager.alloc_free_page(.data_page, &metadata);
    try pager.commit(false);
    try fixture.reopen();
    try fixture.pager.?.rollback();
    try expect_eof(&fixture.pager.?, number);
    try expect_empty_journal(&fixture.pager.?);
}

test "PageManager updates a caller-owned page after its cached frame was evicted" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try fill_clean_cache(pager);
    var changed = try pager.get_page(Page, 1);
    @memset(&changed.content, 99);
    pager.clock_position = 1;
    pager.clock_reference_bits[1] = 0;
    const incoming: u32 = PageManager.MAX_CACHE_SIZE;
    try write_disk_page(pager, incoming, 77);
    try expect_page(pager, incoming, 77);
    try std.testing.expect(!pager.page_table.contains(1));
    try pager.update_page(1, &changed);
    try expect_page(pager, 1, 99);
    try expect_disk_page(pager, 1, 1);
    try pager.commit(true);
    try expect_disk_page(pager, 1, 99);
    try expect_cache_consistent(pager);
}

test "PageManager allocates consecutive pages and persists them across reopen" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    try std.testing.expectEqual(@as(u32, 1), try allocate(pager, 11));
    try std.testing.expectEqual(@as(u32, 2), try allocate(pager, 22));
    try expect_page(pager, 1, 11);
    try expect_page(pager, 2, 22);
    try expect_eof(pager, 1);
    try pager.commit(true);
    try std.testing.expectEqual(@as(usize, 0), dirty_count(pager));
    try expect_empty_journal(pager);
    try fixture.reopen();
    try expect_page(&fixture.pager.?, 1, 11);
    try expect_page(&fixture.pager.?, 2, 22);
    const metadata = try fixture.pager.?.get_page(Metadata, 0);
    try std.testing.expectEqual(@as(u32, 3), metadata.total_pages);
    try expect_eof(&fixture.pager.?, 3);
}

test "PageManager repeated updates stay cached until commit and journal the original once" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    const number = try allocate(pager, 10);
    try pager.commit(true);
    for ([_]u8{ 20, 30 }) |marker| {
        const page = page_with_marker(marker);
        try pager.update_page(number, &page);
        try expect_page(pager, number, marker);
        try expect_disk_page(pager, number, 10);
    }
    try std.testing.expectEqual(@as(usize, 1), dirty_count(pager));
    const originals = try pager.journal.batch_get_original_pages(0);
    try std.testing.expectEqual(@as(u32, 1), originals.n_read);
    try std.testing.expectEqual(number, originals.pages[0].page_number);
    const original = try page_with_marker(10).to_bytes();
    try std.testing.expectEqualSlices(u8, &original, &originals.pages[0].page);
    try pager.commit(true);
    try expect_disk_page(pager, number, 30);
    try expect_empty_journal(pager);
}

test "PageManager free list survives reopen and reuses pages before growing" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    for (1..4) |i| _ = try allocate(pager, @intCast(i));
    try pager.commit(true);
    var metadata = try pager.get_page(Metadata, 0);
    try pager.free(&metadata, 1);
    try pager.free(&metadata, 3);
    try pager.update_page(0, &metadata);
    try pager.commit(true);
    try fixture.reopen();
    const reopened = &fixture.pager.?;
    metadata = try reopened.get_page(Metadata, 0);
    try std.testing.expectEqual(@as(u32, 2), metadata.free_pages);
    try std.testing.expectEqual(@as(u32, 3), metadata.first_free_page);
    try std.testing.expectEqual(@as(u32, 1), (try reopened.get_page(FreePage, 3)).next);
    try std.testing.expectEqual(@as(u32, 0), (try reopened.get_page(FreePage, 1)).next);
    try std.testing.expectEqual(@as(u32, 3), try allocate(reopened, 33));
    try std.testing.expectEqual(@as(u32, 1), try allocate(reopened, 11));
    metadata = try reopened.get_page(Metadata, 0);
    try std.testing.expectEqual(@as(u32, 4), metadata.total_pages);
    try std.testing.expectEqual(@as(u32, 0), metadata.free_pages);
    try std.testing.expectEqual(@as(u32, 0), metadata.first_free_page);
    try std.testing.expectEqual(@as(u32, 4), try allocate(reopened, 44));
    try reopened.commit(true);
    try expect_page(reopened, 1, 11);
    try expect_page(reopened, 2, 2);
    try expect_page(reopened, 3, 33);
    try expect_page(reopened, 4, 44);
}

test "PageManager rollback restores cached originals metadata and new allocations" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    _ = try allocate(pager, 10);
    _ = try allocate(pager, 20);
    try pager.commit(true);
    const before = try pager.get_page(Metadata, 0);
    var metadata = before;
    try pager.free(&metadata, 1);
    try pager.update_page(0, &metadata);
    try std.testing.expectEqual(@as(u32, 1), try allocate(pager, 99));
    _ = try allocate(pager, 30);
    try pager.rollback();
    try std.testing.expectEqualDeep(before, try pager.get_page(Metadata, 0));
    try expect_page(pager, 1, 10);
    try expect_page(pager, 2, 20);
    try expect_eof(pager, 3);
    try expect_empty_journal(pager);
    try std.testing.expectEqual(@as(usize, 0), dirty_count(pager));
    try pager.rollback();
    try expect_page(pager, 1, 10);
}

test "PageManager rollback after reopen restores original pages across partial commits" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    for (1..6) |i| _ = try allocate(pager, @intCast(i));
    try pager.commit(true);
    const before = try pager.get_page(Metadata, 0);
    const changed = page_with_marker(99);
    // Journal positions differ from database page numbers, including page zero.
    try pager.update_page(5, &changed);
    try pager.update_page(1, &changed);
    try pager.commit(false);
    const changed_again = page_with_marker(88);
    try pager.update_page(5, &changed_again);
    _ = try allocate(pager, 66);
    try pager.commit(false);
    try expect_disk_page(pager, 5, 88);
    try fixture.reopen();
    try fixture.pager.?.rollback();
    try std.testing.expectEqualDeep(before, try fixture.pager.?.get_page(Metadata, 0));
    for (1..6) |i| try expect_page(&fixture.pager.?, @intCast(i), @intCast(i));
    try expect_eof(&fixture.pager.?, 6);
    try expect_empty_journal(&fixture.pager.?);
}

test "PageManager final commit clears recovery journal after a partial commit" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    _ = try allocate(pager, 10);
    try pager.commit(true);
    const changed = page_with_marker(20);
    try pager.update_page(1, &changed);
    try pager.commit(false);
    try std.testing.expectEqual(@as(usize, 0), dirty_count(pager));
    try pager.commit(true);
    try expect_empty_journal(pager);
    try fixture.reopen();
    try fixture.pager.?.rollback();
    try expect_page(&fixture.pager.?, 1, 20);
}

test "PageManager rollback reads persisted and in-memory journal batches" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    for (1..71) |i| _ = try allocate(pager, @intCast(i));
    try pager.commit(true);
    const changed = page_with_marker(99);
    // Exceeds both the journal's 64-entry buffer and 16-page recovery batch.
    for (0..70) |i| try pager.update_page(@intCast(70 - i), &changed);
    try pager.rollback();
    for (1..71) |i| try expect_page(pager, @intCast(i), @intCast(i));
    try expect_empty_journal(pager);
}

test "PageManager automatic partial commits and cache eviction remain recoverable" {
    const fixture = try Fixture.init();
    defer fixture.deinit();
    const pager = &fixture.pager.?;
    const count = PageManager.MAX_CACHE_SIZE + 40;
    for (0..count) |_| _ = try allocate(pager, 42);
    // Early pages have been flushed; the tail still exists only in the cache.
    try expect_disk_page(pager, 1, 42);
    try expect_eof(pager, count);
    try expect_page(pager, 1, 42);
    try expect_page(pager, count, 42);
    try pager.rollback();
    try std.testing.expectEqualDeep(Metadata.init(), try pager.get_page(Metadata, 0));
    try expect_eof(pager, 1);
    try expect_empty_journal(pager);
    try std.testing.expectEqual(@as(u32, 1), try allocate(pager, 7));
}
