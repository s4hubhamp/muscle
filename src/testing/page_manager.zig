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
    const number = try pager.alloc_free_page(&metadata);
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
    try std.testing.expectEqual(@as(usize, 0), pager.dirty_pages.len);
    try expect_eof(pager, 1);
    try pager.commit(true);
    try fixture.reopen();
    try std.testing.expectEqualDeep(Metadata.init(), try fixture.pager.?.get_page(Metadata, 0));
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
    try std.testing.expectEqual(@as(usize, 0), pager.dirty_pages.len);
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
    try std.testing.expectEqual(@as(usize, 1), pager.dirty_pages.len);
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
    try std.testing.expectEqual(@as(usize, 0), pager.dirty_pages.len);
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
    try std.testing.expectEqual(@as(usize, 0), pager.dirty_pages.len);
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
    const count = PageManager.MAX_DIRTY_COUNT_BEFORE_COMMIT + 40;
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
