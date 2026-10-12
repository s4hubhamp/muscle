const std = @import("std");
const muscle = @import("../muscle.zig");
const IO = @import("../storage/io.zig").IO;
const PAGE_SIZE = muscle.PAGE_SIZE;

fn absolutePath(tmp: *std.testing.TmpDir, name: []const u8) ![]u8 {
    const dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(dir);
    return std.fs.path.join(std.testing.allocator, &.{ dir, name });
}

test "IO creates files and preserves positional pages across truncate and reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try absolutePath(&tmp, "pages.db");
    defer std.testing.allocator.free(path);
    const first = [_]u8{0x17} ** PAGE_SIZE;
    const third = [_]u8{0x39} ** PAGE_SIZE;
    var buffer: [PAGE_SIZE]u8 = undefined;
    {
        var io = try IO.init(std.testing.allocator, path);
        defer io.deinit();
        try std.testing.expectEqual(@as(usize, 0), try io.read(0, &buffer));
        // Out-of-order writes must use page offsets, not a shared file cursor.
        try std.testing.expectEqual(PAGE_SIZE, try io.write(2, &third));
        try std.testing.expectEqual(PAGE_SIZE, try io.write(0, &first));
        try std.testing.expectEqual(PAGE_SIZE, try io.read(2, &buffer));
        try std.testing.expectEqualSlices(u8, &third, &buffer);
        try std.testing.expectEqual(PAGE_SIZE, try io.read(1, &buffer));
        const zeros = [_]u8{0} ** PAGE_SIZE;
        try std.testing.expectEqualSlices(u8, &zeros, &buffer);
        try io.truncate(2);
        try std.testing.expectEqual(@as(usize, 0), try io.read(2, &buffer));
    }
    var reopened = try IO.init(std.testing.allocator, path);
    defer reopened.deinit();
    try std.testing.expectEqual(PAGE_SIZE, try reopened.read(0, &buffer));
    try std.testing.expectEqualSlices(u8, &first, &buffer);
    try std.testing.expectEqual(@as(usize, 0), try reopened.read(2, &buffer));
    try reopened.truncate(0);
    try std.testing.expectEqual(@as(usize, 0), try reopened.read(0, &buffer));
}

test "IO reports partial EOF and handles empty buffers" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try absolutePath(&tmp, "partial.db");
    defer std.testing.allocator.free(path);
    var io = try IO.init(std.testing.allocator, path);
    defer io.deinit();
    const bytes = "partial page";
    try std.testing.expectEqual(bytes.len, try io.write(0, bytes));
    var buffer = [_]u8{0xff} ** PAGE_SIZE;
    try std.testing.expectEqual(bytes.len, try io.read(0, &buffer));
    try std.testing.expectEqualSlices(u8, bytes, buffer[0..bytes.len]);
    try std.testing.expectEqual(@as(u8, 0xff), buffer[bytes.len]);
    try std.testing.expectEqual(@as(usize, 0), try io.read(0, buffer[0..0]));
    try std.testing.expectEqual(@as(usize, 0), try io.write(0, ""));
}

test "IO exclusive lock rejects another opener and is released on close" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try absolutePath(&tmp, "locked.db");
    defer std.testing.allocator.free(path);
    {
        var owner = try IO.init(std.testing.allocator, path);
        defer owner.deinit();
        try std.testing.expectError(error.WouldBlock, IO.init(std.testing.allocator, path));
    }
    var next_owner = try IO.init(std.testing.allocator, path);
    defer next_owner.deinit();
    try std.testing.expectEqual(@as(usize, 1), try next_owner.write(0, "x"));
}

test "IO page offsets use 64 bits" {
    try std.testing.expectEqual(@as(u64, 4_294_967_296), IO.page_offset(1_048_576));
    try std.testing.expectEqual(@as(u64, 17_592_186_040_320), IO.page_offset(std.math.maxInt(u32)));
}

test "database and journal reopen with Unicode paths longer than 128 bytes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const nested = "directory-with-a-long-name-for-database-io-tests/another-long-directory-for-journal-path-tests/a-third-directory-for-portable-file-paths";
    try tmp.dir.createDirPath(std.testing.io, nested);
    const relative = try std.fs.path.join(std.testing.allocator, &.{ nested, "数据-é.db" });
    defer std.testing.allocator.free(relative);
    const path = try absolutePath(&tmp, relative);
    defer std.testing.allocator.free(path);
    try std.testing.expect(path.len > 128);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    {
        var db = try muscle.database.Muscle.init(std.testing.allocator, path);
        defer db.deinit();
        const create = try db.execute("create table records (id int primary key, score int)", arena.allocator());
        try std.testing.expect(create == .data);
        const insert = try db.execute("insert into records (id, score) values (7, 17)", arena.allocator());
        try std.testing.expect(insert == .data);
    }
    var db = try muscle.database.Muscle.init(std.testing.allocator, path);
    defer db.deinit();
    const result = try db.execute("select score from records where id = 7", arena.allocator());
    try std.testing.expect(result == .data);
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(@as(i64, 17), std.mem.readInt(i64, result.data.rows.items[0][1..9], .little));
}
