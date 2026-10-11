const std = @import("std");
const muscle = @import("../muscle.zig");
const DatabaseFixture = @import("helpers.zig").DatabaseFixture;
const page_types = muscle.page_types;
const serde = muscle.common.serde;
const BoundedArray = muscle.common.BoundedArray;

fn update_fixture() !*DatabaseFixture {
    const fixture = try DatabaseFixture.init();
    errdefer fixture.deinit();
    const created = try fixture.db.execute("create table records (id int primary key, a int, b int, name text(32), active bool)", fixture.allocator());
    try std.testing.expect(created == .data);
    for ([_][]const u8{
        "insert into records (a, b, name, active) values (10, 100, 'first', true)",
        "insert into records (a, b, name, active) values (20, 200, 'second', false)",
        "insert into records (a, b, name, active) values (30, 300, 'third', true)",
    }) |sql| {
        const inserted = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(inserted == .data);
    }
    return fixture;
}

fn expect_update_count(result: muscle.query_result.QueryResultV2, expected: i64) !void {
    if (result == .err) std.debug.print("UPDATE failed: {s}: {s}\n", .{ @errorName(result.err.code), result.err.message });
    try std.testing.expect(result == .data);
    try std.testing.expectEqual(@as(usize, 1), result.columnCount());
    try std.testing.expectEqualStrings("rows_affected", result.data.columns[0].name);
    try std.testing.expectEqualDeep(muscle.DataType.int, result.data.columns[0].data_type);
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(expected, std.mem.readInt(i64, result.data.rows.items[0][0..8], .little));
}

fn expect_sql_rows(fixture: *DatabaseFixture, sql: []const u8, rows: []const []const muscle.Value) !void {
    const result = try fixture.db.execute(sql, fixture.allocator());
    try std.testing.expect(result == .data);
    try std.testing.expectEqual(rows.len, result.rowCount());
    for (rows, result.data.rows.items) |values, row| {
        var expected = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        for (values) |value| try serde.serialize_value(&expected, value);
        try std.testing.expectEqualSlices(u8, expected.const_slice(), row);
    }
}

test "SELECT exact primary key lookups project columns and preserve empty schemas" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    for ([_][]const u8{
        "select name, a from records where id = 2",
        "select name, a from records where 2 = id limit 1",
        "select name, a from records where ((id) = (1 + 1))",
        "select name, a from records where id = 2.0",
    }) |sql| try expect_sql_rows(fixture, sql, &.{&.{ .{ .txt = "second" }, .{ .int = 20 } }});
    for ([_][]const u8{
        "select name, a from records where id = 99",
        "select name, a from records where id = 2.5",
    }) |sql| {
        const result = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(result == .data);
        try std.testing.expectEqual(@as(usize, 0), result.rowCount());
        try std.testing.expectEqual(@as(usize, 2), result.columnCount());
        try std.testing.expectEqualStrings("name", result.data.columns[0].name);
        try std.testing.expectEqualDeep(muscle.DataType{ .txt = 32 }, result.data.columns[0].data_type);
    }
    const incompatible = try fixture.db.execute("select * from records where id = 'bad'", fixture.allocator());
    try std.testing.expect(incompatible == .err);
    try std.testing.expectEqual(error.IncompatibleTypes, incompatible.err.code);
    try expect_sql_rows(fixture, "select id from records where id > 1 limit 1", &.{&.{.{ .int = 2 }}});
    try expect_sql_rows(fixture, "select id from records where id = 2 and a = 10", &.{});
    try expect_sql_rows(fixture, "select id from records where a = 30", &.{&.{.{ .int = 3 }}});
}

test "SELECT exact lookups find text keys across B-tree leaves" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (name text(32) primary key, payload text(900))", fixture.allocator())) == .data);
    const payload = [_]u8{'x'} ** 700;
    for (0..24) |i| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "insert into records (name, payload) values ('key{d:0>2}', '{s}')", .{ i, payload });
        try std.testing.expect((try fixture.db.execute(sql, fixture.allocator())) == .data);
    }
    const table = fixture.db.catalog.find_table("records").?;
    try std.testing.expect(!(try fixture.db.pager.get_page(page_types.Page, table.root)).is_leaf());
    for ([_][]const u8{ "key00", "key12", "key23" }) |key| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "select * from records where name = '{s}'", .{key});
        try expect_sql_rows(fixture, sql, &.{&.{ .{ .txt = key }, .{ .txt = &payload } }});
    }
    try expect_sql_rows(fixture, "select name from records where name = 'missing'", &.{});
    try expect_sql_rows(fixture, "select name from records where name = 'abcdefghijklmnopqrstuvwxyz0123456789'", &.{});
}

fn range_fixture() !*DatabaseFixture {
    const fixture = try DatabaseFixture.init();
    errdefer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, score int, payload text(900))", fixture.allocator())) == .data);
    const payload = [_]u8{'x'} ** 700;
    for (1..25) |i| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "insert into records (id, score, payload) values ({d}, 0, '{s}')", .{ i * 10, payload });
        try std.testing.expect((try fixture.db.execute(sql, fixture.allocator())) == .data);
    }
    const table = fixture.db.catalog.find_table("records").?;
    try std.testing.expect(!(try fixture.db.pager.get_page(page_types.Page, table.root)).is_leaf());
    return fixture;
}

const range_cases = [_]struct { predicate: []const u8, first: usize, end: usize }{
    .{ .predicate = "id >= 60 and id < 190", .first = 6, .end = 19 },
    .{ .predicate = "id > 60 and id <= 190", .first = 7, .end = 20 },
    .{ .predicate = "55 < id", .first = 6, .end = 25 },
    .{ .predicate = "id >= 55", .first = 6, .end = 25 },
    .{ .predicate = "60 >= id", .first = 1, .end = 7 },
    .{ .predicate = "id < 60", .first = 1, .end = 6 },
    .{ .predicate = "id > 10 and id < 20", .first = 1, .end = 1 },
    .{ .predicate = "id >= 60 and id <= 60", .first = 6, .end = 7 },
    .{ .predicate = "id > 60 and id <= 60", .first = 1, .end = 1 },
    .{ .predicate = "id >= 190 and id < 60", .first = 1, .end = 1 },
    .{ .predicate = "id >= 40 and id >= 60 and id < 200 and id <= 190", .first = 6, .end = 20 },
    .{ .predicate = "((id) > (50 + 5)) and (190 > id)", .first = 6, .end = 19 },
    .{ .predicate = "id >= 60.5 and id < 190.5", .first = 7, .end = 20 },
    .{ .predicate = "id <= 60.5", .first = 1, .end = 7 },
    .{ .predicate = "id >= -5", .first = 1, .end = 25 },
    .{ .predicate = "id > 240", .first = 1, .end = 1 },
    .{ .predicate = "id < 10", .first = 1, .end = 1 },
    .{ .predicate = "id < 100000000000000000000.0", .first = 1, .end = 25 },
    .{ .predicate = "id > 100000000000000000000.0", .first = 1, .end = 1 },
    .{ .predicate = "id > -100000000000000000000.0", .first = 1, .end = 25 },
    .{ .predicate = "id < -100000000000000000000.0", .first = 1, .end = 1 },
};

test "SELECT primary key ranges honor boundaries gaps and limits across leaves" {
    const fixture = try range_fixture();
    defer fixture.deinit();
    for (range_cases) |case| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "select id, score from records where {s}", .{case.predicate});
        const result = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(result == .data);
        try std.testing.expectEqual(case.end - case.first, result.rowCount());
        try std.testing.expectEqual(@as(usize, 2), result.columnCount());
        for (result.data.rows.items, case.first..case.end) |row, i| {
            try std.testing.expectEqual(@as(i64, @intCast(i * 10)), std.mem.readInt(i64, row[0..8], .little));
            try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, row[8..16], .little));
        }
    }
    try expect_sql_rows(fixture, "select id from records where id > 55 and id <= 190 limit 3", &.{ &.{.{ .int = 60 }}, &.{.{ .int = 70 }}, &.{.{ .int = 80 }} });
}

test "UPDATE primary key ranges change exactly the matching rows across leaves" {
    const fixture = try range_fixture();
    defer fixture.deinit();
    for (range_cases) |case| {
        try expect_update_count(try fixture.db.execute("update records set score = 0", fixture.allocator()), 24);
        const sql = try std.fmt.allocPrint(fixture.allocator(), "update records set score = id + 1 where {s}", .{case.predicate});
        try expect_update_count(try fixture.db.execute(sql, fixture.allocator()), @intCast(case.end - case.first));
        const result = try fixture.db.execute("select * from records", fixture.allocator());
        try std.testing.expectEqual(@as(usize, 24), result.rowCount());
        for (result.data.rows.items, 1..) |row, i| {
            const key: i64 = @intCast(i * 10);
            try std.testing.expectEqual(key, std.mem.readInt(i64, row[0..8], .little));
            try std.testing.expectEqual(if (i >= case.first and i < case.end) key + 1 else @as(i64, 0), std.mem.readInt(i64, row[8..16], .little));
            try std.testing.expectEqual(@as(u16, 700), std.mem.readInt(u16, row[16..18], .little));
            for (row[18..]) |byte| try std.testing.expectEqual(@as(u8, 'x'), byte);
        }
    }
    const before = try fixture.db.execute("select * from records", fixture.allocator());
    const failed = try fixture.db.execute("update records set score = 5 where id >= 60 and id <= 190; update records set score = 10 / (id - 100) where id >= 90 and id <= 110", fixture.allocator());
    try std.testing.expect(failed == .err);
    try std.testing.expectEqual(error.DivisionByZero, failed.err.code);
    const after = try fixture.db.execute("select * from records", fixture.allocator());
    try std.testing.expectEqualDeep(before.data.rows.items, after.data.rows.items);
}

test "SELECT and UPDATE text primary key ranges span leaves" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (name text(32) primary key, score int, payload text(900))", fixture.allocator())) == .data);
    const payload = [_]u8{'x'} ** 700;
    for (0..16) |i| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "insert into records (name, score, payload) values ('key{d:0>2}', 0, '{s}')", .{ i, payload });
        try std.testing.expect((try fixture.db.execute(sql, fixture.allocator())) == .data);
    }
    const selected = try fixture.db.execute("select name from records where name >= 'key04' and name < 'key12'", fixture.allocator());
    try std.testing.expectEqual(@as(usize, 8), selected.rowCount());
    for (selected.data.rows.items, 4..12) |row, i| {
        const key = try std.fmt.allocPrint(fixture.allocator(), "key{d:0>2}", .{i});
        try std.testing.expectEqualSlices(u8, try serde.serialize(.{ .txt = key }, fixture.allocator()), row);
    }
    try expect_update_count(try fixture.db.execute("update records set score = 7 where 'key04' <= name and 'key12' > name", fixture.allocator()), 8);
    const after = try fixture.db.execute("select score from records", fixture.allocator());
    for (after.data.rows.items, 0..) |row, i| try std.testing.expectEqual(if (i >= 4 and i < 12) @as(i64, 7) else 0, std.mem.readInt(i64, row[0..8], .little));
    try expect_sql_rows(fixture, "select name from records where name > 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'", &.{});
}

test "SELECT and UPDATE real primary key ranges accept numeric bounds" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id real primary key, score int)", fixture.allocator())) == .data);
    for ([_][]const u8{ "1.25", "2.5", "3.75" }) |key| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "insert into records (id, score) values ({s}, 0)", .{key});
        try std.testing.expect((try fixture.db.execute(sql, fixture.allocator())) == .data);
    }
    try expect_sql_rows(fixture, "select id from records where id >= 2 and id < 3.75", &.{&.{.{ .real = 2.5 }}});
    try expect_update_count(try fixture.db.execute("update records set score = 9 where 2 < id and 4 >= id", fixture.allocator()), 2);
    try expect_sql_rows(fixture, "select score from records", &.{ &.{.{ .int = 0 }}, &.{.{ .int = 9 }}, &.{.{ .int = 9 }} });
}

test "SELECT LogicalOrScan merges ordered branches without duplicate rows" {
    const fixture = try range_fixture();
    defer fixture.deinit();
    const cases = [_]struct { predicate: []const u8, first: usize, end: usize }{
        .{ .predicate = "id <= 100 or id >= 60", .first = 1, .end = 25 },
        .{ .predicate = "id >= 60 or id >= 60", .first = 6, .end = 25 },
        .{ .predicate = "id <= 100 or (id > 100 and id <= 190)", .first = 1, .end = 20 },
        .{ .predicate = "id = 60 or id = 60", .first = 6, .end = 7 },
        .{ .predicate = "id = 99 or id = 999", .first = 1, .end = 1 },
        .{ .predicate = "id = 999 or id = 60", .first = 6, .end = 7 },
        .{ .predicate = "id >= 60 or id = 999", .first = 6, .end = 25 },
        .{ .predicate = "(id >= 60 and id < 190) or (id >= 100 and id < 200)", .first = 6, .end = 20 },
        .{ .predicate = "id <= 100 or score = 0", .first = 1, .end = 25 },
        .{ .predicate = "((id = 60 or id > 60) or id = 100) or id >= 190", .first = 6, .end = 25 },
    };
    for (cases) |case| {
        const sql = try std.fmt.allocPrint(fixture.allocator(), "select id from records where {s}", .{case.predicate});
        const result = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(result == .data);
        try std.testing.expectEqual(case.end - case.first, result.rowCount());
        for (result.data.rows.items, case.first..case.end) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i * 10)), std.mem.readInt(i64, row[0..8], .little));
    }
    try expect_sql_rows(fixture, "select id from records where id = 190 or id = 60", &.{ &.{.{ .int = 60 }}, &.{.{ .int = 190 }} });
    const limited = try fixture.db.execute("select id from records where id <= 100 or id >= 60 limit 12", fixture.allocator());
    try std.testing.expectEqual(@as(usize, 12), limited.rowCount());
    for (limited.data.rows.items, 1..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i * 10)), std.mem.readInt(i64, row[0..8], .little));
    const same_projection = try fixture.db.execute("select score from records where id <= 100 or score = 0", fixture.allocator());
    try std.testing.expectEqual(@as(usize, 24), same_projection.rowCount());
}

test "UPDATE LogicalOrScan updates overlapping rows once and preserves batch rollback" {
    const fixture = try range_fixture();
    defer fixture.deinit();
    try expect_update_count(try fixture.db.execute("update records set score = score + 1 where (id >= 60 and id < 190) or (id >= 100 and id <= 190) or id = 60", fixture.allocator()), 14);
    const after = try fixture.db.execute("select id, score from records", fixture.allocator());
    for (after.data.rows.items, 1..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i * 10)), std.mem.readInt(i64, row[0..8], .little));
        try std.testing.expectEqual(if (i >= 6 and i <= 19) @as(i64, 1) else 0, std.mem.readInt(i64, row[8..16], .little));
    }
    try expect_update_count(try fixture.db.execute("update records set score = score + 1 where id = 999 or id = 998", fixture.allocator()), 0);
    try expect_update_count(try fixture.db.execute("update records set score = score + 1 where score = 1 or id = 60", fixture.allocator()), 14);
    const before_failure = try fixture.db.execute("select * from records", fixture.allocator());
    const failed = try fixture.db.execute("update records set score = 9 where id = 60 or id = 70; update records set score = 10 / (id - 100) where id = 90 or id = 100", fixture.allocator());
    try std.testing.expect(failed == .err);
    try std.testing.expectEqual(error.DivisionByZero, failed.err.code);
    const restored = try fixture.db.execute("select * from records", fixture.allocator());
    try std.testing.expectEqualDeep(before_failure.data.rows.items, restored.data.rows.items);
}

test "SQL UPDATE filters rows and preserves unspecified columns" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    const result = try fixture.db.execute("update records set a = a + 5, name = 'changed' where id >= 2 and active = false", fixture.allocator());
    try expect_update_count(result, 1);
    try expect_sql_rows(fixture, "select id, a, b, name, active from records", &.{
        &.{ .{ .int = 1 }, .{ .int = 10 }, .{ .int = 100 }, .{ .txt = "first" }, .{ .bool = true } },
        &.{ .{ .int = 2 }, .{ .int = 25 }, .{ .int = 200 }, .{ .txt = "changed" }, .{ .bool = false } },
        &.{ .{ .int = 3 }, .{ .int = 30 }, .{ .int = 300 }, .{ .txt = "third" }, .{ .bool = true } },
    });
    try expect_update_count(try fixture.db.execute("update records set a = 0 where id = 99", fixture.allocator()), 0);
    try expect_update_count(try fixture.db.execute("update records set active = false", fixture.allocator()), 3);
    try expect_sql_rows(fixture, "select active from records", &.{ &.{.{ .bool = false }}, &.{.{ .bool = false }}, &.{.{ .bool = false }} });
}

test "SQL UPDATE exact primary key lookup changes only the matching row" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    for ([_][]const u8{
        "update records set a = a + 1, b = b + 1 where id = 2",
        "update records set a = a + 1, b = b + 1 where 2 = id",
        "update records set a = a + 1, b = b + 1 where ((id) = (1 + 1))",
        "update records set a = a + 1, b = b + 1 where id = 2.0",
    }) |sql| try expect_update_count(try fixture.db.execute(sql, fixture.allocator()), 1);
    for ([_][]const u8{
        "update records set a = 0 where id = 99",
        "update records set a = 0 where id = 2.5",
    }) |sql| try expect_update_count(try fixture.db.execute(sql, fixture.allocator()), 0);
    try expect_sql_rows(fixture, "select id, a, b from records", &.{
        &.{ .{ .int = 1 }, .{ .int = 10 }, .{ .int = 100 } },
        &.{ .{ .int = 2 }, .{ .int = 24 }, .{ .int = 204 } },
        &.{ .{ .int = 3 }, .{ .int = 30 }, .{ .int = 300 } },
    });
}

test "SQL UPDATE exact lookup supports text and generated primary keys" {
    const cases = [_]struct { create: []const u8, insert: []const u8, update: []const u8 }{
        .{ .create = "create table records (name text(32) primary key, score int)", .insert = "insert into records (name, score) values ('first', 10)", .update = "update records set score = score + 1 where name = 'first'" },
        .{ .create = "create table records (score int)", .insert = "insert into records (score) values (10)", .update = "update records set score = score + 1 where Row_Id = 1" },
    };
    for (cases) |case| {
        const fixture = try DatabaseFixture.init();
        defer fixture.deinit();
        try std.testing.expect((try fixture.db.execute(case.create, fixture.allocator())) == .data);
        try std.testing.expect((try fixture.db.execute(case.insert, fixture.allocator())) == .data);
        try expect_update_count(try fixture.db.execute(case.update, fixture.allocator()), 1);
        try expect_sql_rows(fixture, "select score from records", &.{&.{.{ .int = 11 }}});
    }
}

test "SQL UPDATE evaluates simultaneous assignments and preserves primary keys" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    try expect_update_count(try fixture.db.execute("update records set a = b, b = a", fixture.allocator()), 3);
    try expect_sql_rows(fixture, "select id, a, b from records", &.{
        &.{ .{ .int = 1 }, .{ .int = 100 }, .{ .int = 10 } },
        &.{ .{ .int = 2 }, .{ .int = 200 }, .{ .int = 20 } },
        &.{ .{ .int = 3 }, .{ .int = 300 }, .{ .int = 30 } },
    });
}

test "SQL UPDATE rejects primary key assignments without changing rows or generated keys" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    for ([_][]const u8{
        "update records set id = id + 1",
        "update records set id = id",
        "update records set a = 0, id = 2 where id = 1",
        "update records set id = 100 where id = 99",
    }) |sql| {
        const result = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(result == .err);
        try std.testing.expectEqual(error.PrimaryKeyUpdateNotAllowed, result.err.code);
        const after = try fixture.db.execute("select * from records", fixture.allocator());
        try std.testing.expectEqualDeep(original.data.rows.items, after.data.rows.items);
    }
    const inserted = try fixture.db.execute("insert into records (a, b, name, active) values (40, 400, 'fourth', false)", fixture.allocator());
    try std.testing.expect(inserted == .data);
    try expect_sql_rows(fixture, "select id from records", &.{ &.{.{ .int = 1 }}, &.{.{ .int = 2 }}, &.{.{ .int = 3 }}, &.{.{ .int = 4 }} });
    // Primary keys can still be read in assignment expressions and predicates.
    try expect_update_count(try fixture.db.execute("update records set a = id where id = 4", fixture.allocator()), 1);
}

test "SQL UPDATE rejects generated and text primary key assignments even on empty tables" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    for ([_]struct { create: []const u8, update: []const u8 }{
        .{ .create = "create table generated (score int)", .update = "update generated set Row_Id = 1" },
        .{ .create = "create table named (name text(32) primary key, score int)", .update = "update named set name = 'changed'" },
    }) |case| {
        try std.testing.expect((try fixture.db.execute(case.create, fixture.allocator())) == .data);
        const result = try fixture.db.execute(case.update, fixture.allocator());
        try std.testing.expect(result == .err);
        try std.testing.expectEqual(error.PrimaryKeyUpdateNotAllowed, result.err.code);
    }
}

test "SQL UPDATE rejects unique column assignments and preserves all rows" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, name text(32) unique, code int unique, score int)", fixture.allocator())) == .data);
    try std.testing.expect((try fixture.db.execute("insert into records (name, code, score) values ('first', 10, 1)", fixture.allocator())) == .data);
    try std.testing.expect((try fixture.db.execute("insert into records (name, code, score) values ('second', 20, 2)", fixture.allocator())) == .data);
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    for ([_][]const u8{
        "update records set name = 'changed' where id = 1",
        "update records set code = code + 1",
        "update records set name = name",
        "update records set score = 0, code = 100 where id = 1",
        "update records set code = 100 where id = 99",
        "update records set score = 0; update records set name = 'changed'",
    }) |sql| {
        const result = try fixture.db.execute(sql, fixture.allocator());
        try std.testing.expect(result == .err);
        try std.testing.expectEqual(error.UniqueColumnUpdateNotAllowed, result.err.code);
        const after = try fixture.db.execute("select * from records", fixture.allocator());
        try std.testing.expectEqualDeep(original.data.rows.items, after.data.rows.items);
    }
    // Unique columns may still be read when updating other columns.
    try expect_update_count(try fixture.db.execute("update records set score = code where name = 'first'", fixture.allocator()), 1);
    try expect_sql_rows(fixture, "select id, name, code, score from records", &.{
        &.{ .{ .int = 1 }, .{ .txt = "first" }, .{ .int = 10 }, .{ .int = 10 } },
        &.{ .{ .int = 2 }, .{ .txt = "second" }, .{ .int = 20 }, .{ .int = 2 } },
    });
}

test "SQL UPDATE rejects unique column assignments on empty tables" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (name text(32) unique, score int)", fixture.allocator())) == .data);
    const result = try fixture.db.execute("update records set name = 'changed'", fixture.allocator());
    try std.testing.expect(result == .err);
    try std.testing.expectEqual(error.UniqueColumnUpdateNotAllowed, result.err.code);
}

test "BTree UPDATE moves keys and rejects duplicate destination keys" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    const table = fixture.db.catalog.find_table("records").?;
    var tree = muscle.BTree.init(&fixture.query.context, table.root, .int);
    const key = try serde.serialize(.{ .int = 1 }, fixture.allocator());
    const changed = try fixture.allocator().dupe(u8, original.data.rows.items[0]);
    std.mem.writeInt(i64, changed[0..8], 100, .little);
    try tree.update(key, changed);
    try expect_sql_rows(fixture, "select id, a from records", &.{
        &.{ .{ .int = 2 }, .{ .int = 20 } },
        &.{ .{ .int = 3 }, .{ .int = 30 } },
        &.{ .{ .int = 100 }, .{ .int = 10 } },
    });
    const moved_key = try serde.serialize(.{ .int = 100 }, fixture.allocator());
    try std.testing.expectError(error.KeyNotFound, tree.update(key, changed));
    std.mem.writeInt(i64, changed[0..8], 2, .little);
    try std.testing.expectError(error.DuplicateKey, tree.update(moved_key, changed));
    try expect_sql_rows(fixture, "select id from records", &.{ &.{.{ .int = 2 }}, &.{.{ .int = 3 }}, &.{.{ .int = 100 }} });
    std.mem.writeInt(i64, changed[0..8], 1, .little);
    try tree.update(moved_key, changed);
    const restored = try fixture.db.execute("select * from records", fixture.allocator());
    try std.testing.expectEqualDeep(original.data.rows.items, restored.data.rows.items);
}

test "BTree UPDATE moves keys across leaves and preserves row contents" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, name text(900))", fixture.allocator())) == .data);
    const name = [_]u8{'x'} ** 700;
    const insert = try std.fmt.allocPrint(fixture.allocator(), "insert into records (name) values ('{s}')", .{name});
    for (0..24) |_| try std.testing.expect((try fixture.db.execute(insert, fixture.allocator())) == .data);
    const table = fixture.db.catalog.find_table("records").?;
    const root = try fixture.db.pager.get_page(page_types.Page, table.root);
    try std.testing.expect(!root.is_leaf());
    var tree = muscle.BTree.init(&fixture.query.context, table.root, .int);
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    const changed = try fixture.allocator().dupe(u8, original.data.rows.items[0]);
    var old_key = try serde.serialize(.{ .int = 1 }, fixture.allocator());
    for ([_]i64{ 100, 0 }) |new_key| {
        std.mem.writeInt(i64, changed[0..8], new_key, .little);
        try tree.update(old_key, changed);
        old_key = try serde.serialize(.{ .int = new_key }, fixture.allocator());
        const after = try fixture.db.execute("select * from records", fixture.allocator());
        try std.testing.expectEqual(@as(usize, 24), after.rowCount());
        for (after.data.rows.items, 0..) |row, i| {
            const expected_id: i64 = if (new_key == 100)
                (if (i == 23) 100 else @intCast(i + 2))
            else
                (if (i == 0) 0 else @intCast(i + 1));
            try std.testing.expectEqual(expected_id, std.mem.readInt(i64, row[0..8], .little));
            try std.testing.expectEqualSlices(u8, original.data.rows.items[0][8..], row[8..]);
        }
    }
}

test "SQL UPDATE preserves auto increment behavior on non-primary columns" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, sequence int auto_increment)", fixture.allocator())) == .data);
    try std.testing.expect((try fixture.db.execute("insert into records (id) values (1)", fixture.allocator())) == .data);
    try expect_update_count(try fixture.db.execute("update records set sequence = 100", fixture.allocator()), 1);
    try std.testing.expect((try fixture.db.execute("insert into records (id) values (2)", fixture.allocator())) == .data);
    try expect_sql_rows(fixture, "select id, sequence from records", &.{
        &.{ .{ .int = 1 }, .{ .int = 100 } },
        &.{ .{ .int = 2 }, .{ .int = 101 } },
    });
}

test "SQL UPDATE errors leave all rows unchanged" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    const cases = [_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "update missing set a = 1", .err = error.AnalyzerError },
        .{ .sql = "update records set missing = 1", .err = error.AnalyzerError },
        .{ .sql = "update records set a = missing + 1", .err = error.AnalyzerError },
        .{ .sql = "update records set a = 1 where missing = 1", .err = error.AnalyzerError },
        .{ .sql = "update records set a = 1 where id", .err = error.TypeMismatch },
        .{ .sql = "update records set a = 'bad'", .err = error.TypeMismatch },
        .{ .sql = "update records set a = a / (3 - id)", .err = error.DivisionByZero },
        .{ .sql = "update records set a = 1.5", .err = error.TypeMismatch },
        .{ .sql = "update records set id = null", .err = error.ParserError },
        .{ .sql = "update records set name = 'abcdefghijklmnopqrstuvwxyz0123456789'", .err = error.TextTooLong },
    };
    for (cases) |case| {
        const result = try fixture.db.execute(case.sql, fixture.allocator());
        try std.testing.expect(result == .err);
        try std.testing.expectEqual(case.err, result.err.code);
        const after = try fixture.db.execute("select * from records", fixture.allocator());
        try std.testing.expectEqualDeep(original.data.rows.items, after.data.rows.items);
    }
}

test "SQL UPDATE batch rollback restores rows and preserves generated keys" {
    const fixture = try update_fixture();
    defer fixture.deinit();
    const original = try fixture.db.execute("select * from records", fixture.allocator());
    const result = try fixture.db.execute("update records set a = a + 10; update records set a = a / (3 - id)", fixture.allocator());
    try std.testing.expect(result == .err);
    try std.testing.expectEqual(error.DivisionByZero, result.err.code);
    const after = try fixture.db.execute("select * from records", fixture.allocator());
    try std.testing.expectEqualDeep(original.data.rows.items, after.data.rows.items);
    const inserted = try fixture.db.execute("insert into records (a, b, name, active) values (40, 400, 'fourth', false)", fixture.allocator());
    try std.testing.expect(inserted == .data);
    try expect_sql_rows(fixture, "select id from records", &.{ &.{.{ .int = 1 }}, &.{.{ .int = 2 }}, &.{.{ .int = 3 }}, &.{.{ .int = 4 }} });
}

test "SQL UPDATE scans safely when growing rows split pages" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    _ = try fixture.db.execute("create table records (id int primary key, name text(900))", fixture.allocator());
    for (0..32) |_| _ = try fixture.db.execute("insert into records (name) values ('old')", fixture.allocator());
    const long_name = [_]u8{'x'} ** 700;
    const sql = try std.fmt.allocPrint(fixture.allocator(), "update records set name = '{s}' where id >= 16", .{long_name});
    try expect_update_count(try fixture.db.execute(sql, fixture.allocator()), 17);
    const selected = try fixture.db.execute("select id, name from records", fixture.allocator());
    try std.testing.expectEqual(@as(usize, 32), selected.rowCount());
    for (selected.data.rows.items, 0..) |row, i| {
        var expected = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        try serde.serialize_value(&expected, .{ .int = @intCast(i + 1) });
        try serde.serialize_value(&expected, .{ .txt = if (i + 1 >= 16) &long_name else "old" });
        try std.testing.expectEqualSlices(u8, expected.const_slice(), row);
    }
    try expect_update_count(try fixture.db.execute("update records set name = 'tiny'", fixture.allocator()), 32);
    const shrunk = try fixture.db.execute("select name from records", fixture.allocator());
    try std.testing.expectEqual(@as(usize, 32), shrunk.rowCount());
    for (shrunk.data.rows.items) |row| try std.testing.expectEqualSlices(u8, &.{ 4, 0, 't', 'i', 'n', 'y' }, row);
}

test "create_table keeps explicit and generated primary keys first" {
    const cases = [_]struct { sql: []const u8, key: []const u8 }{
        .{ .sql = "create table records (id int primary key, name text(32), rank int)", .key = "id" },
        .{ .sql = "create table records (name text(32), id int primary key, rank int)", .key = "id" },
        .{ .sql = "create table records (name text(32), rank int, id int primary key)", .key = "id" },
        .{ .sql = "create table records (name text(32), rank int)", .key = "Row_Id" },
        .{ .sql = "create table records (name text(32), Row_Id int, rank int)", .key = "Row_Id" },
    };

    for (cases) |case| {
        const fixture = try DatabaseFixture.init();
        defer fixture.deinit();
        const db = &fixture.db;
        const allocator = fixture.allocator();
        const created = try db.execute(case.sql, allocator);
        try std.testing.expect(created == .data);

        const table = db.catalog.find_table("records").?;
        try std.testing.expectEqual(@as(usize, 3), table.columns.count());
        try std.testing.expectEqualStrings(case.key, table.columns.keys()[0]);
        try std.testing.expectEqualStrings("name", table.columns.keys()[1]);
        try std.testing.expectEqualStrings("rank", table.columns.keys()[2]);
        try std.testing.expect(table.columns.values()[0].unique);
        try std.testing.expect(table.columns.values()[0].not_null);

        // Two inserts exercise key comparison, which reads the row's first bytes.
        for (0..2) |_| {
            _ = try db.execute_query(.{ .insert = .{ .table_name = "records", .values = &.{
                .{ .column_name = "name", .value = .{ .txt = "before" } },
                .{ .column_name = "rank", .value = .{ .int = 10 } },
            } } }, allocator);
        }
        try db.pager.commit(true);
        const reloaded_catalog = try muscle.Catalog_Manager.init(allocator, &db.pager);
        db.catalog.deinit();
        db.catalog = reloaded_catalog;

        // Catalog persistence must preserve the row layout too.
        try std.testing.expectEqualStrings(case.key, db.catalog.find_table("records").?.columns.keys()[0]);
        _ = try db.execute_query(.{ .update = .{ .table_name = "records", .values = &.{
            .{ .column_name = case.key, .value = .{ .int = 1 } },
            .{ .column_name = "name", .value = .{ .txt = "after" } },
            .{ .column_name = "rank", .value = .{ .int = 20 } },
        } } }, allocator);

        var expected = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        try serde.serialize_value(&expected, .{ .int = 1 });
        try serde.serialize_value(&expected, .{ .txt = "after" });
        try serde.serialize_value(&expected, .{ .int = 20 });
        const root = db.catalog.find_table("records").?.root;
        var page = try db.pager.get_page(page_types.Page, root);
        try std.testing.expectEqual(@as(u16, 2), page.num_slots);
        try std.testing.expectEqualSlices(u8, expected.const_slice(), page.cell_at_slot(0).content);

        _ = try db.execute_query(.{ .delete = .{ .table_name = "records", .key = .{ .int = 1 } } }, allocator);
        page = try db.pager.get_page(page_types.Page, root);
        try std.testing.expectEqual(@as(u16, 1), page.num_slots);
        try std.testing.expectEqual(@as(i64, 2), std.mem.readInt(i64, page.cell_at_slot(0).content[0..8], .little));
    }
}

test "QueryResultV2 returns projected schema and encoded rows" {
    const fixture = try DatabaseFixture.init();
    defer fixture.deinit();
    const db = &fixture.db;
    const allocator = fixture.allocator();

    const created = try db.execute("create table records (id int primary key, name text(32), score real, active bool)", allocator);
    try std.testing.expect(created == .data);
    try std.testing.expectEqual(@as(usize, 0), created.data.columns.len);
    try std.testing.expectEqual(@as(usize, 0), created.data.rows.items.len);

    // Schema comes from the projection, even before there is any data.
    const sql = "select name, id, score + 1, active, score > 2 from records";
    const empty = try db.execute(sql, allocator);
    try std.testing.expect(empty == .data);
    try std.testing.expectEqual(@as(usize, 0), empty.data.rows.items.len);
    try std.testing.expectEqual(@as(usize, 5), empty.data.columns.len);
    try std.testing.expectEqualStrings("name", empty.data.columns[0].name);
    try std.testing.expectEqualDeep(muscle.DataType{ .txt = 32 }, empty.data.columns[0].data_type);
    try std.testing.expectEqualStrings("id", empty.data.columns[1].name);
    try std.testing.expectEqualDeep(muscle.DataType.int, empty.data.columns[1].data_type);
    try std.testing.expectEqualStrings("column_3", empty.data.columns[2].name);
    try std.testing.expectEqualDeep(muscle.DataType.real, empty.data.columns[2].data_type);
    try std.testing.expectEqualDeep(muscle.DataType.bool, empty.data.columns[3].data_type);
    try std.testing.expectEqualDeep(muscle.DataType.bool, empty.data.columns[4].data_type);

    // SQL INSERT also describes its returned count.
    const inserted = try db.execute("insert into records (name, score, active) values ('first', 2.5, true)", allocator);
    try std.testing.expect(inserted == .data);
    try std.testing.expectEqual(@as(usize, 1), inserted.data.columns.len);
    try std.testing.expectEqualStrings("rows_created", inserted.data.columns[0].name);
    try std.testing.expectEqualDeep(muscle.DataType.int, inserted.data.columns[0].data_type);
    try std.testing.expectEqual(@as(usize, 1), inserted.data.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, inserted.data.rows.items[0][0..8], .little));
    _ = try db.execute("insert into records (name, score, active) values ('second', 2.5, true)", allocator);

    const selected = try db.execute(sql, allocator);
    try std.testing.expectEqualDeep(empty.data.columns, selected.data.columns);
    try std.testing.expectEqual(@as(usize, 2), selected.data.rows.items.len);
    for (selected.data.rows.items, 0..) |row, i| {
        var expected = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        try serde.serialize_value(&expected, .{ .txt = if (i == 0) "first" else "second" });
        try serde.serialize_value(&expected, .{ .int = @intCast(i + 1) });
        try serde.serialize_value(&expected, .{ .real = 3.5 });
        try serde.serialize_value(&expected, .{ .bool = true });
        try serde.serialize_value(&expected, .{ .bool = true });
        try std.testing.expectEqualSlices(u8, expected.const_slice(), row);
    }

    const all = try db.execute("select * from records", allocator);
    try std.testing.expectEqual(@as(usize, 4), all.data.columns.len);
    for (db.catalog.find_table("records").?.columns.values(), all.data.columns) |column, result_column| {
        try std.testing.expectEqualStrings(column.name, result_column.name);
        try std.testing.expectEqualDeep(column.data_type, result_column.data_type);
    }
    try std.testing.expectEqual(@as(usize, 2), all.data.rows.items.len);
    var expected_all = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
    try serde.serialize_value(&expected_all, .{ .int = 1 });
    try serde.serialize_value(&expected_all, .{ .txt = "first" });
    try serde.serialize_value(&expected_all, .{ .real = 2.5 });
    try serde.serialize_value(&expected_all, .{ .bool = true });
    try std.testing.expectEqualSlices(u8, expected_all.const_slice(), all.data.rows.items[0]);

    const invalid = try db.execute("select missing from records", allocator);
    try std.testing.expect(invalid == .err);
    try std.testing.expectEqual(error.ColumnDoesNotExist, invalid.err.code);
}
