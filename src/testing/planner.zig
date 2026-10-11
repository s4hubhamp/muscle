const std = @import("std");
const muscle = @import("../muscle.zig");

const Parser = muscle.Parser;
const expression = muscle.expression;
const Expression = muscle.expression.Expression;
const BinaryOperator = expression.BinaryOperator;
const QueryFixture = @import("helpers.zig").QueryFixture;
const resolve_expression = muscle.planner.resolve_expression;

test "UPDATE plan retains assignments and predicate and completes once" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    const created = try fixture.db.execute("create table records (id int primary key, score int)", fixture.allocator());
    try std.testing.expect(created == .data);
    const context = &fixture.query.context;
    context.input = "update records set score = score + 1 where id > 3";
    var parser = Parser.init(context);
    const statements = try parser.parse();
    try muscle.analyzer.analyze(context, statements);
    const plans = try muscle.planner.make_plans(context, statements);
    try std.testing.expectEqual(@as(usize, 1), plans.len);
    try std.testing.expect(plans[0] == .update);
    const update = &plans[0].update;
    try std.testing.expectEqualStrings("records", update.table_name);
    try std.testing.expectEqual(@as(usize, 1), update.assignments.len);
    try std.testing.expectEqualStrings("score", update.assignments[0].column);
    try std.testing.expect(update.assignments[0].value == .binary_operation);
    try std.testing.expect(update.where.? == .binary_operation);
    const count = (try update.next()).?;
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, count[0..8], .little));
    try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try update.next());
}

test "UPDATE chooses ExactMatchPlan for primary key equality and filters other predicates" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, score int)", fixture.allocator())) == .data);
    const cases = [_]struct { sql: []const u8, exact: bool, filtered: bool = false, range: bool = false, logical_or: bool = false }{
        .{ .sql = "update records set score = 10 where id = 2", .exact = true },
        .{ .sql = "update records set score = 10 where 2 = id", .exact = true },
        .{ .sql = "update records set score = 10 where ((id) = (1 + 1))", .exact = true },
        .{ .sql = "update records set score = 10 where score = 2", .exact = false, .filtered = true },
        .{ .sql = "update records set score = 10 where id > 2", .exact = false, .range = true },
        .{ .sql = "update records set score = 10 where id = score", .exact = false, .filtered = true },
        .{ .sql = "update records set score = 10 where id = 2 and score = 1", .exact = false, .filtered = true },
        .{ .sql = "update records set score = 10 where id = 2 or id = 3", .exact = false, .logical_or = true },
        .{ .sql = "update records set score = 10", .exact = false },
    };
    for (cases) |case| {
        const context = &fixture.query.context;
        context.input = case.sql;
        var parser = Parser.init(context);
        const statements = try parser.parse();
        try muscle.analyzer.analyze(context, statements);
        const plans = try muscle.planner.make_plans(context, statements);
        const source = plans[0].update.source;
        try std.testing.expectEqual(case.exact, source.* == .exact_match);
        try std.testing.expectEqual(case.filtered, source.* == .filter);
        try std.testing.expectEqual(case.range, source.* == .range_scan);
        try std.testing.expectEqual(case.logical_or, source.* == .logical_or_scan);
        if (!case.exact and !case.filtered and !case.range and !case.logical_or) try std.testing.expect(source.* == .seq_scan);
    }
}

test "UPDATE planning rejects primary key assignments before scanning" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, score int)", fixture.allocator())) == .data);
    const context = &fixture.query.context;
    context.input = "update records set id = id where id > 3";
    var parser = Parser.init(context);
    const statements = try parser.parse();
    // Planning must enforce immutability even when called without the analyzer.
    try std.testing.expectError(error.PrimaryKeyUpdateNotAllowed, muscle.planner.make_plans(context, statements));
}

test "UPDATE planning rejects unique column assignments before scanning" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, code int unique, score int)", fixture.allocator())) == .data);
    const context = &fixture.query.context;
    context.input = "update records set code = code where id > 3";
    var parser = Parser.init(context);
    const statements = try parser.parse();
    try std.testing.expectError(error.UniqueColumnUpdateNotAllowed, muscle.planner.make_plans(context, statements));
}

test "Project chooses ExactMatchPlan only for primary key equality with a constant" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, code int unique, score int)", fixture.allocator())) == .data);
    const cases = [_]struct { sql: []const u8, exact: bool, range: bool = false, logical_or: bool = false }{
        .{ .sql = "select score from records where id = 2", .exact = true },
        .{ .sql = "select score from records where 2 = id", .exact = true },
        .{ .sql = "select score from records where ((id) = (1 + 1))", .exact = true },
        .{ .sql = "select score from records where code = 2", .exact = false },
        .{ .sql = "select score from records where id > 2", .exact = false, .range = true },
        .{ .sql = "select score from records where id = score", .exact = false },
        .{ .sql = "select score from records where id + 1 = 2", .exact = false },
        .{ .sql = "select score from records where id = 2 and score = 10", .exact = false },
        .{ .sql = "select score from records where id = 2 or id = 3", .exact = false, .logical_or = true },
    };
    for (cases) |case| {
        const context = &fixture.query.context;
        context.input = case.sql;
        var parser = Parser.init(context);
        const statements = try parser.parse();
        try muscle.analyzer.analyze(context, statements);
        const plans = try muscle.planner.make_plans(context, statements);
        try std.testing.expect(plans[0] == .project);
        try std.testing.expectEqual(case.exact, plans[0].project.source.* == .exact_match);
        if (case.logical_or) try std.testing.expect(plans[0].project.source.* == .logical_or_scan) else if (case.range) try std.testing.expect(plans[0].project.source.* == .range_scan) else if (!case.exact) try std.testing.expect(plans[0].project.source.* == .filter);
        if (case.exact) {
            try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try plans[0].project.source.exact_match.next());
            try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try plans[0].project.source.exact_match.next());
        }
    }
}

test "ExactMatchPlan yields a matching row once" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (score int)", fixture.allocator())) == .data);
    try std.testing.expect((try fixture.db.execute("insert into records (score) values (42)", fixture.allocator())) == .data);
    const context = &fixture.query.context;
    context.input = "select score from records where Row_Id = 1";
    var parser = Parser.init(context);
    const statements = try parser.parse();
    const plans = try muscle.planner.make_plans(context, statements);
    const exact = &plans[0].project.source.exact_match;
    const row = (try exact.next()).?;
    try std.testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, row[0..8], .little));
    try std.testing.expectEqual(@as(i64, 42), std.mem.readInt(i64, row[8..16], .little));
    try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try exact.next());
}

test "SELECT and UPDATE choose RangeScanPlan for primary key bounds" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, score int)", fixture.allocator())) == .data);
    for ([_][]const u8{ "select score from records", "update records set score = 1" }) |statement| {
        for ([_][]const u8{
            "id >= 2 and id < 5",
            "2 <= id and 5 > id",
            "id >= 1 and id > 2 and id <= 5",
            "id = 3 and id < 5",
        }) |predicate| {
            const context = &fixture.query.context;
            context.input = try std.fmt.allocPrint(fixture.allocator(), "{s} where {s}", .{ statement, predicate });
            var parser = Parser.init(context);
            const statements = try parser.parse();
            const plans = try muscle.planner.make_plans(context, statements);
            const source = if (plans[0] == .project) plans[0].project.source else plans[0].update.source;
            try std.testing.expect(source.* == .range_scan);
            try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try source.range_scan.next());
            try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try source.range_scan.next());
        }
    }
}

test "LogicalOrScan plans optimized nested branches for SELECT and UPDATE" {
    const fixture = try @import("helpers.zig").DatabaseFixture.init();
    defer fixture.deinit();
    try std.testing.expect((try fixture.db.execute("create table records (id int primary key, score int)", fixture.allocator())) == .data);
    for ([_][]const u8{ "select score from records", "update records set score = 1" }) |statement| {
        const context = &fixture.query.context;
        context.input = try std.fmt.allocPrint(fixture.allocator(), "{s} where (id = 2 or id >= 4) or score = 3", .{statement});
        var parser = Parser.init(context);
        const statements = try parser.parse();
        const plans = try muscle.planner.make_plans(context, statements);
        const source = if (plans[0] == .project) plans[0].project.source else plans[0].update.source;
        try std.testing.expect(source.* == .logical_or_scan);
        const scan = &source.logical_or_scan;
        try std.testing.expect(scan.left.* == .logical_or_scan);
        try std.testing.expect(scan.left.logical_or_scan.left.* == .exact_match);
        try std.testing.expect(scan.left.logical_or_scan.right.* == .range_scan);
        try std.testing.expect(scan.right.* == .filter);
        try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try scan.next());
        try std.testing.expectEqual(@as(?muscle.page_types.EncodedCellContent, null), try scan.next());
    }
}

test "resolve binary expressions" {
    const fixture = try QueryFixture.init();
    defer fixture.deinit();
    const context = &fixture.context;
    const table: *const muscle.Table = undefined;

    const Case = struct {
        operator: BinaryOperator,
        left: muscle.Value,
        right: muscle.Value,
        expected: muscle.Value,
    };
    const cases = [_]Case{
        .{ .operator = .eq, .left = .{ .int = 2 }, .right = .{ .int = 2 }, .expected = .{ .bool = true } },
        .{ .operator = .neq, .left = .{ .txt = "a" }, .right = .{ .txt = "b" }, .expected = .{ .bool = true } },
        .{ .operator = .lt, .left = .{ .real = 1.0 }, .right = .{ .real = 2.0 }, .expected = .{ .bool = true } },
        .{ .operator = .lte, .left = .{ .bin = "a" }, .right = .{ .bin = "a" }, .expected = .{ .bool = true } },
        .{ .operator = .gt, .left = .{ .int = 3 }, .right = .{ .int = 2 }, .expected = .{ .bool = true } },
        .{ .operator = .gt, .left = .{ .int = std.math.maxInt(i64) }, .right = .{ .int = std.math.maxInt(i64) - 1 }, .expected = .{ .bool = true } },
        .{ .operator = .gte, .left = .{ .bool = true }, .right = .{ .bool = false }, .expected = .{ .bool = true } },
        .{ .operator = .plus, .left = .{ .int = 2 }, .right = .{ .int = 3 }, .expected = .{ .real = 5 } },
        .{ .operator = .minus, .left = .{ .real = 5.5 }, .right = .{ .real = 2.0 }, .expected = .{ .real = 3.5 } },
        .{ .operator = .mul, .left = .{ .int = 3 }, .right = .{ .int = 4 }, .expected = .{ .real = 12 } },
        .{ .operator = .div, .left = .{ .int = 3 }, .right = .{ .int = 2 }, .expected = .{ .real = 1.5 } },
        .{ .operator = .logical_and, .left = .{ .bool = true }, .right = .{ .bool = false }, .expected = .{ .bool = false } },
        .{ .operator = .logical_or, .left = .{ .bool = true }, .right = .{ .bool = false }, .expected = .{ .bool = true } },
    };

    for (cases) |case| {
        var left = Expression{ .value = case.left };
        var right = Expression{ .value = case.right };
        const expr = Expression{ .binary_operation = .{
            .operator = case.operator,
            .left = &left,
            .right = &right,
        } };

        const actual = try resolve_expression(context, table, "", expr);
        try std.testing.expectEqualDeep(case.expected, actual);
    }

    var numerator = Expression{ .value = .{ .int = 1 } };
    var zero = Expression{ .value = .{ .int = 0 } };
    const divide_by_zero = Expression{ .binary_operation = .{
        .operator = .div,
        .left = &numerator,
        .right = &zero,
    } };
    try std.testing.expectError(
        error.DivisionByZero,
        resolve_expression(context, table, "", divide_by_zero),
    );
}

test "resolve value expressions" {
    const fixture = try QueryFixture.init();
    defer fixture.deinit();
    const context = &fixture.context;
    const table: *const muscle.Table = undefined;
    const values = [_]muscle.Value{
        .{ .int = 42 },
        .{ .real = 1.5 },
        .{ .bool = true },
        .{ .txt = "muscle" },
        .{ .bin = "\x01\x02" },
        .{ .null = {} },
    };

    for (values) |value| {
        const exp = Expression{ .value = value };
        try std.testing.expectEqualDeep(
            value,
            try resolve_expression(context, table, "", exp),
        );
    }
}

test "resolve identifier expressions" {
    const fixture = try QueryFixture.init();
    defer fixture.deinit();
    const context = &fixture.context;
    const allocator = fixture.allocator();
    var table = muscle.Table{
        .root = 0,
        .name = "records",
        .columns = .empty,
        .primary_key = "id",
        .indexes = &.{},
    };
    defer table.columns.deinit(allocator);

    try table.columns.put(allocator, "id", .{ .name = "id", .data_type = .int });
    try table.columns.put(allocator, "ratio", .{ .name = "ratio", .data_type = .real });
    try table.columns.put(allocator, "active", .{ .name = "active", .data_type = .bool });
    try table.columns.put(allocator, "name", .{ .name = "name", .data_type = .{ .txt = 20 } });
    try table.columns.put(allocator, "payload", .{ .name = "payload", .data_type = .{ .bin = 20 } });

    const values = [_]muscle.Value{
        .{ .int = 42 },
        .{ .real = 1.5 },
        .{ .bool = true },
        .{ .txt = "muscle" },
        .{ .bin = "\x01\x02" },
    };
    const identifiers = [_][]const u8{ "id", "ratio", "active", "name", "payload" };
    var row = muscle.common.BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE){};
    for (values) |value| try muscle.common.serde.serialize_value(&row, value);

    for (identifiers, values) |identifier, expected| {
        const exp = Expression{ .identifier = identifier };
        try std.testing.expectEqualDeep(
            expected,
            try resolve_expression(context, &table, row.const_slice(), exp),
        );
    }

    try std.testing.expectError(
        error.ColumnDoesNotExist,
        resolve_expression(
            context,
            &table,
            row.const_slice(),
            .{ .identifier = "missing" },
        ),
    );
}

test "resolve unary expressions" {
    const fixture = try QueryFixture.init();
    defer fixture.deinit();
    const context = &fixture.context;
    const table: *const muscle.Table = undefined;

    var four = Expression{ .value = .{ .int = 4 } };
    const unary_plus = Expression{ .unary_operation = .{
        .operator = .plus,
        .operand = &four,
    } };
    try std.testing.expectEqualDeep(
        muscle.Value{ .real = 4 },
        try resolve_expression(context, table, "", unary_plus),
    );

    var decimal = Expression{ .value = .{ .real = 2.5 } };
    const unary_minus = Expression{ .unary_operation = .{
        .operator = .minus,
        .operand = &decimal,
    } };
    try std.testing.expectEqualDeep(
        muscle.Value{ .real = -2.5 },
        try resolve_expression(context, table, "", unary_minus),
    );

    var boolean = Expression{ .value = .{ .bool = true } };
    const logical_not = Expression{ .unary_operation = .{
        .operator = .not,
        .operand = &boolean,
    } };
    try std.testing.expectEqualDeep(
        muscle.Value{ .bool = false },
        try resolve_expression(context, table, "", logical_not),
    );

    var invalid_operand = Expression{ .value = .{ .txt = "four" } };
    const invalid_unary_minus = Expression{ .unary_operation = .{
        .operator = .minus,
        .operand = &invalid_operand,
    } };
    try std.testing.expectError(
        error.IncompatibleTypes,
        resolve_expression(context, table, "", invalid_unary_minus),
    );
}

test "resolve nested expressions" {
    const fixture = try QueryFixture.init();
    defer fixture.deinit();
    const context = &fixture.context;
    const table: *const muscle.Table = undefined;

    var value = Expression{ .value = .{ .int = 4 } };
    const nested = Expression{ .nested = &value };
    try std.testing.expectEqualDeep(
        muscle.Value{ .int = 4 },
        try resolve_expression(context, table, "", nested),
    );

    const empty_nested = Expression{ .nested = null };
    try std.testing.expectError(
        error.InvalidExpression,
        resolve_expression(context, table, "", empty_nested),
    );
}
