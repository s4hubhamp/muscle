const std = @import("std");
const muscle = @import("./muscle.zig");
const Muscle = muscle.database.Muscle;

const helpers = muscle.common.helpers;

// Validate the statements
pub fn analyze(context: *muscle.QueryContext, statements: []muscle.Parser.Statement) !void {
    for (statements) |statement| {
        switch (statement) {
            .select => |select| {
                // if the table exist
                // if columns exist and columns are not duplicate
                // limit is already validated in parser
                // where clause expressions. all referenced identifiers are valid
                // all order by expressions are identifiers and referenced identifiers are valid

                _ = try validate_table(context, select.table);
            },
            .insert => |insert| {
                // if table exists
                // all columns are already unique and identifiers checked by parser. We need to check if they exist
                // columns and values length should be equal, checked by parser
                // check values expressions list

                const table = try validate_table(context, insert.into);
                for (insert.columns) |col| try validate_column(context, table, col);
                try validate_expressions_list(context, table, insert.values);
            },
            .create_table => {},
            else => {
                unreachable;
            },
        }
    }
}

fn validate_table(context: *muscle.QueryContext, name: []const u8) !*const muscle.Table {
    return context.catalog.find_table(name) orelse {
        try context.set_err(error.AnalyzerError, "Table \"{s}\" does not exist", .{name});
        return error.AnalyzerError;
    };
}

fn validate_column(context: *muscle.QueryContext, table: *const muscle.Table, column: []const u8) !void {
    for (table.columns) |col| {
        if (std.ascii.eqlIgnoreCase(col.name, column)) return;
    }

    try context.set_err(error.AnalyzerError, "Column \"{s}\" does not exist inside table \"{s}\"", .{ column, table.name });
    return error.AnalyzerError;
}

fn validate_expressions_list(context: *muscle.QueryContext, table: *const muscle.Table, expressions: []muscle.Parser.Expression) !void {
    for (expressions) |*expr| _ = try analyze_expression(context, table, expr);
}

fn analyze_expression(context: *muscle.QueryContext, table: *const muscle.Table, expression: *const muscle.Parser.Expression) !?muscle.DataType {
    // validate that all identifiers are valid
    // validate left/right in binary are compatible for binary operation
    // validate operand and operator for unary operation
    return switch (expression.*) {
        .identifier => |identifier| {
            if (table.find_column(identifier)) |column| {
                return column.data_type;
            } else {
                try context.set_err(error.AnalyzerError, "Invalid column name {s}.", .{identifier});
                return error.AnalyzerError;
            }
        },
        .value => |val| switch (val) {
            .txt => .{ .txt = 0 }, // @Hack
            .bin => .{ .bin = 0 }, // @Hack
            .bool => .bool,
            // Parser already parses for i64 or f64 ranges so no need to validate here
            .int => .int,
            .real => .real,
            .null => null, // null represents we cannot determine what the expression evaluates to
        },
        .star => {
            unreachable;
        },
        .binary_operation => |op| {
            const left_type: ?muscle.DataType = try analyze_expression(context, table, op.left);
            const right_type: ?muscle.DataType = try analyze_expression(context, table, op.right);

            if (left_type == null and right_type != null) {
                try context.set_err(error.AnalyzerError, "left and right don't result into same data type for binary operation.", .{});
                return error.AnalyzerError;
            } else if (right_type == null and left_type != null) {
                try context.set_err(error.AnalyzerError, "left and right don't result into same data type for binary operation.", .{});
                return error.AnalyzerError;
            }

            if (@intFromEnum(left_type.?) != @intFromEnum(right_type.?)) {
                try context.set_err(error.AnalyzerError, "left and right don't result into same data type for binary operation.", .{});
                return error.AnalyzerError;
            }

            return switch (op.operator) {
                .eq, .neq, .lt, .lte, .gt, .gte => .bool,
                .plus, .minus, .mul, .div => {
                    if (left_type.? == .int or left_type.? == .real) return left_type;
                    try context.set_err(
                        error.AnalyzerError,
                        "operator {any} is not supported on type {any}",
                        .{ op.operator, left_type.? },
                    );
                    return error.AnalyzerError;
                },
                .logical_and, .logical_or => .bool,
            };
        },
        .unary_operation => |op| {
            // .plus and .minus operators only apply numbers
            // .not applies to boolean
            return switch (op.operator) {
                .plus, .minus => {
                    // At this time i don't want to complicate stuff than it needs to be.
                    // We can support this in future.
                    unreachable;

                    //if (try analyze_expression(context, table, op.operand)) |dt| {
                    //    if (dt != .int or dt != .real) {
                    //        try context.set_err(
                    //            error.AnalyzerError,
                    //            "Operand results into type {any} which is incompatible with operator {any}",
                    //            .{ dt, op.operator },
                    //        );
                    //        return error.AnalyzerError;
                    //    }

                    //    return null;
                    //} else {
                    //    try context.set_err(
                    //        error.AnalyzerError,
                    //        "Operand results into type {any} which is incompatible with operator {any}",
                    //        .{ dt, op.operator },
                    //    );
                    //    return error.AnalyzerError;
                    //}
                },
                .not => {
                    if (try analyze_expression(context, table, op.operand)) |dt| {
                        if (dt != .bool) {
                            try context.set_err(
                                error.AnalyzerError,
                                "Operand results into type {any} while boolean is needed for \"not\"",
                                .{dt},
                            );
                            return error.AnalyzerError;
                        }

                        return .bool;
                    } else {
                        // NOT (NULL)
                        // Typically databases will throw an error here, because "IS NOT NULL" should be used instead
                        // But we will support it because we are free men
                        //
                        //try context.set_err(
                        //    error.AnalyzerError,
                        //    "Operand results into type {any} while boolean is needed for \"not\"",
                        //    .{ dt, op.operator },
                        //);
                        //return error.AnalyzerError;

                        return .bool;
                    }
                },
            };
        },
        .nested => |expr| {
            return if (expr) |unwrapped| return try analyze_expression(context, table, unwrapped) else unreachable;
        },
    };
}

const assert = std.debug.assert;

test "Analyze Expression" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();
    var db = try Muscle.init(std.testing.allocator, file.file_path);
    defer db.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var result = try db.execute("create table books (id INT PRIMARY KEY AUTO_INCREMENT, name Text(200) UNIQUE);", arena.allocator());
    result = try db.execute("insert into authors (id, name) values (1, \"David Goggins\")", arena.allocator());
    assert(result.is_error_result());
    result = try db.execute("insert into books (id, name) values (1, \"can't hurt me\")", arena.allocator());
    assert(!result.is_error_result());
    if (result.is_error_result()) std.debug.print("\nError: {s}\n", .{result.err.message});
}
