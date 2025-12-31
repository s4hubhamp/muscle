const muscle = @import("../muscle.zig");

pub const Expression = union(enum) {
    identifier: []const u8,
    value: muscle.Value,
    star,
    binary_operation: BinaryOperation,
    unary_operation: UnaryOperation,
    nested: ?*Expression,

    pub fn print(self: *Expression) void {
        const std = @import("std");

        switch (self.*) {
            .identifier => |id| std.debug.print("{s}", .{id}),
            .value => |val| {
                switch (val) {
                    .txt => |s| std.debug.print("\"{s}\"", .{s}),
                    .bin => |b| std.debug.print("\"{any}\"", .{b}),
                    .int => |n| std.debug.print("{}", .{n}),
                    .real => |n| std.debug.print("{}", .{n}),
                    .bool => |b| std.debug.print("{}", .{b}),
                    .null => std.debug.print("null", .{}),
                }
            },
            .star => std.debug.print("*", .{}),
            .binary_operation => |bin_op| {
                std.debug.print("(", .{});
                bin_op.left.print();
                std.debug.print(" {} ", .{bin_op.operator});
                bin_op.right.print();
                std.debug.print(")", .{});
            },
            .unary_operation => |un_op| {
                std.debug.print("({} ", .{un_op.operator});
                un_op.operand.print();
                std.debug.print(")", .{});
            },
            .nested => |nested_expr| {
                if (nested_expr) |expr| {
                    std.debug.print("(", .{});
                    expr.print();
                    std.debug.print(")", .{});
                } else {
                    std.debug.print("()", .{});
                }
            },
        }
    }
};

const BinaryOperation = struct {
    operator: BinaryOperator,
    left: *Expression,
    right: *Expression,
};

const UnaryOperation = struct {
    operator: UnaryOperator,
    operand: *Expression,
};

pub const BinaryOperator = enum {
    eq,
    neq,
    lt,
    lte,
    gt,
    gte,
    plus,
    minus,
    mul,
    div,
    logical_and,
    logical_or,
};

pub const UnaryOperator = enum { not, plus, minus };
