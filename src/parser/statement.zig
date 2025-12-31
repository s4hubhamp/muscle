const std = @import("std");
const muscle = @import("../muscle.zig");
const Expression = @import("expression.zig").Expression;

pub const Statement = union(enum) {
    //
    create: Create,
    select: Select,
    delete: Delete,
    update: Update,
    insert: Insert,
    drop: Drop,
    explain: Explain,

    // Transaction
    start,
    rollback,
    commit,
};

const Create = union(enum) {
    database: []const u8,
    table: struct {
        name: []const u8,
        columns: std.ArrayList(muscle.Column),
    },
    index: struct {
        name: []const u8,
        table: []const u8,
        column: []const u8,
        unique: bool,
    },
};

const Select = struct {
    columns: std.ArrayList(Expression),
    table: []const u8,
    where: ?Expression,
    order_by: std.ArrayList(Expression),
    limit: usize,
};

const Delete = struct {
    from: []const u8,
    where: ?Expression,
};

const Update = struct {
    table: []const u8,
    assignments: std.ArrayList(Assignment),
    where: ?Expression,
};

const Assignment = struct {
    identifier: []const u8,
    value: Expression,
};

const Insert = struct {
    into: []const u8,
    columns: []const []const u8,
    values: std.ArrayList(Expression),
};

const Drop = union(enum) {
    table: []const u8,
    database: []const u8,
};

const Explain = struct {
    statement: *Statement,
};
