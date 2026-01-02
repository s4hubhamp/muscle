const std = @import("std");
const muscle = @import("../muscle.zig");
const Expression = @import("expression.zig").Expression;

pub const Statement = union(enum) {
    //
    create_table: CreateTable,
    select: Select,
    delete: Delete,
    update: Update,
    insert: Insert,
    drop_table: DropTable,
    explain: Explain,

    // Transaction
    start,
    rollback,
    commit,
};

pub const ColumnDefinition = struct {
    name: []const u8,
    column_type: muscle.DataType,
    is_primary_key: bool,
    is_unique: bool,
};

const CreateTable = struct {
    table: []const u8,
    columns: std.ArrayList(ColumnDefinition),
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

pub const Assignment = struct {
    column: []const u8,
    value: Expression,
};

const Insert = struct {
    into: []const u8,
    columns: std.ArrayList([]const u8),
    values: std.ArrayList(Expression),
};

const DropTable = union(enum) {
    table: []const u8,
};

const Explain = struct {
    statement: *Statement,
};
