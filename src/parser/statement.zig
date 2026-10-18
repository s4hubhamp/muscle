const muscle = @import("../muscle.zig");

const expression = @import("./expression.zig");
const Expression = expression.Expression;

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

pub const CreateTable = struct {
    table: []const u8,
    columns: []muscle.Column,
    primary_key_column_index: ?usize,
};

pub const Select = struct {
    columns: []Expression,
    table: []const u8,
    where: ?Expression,
    order_by: []Expression,
    limit: ?usize,
};

pub const Delete = struct {
    from: []const u8,
    where: ?Expression,
};

pub const Update = struct {
    table: []const u8,
    assignments: []Assignment,
    where: ?Expression,
};

pub const Assignment = struct {
    column: []const u8,
    value: Expression,
};

pub const Insert = struct {
    into: []const u8,
    columns: [][]const u8,
    values: []Expression,
};

const DropTable = union(enum) {
    table: []const u8,
};

const Explain = struct {
    statement: *Statement,
};
