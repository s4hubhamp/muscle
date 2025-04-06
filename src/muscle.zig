pub const Table = struct {
    // btree root page
    root: u32,
    // incremental counter for internal row id
    row_id: u32,
    // table name
    name: []const u8,
    columns: []const Column,
    // index is outside column because we may have compound indexes
    indexes: []const Index,
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,
    constraints: []const ColumnConstraint,
};
pub const DataType = enum {
    Int,
    UnsignedInt,
    BigInt,
    UnsignedBigInt,
    Bool,
    Varchar,
};
pub const ColumnConstraint = enum {
    PrimaryKey,
    Unique,
};
pub const Index = struct {
    // btree root page
    root: u32,
    // index name
    name: []const u8,
    // column on which index was created
    column_name: []const u8,
    is_unique: bool,
};
