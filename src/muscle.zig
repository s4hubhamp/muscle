pub const RowId = i64;

pub const Table = struct {
    // btree root page
    root: u32,
    // table name
    name: []const u8,
    columns: []Column,
    // index is outside column because we may have compound indexes
    indexes: []Index,
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,

    // constraints
    unique: bool = false,
    not_null: bool = false,
    default: DefaultValue = DefaultValue{ .NULL = {} },
    auto_increment: bool = false,
    // curr max value of int column used in auto_increment
    max_int_value: i64 = 0,
};

pub const DataType = union(enum) { INT, REAL, BOOL, TEXT: usize, BIN: usize };
pub const Value = union(enum) {
    INT: i64,
    REAL: f64,
    BOOL: bool,
    TEXT: []const u8,
    BIN: []const u8,
    NULL: void,
};

const DefaultValueVariant = enum {
    NULL,
    // "HH:MM:SS"
    CURRENT_TIME,
    // "YYYY-MM-DD"
    CURRENT_DATE,
    // "YYYY-MM-DD HH:MM:SS"
    CURRENT_TIMESTAMP,
    // default can be any literan value also
    LITERAL,
};
const DefaultValue = union(DefaultValueVariant) {
    NULL: void,
    CURRENT_TIME: void,
    CURRENT_DATE: void,
    CURRENT_TIMESTAMP: void,
    LITERAL: Value,
};
pub const Index = struct {
    // root page of the index btree
    root: u32,
    // index name
    name: []const u8,
    // column on which index was created
    column_name: []const u8,
    is_unique: bool,
};

pub const PAGE_SIZE = 4096;
pub const PageNumber = u32;
