pub const RowId = u64;

pub const Table = struct {
    // btree root page
    root: u32,
    // incremental counter for internal row id
    last_insert_rowid: RowId,
    // table name
    name: []const u8,
    columns: []const Column,
    // index is outside column because we may have compound indexes
    indexes: []const Index,
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,

    // constraints
    primary_key: bool = false,
    unique: bool = false,
    not_null: bool = false,
    auto_increment: bool = false,
    default: DefaultValue = DefaultValue{ .NULL = {} },
};
const DataTypeEnum = enum {
    // i64
    INT,
    // f64
    REAL,
    // bool
    BOOL,
    TEXT,
    // binary
    BIN,
    NULL,
};
pub const DataType = union(DataTypeEnum) { INT, REAL, BOOL, TEXT: usize, BIN: usize, NULL };

pub const Value = union(DataTypeEnum) {
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
    // btree root page
    root: u32,
    // index name
    name: []const u8,
    // column on which index was created
    column_name: []const u8,
    is_unique: bool,
};

pub const PAGE_SIZE = 4096;
pub const PageNumber = u32;
