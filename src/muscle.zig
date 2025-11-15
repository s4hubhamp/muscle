pub const database = @import("main/database.zig");

pub const common = @import("common.zig");
pub const storage = @import("storage.zig");
pub const execution = @import("execution.zig");

//
// @Todo Types below are internal and should not be exposed outside
//
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
    default: DefaultValue = DefaultValue{ .null = {} },
    auto_increment: bool = false,
    // curr max value of int column used in auto_increment
    max_int_value: i64 = 0,
};

pub const DataType = union(enum) { int, real, bool, txt: usize, bin: usize };
pub const Value = union(enum) {
    int: i64,
    real: f64,
    bool: bool,
    txt: []const u8,
    bin: []const u8,
    null: void,
};

const DefaultValue = union(enum) {
    null: void,
    // "HH:MM:SS"
    current_time: void,
    // "YYYY-MM-DD"
    current_date: void,
    // "YYYY-MM-DD HH:MM:SS"
    current_timestamp: void,
    // default can be any literan value also
    literal: Value,
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

test {
    @import("std").testing.refAllDecls(@This());
}
