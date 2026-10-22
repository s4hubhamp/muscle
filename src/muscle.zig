const std = @import("std");

pub const database = @import("database.zig");
pub const common = @import("common.zig");
pub const PageManager = @import("storage/PageManager.zig");
pub const page_types = @import("storage/page_types.zig");
pub const BTree = @import("execution/BTree.zig");
pub const QueryContext = @import("QueryContext.zig");
pub const query_result = @import("query_result.zig");
pub const Catalog_Manager = @import("catalog/CatalogManager.zig");
pub const Parser = @import("parser/Parser.zig");
pub const expression = @import("parser/expression.zig");
pub const statement = @import("parser/statement.zig");
pub const planner = @import("planner.zig");
pub const analyzer = @import("analyzer.zig");

pub const PageType = page_types.PageType;
pub const Page = page_types._Page;

pub const Table = struct {
    // btree root page
    root: u32,
    // table name
    name: []const u8,
    // columns keyed by name for lookup
    columns: std.array_hash_map.String(Column),
    // primary key column name @Todo do we really need this? because pk is always the first column
    primary_key: []const u8,
    // index is outside column because we may have compound indexes
    indexes: []Index,

    pub fn clone(self: *const @This(), allocator: std.mem.Allocator) !Table {
        var copy = Table{
            .name = try allocator.dupe(u8, self.name),
            .primary_key = try allocator.dupe(u8, self.primary_key),
            .root = self.root,
            .columns = .empty,
            .indexes = &.{},
        };

        for (self.columns.values()) |col| {
            const column_copy = try col.clone(allocator);
            try copy.columns.put(allocator, column_copy.name, column_copy);
        }

        var indexes_array = try std.ArrayList(Index).initCapacity(allocator, self.indexes.len);
        for (self.indexes) |idx| {
            try indexes_array.append(allocator, try idx.clone(allocator));
        }
        copy.indexes = try indexes_array.toOwnedSlice(allocator);

        return copy;
    }

    pub fn deinit(self: *Table, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.primary_key);
        for (self.columns.values()) |*col| {
            allocator.free(col.name);
            switch (col.default) {
                .literal => |val| {
                    switch (val) {
                        .txt, .bin => |slice| allocator.free(slice),
                        else => {},
                    }
                },
                else => {},
            }
        }
        self.columns.deinit(allocator);
        for (self.indexes) |*index| {
            allocator.free(index.name);
            allocator.free(index.column_name);
        }
        allocator.free(self.indexes);
    }
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

    pub fn clone(self: *const Column, allocator: std.mem.Allocator) !Column {
        var cloned = self.*;
        cloned.name = try allocator.dupe(u8, self.name);
        cloned.default = try self.default.clone(allocator);
        return cloned;
    }
};

pub const DataType = union(enum) {
    int,
    real,
    bool,
    // default limit will be max value for u16
    txt: u16,
    bin: u16,
};

pub const Value = union(enum) {
    int: i64,
    real: f64,
    bool: bool,
    txt: []const u8,
    bin: []const u8,
    null,

    fn clone(self: Value, allocator: std.mem.Allocator) !Value {
        return switch (self) {
            .txt => |slice| .{ .txt = try allocator.dupe(u8, slice) },
            .bin => |slice| .{ .bin = try allocator.dupe(u8, slice) },
            else => self,
        };
    }

    pub fn to_f64(self: Value) f64 {
        return switch (self) {
            .int => |v| @floatFromInt(v),
            .real => |v| @floatCast(v),
            else => unreachable,
        };
    }
};

pub const DefaultValue = union(enum) {
    null,
    // "HH:MM:SS"
    current_time,
    // "YYYY-MM-DD"
    current_date,
    // "YYYY-MM-DD HH:MM:SS"
    current_timestamp,
    // default can be any literan value also
    literal: Value,

    fn clone(self: DefaultValue, allocator: std.mem.Allocator) !DefaultValue {
        return switch (self) {
            .literal => .{ .literal = try self.literal.clone(allocator) },
            else => self,
        };
    }
};

pub const Index = struct {
    // root page of the index btree
    root: u32,
    // index name
    name: []const u8,
    // column on which index was created
    column_name: []const u8,
    is_unique: bool,

    pub fn clone(self: Index, allocator: std.mem.Allocator) !Index {
        var cloned = self;
        cloned.name = try allocator.dupe(u8, self.name);
        cloned.column_name = try allocator.dupe(u8, self.column_name);
        return cloned;
    }
};

pub const PAGE_SIZE = 4096;
pub const PageNumber = u32;

test {
    std.testing.refAllDecls(@This());
}
