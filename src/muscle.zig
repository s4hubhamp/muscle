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
pub const analyzer = @import("analyzer.zig");

pub const Table = struct {
    // btree root page
    root: u32,
    // table name
    name: []const u8,
    columns: []Column,
    // index is outside column because we may have compound indexes
    indexes: []Index,

    pub fn clone(self: Table, allocator: std.mem.Allocator) !Table {
        var cloned = self;
        var columns_array = try std.ArrayList(Column).initCapacity(allocator, self.columns.len);
        var indexes_array = try std.ArrayList(Index).initCapacity(allocator, self.indexes.len);

        for (self.columns) |col| try columns_array.append(allocator, try col.clone(allocator));
        for (self.indexes) |idx| try indexes_array.append(allocator, try idx.clone(allocator));

        cloned.name = try allocator.dupe(u8, self.name);
        cloned.columns = try columns_array.toOwnedSlice(allocator);
        cloned.indexes = try indexes_array.toOwnedSlice(allocator);

        return cloned;
    }

    pub fn deinit(self: Table, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.columns) |*col| {
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
        allocator.free(self.columns);

        for (self.indexes) |*index| {
            allocator.free(index.name);
            allocator.free(index.column_name);
        }
        allocator.free(self.indexes);
    }

    pub fn find_column(self: *const Table, name: []const u8) ?*const Column {
        for (self.columns) |*col| {
            if (std.mem.eql(u8, col.name, name)) return col;
        }

        return null;
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

    fn clone(self: *const Column, allocator: std.mem.Allocator) !Column {
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

    fn clone(self: Index, allocator: std.mem.Allocator) !Index {
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
