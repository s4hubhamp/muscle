const std = @import("std");
const muscle = @import("../muscle.zig");
const page_types = muscle.page_types;
const PageManager = muscle.PageManager;

const Self = @This();

allocator: std.mem.Allocator,
metadata: *page_types.DBMetadataPage,
tables: std.StringHashMap(muscle.Table),

const SerializableTable = struct {
    root: u32,
    name: []const u8,
    primary_key: []const u8,
    columns: []const muscle.Column,
    indexes: []const muscle.Index,

    pub fn clone(self: *const @This(), allocator: std.mem.Allocator) !@This() {
        var cloned = self.*;
        var columns_array = try std.ArrayList(muscle.Column).initCapacity(allocator, self.columns.len);
        var indexes_array = try std.ArrayList(muscle.Index).initCapacity(allocator, self.indexes.len);

        for (self.columns) |col| try columns_array.append(allocator, try col.clone(allocator));
        for (self.indexes) |idx| try indexes_array.append(allocator, try idx.clone(allocator));

        cloned.name = try allocator.dupe(u8, self.name);
        cloned.primary_key = try allocator.dupe(u8, self.primary_key);
        cloned.columns = try columns_array.toOwnedSlice(allocator);
        cloned.indexes = try indexes_array.toOwnedSlice(allocator);

        return cloned;
    }
};

pub fn init(allocator: std.mem.Allocator, pager: *PageManager) !Self {
    var self = Self{
        .allocator = allocator,
        .metadata = undefined,
        .tables = undefined,
    };

    self.metadata = try allocator.create(page_types.DBMetadataPage);
    self.tables = .init(allocator);

    self.metadata.* = try pager.get_page(page_types.DBMetadataPage, 0);
    const parsed = try std.json.parseFromSlice(
        []SerializableTable,
        allocator,
        self.metadata.tables[0..self.metadata.tables_len],
        .{ .allocate = .alloc_if_needed },
    );
    defer parsed.deinit();

    for (parsed.value) |*t| {
        const cloned = try t.clone(allocator);
        var table = muscle.Table{
            .name = cloned.name,
            .root = cloned.root,
            .primary_key = cloned.primary_key,
            .columns = .empty,
            .indexes = &.{},
        };

        for (cloned.columns) |col| {
            try table.columns.put(allocator, col.name, col);
        }

        try self.tables.put(cloned.name, table);
    }

    return self;
}

pub fn deinit(self: *Self) void {
    const allocator = self.allocator;

    var tablesIter = self.tables.valueIterator();
    while (tablesIter.next()) |t| {
        t.deinit(allocator);
    }

    self.tables.deinit();
    allocator.destroy(self.metadata);
}

pub fn update_metadata(self: *Self) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var serializableTables = try std.ArrayList(SerializableTable).initCapacity(allocator, self.tables.count());
    var iter = self.tables.valueIterator();
    while (iter.next()) |item| {
        try serializableTables.append(allocator, .{
            .name = item.name,
            .root = item.root,
            .primary_key = item.primary_key,
            .columns = item.columns.values(),
            .indexes = &.{},
        });
    }

    const json = try std.json.Stringify.valueAlloc(allocator, serializableTables.items, .{});

    self.metadata.tables = [_]u8{0} ** 4080;
    self.metadata.tables_len = @intCast(json.len);
    for (json, 0..) |char, i| self.metadata.tables[i] = char;
}

pub fn find_table(self: *const Self, name: []const u8) ?*const muscle.Table {
    return self.tables.getPtr(name);
}

pub fn create_table(self: *Self, pager: *muscle.PageManager, new_table: muscle.Table) !void {
    if (self.tables.contains(new_table.name)) {
        @branchHint(.cold);
        unreachable;
    }
    const copy = try new_table.clone(self.allocator);
    try self.tables.put(copy.name, copy);
    try self.update_metadata();
    try pager.update_page(0, self.metadata);
}

pub fn update_table(self: *Self, pager: *PageManager, updated_table: muscle.Table) !void {
    const copy = try updated_table.clone(self.allocator);

    const old = self.tables.fetchRemove(updated_table.name) orelse unreachable;
    var old_table = old.value;
    old_table.deinit(self.allocator);

    try self.tables.put(copy.name, copy);
    try self.update_metadata();
    try pager.update_page(0, self.metadata);
}
