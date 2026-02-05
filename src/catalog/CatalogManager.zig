const std = @import("std");
const muscle = @import("../muscle.zig");
const page_types = muscle.page_types;
const PageManager = muscle.PageManager;

const Self = @This();

allocator: std.mem.Allocator,
metadata: *page_types.DBMetadataPage,
// @Perf we might consider this to be hash table instead of arraylist for quick access.
tables: std.ArrayList(muscle.Table),

pub fn init(allocator: std.mem.Allocator, pager: *PageManager) !Self {
    var self = Self{
        .allocator = allocator,
        .metadata = try allocator.create(page_types.DBMetadataPage),
        .tables = .empty,
    };

    self.metadata.* = try pager.get_page(page_types.DBMetadataPage, 0);
    const parsed = try std.json.parseFromSlice(
        []muscle.Table,
        allocator,
        self.metadata.tables[0..self.metadata.tables_len],
        .{ .allocate = .alloc_if_needed },
    );
    defer parsed.deinit();

    self.tables = try std.ArrayList(muscle.Table).initCapacity(allocator, parsed.value.len);
    for (parsed.value) |*t| self.tables.appendAssumeCapacity(try t.clone(self.allocator));

    return self;
}

pub fn deinit(self: *Self) void {
    self.allocator.destroy(self.metadata);
    for (self.tables.items) |*t| t.deinit(self.allocator);
    self.tables.deinit(self.allocator);
}

pub fn update_metadata(self: *Self) !void {
    const json = try std.json.Stringify.valueAlloc(self.allocator, self.tables.items, .{});
    defer self.allocator.free(json);

    self.metadata.tables = [_]u8{0} ** 4080;
    self.metadata.tables_len = @intCast(json.len);
    for (json, 0..) |char, i| self.metadata.tables[i] = char;
}

pub fn find_table(self: *const Self, name: []const u8) ?*const muscle.Table {
    for (self.tables.items) |*table| {
        if (std.mem.eql(u8, table.name, name)) {
            return table;
        }
    }

    return null;
}

pub fn create_table(self: *Self, pager: *PageManager, table: muscle.Table) !void {
    try self.tables.append(self.allocator, try table.clone(self.allocator));
    try self.update_metadata();
    try pager.update_page(0, self.metadata);
}

pub fn update_table(self: *Self, pager: *PageManager, updated_table: muscle.Table) !void {
    for (self.tables.items) |*table| {
        if (std.mem.eql(u8, table.name, updated_table.name)) {
            table.deinit(self.allocator);
            table.* = try updated_table.clone(self.allocator);
            try self.update_metadata();
            try pager.update_page(0, self.metadata);
            return;
        }
    }

    unreachable;
}
