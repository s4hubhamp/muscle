const muscle = @import("./muscle.zig");
const std = @import("std");
const query_result = @import("./query_result.zig");

pub const QueryContext = @This();

input: []const u8, // query string
arena: std.mem.Allocator,
result: query_result.QueryResult,
resultV2: query_result.QueryResultV2 = .{ .data = .{} },
pager: *muscle.PageManager,
catalog: *muscle.Catalog_Manager,

pub fn init(
    arena: std.mem.Allocator,
    input: []const u8,
    pager: *muscle.PageManager,
    catalog: *muscle.Catalog_Manager,
) QueryContext {
    return QueryContext{
        .arena = arena,
        .input = input,
        .result = .{ .data = .__void },
        .pager = pager,
        .catalog = catalog,
    };
}

pub fn set_data(self: *QueryContext, data: query_result.Data) void {
    self.result = .{ .data = data };
}

pub fn append_data(self: *QueryContext, data: muscle.page_types.EncodedCellContent) !void {
    try self.resultV2.data.rows.append(self.arena, try self.arena.dupe(u8, data));
}

pub fn set_result_columns(self: *QueryContext, columns: []const query_result.ResultColumn) !void {
    const copy = try self.arena.dupe(query_result.ResultColumn, columns);
    for (copy) |*column| column.name = try self.arena.dupe(u8, column.name);
    self.resultV2 = .{ .data = .{ .columns = copy } };
}

pub fn set_err(self: *QueryContext, code: anyerror, comptime template: []const u8, args: anytype) std.mem.Allocator.Error!void {
    const err = query_result.Error{
        .code = code,
        .message = try std.fmt.allocPrint(self.arena, template, args),
    };
    self.result = .{ .err = err };
    self.resultV2 = .{ .err = err };
}
