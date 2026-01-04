const std = @import("std");
const query_result = @import("./query_result.zig");

pub const QueryContext = @This();

input: []const u8, // query string
arena: std.mem.Allocator,
result: query_result.QueryResult,

pub fn init(arena: std.mem.Allocator, input: []const u8) QueryContext {
    return QueryContext{ .arena = arena, .input = input, .result = .{ .data = .__void } };
}

pub fn set_data(self: *QueryContext, data: query_result.Data) void {
    self.result = .{ .data = data };
}

pub fn set_err(self: *QueryContext, code: anyerror, comptime template: []const u8, args: anytype) std.mem.Allocator.Error!void {
    self.result = .{ .err = .{
        .code = code,
        .message = try std.fmt.allocPrint(self.arena, template, args),
    } };
}
