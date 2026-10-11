const std = @import("std");

/// Pretty-print an arbitrary value to stderr.
///
/// Unlike the standard `{any}` formatter, byte slices and byte arrays are
/// printed as escaped strings and aggregates are expanded with their field
/// names. Pointers are followed up to `max_depth`, which also keeps cyclic
/// data structures from recursing forever.
pub fn print(value: anytype) void {
    std.debug.print("{f}\n", .{detailed(value)});
}

/// Return a formatter suitable for embedding in another format string:
///
///     std.debug.print("payload = {f}\n", .{helpers.detailed(payload)});
pub fn detailed(value: anytype) Detailed(@TypeOf(value)) {
    return .{ .value = value };
}

pub fn Detailed(comptime T: type) type {
    return struct {
        value: T,
        max_depth: usize = 32,

        pub fn format(self: @This(), writer: *std.Io.Writer) std.Io.Writer.Error!void {
            try writeValue(writer, self.value, 0, self.max_depth);
        }
    };
}

fn writeValue(writer: *std.Io.Writer, value: anytype, depth: usize, max_depth: usize) std.Io.Writer.Error!void {
    const T = @TypeOf(value);

    if (depth >= max_depth) return writer.writeAll("...");

    switch (@typeInfo(T)) {
        .void => try writer.writeAll("void"),
        .bool, .int, .float, .comptime_int, .comptime_float => try writer.print("{}", .{value}),
        .null => try writer.writeAll("null"),
        .undefined => try writer.writeAll("undefined"),
        .type => try writer.writeAll(@typeName(value)),
        .enum_literal => try writer.print(".{s}", .{@tagName(value)}),
        .@"enum" => try writer.print(".{s}", .{@tagName(value)}),
        .error_set => try writer.print("error.{s}", .{@errorName(value)}),
        .optional => {
            if (value) |payload| {
                try writeValue(writer, payload, depth, max_depth);
            } else {
                try writer.writeAll("null");
            }
        },
        .error_union => {
            if (value) |payload| {
                try writeValue(writer, payload, depth, max_depth);
            } else |err| {
                try writer.print("error.{s}", .{@errorName(err)});
            }
        },
        .array => |array| {
            if (array.child == u8) {
                try writeString(writer, value[0..]);
            } else {
                try writeSequence(writer, value[0..], depth, max_depth);
            }
        },
        .vector => |vector| {
            try writer.writeAll("[");
            inline for (0..vector.len) |i| {
                if (i != 0) try writer.writeAll(", ");
                try writeValue(writer, value[i], depth + 1, max_depth);
            }
            try writer.writeAll("]");
        },
        .pointer => |pointer| switch (pointer.size) {
            .slice => {
                if (pointer.child == u8) {
                    try writeString(writer, value);
                } else {
                    try writeSequence(writer, value, depth, max_depth);
                }
            },
            .one => {
                try writer.writeAll("&");
                try writeValue(writer, value.*, depth + 1, max_depth);
            },
            .many => {
                if (pointer.child == u8 and pointer.sentinel() != null) {
                    try writeString(writer, std.mem.span(value));
                } else {
                    try writer.print("{s}@{x}", .{ @typeName(T), @intFromPtr(value) });
                }
            },
            .c => {
                if (value == null) {
                    try writer.writeAll("null");
                } else if (pointer.child == u8) {
                    try writeString(writer, std.mem.span(value));
                } else {
                    try writer.print("{s}@{x}", .{ @typeName(T), @intFromPtr(value) });
                }
            },
        },
        .@"struct" => |structure| {
            if (structure.is_tuple) {
                try writer.writeAll(".{");
                inline for (structure.fields, 0..) |field, i| {
                    if (i != 0) try writer.writeAll(",");
                    try writer.writeAll("\n");
                    try writeIndent(writer, depth + 1);
                    try writeValue(writer, @field(value, field.name), depth + 1, max_depth);
                }
            } else {
                try writer.print("{s}{{", .{@typeName(T)});
                inline for (structure.fields, 0..) |field, i| {
                    if (i != 0) try writer.writeAll(",");
                    try writer.writeAll("\n");
                    try writeIndent(writer, depth + 1);
                    try writer.print(".{s} = ", .{field.name});
                    try writeValue(writer, @field(value, field.name), depth + 1, max_depth);
                }
            }
            if (structure.fields.len != 0) {
                try writer.writeAll("\n");
                try writeIndent(writer, depth);
            }
            try writer.writeAll("}");
        },
        .@"union" => |union_info| {
            if (union_info.tag_type) |Tag| {
                const tag: Tag = value;
                try writer.print("{s}{{ .{s}", .{ @typeName(T), @tagName(tag) });
                inline for (union_info.fields) |field| {
                    if (tag == @field(Tag, field.name) and field.type != void) {
                        try writer.writeAll(" = ");
                        try writeValue(writer, @field(value, field.name), depth + 1, max_depth);
                    }
                }
                try writer.writeAll(" }");
            } else {
                try writer.print("{s}{{", .{@typeName(T)});
                inline for (union_info.fields, 0..) |field, i| {
                    if (i != 0) try writer.writeAll(", ");
                    try writer.print(".{s} = ", .{field.name});
                    try writeValue(writer, @field(value, field.name), depth + 1, max_depth);
                }
                try writer.writeAll("}");
            }
        },
        .@"fn", .@"opaque", .frame, .@"anyframe" => try writer.writeAll(@typeName(T)),
        .noreturn => unreachable,
    }
}

fn writeString(writer: *std.Io.Writer, bytes: []const u8) std.Io.Writer.Error!void {
    try writer.print("\"{f}\"", .{std.zig.fmtString(bytes)});
}

fn writeSequence(writer: *std.Io.Writer, values: anytype, depth: usize, max_depth: usize) std.Io.Writer.Error!void {
    try writer.writeAll("[");
    for (values, 0..) |value, i| {
        if (i != 0) try writer.writeAll(",");
        try writer.writeAll("\n");
        try writeIndent(writer, depth + 1);
        try writeValue(writer, value, depth + 1, max_depth);
    }
    if (values.len != 0) {
        try writer.writeAll("\n");
        try writeIndent(writer, depth);
    }
    try writer.writeAll("]");
}

fn writeIndent(writer: *std.Io.Writer, depth: usize) std.Io.Writer.Error!void {
    for (0..depth) |_| try writer.writeAll("  ");
}

test "detailed prints nested values and strings" {
    const Choice = union(enum) {
        text: []const u8,
        number: i32,
        nothing,
    };
    const Sample = struct {
        name: []const u8,
        aliases: []const []const u8,
        choice: Choice,
        missing: ?u8,
    };

    const aliases = [_][]const u8{ "one", "two\nthree" };
    const sample = Sample{
        .name = "muscle",
        .aliases = &aliases,
        .choice = .{ .text = "hello" },
        .missing = null,
    };

    var buffer: [2048]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try detailed(sample).format(&writer);

    const output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, ".name = \"muscle\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"two\\nthree\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".text = \"hello\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".missing = null") != null);
}

const TmpFile = struct {
    tmp_dir: std.testing.TmpDir,
    file_path: []const u8,

    pub fn deinit(self: *TmpFile) void {
        //self.tmp_dir.cleanup();
        std.testing.allocator.free(self.file_path);
    }
};
pub fn get_temp_file_path(file_name: []const u8) !TmpFile {
    var tmp_dir = std.testing.tmpDir(.{});
    // Get temp directory path
    const temp_dir_path = try tmp_dir.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(temp_dir_path);

    // Build database file path
    const file_path = try std.fs.path.join(
        std.testing.allocator,
        &[_][]const u8{
            temp_dir_path,
            file_name,
        },
    );

    return TmpFile{ .tmp_dir = tmp_dir, .file_path = file_path };
}
