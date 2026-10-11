const std = @import("std");
const muscle = @import("../muscle.zig");

const assert = std.debug.assert;
const page_types = muscle.page_types;

pub fn serialize_page(page_struct: anytype) ![muscle.PAGE_SIZE]u8 {
    comptime {
        if (@sizeOf(@TypeOf(page_struct)) != muscle.PAGE_SIZE)
            @compileError("Struct size must equal PAGE_SIZE");
    }

    var buffer: [muscle.PAGE_SIZE]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try writer.writeStruct(page_struct, .little);
    return buffer;
}

pub fn deserialize_page(comptime T: type, buffer: []const u8) !T {
    assert(buffer.len == muscle.PAGE_SIZE);

    var reader: std.Io.Reader = .fixed(buffer);
    return try reader.takeStruct(T, .little);
}

pub fn deserialize_value(
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    content: muscle.page_types.EncodedCellContent,
    column_name: []const u8,
) !muscle.Value {
    var offset: usize = 0;
    for (table.columns.values()) |column| {
        const encoded_len: usize = switch (column.data_type) {
            .int, .real => @sizeOf(i64),
            .bool => 1,
            .txt, .bin => blk: {
                if (content.len -| offset < @sizeOf(u16)) {
                    try context.set_err(error.CorruptedData, "Row data is truncated.", .{});
                    return error.CorruptedData;
                }
                const len = std.mem.readInt(
                    u16,
                    content[offset..][0..@sizeOf(u16)],
                    .little,
                );
                break :blk @sizeOf(u16) + @as(usize, len);
            },
        };

        if (content.len -| offset < encoded_len) {
            try context.set_err(error.CorruptedData, "Row data is truncated.", .{});
            return error.CorruptedData;
        }

        if (std.mem.eql(u8, column.name, column_name)) {
            return switch (column.data_type) {
                .int => .{ .int = std.mem.readInt(
                    i64,
                    content[offset..][0..@sizeOf(i64)],
                    .little,
                ) },
                .real => .{ .real = @bitCast(std.mem.readInt(
                    i64,
                    content[offset..][0..@sizeOf(i64)],
                    .little,
                )) },
                .bool => .{ .bool = content[offset] != 0 },
                .txt => .{ .txt = content[offset + @sizeOf(u16) .. offset + encoded_len] },
                .bin => .{ .bin = content[offset + @sizeOf(u16) .. offset + encoded_len] },
            };
        }

        offset += encoded_len;
    }
    try context.set_err(error.ColumnDoesNotExist, "Referenced column does not exist {s}.", .{column_name});
    return error.ColumnDoesNotExist;
}

pub fn serialize(val: muscle.Value, allocator: std.mem.Allocator) ![]u8 {
    switch (val) {
        .int => |i| {
            const tmp = try allocator.alloc(u8, 8);
            std.mem.writeInt(i64, @ptrCast(tmp), i, .little);
            return tmp;
        },
        .real => |i| {
            const tmp = try allocator.alloc(u8, 8);
            std.mem.writeInt(i64, @ptrCast(tmp), @as(i64, @bitCast(i)), .little);
            return tmp;
        },
        .bool => |b| {
            var tmp = try allocator.alloc(u8, 1);
            tmp[0] = if (b) 1 else 0;
            return tmp;
        },
        .txt => |str| {
            const tmp = try allocator.alloc(u8, 2 + str.len);
            std.mem.writeInt(u16, @ptrCast(tmp), @intCast(str.len), .little);
            for (tmp[2..], str[0..]) |*ptr, byte| {
                ptr.* = byte;
            }
            return tmp;
        },
        .bin => |blob| {
            const tmp = try allocator.alloc(u8, 2 + blob.len);
            std.mem.writeInt(u16, @ptrCast(tmp), @intCast(blob.len), .little);
            for (tmp[2..], blob[0..]) |*ptr, byte| {
                ptr.* = byte;
            }
            return tmp;
        },
        .null => {
            // @Todo how to serialize nulls? once we support null as a value we also need to support how to understand and deserialize it when doing select for example
            unreachable;
        },
    }
}

pub fn serialize_value(buffer: *muscle.common.BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE), val: muscle.Value) !void {
    switch (val) {
        .int => |i| {
            if (buffer.capacity() - buffer.len < 8) return error.RowTooBig;

            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, i, .little);
            buffer.push_slice(&tmp);
        },
        .real => |i| {
            if (buffer.capacity() - buffer.len < 8) return error.RowTooBig;

            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, @as(i64, @bitCast(i)), .little);
            buffer.push_slice(&tmp);
        },
        .bool => |b| {
            if (buffer.capacity() - buffer.len < 8) return error.RowTooBig;

            buffer.push(if (b) 1 else 0);
        },
        .txt => |str| {
            if (buffer.capacity() - buffer.len < (2 + str.len)) return error.RowTooBig;

            var tmp: [2]u8 = undefined;
            std.mem.writeInt(
                u16,
                &tmp,
                @intCast(str.len), // safe because we check the length before reaching here
                .little,
            );

            buffer.push_slice(&tmp);
            buffer.push_slice(str);
        },
        .bin => |blob| {
            if (buffer.capacity() - buffer.len < (2 + blob.len)) return error.RowTooBig;

            var tmp: [2]u8 = undefined;
            std.mem.writeInt(
                u16,
                &tmp,
                @intCast(blob.len), // safe because we check the length before reaching here
                .little,
            );

            buffer.push_slice(&tmp);
            buffer.push_slice(blob);
        },
        .null => {
            // @Todo how to serialize nulls? once we support null as a value we also need to support how to understand and deserialize it when doing select for example
            unreachable;
        },
    }
}

pub fn compare_serialized_bytes(data_type: muscle.DataType, a: []const u8, b: []const u8) std.math.Order {
    switch (data_type) {
        .int => {
            // we don't have variable sized integers yet
            assert(a.len == @sizeOf(i64));
            assert(a.len == b.len);
            const _a = std.mem.readInt(i64, @ptrCast(a), .little);
            const _b = std.mem.readInt(i64, @ptrCast(b), .little);
            return std.math.order(_a, _b);
        },
        .real => {
            // we don't have variable sized floats yet
            assert(a.len == @sizeOf(f64));
            assert(a.len == b.len);
            const _a = @as(f64, @bitCast(std.mem.readInt(i64, @ptrCast(a), .little)));
            const _b = @as(f64, @bitCast(std.mem.readInt(i64, @ptrCast(b), .little)));
            return std.math.order(_a, _b);
        },
        // strings and blobs are compared lexicographically
        .txt, .bin => {
            //
            // @Note we don't need to determine length as a and b are only key and value slices.
            // But these asserts are faily cheap so we are gonna keep them as is.
            //
            // Ensure we have at least the length prefix
            assert(a.len >= @sizeOf(u16));
            assert(b.len >= @sizeOf(u16));

            // Read the lengths
            const len_a = std.mem.readInt(u16, a[0..@sizeOf(u16)], .little);
            const len_b = std.mem.readInt(u16, b[0..@sizeOf(u16)], .little);

            // Ensure the buffers contain the full data
            assert(a.len >= @sizeOf(u16) + len_a);
            assert(b.len >= @sizeOf(u16) + len_b);

            // Extract the actual data (skip the length prefix)
            const data_a = a[@sizeOf(u16) .. @sizeOf(u16) + len_a];
            const data_b = b[@sizeOf(u16) .. @sizeOf(u16) + len_b];

            // Compare lexicographically
            return std.mem.order(u8, data_a, data_b);
        },
        else => {
            // we don't support booleans as primary keys right now
            unreachable;
        },
    }
}
