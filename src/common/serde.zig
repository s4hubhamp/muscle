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
    var stream = std.io.fixedBufferStream(&buffer);
    var writer = stream.writer();

    try writer.writeStructEndian(page_struct, .little);
    return buffer;
}

pub fn deserialize_page(comptime T: type, buffer: []const u8) !T {
    assert(buffer.len == muscle.PAGE_SIZE);

    var stream = std.io.fixedBufferStream(buffer);
    var reader = stream.reader();
    const page_struct = try reader.readStructEndian(T, .little);
    return page_struct;
}

pub fn serailize_value(buffer: *muscle.common.BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE), val: muscle.Value) !void {
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

// @Todo we don't use this right now!
pub fn deserialize_value(buffer: []const u8, data_type: muscle.DataType) muscle.Value {
    switch (data_type) {
        .int => {
            // we don't have variable sized integers yet
            assert(buffer.len >= @sizeOf(i64));
            return .{ .int = std.mem.readInt(i64, buffer[0..@sizeOf(i64)], .little) };
        },
        .real => {
            // we don't have variable sized floats yet
            assert(buffer.len >= @sizeOf(f64));
            return .{ .int = @as(f64, @bitCast(std.mem.readInt(i64, buffer[0..@sizeOf(i64)], .little))) };
        },
        // strings and blobs are compared lexicographically
        .txt => {
            const len = std.mem.readInt(u16, buffer[0..@sizeOf(u16)], .little);
            return .{ .txt = buffer[@sizeOf(u16) .. @sizeOf(u16) + len] };
        },
        .bin => {
            const len = std.mem.readInt(u16, buffer[0..@sizeOf(u16)], .little);
            return .{ .bin = buffer[@sizeOf(u16) .. @sizeOf(u16) + len] };
        },
        else => {
            // we don't support booleans as primary keys right now
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
