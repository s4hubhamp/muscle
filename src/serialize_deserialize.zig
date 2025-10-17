const std = @import("std");
const muscle = @import("muscle");
const page = @import("./btree/page.zig");
const InsertPayload = @import("./execution_engine.zig").InsertPayload;

const assert = std.debug.assert;

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

pub fn serailize_value(buffer: *std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE), val: muscle.Value) !void {
    switch (val) {
        .BIN => |blob| {
            // len will take usize bytes
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(usize, &tmp, blob.len, .little);
            try buffer.appendSlice(&tmp);
            try buffer.appendSlice(blob);
        },
        .INT => |i| {
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, i, .little);
            try buffer.appendSlice(&tmp);
        },
        .REAL => |i| {
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, @as(i64, @bitCast(i)), .little);
            try buffer.appendSlice(&tmp);
        },
        .BOOL => |b| {
            try buffer.append(if (b) 1 else 0);
        },
        .TEXT => |str| {
            // len will take usize bytes
            var tmp: [8]u8 = undefined; // TODO: Maybe we don't need this to be usize?
            std.mem.writeInt(usize, &tmp, str.len, .little);
            try buffer.appendSlice(&tmp);
            try buffer.appendSlice(str);
        },
        .NULL => {
            // @Todo how to serialize nulls? once we support null as a value we also need to support how to understand and deserialize it when doing select for example
            unreachable;
        },
    }
}

// @Todo we don't use this right now!
pub fn deserialize_value(buffer: []const u8, data_type: muscle.DataType) muscle.Value {
    switch (data_type) {
        .INT => {
            // we don't have variable sized integers yet
            assert(buffer.len >= @sizeOf(i64));
            return .{ .INT = std.mem.readInt(i64, buffer[0..@sizeOf(i64)], .little) };
        },
        .REAL => {
            // we don't have variable sized floats yet
            assert(buffer.len >= @sizeOf(f64));
            return .{ .INT = @as(f64, @bitCast(std.mem.readInt(i64, buffer[0..@sizeOf(i64)], .little))) };
        },
        // strings and blobs are compared lexicographically
        .TEXT => {
            const len = std.mem.readInt(usize, buffer[0..@sizeOf(usize)], .little);
            return .{ .TEXT = buffer[@sizeOf(usize) .. @sizeOf(usize) + len] };
        },
        .BIN => {
            const len = std.mem.readInt(usize, buffer[0..@sizeOf(usize)], .little);
            return .{ .BIN = buffer[@sizeOf(usize) .. @sizeOf(usize) + len] };
        },
        else => {
            // we don't support booleans as primary keys right now
            unreachable;
        },
    }
}

// @Todo remove this
pub fn serialize_rowid(buffer: *[8]u8, id: muscle.RowId) !void {
    std.mem.writeInt(muscle.RowId, buffer, id, .little);
}

pub fn compare_serialized_bytes(data_type: muscle.DataType, a: []const u8, b: []const u8) std.math.Order {
    switch (data_type) {
        .INT => {
            // we don't have variable sized integers yet
            assert(a.len == @sizeOf(i64));
            assert(a.len == b.len);
            const _a = std.mem.readInt(i64, @ptrCast(a), .little);
            const _b = std.mem.readInt(i64, @ptrCast(b), .little);
            return std.math.order(_a, _b);
        },
        .REAL => {
            // we don't have variable sized floats yet
            assert(a.len == @sizeOf(f64));
            assert(a.len == b.len);
            const _a = @as(f64, @bitCast(std.mem.readInt(i64, @ptrCast(a), .little)));
            const _b = @as(f64, @bitCast(std.mem.readInt(i64, @ptrCast(b), .little)));
            return std.math.order(_a, _b);
        },
        // strings and blobs are compared lexicographically
        .TEXT, .BIN => {
            // first 8 btyes is length and later we have actual text data
            return std.mem.order(u8, a[@sizeOf(usize)..], b[@sizeOf(usize)..]);
        },
        else => {
            // we don't support booleans as primary keys right now
            unreachable;
        },
    }
}
