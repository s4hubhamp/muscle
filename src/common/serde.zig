const std = @import("std");
const muscle = @import("../muscle.zig");

const assert = std.debug.assert;
const page_types = muscle.page_types;

pub fn serialize_page(page_struct: anytype) [muscle.PAGE_SIZE]u8 {
    comptime {
        if (@sizeOf(@TypeOf(page_struct)) != muscle.PAGE_SIZE)
            @compileError("Struct size must equal PAGE_SIZE");
    }

    var buffer: [muscle.PAGE_SIZE]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    writer.writeStruct(page_struct, .little) catch unreachable;
    return buffer;
}

pub fn deserialize_page(comptime T: type, buffer: []const u8) !T {
    assert(buffer.len == muscle.PAGE_SIZE);

    var reader: std.Io.Reader = .fixed(buffer);
    return try reader.takeStruct(T, .little);
}

/// Every value starts with one byte: 0 = NULL (no payload), 1 = present.
/// The same encoding is used for stored rows, keys, and query results.
pub fn deserialize_value(
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    content: muscle.page_types.EncodedCellContent,
    column_name: []const u8,
) !muscle.Value {
    var offset: usize = 0;
    for (table.columns.values()) |column| {
        const value = read_value(content, &offset, column.data_type) catch |err| {
            try context.set_err(err, "Row data is invalid or truncated.", .{});
            return err;
        };
        if (std.mem.eql(u8, column.name, column_name)) return value;
    }
    try context.set_err(error.ColumnDoesNotExist, "Referenced column does not exist {s}.", .{column_name});
    return error.ColumnDoesNotExist;
}

pub fn get_serialized_slice(
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    content: muscle.page_types.EncodedCellContent,
    column_name: []const u8,
) ![]const u8 {
    var offset: usize = 0;
    for (table.columns.values()) |column| {
        const value = read_value_raw(content, &offset, column.data_type) catch |err| {
            try context.set_err(err, "Row data is invalid or truncated.", .{});
            return err;
        };
        if (std.mem.eql(u8, column.name, column_name)) return value;
    }
    try context.set_err(error.ColumnDoesNotExist, "Referenced column does not exist {s}.", .{column_name});
    return error.ColumnDoesNotExist;
}

fn read_bytes(content: []const u8, offset: *usize, len: usize) ![]const u8 {
    if (offset.* > content.len or content.len - offset.* < len) return error.CorruptedData;
    const bytes = content[offset.*..][0..len];
    offset.* += len;
    return bytes;
}

/// Read one column and advance past exactly its marker and optional payload.
pub fn read_value(content: []const u8, offset: *usize, data_type: muscle.DataType) !muscle.Value {
    switch ((try read_bytes(content, offset, 1))[0]) {
        0 => return .null,
        1 => {},
        else => return error.CorruptedData,
    }

    return switch (data_type) {
        .int => .{ .int = std.mem.readInt(i64, (try read_bytes(content, offset, 8))[0..8], .little) },
        .real => .{ .real = @bitCast(std.mem.readInt(u64, (try read_bytes(content, offset, 8))[0..8], .little)) },
        .bool => blk: {
            const byte = (try read_bytes(content, offset, 1))[0];
            if (byte > 1) return error.CorruptedData;
            break :blk .{ .bool = byte != 0 };
        },
        .txt, .bin => blk: {
            const len = std.mem.readInt(u16, (try read_bytes(content, offset, 2))[0..2], .little);
            const bytes = try read_bytes(content, offset, len);
            break :blk if (data_type == .txt) .{ .txt = bytes } else .{ .bin = bytes };
        },
    };
}

pub fn read_value_raw(content: []const u8, offset: *usize, data_type: muscle.DataType) ![]const u8 {
    return switch (data_type) {
        .int, .real => (try read_bytes(content, offset, 9)),
        .bool => (try read_bytes(content, offset, 2)),
        .txt, .bin => blk: {
            const start = offset.*;
            _ = try read_bytes(content, offset, 1);
            const len = std.mem.readInt(u16, (try read_bytes(content, offset, 2))[0..2], .little);
            _ = try read_bytes(content, offset, len);
            break :blk content[start .. start + 1 + 2 + len];
        },
    };
}

fn encoded_size(value: muscle.Value) !usize {
    // increment 1 for null bit
    return 1 + try switch (value) {
        .null => 0,
        .int => @sizeOf(i64),
        .real => @sizeOf(f64),
        .bool => 1,
        .txt => |text| if (text.len <= std.math.maxInt(u16)) @sizeOf(u16) + text.len else error.TextTooLong,
        .bin => |bytes| if (bytes.len <= std.math.maxInt(u16)) @sizeOf(u16) + bytes.len else error.BinaryTooLarge,
    };
}

fn write_value(bytes: []u8, value: muscle.Value) void {
    bytes[0] = if (value == .null) 0 else 1;
    switch (value) {
        .null => {},
        .int => |number| std.mem.writeInt(i64, bytes[1..9], number, .little),
        .real => |number| std.mem.writeInt(u64, bytes[1..9], @bitCast(number), .little),
        .bool => |boolean| bytes[1] = @intFromBool(boolean),
        .txt, .bin => |payload| {
            std.mem.writeInt(u16, bytes[1..3], @intCast(payload.len), .little);
            @memcpy(bytes[3..], payload);
        },
    }
}

pub fn serialize(value: muscle.Value, allocator: std.mem.Allocator) ![]u8 {
    const bytes = try allocator.alloc(u8, try encoded_size(value));
    write_value(bytes, value);
    return bytes;
}

pub fn serialize_value(buffer: *muscle.common.BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE), value: muscle.Value) !void {
    const len = try encoded_size(value);
    // Check the marker AND payload before changing the row buffer.
    if (buffer.capacity() - buffer.len < len) return error.RowTooBig;
    write_value(buffer.unused_capacity_slice()[0..len], value);
    buffer.len += @intCast(len);
}

pub fn compare_serialized_bytes(data_type: muscle.DataType, a: []const u8, b: []const u8) std.math.Order {
    assert(a.len > 1 and b.len > 1);

    // We can't compare null values
    if (a[0] == 0 or b[0] == 0) {
        unreachable;
    }

    const _a = a[1..];
    const _b = b[1..];

    switch (data_type) {
        .int => {
            // we don't have variable sized integers yet
            assert(_a.len == @sizeOf(i64));
            assert(_a.len == _b.len);
            return std.math.order(
                std.mem.readInt(i64, @ptrCast(_a), .little),
                std.mem.readInt(i64, @ptrCast(_b), .little),
            );
        },
        .real => {
            // we don't have variable sized floats yet
            assert(_a.len == @sizeOf(f64));
            assert(_a.len == _b.len);
            return std.math.order(
                @as(f64, @bitCast(std.mem.readInt(u64, @ptrCast(_a), .little))),
                @as(f64, @bitCast(std.mem.readInt(u64, @ptrCast(_b), .little))),
            );
        },
        // strings and blobs are compared lexicographically
        .txt, .bin => {
            //
            // @Note we don't need to determine length as a and b are only key and value slices.
            // But these asserts are faily cheap so we are gonna keep them as is.
            //
            // Ensure we have at least the length prefix
            assert(_a.len >= @sizeOf(u16));
            assert(_b.len >= @sizeOf(u16));

            // Read the lengths
            const len_a = std.mem.readInt(u16, _a[0..@sizeOf(u16)], .little);
            const len_b = std.mem.readInt(u16, _b[0..@sizeOf(u16)], .little);

            // Ensure the buffers contain the full data
            assert(_a.len >= @sizeOf(u16) + len_a);
            assert(_b.len >= @sizeOf(u16) + len_b);

            // Extract the actual data (skip the length prefix)
            const data_a = _a[@sizeOf(u16) .. @sizeOf(u16) + len_a];
            const data_b = _b[@sizeOf(u16) .. @sizeOf(u16) + len_b];

            // Compare lexicographically
            return std.mem.order(u8, data_a, data_b);
        },
        else => {
            // we don't support booleans as primary keys right now
            unreachable;
        },
    }
}

test {
    _ = @import("../testing/serde.zig");
}
