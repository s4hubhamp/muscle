const std = @import("std");
const muscle = @import("muscle");
const page = @import("./btree/page.zig");
const InsertPayload = @import("./execution_engine.zig").InsertPayload;

const assert = std.debug.assert;

pub fn serialize_page(page_struct: anytype) ![muscle.PAGE_SIZE]u8 {
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

// Serialize the row and returns a Cell
pub fn serialize_row(
    buffer: *std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE),
    table: muscle.Table,
    rowid: muscle.RowId,
    payload: InsertPayload,
) !struct { cell: page.Cell, rowid_slice: []const u8 } {
    assert(payload.columns.len == payload.values.len);
    var cell: page.Cell = undefined;

    // serialize the row id
    const serialized_rowid_slice = try serialize_rowid(buffer, rowid);

    // serialize the payload
    for (table.columns) |column| {
        var found = false;
        for (payload.columns, 0..) |col, i| {
            if (std.mem.eql(u8, column.name, col.name)) {
                found = true;

                // TODO we are not validating against data type. where do we check if the
                // data type on column definition is correct.
                // TODO we are also not validating to check if the text length is less than
                // equal to set length on Text like columns (TEXT, BIN)

                // serialize and add the value to buffer
                try serailize_value(buffer, payload.values[i]);
            }
        }

        // if the value is not provided in payload
        // we will try to use default value
        if (!found) {
            switch (column.default) {
                .NULL => {
                    const default_null_value = muscle.Value{ .NULL = {} };
                    // if column has non null constraint then we can't set the value to null
                    if (column.not_null) return error.ValueNotProvided;
                    try serailize_value(buffer, default_null_value);
                },
                else => {
                    unreachable;
                },
            }
        }
    }

    cell.content = buffer.constSlice();
    cell.size = @as(u16, @intCast(6 + cell.content.len));
    cell.left_child = 0;

    return .{ .cell = cell, .rowid_slice = serialized_rowid_slice };
}

fn serailize_value(buffer: *std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE), val: muscle.Value) !void {
    switch (val) {
        .BIN => |blob| {
            // len will take usize bytes
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(usize, &tmp, blob.len, .little);
            try buffer.appendSlice(&tmp);
            try buffer.appendSlice(blob);
        },
        .INT => |i| {
            // Int is i64 so 8 bytes
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, i, .little);
            try buffer.appendSlice(&tmp);
        },
        .REAL => |i| {
            // Int is f64 so 8 bytes
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, @as(i64, @bitCast(i)), .little);
            try buffer.appendSlice(&tmp);
        },
        .BOOL => |b| {
            try buffer.append(if (b) 1 else 0);
        },
        .TEXT => |str| {
            // len will take usize bytes
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(usize, &tmp, str.len, .little);
            try buffer.appendSlice(&tmp);
            try buffer.appendSlice(str);
        },
        .NULL => {
            try buffer.append(0);
        },
    }
}

pub fn serialize_rowid(buffer: *std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE), id: muscle.RowId) ![]const u8 {
    var tmp: [8]u8 = undefined; // wide enough to store u128
    std.mem.writeInt(muscle.RowId, &tmp, id, .little);
    try buffer.appendSlice(&tmp);
    return buffer.constSlice();
}

//pub fn main() !void {
//    //var buffer = [_]u8{0} ** 100;
//    //var stream = std.io.fixedBufferStream(&buffer);
//    //var writer = stream.writer();

//    const S = extern struct {
//        a: u16,
//        b: u16,
//        c: u32,
//        d: [5]u8,
//    };

//    const s = S{ .a = 256, .b = 257, .c = 258, .d = .{ 1, 2, 3, 4, 5 } };

//    var serialized = try serialize_page(s);
//    std.debug.print("serialized: {X}\n", .{serialized});

//    const deserialized = try deserialize_page(S, &serialized);
//    std.debug.print("deserialized: {any}\n", .{deserialized});

//    //try writer.writeStruct(s);
//    //try writer.writeStructEndian(s, .big);

//}

//fn serializeAndDeserialize(comptime T: type, value: T, buffer: []u8) !T {
//    const writer = std.io.fixedBufferStream(buffer).writer();
//    const reader = std.io.fixedBufferStream(buffer).reader();

//    // Serialize the struct
//    try serializeStruct(T, value, writer);

//    // Rewind the buffer to the start for deserialization
//    try std.io.fixedBufferStream(buffer).seekTo(0);

//    // Deserialize the struct back into the value
//    return try deserializeStruct(T, reader);
//}

//fn serializeStruct(comptime T: type, value: T, writer: anytype) !void {
//    const info = @typeInfo(T);
//    switch (info) {
//        .Struct => |s| {
//            inline for (s.fields) |field| {
//                const field_value = @field(value, field.name);
//                try serializeField(field_value, writer);
//            }
//        },
//        else => @compileError("Expected a struct"),
//    }
//}

//fn serializeField(value: anytype, writer: anytype) !void {
//    switch (@TypeOf(value)) {
//        u8 => try writer.writeByte(value),
//        u16 => try writer.writeIntLittle(u16, value),
//        u32 => try writer.writeIntLittle(u32, value),
//        u64 => try writer.writeIntLittle(u64, value),
//        else => @compileError("Unsupported field type"),
//    }
//}

//fn deserializeStruct(comptime T: type, reader: anytype) !T {
//    var result: T = undefined;
//    const info = @typeInfo(T);

//    switch (info) {
//        .Struct => |s| {
//            inline for (s.fields) |field| {
//                const FieldType = field.field_type;
//                const value = try deserializeField(FieldType, reader);
//                @field(result, field.name) = value;
//            }
//        },
//        else => @compileError("Expected a struct"),
//    }

//    return result;
//}

//fn deserializeField(comptime T: type, reader: anytype) !T {
//    return switch (T) {
//        u8 => try reader.readByte(),
//        u16 => try reader.readIntLittle(u16),
//        u32 => try reader.readIntLittle(u32),
//        u64 => try reader.readIntLittle(u64),
//        else => @compileError("Unsupported field type for deserialization"),
//    };
//}
