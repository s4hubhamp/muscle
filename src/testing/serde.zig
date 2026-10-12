const std = @import("std");
const muscle = @import("../muscle.zig");
const serde = muscle.common.serde;
const Row = muscle.common.BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE);

test "nullable encoding distinguishes NULL from zero false and empty values" {
    const cases = [_]struct { value: muscle.Value, data_type: muscle.DataType, bytes: []const u8 }{
        .{ .value = .null, .data_type = .int, .bytes = &.{0} },
        .{ .value = .{ .int = 0 }, .data_type = .int, .bytes = &.{ 1, 0, 0, 0, 0, 0, 0, 0, 0 } },
        .{ .value = .{ .real = 0 }, .data_type = .real, .bytes = &.{ 1, 0, 0, 0, 0, 0, 0, 0, 0 } },
        .{ .value = .{ .bool = false }, .data_type = .bool, .bytes = &.{ 1, 0 } },
        .{ .value = .{ .txt = "" }, .data_type = .{ .txt = 8 }, .bytes = &.{ 1, 0, 0 } },
        .{ .value = .{ .bin = "" }, .data_type = .{ .bin = 8 }, .bytes = &.{ 1, 0, 0 } },
        .{ .value = .{ .txt = "abc" }, .data_type = .{ .txt = 8 }, .bytes = &.{ 1, 3, 0, 'a', 'b', 'c' } },
    };
    for (cases) |case| {
        const allocated = try serde.serialize(case.value, std.testing.allocator);
        defer std.testing.allocator.free(allocated);
        try std.testing.expectEqualSlices(u8, case.bytes, allocated);
        var row: Row = .{};
        try serde.serialize_value(&row, case.value);
        try std.testing.expectEqualSlices(u8, case.bytes, row.const_slice());
        var offset: usize = 0;
        try std.testing.expectEqualDeep(case.value, try serde.read_value(row.const_slice(), &offset, case.data_type));
        try std.testing.expectEqual(case.bytes.len, offset);
    }
}

test "row decoding skips NULL without skipping the following column payload" {
    const fixture = try @import("helpers.zig").QueryFixture.init();
    defer fixture.deinit();
    const types = [_]muscle.DataType{ .int, .real, .bool, .{ .txt = 32 }, .{ .bin = 32 } };
    for (types) |data_type| {
        var table: muscle.Table = .{ .root = 0, .name = "records", .primary_key = "id", .columns = .empty, .indexes = &.{} };
        try table.columns.put(fixture.allocator(), "id", .{ .name = "id", .data_type = .int });
        try table.columns.put(fixture.allocator(), "nullable", .{ .name = "nullable", .data_type = data_type });
        try table.columns.put(fixture.allocator(), "tail", .{ .name = "tail", .data_type = .int });
        var row: Row = .{};
        for ([_]muscle.Value{ .{ .int = 1 }, .null, .{ .int = 42 } }) |value| try serde.serialize_value(&row, value);
        try std.testing.expectEqualDeep(muscle.Value.null, try serde.deserialize_value(&fixture.context, &table, row.const_slice(), "nullable"));
        try std.testing.expectEqualDeep(muscle.Value{ .int = 42 }, try serde.deserialize_value(&fixture.context, &table, row.const_slice(), "tail"));
    }
}

test "nullable decoding rejects missing markers invalid flags and truncated payloads" {
    const cases = [_]struct { bytes: []const u8, data_type: muscle.DataType }{
        .{ .bytes = &.{}, .data_type = .int },
        .{ .bytes = &.{2}, .data_type = .int },
        .{ .bytes = &.{ 1, 42 }, .data_type = .int },
        .{ .bytes = &.{1}, .data_type = .bool },
        .{ .bytes = &.{ 1, 2 }, .data_type = .bool },
        .{ .bytes = &.{ 1, 0 }, .data_type = .{ .txt = 32 } },
        .{ .bytes = &.{ 1, 3, 0, 'x' }, .data_type = .{ .bin = 32 } },
    };
    for (cases) |case| {
        var offset: usize = 0;
        try std.testing.expectError(error.CorruptedData, serde.read_value(case.bytes, &offset, case.data_type));
    }
    var offset: usize = 99;
    try std.testing.expectError(error.CorruptedData, serde.read_value(&.{0}, &offset, .int));
}

test "serializing near row capacity accounts for marker and leaves failures unchanged" {
    var row: Row = .{};
    row.len = row.capacity() - 2;
    @memset(row.slice(), 0xaa);
    try serde.serialize_value(&row, .{ .bool = false });
    try std.testing.expectEqual(row.capacity(), row.len);
    try std.testing.expectEqualSlices(u8, &.{ 1, 0 }, row.const_slice()[row.len - 2 ..]);
    try std.testing.expectError(error.RowTooBig, serde.serialize_value(&row, .null));
    try std.testing.expectEqual(row.capacity(), row.len);
    row.len = row.capacity() - 8;
    try std.testing.expectError(error.RowTooBig, serde.serialize_value(&row, .{ .int = 10 }));
    try std.testing.expectEqual(row.capacity() - 8, row.len);
    row.len = row.capacity() - 1;
    try serde.serialize_value(&row, .null);
    try std.testing.expectEqual(@as(u8, 0), row.const_slice()[row.len - 1]);
}

test "nullable key slices match serialized keys and real ordering remains numeric" {
    const cases = [_]struct { value: muscle.Value, data_type: muscle.DataType }{
        .{ .value = .{ .int = 42 }, .data_type = .int },
        .{ .value = .{ .real = -2.5 }, .data_type = .real },
        .{ .value = .{ .txt = "key" }, .data_type = .{ .txt = 8 } },
        .{ .value = .{ .bin = "" }, .data_type = .{ .bin = 8 } },
    };
    for (cases) |case| {
        var row: Row = .{};
        try serde.serialize_value(&row, case.value);
        const key_len = row.len;
        try serde.serialize_value(&row, .{ .int = 99 });
        const leaf = muscle.page_types.Cell.init(row.const_slice(), null);
        try std.testing.expectEqualSlices(u8, row.const_slice()[0..key_len], leaf.get_keys_slice(false, case.data_type));
        const internal = muscle.page_types.Cell.init(row.const_slice()[0..key_len], 1);
        try std.testing.expectEqualSlices(u8, row.const_slice()[0..key_len], internal.get_keys_slice(true, case.data_type));
    }
    const low = try serde.serialize(.{ .real = -10.5 }, std.testing.allocator);
    defer std.testing.allocator.free(low);
    const high = try serde.serialize(.{ .real = -2.5 }, std.testing.allocator);
    defer std.testing.allocator.free(high);
    try std.testing.expectEqual(std.math.Order.lt, serde.compare_serialized_bytes(.real, low, high));
}
