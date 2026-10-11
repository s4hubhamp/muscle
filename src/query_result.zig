const std = @import("std");
const muscle = @import("muscle.zig");

pub const QueryResult = union(enum) {
    data: Data,
    err: Error,

    pub fn is_error_result(self: *const QueryResult) bool {
        return self.* == .err;
    }
};

pub const QueryResultV2 = union(enum) {
    data: ResultSet,
    err: Error,

    pub fn is_error_result(self: *const QueryResultV2) bool {
        return self.* == .err;
    }

    /// Number of returned rows, or zero for an error result.
    pub fn rowCount(self: *const QueryResultV2) usize {
        return switch (self.*) {
            .data => |data| data.rows.items.len,
            .err => 0,
        };
    }

    /// Number of result columns, including when no rows are returned.
    pub fn columnCount(self: *const QueryResultV2) usize {
        return switch (self.*) {
            .data => |data| data.columns.len,
            .err => 0,
        };
    }

    /// Total encoded row bytes, excluding schema and allocation overhead.
    /// Returns zero for an error result.
    pub fn size(self: *const QueryResultV2) usize {
        return switch (self.*) {
            .data => |data| blk: {
                var bytes: usize = 0;
                for (data.rows.items) |row| bytes += row.len;
                break :blk bytes;
            },
            .err => 0,
        };
    }

    /// Print a table to stderr, using the same output as the `{f}` formatter.
    pub fn print(self: *const QueryResultV2) void {
        std.debug.print("{f}", .{self.*});
    }

    pub fn format(self: QueryResultV2, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        self.write_table(writer, arena.allocator()) catch |err| {
            if (err == error.WriteFailed) return error.WriteFailed;
            try writer.print("Error printing results: {s}\n", .{@errorName(err)});
        };
    }

    fn write_table(self: *const QueryResultV2, writer: *std.Io.Writer, allocator: std.mem.Allocator) !void {
        const result = switch (self.*) {
            .err => |err| return writer.print("Error ({s}): {s}\n", .{ @errorName(err.code), err.message }),
            .data => |data| data,
        };
        if (result.columns.len == 0) {
            if (result.rows.items.len != 0) return error.CorruptedData;
            return writer.writeAll("(0 rows)\n");
        }

        const widths = try allocator.alloc(usize, result.columns.len);
        const headers = try allocator.alloc([]const u8, result.columns.len);
        for (result.columns, headers, widths) |column, *header, *width| {
            header.* = try table_text(allocator, column.name);
            width.* = header.len;
        }
        const cell_count = std.math.mul(usize, result.columns.len, result.rows.items.len) catch return error.OutOfMemory;
        const cells = try allocator.alloc([]const u8, cell_count);
        for (result.rows.items, 0..) |row, row_index| {
            var offset: usize = 0;
            for (result.columns, widths, 0..) |column, *width, column_index| {
                const value = try read_result_value(row, &offset, column.data_type);
                const text = switch (value) {
                    .int => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
                    .real => |number| try std.fmt.allocPrint(allocator, "{d}", .{number}),
                    .bool => |boolean| if (boolean) "true" else "false",
                    .txt => |text| try table_text(allocator, text),
                    .bin => |bytes| try std.fmt.allocPrint(allocator, "0x{x}", .{bytes}),
                    .null => unreachable,
                };
                cells[row_index * result.columns.len + column_index] = text;
                width.* = @max(width.*, text.len);
            }
            if (offset != row.len) return error.CorruptedData;
        }

        try table_border(writer, widths);
        try table_row(writer, headers, widths);
        try table_border(writer, widths);
        for (0..result.rows.items.len) |i| {
            try table_row(writer, cells[i * result.columns.len ..][0..result.columns.len], widths);
        }
        if (result.rows.items.len != 0) try table_border(writer, widths);
        try writer.print("({d} {s})\n", .{ result.rows.items.len, if (result.rows.items.len == 1) "row" else "rows" });
    }
};

fn table_border(writer: *std.Io.Writer, widths: []const usize) !void {
    try writer.writeByte('+');
    for (widths) |width| {
        try writer.splatByteAll('-', width + 2);
        try writer.writeByte('+');
    }
    try writer.writeByte('\n');
}

fn table_row(writer: *std.Io.Writer, cells: []const []const u8, widths: []const usize) !void {
    try writer.writeByte('|');
    for (cells, widths) |cell, width| {
        try writer.writeByte(' ');
        try writer.writeAll(cell);
        try writer.splatByteAll(' ', width - cell.len + 1);
        try writer.writeByte('|');
    }
    try writer.writeByte('\n');
}

// Keep newlines, control characters and delimiters inside a single table cell.
fn table_text(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    for (bytes) |byte| {
        switch (byte) {
            '\n' => try output.writer.writeAll("\\n"),
            '\r' => try output.writer.writeAll("\\r"),
            '\t' => try output.writer.writeAll("\\t"),
            '\\' => try output.writer.writeAll("\\\\"),
            '|' => try output.writer.writeAll("\\|"),
            0...8, 11...12, 14...31, 127 => try output.writer.print("\\x{x:0>2}", .{byte}),
            else => try output.writer.writeByte(byte),
        }
    }
    return try output.toOwnedSlice();
}

fn read_result_bytes(row: []const u8, offset: *usize, len: usize) ![]const u8 {
    if (offset.* > row.len or row.len - offset.* < len) return error.CorruptedData;
    const bytes = row[offset.*..][0..len];
    offset.* += len;
    return bytes;
}

fn read_result_value(row: []const u8, offset: *usize, data_type: muscle.DataType) !muscle.Value {
    return switch (data_type) {
        .int => .{ .int = std.mem.readInt(i64, (try read_result_bytes(row, offset, 8))[0..8], .little) },
        .real => .{ .real = @bitCast(std.mem.readInt(u64, (try read_result_bytes(row, offset, 8))[0..8], .little)) },
        .bool => .{ .bool = (try read_result_bytes(row, offset, 1))[0] != 0 },
        .txt, .bin => blk: {
            const len = std.mem.readInt(u16, (try read_result_bytes(row, offset, 2))[0..2], .little);
            const bytes = try read_result_bytes(row, offset, len);
            break :blk if (data_type == .txt) .{ .txt = bytes } else .{ .bin = bytes };
        },
    };
}

pub const ResultColumn = struct {
    name: []const u8,
    data_type: muscle.DataType,
};

pub const ResultSet = struct {
    // Columns follow the same order as the values encoded in each row.
    columns: []const ResultColumn = &.{},
    rows: std.ArrayList(muscle.page_types.EncodedCellContent) = .empty,
};

pub const Error = struct {
    code: anyerror,
    message: []const u8,
};

pub const InsertResult = struct {
    rows_created: u32,
};

pub const UpdateResult = struct {
    rows_affected: u32,
};

pub const SelectTableMetadataResult = struct {
    root_page: muscle.PageNumber,
    table_columns: std.ArrayList(muscle.Column),
    btree_height: u16,

    btree_leaf_cells: u32,
    btree_internal_cells: u32,
    btree_leaf_pages: u16,
    btree_internal_pages: u16,

    pages: std.AutoHashMap(muscle.PageNumber, DBPageMetadata),

    pub const DBPageCellMetadata = struct {
        key: []u8,
        value: []u8,
        size: u16,
        left_child: muscle.PageNumber,
    };

    pub const DBPageMetadata = struct {
        page: muscle.PageNumber,
        right_child: muscle.PageNumber,
        content_size: u16,
        free_space: u16,
        left: muscle.PageNumber,
        right: muscle.PageNumber,
        cells: std.ArrayList(DBPageCellMetadata),
    };

    pub fn print(self: *const SelectTableMetadataResult) void {
        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metadata    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});

        std.debug.print("Root Page:             {}\n", .{self.root_page});
        std.debug.print("Total Rows:            {}\n\n", .{self.btree_leaf_cells});
        std.debug.print("BTree Height:          {}\n", .{self.btree_height});
        std.debug.print("BTree Internal Pages:  {}\n", .{self.btree_internal_pages});
        std.debug.print("BTree Leaf Pages:      {}\n", .{self.btree_leaf_pages});
        std.debug.print("Btree Internal Cells:  {}\n", .{self.btree_internal_cells});

        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   End Metadata  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});
    }
};

pub const SelectDatabaseMetadataResult = struct {
    n_total_pages: u32,
    n_free_pages: u32,
    first_free_page: u32,
    free_pages: muscle.common.BoundedArray(muscle.PageNumber, 128),
};

pub const SelectResult = struct {
    columns: std.ArrayList(muscle.Column),
    rows: std.ArrayList(std.ArrayList(u8)), // @Todo update to use EncodedCellContent instead of ArrayList(u8)
};

pub const Data = union(enum) {
    insert: InsertResult,
    update: UpdateResult,
    select: SelectResult,

    //
    select_table_info: SelectTableMetadataResult,
    select_database_info: SelectDatabaseMetadataResult,

    // void result type when we don't have any data
    __void,
};

test "QueryResultV2 prints decoded values in aligned columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var row = muscle.common.BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE){};
    const values = [_]muscle.Value{
        .{ .int = -42 },      .{ .real = -2.5 },        .{ .bool = false },
        .{ .txt = "A\nB|C" }, .{ .bin = &.{ 0, 255 } },
    };
    for (values) |value| try muscle.common.serde.serialize_value(&row, value);
    var result = QueryResultV2{ .data = .{ .columns = &.{
        .{ .name = "id", .data_type = .int },
        .{ .name = "score", .data_type = .real },
        .{ .name = "active", .data_type = .bool },
        .{ .name = "name", .data_type = .{ .txt = 32 } },
        .{ .name = "blob", .data_type = .{ .bin = 32 } },
    } } };
    try result.data.rows.append(allocator, row.const_slice());

    var buffer: [1024]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try result.write_table(&writer, allocator);
    const expected =
        \\+-----+-------+--------+---------+--------+
        \\| id  | score | active | name    | blob   |
        \\+-----+-------+--------+---------+--------+
        \\| -42 | -2.5  | false  | A\nB\|C | 0x00ff |
        \\+-----+-------+--------+---------+--------+
        \\(1 row)
        \\
    ;
    try std.testing.expectEqualStrings(expected, writer.buffered());

    // The public formatter uses the same representation as print().
    writer = .fixed(&buffer);
    try writer.print("{f}", .{result});
    try std.testing.expectEqualStrings(expected, writer.buffered());
}

test "QueryResultV2 prints empty results and errors and rejects malformed rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var buffer: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    var result = QueryResultV2{ .data = .{} };
    try result.write_table(&writer, allocator);
    try std.testing.expectEqualStrings("(0 rows)\n", writer.buffered());

    result.data.columns = &.{.{ .name = "id", .data_type = .int }};
    writer = .fixed(&buffer);
    try result.write_table(&writer, allocator);
    try std.testing.expectEqualStrings("+----+\n| id |\n+----+\n(0 rows)\n", writer.buffered());

    // Fixed-size values and length-prefixed values both require complete bytes.
    const malformed_rows = [_][]const u8{ &.{1}, &.{ 5, 0, 'a' }, &.{5} };
    for (malformed_rows, 0..) |row, i| {
        result.data.columns = if (i == 0) &.{.{ .name = "id", .data_type = .int }} else &.{.{ .name = "name", .data_type = .{ .txt = 32 } }};
        result.data.rows.clearRetainingCapacity();
        try result.data.rows.append(allocator, row);
        writer = .fixed(&buffer);
        try std.testing.expectError(error.CorruptedData, result.write_table(&writer, allocator));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    }
    writer = .fixed(&buffer);
    try result.format(&writer);
    try std.testing.expectEqualStrings("Error printing results: CorruptedData\n", writer.buffered());

    result = .{ .err = .{ .code = error.ColumnDoesNotExist, .message = "Missing column id" } };
    writer = .fixed(&buffer);
    try result.write_table(&writer, allocator);
    try std.testing.expectEqualStrings("Error (ColumnDoesNotExist): Missing column id\n", writer.buffered());
}

test "QueryResultV2 sizes columns using every row" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var result = QueryResultV2{ .data = .{ .columns = &.{.{ .name = "id", .data_type = .int }} } };
    for ([_]i64{ 1, -1234 }) |number| {
        try result.data.rows.append(allocator, try muscle.common.serde.serialize(.{ .int = number }, allocator));
    }
    var buffer: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try result.write_table(&writer, allocator);
    try std.testing.expectEqualStrings(
        \\+-------+
        \\| id    |
        \\+-------+
        \\| 1     |
        \\| -1234 |
        \\+-------+
        \\(2 rows)
        \\
    , writer.buffered());
}
