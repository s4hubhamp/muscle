// Execution engine is responsible to run query and return results
// It's job is to understand the query and choose most optimal way to calculate results
pub const ExecutionEngine = struct {
    allocator: std.mem.Allocator,
    pager: Pager,

    pub fn init(allocator: std.mem.Allocator, database_file_path: []const u8) !ExecutionEngine {
        return ExecutionEngine{
            .allocator = allocator,
            .pager = try Pager.init(database_file_path, allocator),
        };
    }

    pub fn create_table(
        self: *ExecutionEngine,
        table_name: []const u8,
        columns: []muscle.Column,
    ) !void {
        var page_zero = self.pager.get_metadata_page();
        const parsed = try page_zero.parse_tables(self.allocator);
        var tables_list = std.ArrayList(muscle.Table).init(self.allocator);

        defer {
            parsed.deinit();
            tables_list.deinit();
        }

        for (parsed.value) |table| {
            if (std.mem.eql(u8, table.name, table_name)) {
                return error.DuplicateTable;
            }
        }

        try tables_list.appendSlice(parsed.value);

        // append a new table entry
        try tables_list.append(muscle.Table{
            .root = self.pager.get_free_page(),
            .row_id = 1, // row id will start from 1
            .name = table_name,
            .columns = columns,
            .indexes = &[0]muscle.Index{},
        });

        // update tables
        try page_zero.set_tables(self.allocator, tables_list.items[0..]);
    }
};

pub const QueryType = enum {
    CreateTable,
    DropTable,
    DropIndex,
};

const std = @import("std");
const print = std.debug.print;
const muscle = @import("././muscle.zig");
const Pager = @import("./btree/pager.zig").Pager;
const ParsedDBMetadata = @import("./btree/page.zig").ParsedDBMetadata;
