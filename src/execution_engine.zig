const std = @import("std");
const print = std.debug.print;
const muscle = @import("muscle");
const Pager = @import("./btree/pager.zig").Pager;
const ParsedDBMetadata = @import("./btree/page.zig").ParsedDBMetadata;

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

    pub fn execute_query(self: *ExecutionEngine, query: Query) !void {
        var commit = false;
        switch (query) {
            Query.CreateTable => |payload| {
                try self.create_table(payload);
                commit = true;
            },
            else => @panic("not implemented"),
        }

        if (commit) {
            // commit. Failure here means that rollback is also failed
            // we don't want to catch the failure in rollback
            try self.pager.commit();
        }
    }

    fn create_table(self: *ExecutionEngine, payload: CreateTablePayload) !void {
        const table_name = payload.table_name;
        const columns = payload.columns;

        var page_zero = try self.pager.get_metadata_page();
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
            .root = try self.pager.get_free_page(),
            .row_id = 1, // row id will start from 1
            .name = table_name,
            .columns = columns,
            .indexes = &[0]muscle.Index{},
        });

        // update tables
        try page_zero.set_tables(self.allocator, tables_list.items[0..]);
    }
};

const CreateTablePayload = struct {
    table_name: []const u8,
    columns: []const muscle.Column,
};

const DropTablePayload = struct {};
const DropIndexPayload = struct {};
pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
};
