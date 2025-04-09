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
        var pager = try Pager.init(database_file_path, allocator);

        // call rollback to sync if we had crashed earlier
        print("Calling Rollback after instance start.\n", .{});
        try pager.rollback();

        return ExecutionEngine{
            .allocator = allocator,
            .pager = pager,
        };
    }

    pub fn deinit(self: *ExecutionEngine) void {
        self.pager.deinit();
    }

    pub fn execute_query(self: *ExecutionEngine, query: Query) !void {
        var is_update_query = false;
        // for update queries, they can fail after updating some records
        var rollback_partially_done_updates = false;
        var client_err: ?anyerror = null;

        switch (query) {
            Query.CreateTable => |payload| {
                is_update_query = true;
                self.create_table(payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.DropTable => |payload| {
                is_update_query = true;
                self.drop_table(payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            else => @panic("not implemented"),
        }

        if (rollback_partially_done_updates) {
            print("Processing failed before final commit. Calling Rollback.\n", .{});
            try self.pager.rollback();
        } else if (is_update_query) {
            // at this point we may've done partial updates and they are succeeded. But
            // if we can still fail to do last commit.
            self.pager.commit(true) catch |err| {
                client_err = err;
                // journal might be in bad state but if it's not rollback will succeed
                // or else we will crash.
                try self.pager.rollback();
            };
        }

        if (client_err != null)
            print("client_err: {any}\n", .{client_err});
    }

    fn create_table(self: *ExecutionEngine, payload: CreateTablePayload) !void {
        const table_name = payload.table_name;
        const columns = payload.columns;

        var page_zero = self.pager.get_metadata_page().*; // .* makes copy
        const parsed = try page_zero.parse_tables(self.allocator);
        var tables_list = std.ArrayList(muscle.Table).init(self.allocator);

        defer {
            parsed.deinit();
            tables_list.deinit();
        }

        for (parsed.value) |table| {
            if (std.mem.eql(u8, table.name, table_name)) {
                return error.DuplicateTableName;
            }
        }

        try tables_list.appendSlice(parsed.value);

        const root_page_number = try self.pager.get_free_page();
        // append a new table entry
        try tables_list.append(muscle.Table{
            .root = root_page_number,
            .row_id = 1, // row id will start from 1
            .name = table_name,
            .columns = columns,
            .indexes = &[0]muscle.Index{},
        });

        // update tables
        try page_zero.set_tables(self.allocator, tables_list.items[0..]);
        // put updates into cache
        try self.pager.update_page(0, std.mem.toBytes(page_zero));
    }

    fn drop_table(self: *ExecutionEngine, payload: DropTablePayload) !void {
        const table_name = payload.table_name;

        var page_zero = self.pager.get_metadata_page().*; // makes a copy
        const parsed = try page_zero.parse_tables(self.allocator);
        var tables_list = std.ArrayList(muscle.Table).init(self.allocator);

        defer {
            parsed.deinit();
        }

        var index: ?usize = null;
        for (parsed.value, 0..) |table, i| {
            if (std.mem.eql(u8, table.name, table_name)) {
                index = i;
            }
        }

        if (index == null) {
            return error.TableNotFound;
        }

        if (index == 0) {
            try tables_list.appendSlice(parsed.value[1..]);
        } else {
            try tables_list.appendSlice(parsed.value[0..index.?]);
            try tables_list.appendSlice(parsed.value[index.? + 1 ..]);
        }

        // update tables
        try page_zero.set_tables(self.allocator, tables_list.items[0..]);
        // put updates into cache
        try self.pager.update_page(0, std.mem.toBytes(page_zero));
    }
};

const CreateTablePayload = struct {
    table_name: []const u8,
    columns: []const muscle.Column,
};

const DropTablePayload = struct {
    table_name: []const u8,
};
const DropIndexPayload = struct {};
pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
};
