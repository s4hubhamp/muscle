const std = @import("std");
const print = std.debug.print;
const muscle = @import("muscle");
const Pager = @import("./btree/pager.zig").Pager;
const page = @import("./btree/page.zig");

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

        //
        // The details that almost all the queries need are prepared here.
        // Tried to keep parsed tables inside the pager but to make sure it syncs
        // with current state of raw metadata page and through all rollbacks/commits
        // is pretty difficult.
        // Perhaps the correct thing to do is making the decoding of metadata page faster,
        // we never keep it in journal to keep the journal simpler.
        //
        const metadata_page = self.pager.get_metadata_page(); // .* makes copy
        const parsed = try metadata_page.parse_tables(self.allocator);
        const tables = parsed.value;
        defer {
            parsed.deinit();
        }

        switch (query) {
            Query.CreateTable => |payload| {
                is_update_query = true;
                self.create_table(metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.DropTable => |payload| {
                is_update_query = true;
                self.drop_table(metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.Insert => |payload| {
                is_update_query = true;
                self.insert(metadata_page, tables, payload) catch |err| {
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

    fn create_table(
        self: *ExecutionEngine,
        metadata: *const page.DBMetadataPage,
        tables: []muscle.Table,
        payload: CreateTablePayload,
    ) !void {
        const table_name = payload.table_name;
        const columns = payload.columns;

        var metadata_page_copy = metadata.*; // .* makes copy
        var tables_list = std.ArrayList(muscle.Table).init(self.allocator);
        defer {
            tables_list.deinit();
        }

        for (tables) |table| {
            if (std.mem.eql(u8, table.name, table_name)) {
                return error.DuplicateTableName;
            }
        }

        try tables_list.appendSlice(tables);

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
        try metadata_page_copy.set_tables(self.allocator, tables_list.items[0..]);
        // put updates into cache
        try self.pager.update_page(0, std.mem.toBytes(metadata_page_copy));
    }

    fn drop_table(
        self: *ExecutionEngine,
        metadata: *const page.DBMetadataPage,
        tables: []muscle.Table,
        payload: DropTablePayload,
    ) !void {
        _ = self;
        _ = metadata;
        _ = tables;
        _ = payload;
    }

    fn insert(
        self: *ExecutionEngine,
        metadata: *const page.DBMetadataPage,
        tables: []muscle.Table,
        payload: InsertPayload,
    ) !void {
        // 1. find the root page number
        // 2. call the btree to insert
        _ = self;
        _ = metadata;
        _ = tables;
        _ = payload;
        unreachable;
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

const InsertPayload = struct {
    table_name: []const u8,
    columns: []const muscle.Column,
    values: []const u32,
};

pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
    Insert: InsertPayload,
};
