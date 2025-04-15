const std = @import("std");
const muscle = @import("muscle");
const Pager = @import("./btree/pager.zig").Pager;
const page = @import("./btree/page.zig");
const BTree = @import("./btree/btree.zig").BTree;
const serde = @import("./serialize_deserialize.zig");

const print = std.debug.print;
const assert = std.debug.assert;

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

        return ExecutionEngine{ .allocator = allocator, .pager = pager };
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
        const metadata_page = try self.pager.get_page(page.DBMetadataPage, 0);
        const parsed = try metadata_page.parse_tables(self.allocator);
        const tables = parsed.value;
        defer {
            parsed.deinit();
        }

        switch (query) {
            Query.CreateTable => |payload| {
                is_update_query = true;
                self.create_table(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.DropTable => |payload| {
                is_update_query = true;
                self.drop_table(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.Insert => |payload| {
                is_update_query = true;
                self.insert(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.Select => |payload| {
                self.select(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
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

        const root_page_number = try self.pager.alloc_free_page();
        // update root page
        const root_page = page.Page.init();
        try self.pager.update_page(root_page_number, &root_page);

        // append a new table entry
        try tables_list.append(muscle.Table{
            .root = root_page_number,
            .last_insert_rowid = 0, // row id will start from 1
            .name = table_name,
            .columns = columns,
            .indexes = &[0]muscle.Index{},
        });

        // update tables
        try metadata_page_copy.set_tables(self.allocator, tables_list.items[0..]);
        // put updates into cache
        try self.pager.update_page(0, &metadata_page_copy);
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

    // TODO
    // const CompareFn = fn (key: []u8, val: []u8) union(enum) { equal, greater, less };

    fn insert(
        self: *ExecutionEngine,
        metadata_page: *const page.DBMetadataPage,
        tables: []muscle.Table,
        payload: InsertPayload,
    ) !void {
        // 1. find the root page number
        // 2. call the btree to insert

        var table: ?muscle.Table = null;
        for (tables) |t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        // the validation against schema should happen here.
        //
        // the validation for uniqueness will happen inside btree.
        // Let's take two scenarios.
        // 1. Single column index (unique and non unique)
        // 2. Multi column index  (unique and non unique)
        //
        // Let's say we have an unique index on `colum 1` then it means that our index_col_1
        // should not allow duplicate value for column 1.
        // I think we have to first scan the index to check if key already exists.
        // If it does not exist then only we will call btree.insert on a table.
        //
        // for now we don't have any indexes, so we will not do any validation.
        //

        // If the passed payload size is greater than max content that a single page can hold
        // this will overflow.
        // so the max size of a single row for now is equal to `page.Page.CONTENT_MAX_SIZE`
        // This is important because our btree.balance function operates on only one overflow cell as
        // argument.
        var buffer = try std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE).init(0);

        // serialize row
        const serialized = try serde.serialize_row(&buffer, table.?, table.?.last_insert_rowid + 1, payload);

        var btree = BTree.init(&self.pager, self.allocator);
        try btree.insert(
            table.?.root,
            serialized.rowid_slice,
            serialized.cell,
        );

        // update metadata
        table.?.last_insert_rowid += 1;
        var updated_metadata = metadata_page.*;
        try updated_metadata.set_tables(self.allocator, tables);
        try self.pager.update_page(0, &updated_metadata);
    }

    fn select(
        self: *ExecutionEngine,
        _: *const page.DBMetadataPage,
        tables: []muscle.Table,
        payload: SelectPayload,
    ) !void {
        var table: ?muscle.Table = null;
        for (tables) |t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        // this will be to find the leftmost node and then just traverse the leaf node's
        // using .next pointers
        print("{any}\n", .{self.pager.get_page(page.Page, table.?.root)});
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

pub const InsertPayload = struct {
    table_name: []const u8,
    columns: []const muscle.Column,
    values: []const muscle.Value,
};

const SelectPayload = struct {
    table_name: []const u8,
    // columns: []const muscle.Column,
    // limit: usize
};

pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
    Insert: InsertPayload,
    Select: SelectPayload,
};
