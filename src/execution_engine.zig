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

    const ExecuteQueryResults = union(enum) {
        SelectTableMetadataQueryResult: SelectTableMetadataQueryResult,
        SelectDatabaseMetadataResult: SelectDatabaseMetadataResult,
    };
    pub fn execute_query(self: *ExecutionEngine, query: Query) !?ExecuteQueryResults {
        var results: ?ExecuteQueryResults = null;
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
        var metadata_page = try self.pager.get_page(page.DBMetadataPage, 0);
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
                try self.insert(&metadata_page, tables, payload);
                //catch |err| {
                //    client_err = err;
                //    rollback_partially_done_updates = true;
                //};
            },
            Query.Delete => |payload| {
                is_update_query = true;
                self.delete(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    rollback_partially_done_updates = true;
                };
            },
            Query.Select => |payload| {
                self.select(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                };
            },
            Query.SelectTableMetadata => |payload| {
                results = self.select_table_metadata(&metadata_page, tables, payload) catch |err| {
                    client_err = err;
                    return null;
                };
            },
            Query.SelectDatabaseMetadata => {
                results = self.select_database_metadata(&metadata_page) catch |err| {
                    client_err = err;
                    return null;
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

        return results;
    }

    fn create_table(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: CreateTablePayload,
    ) !void {
        const table_name = payload.table_name;
        const columns = payload.columns;

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

        const root_page_number = try self.pager.alloc_free_page(metadata);
        // initialize root page
        const root_page = page.Page.init();
        try self.pager.update_page(root_page_number, &root_page);

        // append a new table entry
        try tables_list.append(muscle.Table{
            .root = root_page_number,
            .largest_rowid = 0, // row id will start from 1
            .name = table_name,
            .columns = columns,
            .indexes = &[0]muscle.Index{},
        });

        // update tables
        try metadata.set_tables(self.allocator, tables_list.items[0..]);
        // update metadata
        try self.pager.update_page(0, metadata);
    }

    fn drop_table(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
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
        metadata_page: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: InsertPayload,
    ) !void {
        // 1. find the root page number
        // 2. call the btree to insert

        var table: ?*muscle.Table = null;
        for (tables) |*t| {
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
        const cell_and_rowid = try serde.serialize_row(&buffer, table.?, table.?.largest_rowid, payload);
        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(&self.pager, metadata_page, self.allocator);
        try btree.insert(
            table.?.root,
            cell_and_rowid.cell,
        );

        // update metadata
        // when user provides smaller rowid's we will not to update largest rowid
        if (cell_and_rowid.rowid > table.?.largest_rowid) {
            table.?.largest_rowid = cell_and_rowid.rowid;
        }
        try metadata_page.set_tables(self.allocator, tables);
        try self.pager.update_page(0, metadata_page);
        // nocheckin
        //print(
        //    "first_free_page: {any} free_pages: {any} total_pages: {any}\n",
        //    .{ metadata_page.first_free_page, metadata_page.free_pages, metadata_page.total_pages },
        //);
    }

    fn delete(
        self: *ExecutionEngine,
        metadata_page: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: DeletePayload,
    ) !void {
        // 1. find the root page number
        // 2. call the btree to insert

        var table: ?*muscle.Table = null;
        for (tables) |*t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        // for now we only support deleting via rowID.
        assert(payload.key.len == 8);

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(&self.pager, metadata_page, self.allocator);
        try btree.delete(
            table.?.root,
            payload.key,
        );

        // update metadata
        try self.pager.update_page(0, metadata_page);
    }

    fn select(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: SelectPayload,
    ) !void {
        std.debug.print("\n\n*****************************************************************\n", .{});
        var table: ?muscle.Table = null;
        for (tables) |t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        var serial: usize = 1;
        var curr_page_number = table.?.root;

        // find the leftmost leaf node
        var curr_page = try self.pager.get_page(page.Page, curr_page_number);
        while (!curr_page.is_leaf()) {
            assert(curr_page.cell_at_slot(0).left_child != 0);
            curr_page_number = curr_page.cell_at_slot(0).left_child;
            curr_page = try self.pager.get_page(page.Page, curr_page_number);
        }

        // traverse using .right pointers and print rows
        while (true) {
            std.debug.print("------------------\npage_number: {}, content_size: {}, free_space: {}, num_slots={}, right_child={}, left={}, right={}\n", .{
                curr_page_number,
                curr_page.content_size,
                curr_page.free_space(),
                curr_page.num_slots,
                curr_page.right_child,
                curr_page.left,
                curr_page.right,
            });
            // there shouldn't be any free pages attached to btree
            assert(curr_page.free_space() != 0);

            for (0..curr_page.num_slots) |slot_index| {
                const cell = curr_page.cell_at_slot(@intCast(slot_index));
                print(" serial={}  id={d}", .{
                    serial,
                    std.mem.readInt(muscle.RowId, cell.get_keys_slice(false)[0..@sizeOf(usize)], .little),
                });
                serial += 1;

                var offset: usize = 8; // first 8 bytes are occupied by rowID
                for (table.?.columns) |column| {
                    // null value
                    if (cell.content[offset] == 0) {
                        print("  {s}={any}", .{ column.name, "NULL" });
                        continue;
                    }

                    switch (column.data_type) {
                        .BIN, .TEXT => {
                            const len = std.mem.readInt(usize, cell.content[offset..][0..@sizeOf(usize)], .little);
                            offset += @sizeOf(usize);
                            print("  {s}={s}", .{ column.name, cell.content[offset..][0..len] });
                            offset += len;
                        },
                        .INT => {
                            print("  {s}={}", .{
                                column.name,
                                std.mem.readInt(i64, cell.content[offset..][0..@sizeOf(i64)], .little),
                            });
                            offset += @sizeOf(i64);
                        },
                        .REAL => {
                            print("  {s}={}", .{
                                column.name,
                                @as(f64, @bitCast(std.mem.readInt(i64, cell.content[offset..][0..@sizeOf(i64)], .little))),
                            });
                            offset += @sizeOf(i64);
                        },
                        .BOOL => {
                            print(
                                "  {s}={any}",
                                .{ column.name, if (cell.content[offset] == 1) true else false },
                            );
                            offset += 1;
                        },
                    }
                }
                print("\n", .{});
            }

            if (curr_page.right == 0) break;
            curr_page_number = curr_page.right;
            curr_page = try self.pager.get_page(page.Page, curr_page_number);
        }

        print("\n\ndatabase metadata: total_pages: {any} free_pages:{any} first_free_page:{any}", .{
            metadata.total_pages, metadata.free_pages, metadata.first_free_page,
        });

        std.debug.print("\n\n*****************************************************************", .{});
    }

    fn select_table_metadata(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: SelectPayload,
    ) !ExecuteQueryResults {
        _ = metadata;
        var table: ?muscle.Table = null;
        for (tables) |t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        // use BFS and insert all the page info inside the hash table
        // also record total cells

        var result = SelectTableMetadataQueryResult{
            .root_page = table.?.root,
            .largest_rowid = table.?.largest_rowid,
            .btree_height = 0,
            .btree_leaf_cells = 0,
            .btree_internal_cells = 0,
            .btree_leaf_pages = 0,
            .btree_internal_pages = 0,
            .pages = std.AutoHashMap(muscle.PageNumber, SelectTableMetadataQueryResult.DBPageMetadata).init(self.allocator),
        };

        var first_page_in_level: ?muscle.PageNumber = table.?.root;
        var curr_page_number: muscle.PageNumber = undefined;
        var curr_page: page.Page = undefined;

        const collect_page_info = struct {
            fn f(map: *std.AutoHashMap(muscle.PageNumber, SelectTableMetadataQueryResult.DBPageMetadata), _page_number: muscle.PageNumber, _page: page.Page) !void {
                var page_info = SelectTableMetadataQueryResult.DBPageMetadata{
                    .page = _page_number,
                    .right_child = _page.right_child,
                    .content_size = _page.content_size,
                    .free_space = _page.free_space(),
                    .left = _page.left,
                    .right = _page.right,
                    .cells = std.ArrayList(SelectTableMetadataQueryResult.DBPageCellMetadata).init(map.allocator),
                };

                for (0.._page.num_slots) |slot| {
                    const cell = _page.cell_at_slot(@intCast(slot));

                    try page_info.cells.append(.{
                        .key = try map.allocator.dupe(u8, cell.get_keys_slice(!_page.is_leaf())),
                        .value = try map.allocator.dupe(u8, cell.content),
                        .size = cell.size,
                        .left_child = cell.left_child,
                    });
                }

                assert(map.contains(_page_number) == false);
                try map.put(_page_number, page_info);
            }
        }.f;

        while (first_page_in_level != null) {
            result.btree_height += 1;
            curr_page_number = first_page_in_level.?;
            curr_page = try self.pager.get_page(page.Page, curr_page_number);
            first_page_in_level = if (!curr_page.is_leaf()) curr_page.child(0) else null;

            while (curr_page_number != 0) {
                try collect_page_info(&result.pages, curr_page_number, curr_page);

                if (curr_page.is_leaf()) {
                    result.btree_leaf_cells += curr_page.num_slots;
                    result.btree_leaf_pages += 1;
                } else {
                    result.btree_internal_cells += curr_page.num_slots;
                    result.btree_internal_pages += 1;
                }

                curr_page_number = curr_page.right;
                curr_page = try self.pager.get_page(page.Page, curr_page_number);
            }
        }

        return ExecuteQueryResults{ .SelectTableMetadataQueryResult = result };
    }

    fn select_database_metadata(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
    ) !ExecuteQueryResults {
        var free_pages = try std.BoundedArray(muscle.PageNumber, 128).init(0);

        if (metadata.first_free_page > 0) {
            var curr_page_number = metadata.first_free_page;
            while (curr_page_number > 0) {
                // assert no duplicates
                for (free_pages.constSlice()) |i| {
                    assert(i != curr_page_number);
                }

                try free_pages.append(curr_page_number);
                const curr_page = try self.pager.get_page(page.FreePage, curr_page_number);
                curr_page_number = curr_page.next;
            }
        }

        print("\n--------------------------------- DATABASE METADATA ------------------------------\n", .{});
        print("Total pages: {any}\n", .{metadata.total_pages});
        print("Free pages: {any}\n", .{metadata.free_pages});
        print("First free page: {any}\n", .{metadata.first_free_page});

        print("\nFree pages: ", .{});
        for (free_pages.constSlice()) |page_number| {
            print("{d} -> ", .{page_number});
        }
        print("0\n", .{});
        print("\n-----------------------------------------------------------------------------------\n", .{});

        assert(metadata.free_pages == free_pages.len);

        return ExecuteQueryResults{ .SelectDatabaseMetadataResult = SelectDatabaseMetadataResult{
            .n_total_pages = metadata.total_pages,
            .n_free_pages = metadata.free_pages,
            .first_free_page = metadata.first_free_page,
            .free_pages = free_pages,
        } };
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

const DeletePayload = struct {
    table_name: []const u8,
    key: []const u8,
    // columns: []const muscle.Column,
    // limit: usize
};

pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
    Insert: InsertPayload,
    Select: SelectPayload,
    Delete: DeletePayload,
    SelectTableMetadata: SelectPayload,
    SelectDatabaseMetadata: void,
};

pub const SelectDatabaseMetadataResult = struct {
    n_total_pages: u32,
    n_free_pages: u32,
    first_free_page: u32,
    free_pages: std.BoundedArray(muscle.PageNumber, 128),
};

pub const SelectTableMetadataQueryResult = struct {
    root_page: muscle.PageNumber,
    largest_rowid: muscle.RowId,
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

    pub fn print(self: *const SelectTableMetadataQueryResult) void {
        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metadata    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});

        std.debug.print("Root Page:             {}\n", .{self.root_page});
        std.debug.print("Largest Rowid:         {}\n", .{self.largest_rowid});
        std.debug.print("Total Rows:            {}\n\n", .{self.btree_leaf_cells});
        std.debug.print("BTree Height:          {}\n", .{self.btree_height});
        std.debug.print("BTree Internal Pages:  {}\n", .{self.btree_internal_pages});
        std.debug.print("BTree Leaf Pages:      {}\n", .{self.btree_leaf_pages});
        std.debug.print("Btree Internal Cells:  {}\n", .{self.btree_internal_cells});

        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   End Metadata  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});
    }

    pub fn deinit(self: *SelectTableMetadataQueryResult) void {
        var allocator = self.pages.allocator;
        var iter = self.pages.valueIterator();
        while (iter.next()) |_page| {
            while (_page.cells.pop()) |cell| {
                allocator.free(cell.key);
                allocator.free(cell.value);
            }

            _page.cells.deinit();
        }

        self.pages.deinit();
    }
};
