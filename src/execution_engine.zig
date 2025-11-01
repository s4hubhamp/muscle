const std = @import("std");
const muscle = @import("muscle");

const errors = @import("./errors.zig");
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

    pub fn execute_query(self: *ExecutionEngine, query: Query) !QueryResult {
        var is_update_query = false;
        var should_rollback = false;

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

        const results = switch (query) {
            Query.CreateTable => |payload| {
                is_update_query = true;
                return self.create_table(&metadata_page, tables, payload) catch |err| {
                    should_rollback = true;
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.DropTable => |payload| {
                is_update_query = true;
                return self.drop_table(&metadata_page, tables, payload) catch |err| {
                    should_rollback = true;
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.Insert => |payload| {
                is_update_query = true;
                return self.insert(&metadata_page, tables, payload) catch |err| {
                    should_rollback = true;
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.Update => |payload| {
                is_update_query = true;
                return self.update(&metadata_page, tables, payload) catch |err| {
                    should_rollback = true;
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.Delete => |payload| {
                is_update_query = true;
                return self.delete(&metadata_page, tables, payload) catch |err| {
                    should_rollback = true;
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.Select => |payload| {
                return self.select(&metadata_page, tables, payload) catch |err| {
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.SelectTableMetadata => |payload| {
                return self.select_table_metadata(&metadata_page, tables, payload) catch |err| {
                    return try maybe_create_client_error_response(err);
                };
            },
            Query.SelectDatabaseMetadata => {
                return self.select_database_metadata(&metadata_page) catch |err| {
                    return try maybe_create_client_error_response(err);
                };
            },
            else => @panic("not implemented"),
        };

        // Handle rollback/commit logic
        if (should_rollback) {
            print("Processing failed. Calling Rollback.\n", .{});
            self.pager.rollback() catch |rollback_err| {
                // If rollback fails, this is a critical system error
                print("CRITICAL FAILURE: Rollback failed: {any}\n", .{rollback_err});
                //@panic("Database in inconsistent state - rollback failed");
                return rollback_err;
            };
        } else if (is_update_query) {
            self.pager.commit(true) catch |commit_err| {
                print("CRITICAL FAILURE: commit failed. {any}\n", .{commit_err});
                return commit_err;
            };
        }

        return results;
    }

    // gracefully resolves client errors and creates error QueryResult
    fn maybe_create_client_error_response(err: anyerror) !QueryResult {
        const classification = errors.classify_error(err);
        switch (classification) {
            .Client => {
                return QueryResult.error_result(err, @errorName(err));
            },
            .System => {
                return err;
            },
        }
    }

    fn create_table(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: CreateTablePayload,
    ) !QueryResult {
        var tables_list = try std.ArrayList(muscle.Table).initCapacity(self.allocator, tables.len + 1);
        var columns = try std.ArrayList(muscle.Column).initCapacity(self.allocator, payload.columns.len + 1);
        defer {
            tables_list.deinit();
            columns.deinit();
        }

        for (tables) |table| {
            if (std.mem.eql(u8, table.name, payload.table_name)) {
                return error.DuplicateTableName;
            }
        }

        try tables_list.appendSlice(tables);

        // check for duplicate column names
        for (payload.columns[0 .. payload.columns.len - 1], 0..) |*col, i| {
            for (payload.columns[i + 1 ..]) |*col2| {
                if (std.mem.eql(u8, col.name, col2.name)) return error.DuplicateColumnName;
            }
        }

        for (payload.columns, 0..) |*c, i| {
            var column = c.*;

            if (column.auto_increment and column.data_type != .INT) {
                return error.AutoIncrementColumnMustBeInteger;
            }

            if (i == payload.primary_key_column_index) {
                switch (column.data_type) {
                    // bools can't be primary key
                    .BOOL => return error.BadPrimaryKeyType,
                    // for text and binaries we have a limit on length
                    .TEXT, .BIN => |len| {
                        // for text we store len as first param
                        if (len > page.INTERNAL_CELL_CONTENT_SIZE_LIMIT - @sizeOf(usize)) {
                            return error.PrimaryKeyMaxLengthExceeded;
                        }
                    },
                    else => {},
                }

                // primary key should always be unique
                column.unique = true;
                column.not_null = true;

                // if data_type is integer then it's basically alias to default primary key so we will enable all the defaults
                if (column.data_type == .INT) {
                    column.auto_increment = true;
                    column.max_int_value = 0;
                }

                // primary key column gets stored at beginning
                try columns.insert(0, column);
            } else {
                // @Perf Can we reorder columns for efficient operations?
                try columns.append(column);
            }
        }

        // create a default primary key column if not provided
        if (payload.primary_key_column_index == null) {
            const DEFAULT_PRIMARY_KEY_COLUMN_NAME = "Row_Id";
            const DEFAULT_PRIMARY_KEY_COLUMN_TYPE = .INT;
            try columns.insert(0, muscle.Column{
                .name = DEFAULT_PRIMARY_KEY_COLUMN_NAME,
                .data_type = DEFAULT_PRIMARY_KEY_COLUMN_TYPE,
                .auto_increment = true,
                .not_null = true,
                .unique = true,
                .max_int_value = 0,
            });
        }

        const root_page_number = try self.pager.alloc_free_page(metadata);
        // initialize root page
        const root_page = page.Page.init();
        try self.pager.update_page(root_page_number, &root_page);

        const table = muscle.Table{
            .root = root_page_number,
            .name = payload.table_name,
            .columns = columns.items,
            .indexes = &[0]muscle.Index{},
        };

        // append a new table entry
        try tables_list.append(table);

        // update tables
        try metadata.set_tables(self.allocator, tables_list.items[0..]);
        // update metadata
        try self.pager.update_page(0, metadata);

        return QueryResult.success_result(.{ .__void = {} });
    }

    fn drop_table(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: DropTablePayload,
    ) !QueryResult {
        _ = self;
        _ = metadata;
        _ = tables;
        _ = payload;

        unreachable;
    }

    fn insert(
        self: *ExecutionEngine,
        metadata_page: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: InsertPayload,
    ) !QueryResult {
        var table: ?*muscle.Table = null;
        for (tables) |*t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        const find_value = struct {
            fn f(
                column_name: []const u8,
                values: []const InsertPayload.Value,
            ) ?*const InsertPayload.Value {
                for (values) |*val| {
                    if (std.mem.eql(u8, val.column_name, column_name)) return val;
                }

                return null;
            }
        }.f;

        const find_column = struct {
            fn f(
                columns: []const muscle.Column,
                column_name: []const u8,
            ) bool {
                for (columns) |*col| {
                    if (std.mem.eql(u8, col.name, column_name)) return true;
                }

                return false;
            }
        }.f;

        // check for duplicate columns and whether columns even exists
        if (payload.values.len > 1) {
            for (payload.values, 0..) |*v1, i| {
                if (!find_column(table.?.columns, v1.column_name))
                    return error.ColumnDoesNotExist;
                for (payload.values[i + 1 ..]) |*v2| {
                    if (std.mem.eql(u8, v1.column_name, v2.column_name))
                        return error.DuplicateColumns;
                }
            }
        }

        // If the passed payload size is greater than max content that a single page can hold
        // this will overflow.
        // so the max size of a single row for now is equal to `page.Page.CONTENT_MAX_SIZE`
        var buffer = try std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE).init(0);
        var primary_key_bytes: []const u8 = undefined;
        var primary_key_type: muscle.DataType = undefined;

        // for each column find value
        for (table.?.columns, 0..) |*column, i| {
            const value = find_value(column.name, payload.values);
            var final_value_to_serialize: muscle.Value = .{ .NULL = {} };

            // if value is not provided or it's provided and null
            if (value == null or value.?.value == .NULL) {
                if (column.auto_increment) {
                    // increment value and serialize
                    column.max_int_value += 1;
                    final_value_to_serialize = muscle.Value{ .INT = column.max_int_value };
                } else if (column.not_null) {
                    return error.MissingValue;
                } else {
                    // use default value on column definition and serialize
                    // @Todo
                    unreachable;
                }
            } else {
                final_value_to_serialize = value.?.value;

                if (column.auto_increment) {
                    // here we have auto_increment but we are still getting value
                    // need to adjust max value if current value is bigger
                    if (column.max_int_value < final_value_to_serialize.INT) {
                        column.max_int_value = final_value_to_serialize.INT;
                    }
                }

                // validate the type of value
                switch (column.data_type) {
                    .INT => if (final_value_to_serialize != .INT and final_value_to_serialize != .NULL)
                        return error.TypeMismatch,
                    .REAL => if (final_value_to_serialize != .REAL and final_value_to_serialize != .NULL)
                        return error.TypeMismatch,
                    .BOOL => if (final_value_to_serialize != .BOOL and final_value_to_serialize != .NULL)
                        return error.TypeMismatch,
                    .TEXT => |len| switch (final_value_to_serialize) {
                        .TEXT => |text| {
                            if (text.len > len) return error.TextTooLong;
                        },
                        .NULL => {},
                        else => return error.TypeMismatch,
                    },
                    .BIN => |len| switch (final_value_to_serialize) {
                        .BIN => |bin| {
                            if (bin.len > len) return error.BinaryTooLarge;
                        },
                        .NULL => {},
                        else => return error.TypeMismatch,
                    },
                }
            }

            serde.serailize_value(&buffer, final_value_to_serialize) catch {
                return error.RowTooBig;
            };

            // first column is always the primary key
            if (i == 0) {
                primary_key_bytes = buffer.constSlice();
                primary_key_type = column.data_type;
                if (primary_key_bytes.len > page.INTERNAL_CELL_CONTENT_SIZE_LIMIT) {
                    return error.KeyTooLong;
                }
            }
        }

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(
            &self.pager,
            metadata_page,
            table.?.root,
            primary_key_type,
            self.allocator,
        );
        try btree.insert(primary_key_bytes, buffer.constSlice());

        // update metadata
        try metadata_page.set_tables(self.allocator, tables);
        try self.pager.update_page(0, metadata_page);

        return QueryResult.success_result(.{ .__void = {} });
    }

    fn update(
        self: *ExecutionEngine,
        metadata_page: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: UpdatePayload,
    ) !QueryResult {
        _ = self;
        _ = metadata_page;
        _ = tables;
        _ = payload;

        unreachable;
    }

    fn delete(
        self: *ExecutionEngine,
        metadata_page: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: DeletePayload,
    ) !QueryResult {
        var table: ?*muscle.Table = null;
        for (tables) |*t| {
            if (std.mem.eql(u8, t.name, payload.table_name)) {
                table = t;
            }
        }

        if (table == null) {
            return error.TableNotFound;
        }

        if (@intFromEnum(table.?.columns[0].data_type) != @intFromEnum(payload.key)) {
            return error.TypeMismatch;
        }

        // primary key has size limit
        var buffer = try std.BoundedArray(u8, page.Page.CONTENT_MAX_SIZE).init(0);
        try serde.serailize_value(&buffer, payload.key);

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(
            &self.pager,
            metadata_page,
            table.?.root,
            table.?.columns[0].data_type,
            self.allocator,
        );
        try btree.delete(buffer.constSlice());

        // update metadata
        try self.pager.update_page(0, metadata_page);

        return QueryResult.success_result(.{ .__void = {} });
    }

    fn select(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: SelectPayload,
    ) !QueryResult {
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

        std.debug.print("\n\n*****************************************************************\n", .{});

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
                print(" serial={} size={}", .{ serial, cell.size + @sizeOf(page.Page.SlotArrayEntry) });
                serial += 1;

                var offset: usize = 0;
                for (table.?.columns) |column| {
                    switch (column.data_type) {
                        .BIN, .TEXT => {
                            const len = std.mem.readInt(usize, cell.content[offset..][0..@sizeOf(usize)], .little);
                            offset += @sizeOf(usize);
                            if (len > 10) {
                                print("  {s}={s}...({d})", .{ column.name, cell.content[offset..][0..10], len });
                            } else {
                                print("  {s}={s}", .{ column.name, cell.content[offset..][0..len] });
                            }

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

        return QueryResult.success_result(.{ .__void = {} });
    }

    fn select_table_metadata(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
        tables: []muscle.Table,
        payload: SelectPayload,
    ) !QueryResult {
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

        var result = SelectTableMetadataResult{
            .root_page = table.?.root,
            .btree_height = 0,
            .btree_leaf_cells = 0,
            .btree_internal_cells = 0,
            .btree_leaf_pages = 0,
            .btree_internal_pages = 0,
            .table_columns = try std.ArrayList(muscle.Column).initCapacity(self.allocator, table.?.columns.len),
            .pages = std.AutoHashMap(muscle.PageNumber, SelectTableMetadataResult.DBPageMetadata).init(self.allocator),
        };

        // copy columns
        try result.table_columns.appendSlice(table.?.columns);

        const primary_key_data_type = table.?.columns[0].data_type;
        var first_page_in_level: ?muscle.PageNumber = table.?.root;
        var curr_page_number: muscle.PageNumber = undefined;
        var curr_page: page.Page = undefined;

        const collect_page_info = struct {
            fn f(
                map: *std.AutoHashMap(muscle.PageNumber, SelectTableMetadataResult.DBPageMetadata),
                _page_number: muscle.PageNumber,
                _page: page.Page,
                _primary_key_data_type: muscle.DataType,
            ) !void {
                var page_info = SelectTableMetadataResult.DBPageMetadata{
                    .page = _page_number,
                    .right_child = _page.right_child,
                    .content_size = _page.content_size,
                    .free_space = _page.free_space(),
                    .left = _page.left,
                    .right = _page.right,
                    .cells = std.ArrayList(SelectTableMetadataResult.DBPageCellMetadata).init(map.allocator),
                };

                for (0.._page.num_slots) |slot| {
                    const cell = _page.cell_at_slot(@intCast(slot));

                    try page_info.cells.append(.{
                        .key = try map.allocator.dupe(u8, cell.get_keys_slice(
                            !_page.is_leaf(),
                            _primary_key_data_type,
                        )),
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
            first_page_in_level =
                if (!curr_page.is_leaf()) curr_page.child_at_slot(0) else null;

            while (curr_page_number != 0) {
                try collect_page_info(&result.pages, curr_page_number, curr_page, primary_key_data_type);

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

        return QueryResult.success_result(.{ .SelectTableMetadata = result });
    }

    fn select_database_metadata(
        self: *ExecutionEngine,
        metadata: *page.DBMetadataPage,
    ) !QueryResult {
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

        return QueryResult.success_result(.{ .SelectDatabaseMetadata = SelectDatabaseMetadataResult{
            .n_total_pages = metadata.total_pages,
            .n_free_pages = metadata.free_pages,
            .first_free_page = metadata.first_free_page,
            .free_pages = free_pages,
        } });
    }
};

// @todo normalize this. Also have a result type for all queries instead of returning null
const ExecuteQueryResults = union(enum) {
    SelectTableMetadataResult: SelectTableMetadataResult,
    SelectDatabaseMetadataResult: SelectDatabaseMetadataResult,
};

pub const QueryResult = struct {
    status: QueryStatus,
    data: QueryResultData,
    // @todo
    rows_affected: ?u32 = null,

    pub const QueryStatus = enum {
        Success,
        Error,
    };

    pub const ErrorResult = struct {
        error_code: anyerror,
        error_message: []const u8,
    };

    pub const QueryResultData = union(enum) {
        //
        // DDL Operations
        //
        //CreateTable: CreateTableResult,
        //DropTable: DropTableResult,

        //
        // DML Operations
        //
        //Insert: InsertResult,
        //Update: UpdateResult,
        //Delete: DeleteResult,

        //
        // Query Operations
        //
        // Select: SelectResult,
        SelectTableMetadata: SelectTableMetadataResult,
        SelectDatabaseMetadata: SelectDatabaseMetadataResult,

        // Error case
        Error: ErrorResult,

        // Temp void result type
        __void: void,
    };

    pub fn success_result(data: QueryResultData) QueryResult {
        return QueryResult{
            .status = .Success,
            .data = data,
        };
    }

    pub fn error_result(err: anyerror, message: []const u8) QueryResult {
        return QueryResult{
            .status = .Error,
            .data = .{ .Error = ErrorResult{ .error_code = err, .error_message = message } },
        };
    }

    pub fn deinit(self: *QueryResult, allocator: std.mem.Allocator) void {
        switch (self.data) {
            .SelectTableMetadata => |*result| result.deinit(),
            .Select => |*result| result.deinit(allocator),
            else => {},
        }
    }

    pub fn is_error_result(self: *const QueryResult) bool {
        return self.status == .Error;
    }
};

const CreateTablePayload = struct {
    table_name: []const u8,
    columns: []const muscle.Column,
    primary_key_column_index: ?usize = null,
};

const DropTablePayload = struct {
    table_name: []const u8,
};

const DropIndexPayload = struct {};

pub const InsertPayload = struct {
    pub const Value = struct {
        column_name: []const u8,
        value: muscle.Value,
    };

    table_name: []const u8,
    values: []const Value,
};
pub const UpdatePayload = InsertPayload;

const SelectPayload = struct {
    table_name: []const u8,
    // columns: []const muscle.Column,
    // limit: usize
};

const DeletePayload = struct {
    table_name: []const u8,
    key: muscle.Value,
};

pub const Query = union(enum) {
    CreateTable: CreateTablePayload,
    DropTable: DropTablePayload,
    DropIndex: DropIndexPayload,
    Insert: InsertPayload,
    Update: UpdatePayload,
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

    pub fn deinit(self: *SelectTableMetadataResult) void {
        var allocator = self.pages.allocator;
        var iter = self.pages.valueIterator();
        while (iter.next()) |_page| {
            while (_page.cells.pop()) |cell| {
                allocator.free(cell.key);
                allocator.free(cell.value);
            }

            _page.cells.deinit();
        }

        self.table_columns.deinit();
        self.pages.deinit();
    }
};

//const ExecutionPlan = struct {
//    steps: [32]ExecutionStep,
//    n_steps: usize,
//};

//const ExecutionStep = union(enum) {
//    .TABLE_SCAN: struct {
//        .table_name: []const u8,
//    },
//    .INDEX_SCAN: struct {
//        .index_name: []const u8,
//    },
//    .ASSERT_UNIQUE: struct {
//        .index_name: []const u8,
//    },
//};
