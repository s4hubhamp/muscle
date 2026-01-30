const std = @import("std");
const muscle = @import("./muscle.zig");
const query_result = @import("query_result.zig");
const QueryContext = @import("QueryContext.zig");

const print = std.debug.print;
const assert = std.debug.assert;
const BTree = muscle.BTree;
const errors = muscle.common.errors;
const page_types = muscle.page_types;
const PageManager = muscle.PageManager;
const serde = muscle.common.serde;
const BoundedArray = muscle.common.BoundedArray;

// The database object. This is main API to interact with the database.
pub const Muscle = struct {
    allocator: std.mem.Allocator,
    pager: PageManager,
    catalog: muscle.Catalog_Manager,

    pub fn init(allocator: std.mem.Allocator, database_file_path: []const u8) !Muscle {
        var pager = try PageManager.init(database_file_path, allocator);

        // call rollback to sync if we had crashed earlier
        print("Calling Rollback after instance start.\n", .{});
        try pager.rollback();

        // rollback could update catalog hence we init it after rollback
        const catalog = try muscle.Catalog_Manager.init(allocator, &pager);

        return Muscle{ .allocator = allocator, .pager = pager, .catalog = catalog };
    }

    pub fn deinit(self: *Muscle) void {
        self.pager.deinit();
        self.catalog.deinit();
    }

    //pub fn execute(self: *Muscle, arena: std.mem.Allocator, query: []const u8) !void {
    //    var context = QueryContext.init(arena, query);

    //    var parser = muscle.parser.Parser.init(&context);
    //    const statements = parser.parse();

    //    print("statements {any}\n", .{statements});
    //}

    pub fn execute_query(self: *Muscle, query: Query, arena: std.mem.Allocator) !query_result.QueryResult {
        var context = QueryContext.init(arena, "", &self.pager, &self.catalog);

        var is_update_query = false;
        var should_rollback = false;

        switch (query) {
            Query.create_table => |payload| {
                is_update_query = true;
                self.create_table(&context, payload) catch |err| {
                    should_rollback = true;
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.drop_table => |payload| {
                is_update_query = true;
                self.drop_table(&context, payload) catch |err| {
                    should_rollback = true;
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.insert => |payload| {
                is_update_query = true;
                self.insert(&context, payload) catch |err| {
                    should_rollback = true;
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.update => |payload| {
                is_update_query = true;
                self.update(&context, payload) catch |err| {
                    should_rollback = true;
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.delete => |payload| {
                is_update_query = true;
                self.delete(&context, payload) catch |err| {
                    should_rollback = true;
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.select => |payload| {
                self.select(&context, payload) catch |err| {
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.select_table_info => |payload| {
                self.select_table_info(&context, payload) catch |err| {
                    try maybe_create_client_error_response(&context, err);
                };
            },
            Query.select_database_info => {
                self.select_database_info(&context) catch |err| {
                    try maybe_create_client_error_response(&context, err);
                };
            },
        }

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

        return context.result;
    }

    // gracefully resolves client errors and creates error QueryResult
    fn maybe_create_client_error_response(context: *QueryContext, err: anyerror) !void {
        const classification = errors.classify_error(err);
        switch (classification) {
            .client => {
                try context.set_err(err, "Unhandled client error: {any}. Should probably have a nicer error message.", .{err});
            },
            .system => {
                return err;
            },
        }
    }

    fn create_table(self: *Muscle, context: *QueryContext, payload: CreateTablePayload) !void {
        _ = self;

        if (context.catalog.find_table(payload.table_name) != null) {
            try context.set_err(error.DuplicateTableName, "Table {s} already exists", .{payload.table_name});
            return error.DuplicateTableName;
        }

        // check for duplicate column names
        for (payload.columns[0 .. payload.columns.len - 1], 0..) |*col, i| {
            for (payload.columns[i + 1 ..]) |*col2| {
                if (std.mem.eql(u8, col.name, col2.name)) {
                    try context.set_err(error.DuplicateColumnName, "Duplicate columns with name {s}", .{col.name});
                    return error.DuplicateColumnName;
                }
            }
        }

        var columns = try std.ArrayList(muscle.Column).initCapacity(context.arena, payload.columns.len + 1);

        for (payload.columns, 0..) |*c, i| {
            var column = c.*;

            if (column.auto_increment and column.data_type != .int) {
                try context.set_err(
                    error.AutoIncrementColumnMustBeInteger,
                    "Auto increment column must be of type Integer. Column {s} is {s}",
                    .{ column.name, @tagName(column.data_type) },
                );
                return error.AutoIncrementColumnMustBeInteger;
            }

            if (i == payload.primary_key_column_index) {
                switch (column.data_type) {
                    // bools can't be primary key
                    .bool => return error.BadPrimaryKeyType,
                    // for text and binaries we have a limit on length
                    .txt, .bin => |len| {
                        // for text we store len as first param
                        if (len > page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT - @sizeOf(u16)) {
                            try context.set_err(
                                error.PrimaryKeyMaxLengthExceeded,
                                "Primary key column length cannot exceed {d}",
                                .{page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT - @sizeOf(u16)},
                            );
                            return error.PrimaryKeyMaxLengthExceeded;
                        }
                    },
                    else => {},
                }

                // primary key should always be unique
                column.unique = true;
                column.not_null = true;

                // if data_type is integer then it's basically alias to default primary key so we will enable all the defaults
                if (column.data_type == .int) {
                    column.auto_increment = true;
                    column.max_int_value = 0;
                }

                // primary key column gets stored at beginning
                try columns.insert(context.arena, 0, column);
            } else {
                // @Perf Can we reorder columns for efficient operations?
                try columns.append(context.arena, column);
            }
        }

        // create a default primary key column if not provided
        if (payload.primary_key_column_index == null) {
            const DEFAULT_PRIMARY_KEY_COLUMN_NAME = "Row_Id";
            const DEFAULT_PRIMARY_KEY_COLUMN_TYPE = .int;
            try columns.insert(context.arena, 0, muscle.Column{
                .name = DEFAULT_PRIMARY_KEY_COLUMN_NAME,
                .data_type = DEFAULT_PRIMARY_KEY_COLUMN_TYPE,
                .auto_increment = true,
                .not_null = true,
                .unique = true,
                .max_int_value = 0,
            });
        }

        const root_page_number = try context.pager.alloc_free_page(context.catalog.metadata);
        // initialize root page
        const root_page = page_types.Page.init();
        try context.pager.update_page(root_page_number, &root_page);

        const table = muscle.Table{
            .root = root_page_number,
            .name = payload.table_name,
            .columns = columns.items,
            .indexes = &.{},
        };

        try context.catalog.create_table(context.pager, table);
    }

    fn drop_table(self: *Muscle, context: *QueryContext, payload: DropTablePayload) !void {
        _ = self;
        _ = context;
        _ = payload;

        unreachable;
    }

    fn insert(self: *Muscle, context: *QueryContext, payload: InsertPayload) !void {
        _ = self;

        var table: muscle.Table = if (context.catalog.find_table(payload.table_name)) |t|
            try t.clone(context.arena)
        else
            return error.TableNotFound;

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

        // check for columns and duplicates
        if (payload.values.len > 1) {
            for (payload.values, 0..) |*v1, i| {
                if (table.find_column(v1.column_name) == null) return error.ColumnDoesNotExist;

                for (payload.values[i + 1 ..]) |*v2| {
                    if (std.mem.eql(u8, v1.column_name, v2.column_name))
                        return error.DuplicateColumns;
                }
            }
        }

        // If the passed payload size is greater than max content that a single page can hold
        // this will overflow.
        // so the max size of a single row for now is equal to `page_types.Page.CONTENT_MAX_SIZE`
        var buffer = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        var primary_key_bytes: []const u8 = undefined;
        var primary_key_type: muscle.DataType = undefined;

        // for each column find value
        for (table.columns, 0..) |*column, i| {
            const value = find_value(column.name, payload.values);
            var final_value_to_serialize: muscle.Value = .{ .null = {} };

            // if value is not provided or it's provided and null
            if (value == null or value.?.value == .null) {
                if (column.auto_increment) {
                    // increment value and serialize
                    column.max_int_value += 1;
                    final_value_to_serialize = muscle.Value{ .int = column.max_int_value };
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
                    if (column.max_int_value < final_value_to_serialize.int) {
                        column.max_int_value = final_value_to_serialize.int;
                    }
                }

                // validate the type of value
                switch (column.data_type) {
                    .int => if (final_value_to_serialize != .int and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .real => if (final_value_to_serialize != .real and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .bool => if (final_value_to_serialize != .bool and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .txt => |len| switch (final_value_to_serialize) {
                        .txt => |text| {
                            if (text.len > len) return error.TextTooLong;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                    .bin => |len| switch (final_value_to_serialize) {
                        .bin => |bin| {
                            if (bin.len > len) return error.BinaryTooLarge;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                }
            }

            try serde.serailize_value(&buffer, final_value_to_serialize);

            // first column is always the primary key
            if (i == 0) {
                primary_key_bytes = buffer.const_slice();
                primary_key_type = column.data_type;
                if (primary_key_bytes.len > page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT) {
                    return error.KeyTooLong;
                }
            }
        }

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(context, table.root, primary_key_type);
        try btree.insert(primary_key_bytes, buffer.const_slice());

        // update metadata
        try context.catalog.update_table(context.pager, table);

        context.set_data(.{ .insert = .{ .rows_created = 1 } });
    }

    fn update(self: *Muscle, context: *QueryContext, payload: UpdatePayload) !void {
        _ = self;

        var table: muscle.Table = if (context.catalog.find_table(payload.table_name)) |t|
            try t.clone(context.arena)
        else
            return error.TableNotFound;

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

        // check for duplicate columns and whether columns even exists
        if (payload.values.len > 1) {
            for (payload.values, 0..) |*v1, i| {
                if (table.find_column(v1.column_name) == null) return error.ColumnDoesNotExist;
                for (payload.values[i + 1 ..]) |*v2| {
                    if (std.mem.eql(u8, v1.column_name, v2.column_name)) return error.DuplicateColumns;
                }
            }
        }

        // If the passed payload size is greater than max content that a single page can hold
        // this will overflow.
        // so the max size of a single row for now is equal to `page_types.Page.CONTENT_MAX_SIZE`
        var buffer = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        var primary_key_bytes: []const u8 = undefined;
        var primary_key_type: muscle.DataType = undefined;

        // for each column find value
        for (table.columns, 0..) |*column, i| {
            const value = find_value(column.name, payload.values);
            var final_value_to_serialize: muscle.Value = .{ .null = {} };

            // if value is not provided or it's provided and null
            if (value == null or value.?.value == .null) {
                if (column.auto_increment) {
                    // increment value and serialize
                    column.max_int_value += 1;
                    final_value_to_serialize = muscle.Value{ .int = column.max_int_value };
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
                    if (column.max_int_value < final_value_to_serialize.int) {
                        column.max_int_value = final_value_to_serialize.int;
                    }
                }

                // validate the type of value
                switch (column.data_type) {
                    .int => if (final_value_to_serialize != .int and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .real => if (final_value_to_serialize != .real and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .bool => if (final_value_to_serialize != .bool and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .txt => |len| switch (final_value_to_serialize) {
                        .txt => |text| {
                            if (text.len > len) return error.TextTooLong;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                    .bin => |len| switch (final_value_to_serialize) {
                        .bin => |bin| {
                            if (bin.len > len) return error.BinaryTooLarge;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                }
            }

            try serde.serailize_value(&buffer, final_value_to_serialize);

            // first column is always the primary key
            if (i == 0) {
                primary_key_bytes = buffer.const_slice();
                primary_key_type = column.data_type;
                if (primary_key_bytes.len > page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT) {
                    return error.KeyTooLong;
                }
            }
        }

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(context, table.root, primary_key_type);
        try btree.update(primary_key_bytes, buffer.const_slice());

        // update metadata
        try context.catalog.update_table(context.pager, table);

        context.set_data(.{ .update = .{ .rows_affected = 1 } });
    }

    fn delete(
        self: *Muscle,
        context: *QueryContext,
        payload: DeletePayload,
    ) !void {
        _ = self;

        const table = context.catalog.find_table(payload.table_name) orelse return error.TableNotFound;

        // right now all deletes are by pk
        if (@intFromEnum(table.columns[0].data_type) != @intFromEnum(payload.key)) {
            return error.TypeMismatch;
        }

        // primary key has size limit
        var buffer = BoundedArray(u8, page_types.Page.CONTENT_MAX_SIZE){};
        try serde.serailize_value(&buffer, payload.key);

        // Important to initiate BTree everytime OR have correct reference to metadata_page every time
        var btree = BTree.init(context, table.root, table.columns[0].data_type);
        try btree.delete(buffer.const_slice());
    }

    fn select(
        self: *Muscle,
        context: *QueryContext,
        payload: SelectPayload,
    ) !void {
        _ = self;

        const table = context.catalog.find_table(payload.table_name) orelse return error.TableNotFound;

        var result_columns = try std.ArrayList(muscle.Column).initCapacity(context.arena, if (payload.columns.len > 0) payload.columns.len else table.columns.len);
        var result = query_result.SelectResult{
            .columns = result_columns,
            .rows = try std.ArrayList(std.ArrayList(u8)).initCapacity(
                context.arena,
                payload.limit,
            ),
        };

        if (payload.columns.len == 0) {
            try result_columns.appendSlice(context.arena, table.columns);
        } else {
            // validate selected columns and copy inside the results_columns
            for (payload.columns) |col| {
                if (table.find_column(col)) |column| {
                    try result_columns.append(context.arena, column.*);
                } else {
                    return error.ColumnNotFound;
                }
            }
        }

        var serial: usize = 1;
        var curr_page_number = table.root;

        // find the leftmost leaf node
        var curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
        while (!curr_page.is_leaf()) {
            assert(curr_page.cell_at_slot(0).left_child != 0);
            curr_page_number = curr_page.cell_at_slot(0).left_child;
            curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
        }

        //std.debug.print("\n\n*****************************************************************\n", .{});

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

            for (0..curr_page.num_slots) |slot_index| {
                const cell = curr_page.cell_at_slot(@intCast(slot_index));
                var row_bytes = std.ArrayList(u8){};

                print(" serial={} size={}", .{ serial, cell.size + @sizeOf(page_types.Page.SlotArrayEntry) });
                serial += 1;

                var offset: usize = 0;
                for (result_columns.items) |*column| {
                    switch (column.data_type) {
                        .bin, .txt => {
                            const len = std.mem.readInt(u16, cell.content[offset..][0..@sizeOf(u16)], .little);

                            {
                                offset += @sizeOf(u16);
                                if (len > 10) {
                                    print("  {s}={s}...({d})", .{ column.name, cell.content[offset..][0..10], len });
                                } else {
                                    print("  {s}={s}", .{ column.name, cell.content[offset..][0..len] });
                                }
                            }

                            try row_bytes.appendSlice(context.arena, cell.content[offset..(offset + len)]);
                            offset += len;
                        },
                        .int => {
                            print("  {s}={}", .{
                                column.name,
                                std.mem.readInt(i64, cell.content[offset..][0..@sizeOf(i64)], .little),
                            });

                            try row_bytes.appendSlice(context.arena, cell.content[offset..(offset + @sizeOf(i64))]);
                            offset += @sizeOf(i64);
                        },
                        .real => {
                            print("  {s}={}", .{
                                column.name,
                                @as(f64, @bitCast(std.mem.readInt(i64, cell.content[offset..][0..@sizeOf(i64)], .little))),
                            });

                            try row_bytes.appendSlice(context.arena, cell.content[offset..(offset + @sizeOf(i64))]);
                            offset += @sizeOf(i64);
                        },
                        .bool => {
                            print(
                                "  {s}={any}",
                                .{ column.name, if (cell.content[offset] == 1) true else false },
                            );

                            try row_bytes.appendSlice(context.arena, cell.content[offset..(offset + 1)]);
                            offset += 1;
                        },
                    }
                }

                row_bytes.shrinkAndFree(context.arena, row_bytes.items.len);
                try result.rows.append(context.arena, row_bytes);
                print("\n", .{});
            }

            if (curr_page.right == 0) break;
            curr_page_number = curr_page.right;
            curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
        }

        print(
            "\n\ndatabase metadata: total_pages: {any} free_pages:{any} first_free_page:{any}",
            .{
                context.catalog.metadata.total_pages,
                context.catalog.metadata.free_pages,
                context.catalog.metadata.first_free_page,
            },
        );

        std.debug.print("\n\n*****************************************************************\n\n", .{});

        context.set_data(.{ .select = result });
    }

    fn select_table_info(
        self: *Muscle,
        context: *QueryContext,
        payload: SelectTableMetadata,
    ) !void {
        _ = self;
        const SelectTableMetadataResult = query_result.SelectTableMetadataResult;

        const table = context.catalog.find_table(payload.table_name) orelse return error.TableNotFound;

        // use BFS and insert all the page info inside the hash table
        // also record total cells
        var result = SelectTableMetadataResult{
            .root_page = table.root,
            .btree_height = 0,
            .btree_leaf_cells = 0,
            .btree_internal_cells = 0,
            .btree_leaf_pages = 0,
            .btree_internal_pages = 0,
            .table_columns = try std.ArrayList(muscle.Column)
                .initCapacity(context.arena, table.columns.len),
            .pages = std.AutoHashMap(muscle.PageNumber, SelectTableMetadataResult.DBPageMetadata)
                .init(context.arena),
        };

        // copy columns
        try result.table_columns.appendSlice(context.arena, table.columns);

        const primary_key_data_type = table.columns[0].data_type;
        var first_page_in_level: ?muscle.PageNumber = table.root;
        var curr_page_number: muscle.PageNumber = undefined;
        var curr_page: page_types.Page = undefined;

        const collect_page_info = struct {
            fn f(
                map: *std.AutoHashMap(muscle.PageNumber, SelectTableMetadataResult.DBPageMetadata),
                _page_number: muscle.PageNumber,
                _page: page_types.Page,
                _primary_key_data_type: muscle.DataType,
            ) !void {
                var page_info = SelectTableMetadataResult.DBPageMetadata{
                    .page = _page_number,
                    .right_child = _page.right_child,
                    .content_size = _page.content_size,
                    .free_space = _page.free_space(),
                    .left = _page.left,
                    .right = _page.right,
                    .cells = std.ArrayList(SelectTableMetadataResult.DBPageCellMetadata){},
                };

                for (0.._page.num_slots) |slot| {
                    const cell = _page.cell_at_slot(@intCast(slot));

                    try page_info.cells.append(map.allocator, .{
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
            curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
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
                curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
            }
        }

        context.set_data(.{ .select_table_info = result });
    }

    fn select_database_info(self: *Muscle, context: *QueryContext) !void {
        _ = self;

        var free_pages = BoundedArray(muscle.PageNumber, 128){};
        const metadata = context.catalog.metadata;

        if (metadata.first_free_page > 0) {
            var curr_page_number = metadata.first_free_page;
            while (curr_page_number > 0) {
                // assert no duplicates
                for (free_pages.const_slice()) |i| {
                    assert(i != curr_page_number);
                }

                free_pages.push(curr_page_number);
                const curr_page = try context.pager.get_page(page_types.FreePage, curr_page_number);
                curr_page_number = curr_page.next;
            }
        }

        print("\n--------------------------------- DATABASE METADATA ------------------------------\n", .{});
        print("Total pages: {any}\n", .{metadata.total_pages});
        print("Free pages: {any}\n", .{metadata.free_pages});
        print("First free page: {any}\n", .{metadata.first_free_page});

        print("\nFree pages: ", .{});
        for (free_pages.const_slice()) |page_number| {
            print("{d} -> ", .{page_number});
        }
        print("0\n", .{});
        print("\n-----------------------------------------------------------------------------------\n", .{});

        assert(metadata.free_pages == free_pages.len);

        context.set_data(.{ .select_database_info = query_result.SelectDatabaseMetadataResult{
            .n_total_pages = metadata.total_pages,
            .n_free_pages = metadata.free_pages,
            .first_free_page = metadata.first_free_page,
            .free_pages = free_pages,
        } });
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
    // zero columns indicates we want all columns
    columns: []const []const u8 = &[_][]const u8{},
    // zero limit indicates we want all data
    limit: usize = 0,
};

const SelectTableMetadata = struct {
    table_name: []const u8,
};

const DeletePayload = struct {
    table_name: []const u8,
    key: muscle.Value,
};

pub const Query = union(enum) {
    create_table: CreateTablePayload,
    drop_table: DropTablePayload,
    insert: InsertPayload,
    update: UpdatePayload,
    select: SelectPayload,
    delete: DeletePayload,
    select_table_info: SelectTableMetadata,
    select_database_info,
};
