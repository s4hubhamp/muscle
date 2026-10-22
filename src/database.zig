const std = @import("std");
const muscle = @import("./muscle.zig");
const query_result = @import("query_result.zig");
const QueryContext = @import("QueryContext.zig");

const logger = std.log.scoped(.executor);

const assert = std.debug.assert;
const BTree = muscle.BTree;
const errors = muscle.common.errors;
const page_types = muscle.page_types;
const PageManager = muscle.PageManager;
const BoundedArray = muscle.common.BoundedArray;
const serde = muscle.common.serde;

// The database object. This is main API to interact with the database.
pub const Muscle = struct {
    allocator: std.mem.Allocator,
    pager: PageManager,
    catalog: muscle.Catalog_Manager,

    pub fn init(allocator: std.mem.Allocator, database_file_path: []const u8) !Muscle {
        var pager = try PageManager.init(database_file_path, allocator);
        errdefer pager.deinit();

        // call rollback to sync if we had crashed earlier
        logger.debug("Calling Rollback after instance start.", .{});
        try pager.rollback();

        // rollback could update catalog hence we init it after rollback
        // Important: This should be called after page.init()
        const catalog = try muscle.Catalog_Manager.init(allocator, &pager);

        return Muscle{ .allocator = allocator, .pager = pager, .catalog = catalog };
    }

    pub fn deinit(self: *Muscle) void {
        self.pager.deinit();
        self.catalog.deinit();
    }

    pub fn execute(self: *Muscle, query: []const u8, arena: std.mem.Allocator) !query_result.QueryResult {
        var context = QueryContext.init(arena, query, &self.pager, &self.catalog);
        var is_update_query = false;
        var should_rollback = false;
        errdefer if (is_update_query) {
            self.rollback_and_refresh() catch |err| logger.err("Failed to roll back query: {any}\n", .{err});
        };

        var parser = muscle.Parser.init(&context);
        const statements = parser.parse() catch |err| {
            try maybe_create_client_error_response(&context, err);
            return context.result;
        };

        muscle.analyzer.analyze(&context, statements) catch |err| {
            try maybe_create_client_error_response(&context, err);
            return context.result;
        };

        const plans = muscle.planner.make_plans(&context, statements) catch |err| {
            try maybe_create_client_error_response(&context, err);
            return context.result;
        };

        execution: for (plans) |*plan| {
            switch (plan.*) {
                .unplanned => |stmt| {
                    switch (stmt) {
                        .create_table => |payload| {
                            is_update_query = true;
                            self.create_table(&context, payload) catch |err| {
                                should_rollback = true;
                                try maybe_create_client_error_response(&context, err);
                                break :execution;
                            };
                            try context.set_result_columns(&.{.{ .name = "Status", .data_type = .{ .txt = 0 } }});
                            try context.append_data(try serde.serialize(.{ .txt = "Table created." }, context.arena));
                        },
                        else => unreachable,
                    }
                },

                .project => |*project| {
                    try context.set_result_columns(project.columns);
                    while (project.next() catch |err| {
                        should_rollback = is_update_query;
                        try maybe_create_client_error_response(&context, err);
                        break :execution;
                    }) |row| try context.append_data(row);
                },

                .insert => |*insert_plan| {
                    is_update_query = true;
                    try context.set_result_columns(&.{.{ .name = "rows_created", .data_type = .int }});
                    if (insert_plan.next() catch |err| {
                        should_rollback = true;
                        try maybe_create_client_error_response(&context, err);
                        break :execution;
                    }) |rows_created| try context.append_data(rows_created);
                },

                .update => |*update_plan| {
                    is_update_query = true;
                    try context.set_result_columns(&.{.{ .name = "rows_affected", .data_type = .int }});
                    if (update_plan.next() catch |err| {
                        should_rollback = true;
                        try maybe_create_client_error_response(&context, err);
                        break :execution;
                    }) |rows_affected| try context.append_data(rows_affected);
                },

                .delete => |*delete_plan| {
                    is_update_query = true;
                    try context.set_result_columns(&.{.{ .name = "rows_affected", .data_type = .int }});
                    if (delete_plan.next() catch |err| {
                        should_rollback = true;
                        try maybe_create_client_error_response(&context, err);
                        break :execution;
                    }) |rows_affected| try context.append_data(rows_affected);
                },

                .seq_scan => {
                    unreachable;
                },

                .sort, .exact_match, .range_scan, .index_scan, .filter, .logical_or_scan => {},
            }
        }

        if (should_rollback) {
            logger.warn("Processing failed. Calling Rollback. {f}", .{context.result});

            self.rollback_and_refresh() catch |rollback_err| {
                // If rollback fails, this is a critical system error
                logger.err("rollback failure: {any}\n", .{rollback_err});
                @panic("Unexpected rollback failure");
            };
        } else if (is_update_query) {
            self.pager.commit(true) catch |commit_err| {
                logger.err("commit failure: {any}\n", .{commit_err});
                @panic("Unexpected commit failure");
            };
        }

        return context.result;
    }

    fn rollback_and_refresh(self: *Muscle) !void {
        try self.pager.rollback();
        const catalog = try muscle.Catalog_Manager.init(self.allocator, &self.pager);
        self.catalog.deinit();
        self.catalog = catalog;
    }

    // gracefully resolves client errors and creates error QueryResult
    fn maybe_create_client_error_response(context: *QueryContext, err: anyerror) !void {
        const classification = errors.classify_error(err);
        switch (classification) {
            .client => {
                if (context.result != .err) try context.set_err(err, "{s}", .{@errorName(err)});
            },
            .system => {
                return err;
            },
        }
    }

    fn create_table(self: *Muscle, context: *QueryContext, payload: muscle.statement.CreateTable) !void {
        _ = self;

        if (context.catalog.find_table(payload.table) != null) {
            try context.set_err(error.DuplicateTableName, "Table {s} already exists", .{payload.table});
            return error.DuplicateTableName;
        }

        var new_table = muscle.Table{
            .root = 0,
            .name = payload.table,
            .primary_key = "",
            .columns = .empty,
            .indexes = &.{},
        };

        for (payload.columns, 0..) |*column, i| {
            if (column.auto_increment and column.data_type != .int) {
                try context.set_err(
                    error.AutoIncrementConstraintViolation,
                    "Auto increment column must be of type Integer. Column {s} is {s}",
                    .{ column.name, @tagName(column.data_type) },
                );
                return error.AutoIncrementConstraintViolation;
            }

            if (i == payload.primary_key_column_index) {
                switch (column.data_type) {
                    // bools can't be primary key
                    .bool => return error.BadPrimaryKeyType,
                    // for text and binaries we have a limit on length
                    .txt, .bin => |len| {
                        // for text we store len as first param
                        if (len > page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT - 1 - @sizeOf(u16)) {
                            try context.set_err(
                                error.PrimaryKeyMaxLengthExceeded,
                                "Primary key column length cannot exceed {d}",
                                .{page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT - 1 - @sizeOf(u16)},
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

                new_table.primary_key = column.name;
            }
        }

        // Rows are serialized in column order, and BTree operations read the key
        // from the beginning of each row. Always insert the primary key first.
        if (payload.primary_key_column_index) |primary_key_index| {
            const column = payload.columns[primary_key_index];
            try new_table.columns.put(context.arena, column.name, column);
        } else {
            const DEFAULT_PRIMARY_KEY_COLUMN_NAME = "Row_Id";
            const DEFAULT_PRIMARY_KEY_COLUMN_TYPE = .int;
            try new_table.columns.put(context.arena, DEFAULT_PRIMARY_KEY_COLUMN_NAME, muscle.Column{
                .name = DEFAULT_PRIMARY_KEY_COLUMN_NAME,
                .data_type = DEFAULT_PRIMARY_KEY_COLUMN_TYPE,
                .auto_increment = true,
                .not_null = true,
                .unique = true,
                .max_int_value = 0,
            });
            new_table.primary_key = DEFAULT_PRIMARY_KEY_COLUMN_NAME;
        }

        for (payload.columns) |column| {
            if (std.mem.eql(u8, column.name, new_table.primary_key)) continue;
            try new_table.columns.put(context.arena, column.name, column);
        }

        const root_page_number = try context.pager.alloc_free_page(.data_page, context.catalog.metadata);
        // initialize root page
        const root_page = page_types.Page.init();
        try context.pager.update_page(root_page_number, &root_page);
        new_table.root = root_page_number;
        try context.catalog.create_table(context.pager, new_table);
    }

    pub fn select_table_info(
        self: *Muscle,
        table_name: []const u8,
        arena: std.mem.Allocator,
    ) !SelectTableMetadataResult {
        var context = QueryContext.init(arena, "", &self.pager, &self.catalog);

        const table = try if (context.catalog.find_table(table_name)) |t|
            t.clone(context.arena)
        else
            error.TableNotFound;

        // use BFS and insert all the page info inside the hash table
        // also record total cells
        var result = SelectTableMetadataResult{
            .root_page = table.root,
            .btree_height = 0,
            .btree_leaf_cells = 0,
            .btree_internal_cells = 0,
            .btree_leaf_pages = 0,
            .btree_internal_pages = 0,
            .table_columns = try std.ArrayList(muscle.Column).initCapacity(context.arena, table.columns.count()),
            .pages = std.AutoHashMap(muscle.PageNumber, SelectTableMetadataResult.DBPageMetadata).init(context.arena),
        };

        // copy columns
        for (table.columns.values()) |column| {
            try result.table_columns.append(context.arena, column);
        }

        const primary_key_data_type = table.columns.get(table.primary_key).?.data_type;
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
                    .cells = std.ArrayList(SelectTableMetadataResult.DBPageCellMetadata).empty,
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
                if (curr_page_number != 0) {
                    curr_page = try context.pager.get_page(page_types.Page, curr_page_number);
                }
            }
        }

        return result;
    }

    pub fn select_database_info(self: *Muscle, arena: std.mem.Allocator) !SelectDatabaseMetadataResult {
        var context = QueryContext.init(arena, "", &self.pager, &self.catalog);

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

        assert(metadata.free_pages == free_pages.len);

        return SelectDatabaseMetadataResult{
            .n_total_pages = metadata.total_pages,
            .n_free_pages = metadata.free_pages,
            .first_free_page = metadata.first_free_page,
            .free_pages = free_pages,
        };
    }
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

    pub fn print(self: *const SelectDatabaseMetadataResult) void {
        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%     Metadata    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});

        std.debug.print("Tatal Pages:       {}\n", .{self.n_total_pages});
        std.debug.print("Free Pages:        {}\n\n", .{self.n_free_pages});
        std.debug.print("First Free Page:   {}\n", .{self.first_free_page});

        std.debug.print("\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   End Metadata  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n", .{});
    }
};

test {
    _ = @import("testing/database.zig");
}
