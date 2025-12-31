const std = @import("std");
const muscle = @import("../muscle.zig");

pub const QueryResult = union(enum) {
    data: Data,
    err: Error,

    pub fn is_error_result(self: *const QueryResult) bool {
        return self.* == .err;
    }
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
    free_pages: std.BoundedArray(muscle.PageNumber, 128),
};

pub const SelectResult = struct {
    columns: std.ArrayList(muscle.Column),
    rows: std.ArrayList(std.ArrayList(u8)),
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
