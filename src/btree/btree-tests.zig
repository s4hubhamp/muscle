const std = @import("std");
const muscle = @import("muscle");
const execution = @import("../execution_engine.zig");
const serde = @import("../serialize_deserialize.zig");
const Pager = @import("./pager.zig").Pager;
const Page = @import("./page.zig").Page;
const FreePage = @import("./page.zig").FreePage;
const DBMetadataPage = @import("./page.zig").DBMetadataPage;

const assert = std.debug.assert;
const SelectTableMetadataResult = execution.SelectTableMetadataResult;
const Query = execution.Query;

test "test tree operations on text primary key" {
    const database_file = "/Users/shupawar/x/muscle/muscle";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var engine = try execution.ExecutionEngine.init(allocator, database_file);

    defer {
        engine.deinit();
        const deinit_status = gpa.deinit();
        //fail test; can't try in defer as defer is executed after we return
        if (deinit_status == .leak) std.testing.expect(false) catch @panic("Memory leak while deiniting");
    }

    const table_name = "devices";

    {
        const table_columns = [_]muscle.Column{
            muscle.Column{
                .name = "pk",
                .data_type = muscle.DataType{ .TEXT = 2023 }, // 2031(max) - 8(Str len after serialization)
            },
            .{
                .name = "datetime",
                .data_type = muscle.DataType{ .INT = {} },
            },
        };
        const create_table_query: Query = Query{ .CreateTable = .{
            .table_name = table_name,
            .columns = &table_columns,
            .primary_key_column_index = 0,
        } };
        _ = try engine.execute_query(create_table_query);
    }

    const get_insert_query = struct {
        fn f(pk: []const u8) Query {
            return Query{ .Insert = .{ .table_name = table_name, .values = &.{
                .{
                    .column_name = "pk",
                    .value = .{ .TEXT = pk },
                },
                .{
                    .column_name = "datetime",
                    .value = .{ .INT = @intCast(std.time.nanoTimestamp()) },
                },
            } } };
        }
    }.f;

    var delete_query: Query = Query{ .Delete = .{ .table_name = table_name, .key = undefined } };
    const select_metadata_query = Query{ .SelectTableMetadata = .{ .table_name = table_name } };
    const select_query = Query{ .Select = .{ .table_name = table_name } };
    //const select_database_metadata_query = Query{ .SelectDatabaseMetadata = {} };

    // Case: Inserting inside root when root is a leaf node.
    _ = try engine.execute_query(get_insert_query("a"));
    var metadata = try engine.execute_query(select_metadata_query);
    try validate_btree(&metadata.data.SelectTableMetadata);
    assert(metadata.data.SelectTableMetadata.btree_height == 1);
    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 1);
    metadata.data.SelectTableMetadata.deinit();

    // Case: Deleting when root is leaf node
    delete_query.Delete.key = .{ .TEXT = "a" };
    _ = try engine.execute_query(delete_query);
    metadata = try engine.execute_query(select_metadata_query);
    try validate_btree(&metadata.data.SelectTableMetadata);
    assert(metadata.data.SelectTableMetadata.btree_height == 1);
    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 0);
    metadata.data.SelectTableMetadata.deinit();

    // Case: Splitting root node
    {
        var txt = [_]u8{65} ** 2023;
        _ = try engine.execute_query(get_insert_query(&txt));
        for (&txt) |*char| char.* = 66;
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        metadata.data.SelectTableMetadata.btree_height = 2;
        metadata.data.SelectTableMetadata.btree_leaf_cells = 2;
        metadata.data.SelectTableMetadata.deinit();
    }

    // tree state
    // 1.             (1)[AAAA..]
    // 2. (2)[AAAAAA..]         (3)[BBBBBBB..]

    // Case: Deleting leaf node which leads to root becoming empty
    {
        const txt = [_]u8{65} ** 2023;
        delete_query.Delete.key = .{ .TEXT = &txt };
        _ = try engine.execute_query(delete_query);
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 1);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 1);
        metadata.data.SelectTableMetadata.deinit();
    }

    // tree state
    // 1.       (1)[BBBB..]

    // Case: Adding divider key inside root
    {
        var txt = [_]u8{65} ** 2023;
        _ = try engine.execute_query(get_insert_query(&txt));
        for (&txt) |*char| char.* = 67;
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 2);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
        metadata.data.SelectTableMetadata.deinit();
    }

    // tree state
    // 1.             (1)[AAAA..,              BBBBBB..]
    // 2. (3)[AAAAAA..]         (2)[BBBBBBB..]          (4)[CCCCC..]

    // Case: Splitting root when root is internal node
    {
        var txt = [_]u8{68} ** 2023;
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 3);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 4);
        metadata.data.SelectTableMetadata.deinit();
    }

    // tree state
    // 1.                         (1)[BBBB..]
    // 1.             (6)[AAAA..,                       CCCCC..]
    // 2. (3)[AAAAAA..]         (2)[BBBBBBB..]  (4)[CCCCC..]    (5)[DDDDD..]

    // Case: Internal node merge
    {
        var txt = [_]u8{66} ** 2023;
        delete_query.Delete.key = .{ .TEXT = &txt };
        _ = try engine.execute_query(delete_query);
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 2);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
        metadata.data.SelectTableMetadata.deinit();

        // insert B back
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        metadata.data.SelectTableMetadata.deinit();

        // Delete A
        for (&txt) |*char| char.* = 65;
        delete_query.Delete.key = .{ .TEXT = &txt };
        _ = try engine.execute_query(delete_query);
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 2);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
        metadata.data.SelectTableMetadata.deinit();

        // insert A back
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        metadata.data.SelectTableMetadata.deinit();

        // Delete C
        for (&txt) |*char| char.* = 67;
        delete_query.Delete.key = .{ .TEXT = &txt };
        _ = try engine.execute_query(delete_query);
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 2);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
        metadata.data.SelectTableMetadata.deinit();

        // insert C back
        _ = try engine.execute_query(get_insert_query(&txt));
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        metadata.data.SelectTableMetadata.deinit();

        // Delete D
        for (&txt) |*char| char.* = 68;
        delete_query.Delete.key = .{ .TEXT = &txt };
        _ = try engine.execute_query(delete_query);
        metadata = try engine.execute_query(select_metadata_query);
        try validate_btree(&metadata.data.SelectTableMetadata);
        assert(metadata.data.SelectTableMetadata.btree_height == 2);
        assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
        metadata.data.SelectTableMetadata.deinit();
    }

    // tree state
    // 1.             (1)[AAAA..,           BBBBB..]
    // 2. (4)[AAAAAA..]         (5)[BBBBB..]        (7)[CCCCC..]

    _ = try engine.execute_query(select_query);
}

fn validate_btree(metadata: *const SelectTableMetadataResult) !void {
    const t = std.time.milliTimestamp();

    var allocator = metadata.pages.allocator;
    const NodeData = struct {
        min_key: ?[]const u8,
        max_key: ?[]const u8,
        page: muscle.PageNumber,
    };

    const L = std.DoublyLinkedList(NodeData);
    var queue = L{};
    const primary_key_data_type = metadata.table_columns.items[0].data_type;

    // if root is empty then it should be a leaf node
    if (metadata.pages.getPtr(metadata.root_page)) |page| {
        if (page.content_size == 0) {
            assert(page.cells.items.len == 0);
            assert(page.right_child == 0);
            assert(metadata.pages.count() == 1);
        } else {
            var node = try allocator.create(L.Node);
            node.data = .{
                .min_key = null,
                .max_key = null,
                .page = page.page,
            };
            queue.append(node);
        }
    } else unreachable;

    while (queue.popFirst()) |node| {
        defer allocator.destroy(node);

        const data = node.data;
        const page_meta = metadata.pages.getPtr(data.page).?;
        const is_leaf = page_meta.right_child == 0;

        // every page should have atleast one cell
        assert(page_meta.cells.items.len > 0);

        for (page_meta.cells.items, 0..) |cell, slot| {
            const curr_key = cell.key;

            // assert that keys are in order
            if (slot > 0) {
                assert(serde.compare_serialized_bytes(
                    primary_key_data_type,
                    page_meta.cells.items[slot - 1].key,
                    curr_key,
                ) == std.math.Order.lt);
            }

            if (data.min_key) |min_key| {
                assert(serde.compare_serialized_bytes(primary_key_data_type, min_key, curr_key) == std.math.Order.lt);
            }

            if (data.max_key) |max_key| {
                assert(serde.compare_serialized_bytes(primary_key_data_type, curr_key, max_key).compare(.lte));
            }

            // For internal nodes validate cells left_child is non zero and push data inside the queue
            if (!is_leaf) {
                const pivot_cell = cell;
                const pivot_cell_slot = slot;

                // child is in the middle between pivot cell and cell before pivot cell.
                // for the left child all the keys must be less than or equal to pivot cell
                // And all the keys must be greater than one cell before pivot cell.

                assert(pivot_cell.left_child != 0);

                const next_min_key =
                    if (pivot_cell_slot > 0)
                        page_meta.cells.items[pivot_cell_slot - 1].key
                    else
                        data.min_key;

                const next_max_key = pivot_cell.key;

                // append child to queue
                const child_node = try allocator.create(L.Node);
                child_node.data = .{
                    .min_key = next_min_key,
                    .max_key = next_max_key,
                    .page = cell.left_child,
                };
                queue.append(child_node);
            } else {
                // for leaf nodes left_child should be zero
                assert(cell.left_child == 0);
            }

            // validate left and right pointers
            if (page_meta.left != 0) {
                const left = metadata.pages.getPtr(page_meta.left).?;
                assert(left.right == page_meta.page);
            }

            if (page_meta.right != 0) {
                const right = metadata.pages.getPtr(page_meta.right).?;
                assert(right.left == page_meta.page);
            }
        }

        // append right child to queue
        if (!is_leaf) {
            const min_key = page_meta.cells.items[page_meta.cells.items.len - 1].key;
            const max_key = data.max_key;

            const child_node = try allocator.create(L.Node);
            child_node.data = .{
                .min_key = min_key,
                .max_key = max_key,
                .page = page_meta.right_child,
            };
            queue.append(child_node);
        }
    }

    std.debug.print("{}ms VALID\n", .{std.time.milliTimestamp() - t});
}

//test "test tree operations on int primary key" {
//    const database_file = "/Users/shupawar/x/muscle/muscle";

//    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//    const allocator = gpa.allocator();
//    var engine = try execution.ExecutionEngine.init(allocator, database_file);

//    defer {
//        engine.deinit();
//        const deinit_status = gpa.deinit();
//        //fail test; can't try in defer as defer is executed after we return
//        if (deinit_status == .leak) std.testing.expect(false) catch @panic("Memory leak while deiniting");
//    }

//    const table_name = "users";

//    {
//        const table_columns = [_]muscle.Column{
//            muscle.Column{
//                .name = "pk",
//                .data_type = muscle.DataType{ .INT = {} },
//            },
//            muscle.Column{
//                .name = "name",
//                .data_type = muscle.DataType{ .TEXT = 1350 },
//            },
//        };
//        const create_table_query: Query = Query{ .CreateTable = .{
//            .table_name = table_name,
//            .columns = &table_columns,
//            .primary_key_column_index = 0,
//        } };
//        _ = try engine.execute_query(create_table_query);
//    }

//    const value: [1350]u8 = [_]u8{88} ** 1350; // Each leaf page can hold only 2 rows.
//    var values_with_rowid = [_]execution.InsertPayload.Value{
//        .{
//            .column_name = "pk",
//            .value = .{ .INT = 1 },
//        },
//        .{
//            .column_name = "name",
//            .value = .{ .TEXT = &value },
//        },
//    };
//    const values_without_rowid = [_]execution.InsertPayload.Value{
//        .{
//            .column_name = "name",
//            .value = .{ .TEXT = &value },
//        },
//    };

//    const get_insert_query = struct {
//        fn f(rowid: ?muscle.RowId, _values_with_rowid: *[2]execution.InsertPayload.Value) Query {
//            if (rowid) |id| {
//                _values_with_rowid[0].value.INT = id;
//                return Query{ .Insert = .{ .table_name = table_name, .values = _values_with_rowid } };
//            }

//            return Query{ .Insert = .{ .table_name = table_name, .values = &values_without_rowid } };
//        }
//    }.f;

//    var delete_query: Query = Query{ .Delete = .{ .table_name = table_name, .key = undefined } };
//    const select_metadata_query = Query{ .SelectTableMetadata = .{ .table_name = table_name } };
//    const select_database_metadata_query = Query{ .SelectDatabaseMetadata = {} };

//    // Case: Inserting inside root when root is a leaf node.
//    _ = try engine.execute_query(get_insert_query(1, &values_with_rowid));
//    var metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_height == 1);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 1);
//    metadata.data.SelectTableMetadata.deinit();

//    // Case: Deleting when root is a leaf node.
//    delete_query.Delete.key = .{ .INT = 1 };
//    _ = try engine.execute_query(delete_query);

//    metadata = try engine.execute_query(select_metadata_query);
//    assert(metadata.data.SelectTableMetadata.btree_height == 1);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 0);
//    assert(metadata.data.SelectTableMetadata.table_columns.items[0].max_int_value == 1);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    metadata.data.SelectTableMetadata.deinit();

//    // Case: Splitting of root when root is a leaf node.
//    _ = try engine.execute_query(get_insert_query(2, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(3, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(4, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(5, &values_with_rowid));
//    metadata = try engine.execute_query(select_metadata_query);
//    assert(metadata.data.SelectTableMetadata.btree_height == 2);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 4);
//    assert(metadata.data.SelectTableMetadata.table_columns.items[0].max_int_value == 5);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state, 3 is inside root and first leaf has 2, 3 and second leaf has 4, 5
//    // 1:       [3]
//    // 2: [2, 3]   [4, 5]

//    // Case: Distributing the leaf node, Creating new leaf
//    _ = try engine.execute_query(get_insert_query(null, &values_with_rowid));
//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 5);
//    assert(metadata.data.SelectTableMetadata.table_columns.items[0].max_int_value == 6);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.         (1)[ 3,       5]
//    // 2.    (2)[2, 3] (3)[4, 5] (4)[6]
//    //
//    // Total pages = 5

//    // Case: Distributing leaf node, Deleting last leaf
//    delete_query.Delete.key = .{ .INT = 6 };
//    _ = try engine.execute_query(delete_query);
//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 4);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_pages == 2);
//    assert(metadata.data.SelectTableMetadata.btree_internal_cells == 1);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.          (1)[3]
//    // 2.    (2)[2, 3] (3)[4, 5]
//    //
//    // Total pages = 5
//    // Free pages  = [4]

//    metadata = try engine.execute_query(select_database_metadata_query);
//    assert(metadata.?.SelectDatabaseMetadataResult.n_free_pages == 1);
//    assert(metadata.?.SelectDatabaseMetadataResult.first_free_page == 4);

//    // Case: Distributing leaf node, Deleting first leaf
//    // Case: When root turns into leaf node
//    delete_query.Delete.key = .{ .INT = 2 };
//    _ = try engine.execute_query(delete_query);
//    delete_query.Delete.key = .{ .INT = 3 };
//    _ = try engine.execute_query(delete_query);
//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_height == 1);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 2);
//    assert(metadata.data.SelectTableMetadata.btree_internal_pages == 0);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_pages == 1);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.      (1)[4, 5]
//    //
//    // Total pages = 5
//    // Free pages  = [3 -> 2 -> 4]

//    metadata = try engine.execute_query(select_database_metadata_query);
//    assert(metadata.?.SelectDatabaseMetadataResult.n_free_pages == 3);

//    // Case: Distributing leaf node, Deleting middle leaf
//    _ = try engine.execute_query(get_insert_query(1, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(2, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(3, &values_with_rowid));
//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_height == 2);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 5);
//    assert(metadata.data.SelectTableMetadata.btree_internal_pages == 1);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_pages == 3);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.      (1)[2,     4]
//    // 2. (3)[1, 2] (2)[3, 4]  (4)[5]
//    //
//    // Total pages = 5
//    // Free pages  = []

//    metadata = try engine.execute_query(select_database_metadata_query);
//    assert(metadata.?.SelectDatabaseMetadataResult.n_free_pages == 0);

//    delete_query.Delete.key = .{ .INT = 3 };
//    _ = try engine.execute_query(delete_query);
//    delete_query.Delete.key = .{ .INT = 4 };
//    _ = try engine.execute_query(delete_query);
//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    assert(metadata.data.SelectTableMetadata.btree_height == 2);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_cells == 3);
//    assert(metadata.data.SelectTableMetadata.btree_internal_pages == 1);
//    assert(metadata.data.SelectTableMetadata.btree_leaf_pages == 2);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.      (1)[2]
//    // 2. (3)[1, 2] (4)[5]
//    //
//    // Total pages = 5
//    // Free pages  = [2]

//    //// Case: Distributing internal node, Freeing up the internal node
//    _ = try engine.execute_query(get_insert_query(3, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(4, &values_with_rowid));
//    _ = try engine.execute_query(get_insert_query(6, &values_with_rowid));

//    for (0..504) |_| {
//        _ = try engine.execute_query(get_insert_query(null, &values_with_rowid));
//    }

//    _ = try engine.execute_query(get_insert_query(null, &values_with_rowid));

//    metadata = try engine.execute_query(select_metadata_query);
//    try validate_btree(&.data.SelectTableMetadata.?.SelectTableMetadataResult);
//    metadata.data.SelectTableMetadata.deinit();

//    // Tree state
//    // 1.         (1)[2]
//    // 2. (258)[]......(259)[]
//    // 3. ....................  <- third level

//    // Tree state
//    // 1.    (1)[2,       *]
//    // 2. (258)[]. (259)[] (514)[]
//    // 3. ....................  <- third level

//    //for (0..10000) |_| {
//    //    _ = try engine.execute_query(get_insert_query(null, &values_with_rowid));
//    //}

//    //const select_query = Query{ .Select = .{ .table_name = "users" } };
//    //_ = try engine.execute_query(select_query);
//}
