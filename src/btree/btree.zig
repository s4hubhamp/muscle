const std = @import("std");
const muscle = @import("muscle");
const Pager = @import("./pager.zig").Pager;
const page = @import("./page.zig");

// Btree gets the root page and will do search, insert, delete operations
// execution engine will provide the root page information to btree
// that means execution engine needs to have access to file
pub const BTree = struct {
    allocator: std.mem.Allocator,
    pager: *Pager,

    pub fn init(pager: *Pager, allocator: std.mem.Allocator) BTree {
        return BTree{ .pager = pager, .allocator = allocator };
    }

    const PathDetail = struct {
        parent: muscle.PageNumber,
        child: muscle.PageNumber, // this is here alongside the child_index to avoid array access
        child_index: u16, // this is the index in `.children` array.
    };

    pub fn search(
        self: *BTree,
        root: muscle.PageNumber,
        key: []const u8,
    ) !std.ArrayList(PathDetail) {
        var path = std.ArrayList(PathDetail).init(self.allocator);
        var page_number = root;
        var node = try self.pager.get_page(page.Page, page_number);
        const is_leaf = node.is_leaf();

        // steps:
        // 1. iterate over the cells inside this page and call
        // compare function

        // while we don't reach to leaf node
        while (!is_leaf) {
            const search_result = node.search(key);
            const child_index = sw: switch (search_result) {
                .found => |i| {
                    break :sw i;
                },
                .go_down => |i| {
                    break :sw i;
                },
            };
            const path_detail = PathDetail{
                .parent = page_number,
                .child_index = child_index,
                .child = node.child(child_index),
            };
            page_number = path_detail.child;
            node = try self.pager.get_page(page.Page, page_number);
            try path.append(path_detail);
        }

        return path;
    }

    pub fn insert(
        self: *BTree,
        root: muscle.PageNumber,
        key: []const u8,
        cell: page.Cell,
    ) !void {
        var path = try self.search(root, key);
        var leaf = root;
        var leaf_index: u16 = undefined;
        var leaf_parent: muscle.PageNumber = undefined;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent = leaf_parent_info.parent;
            leaf_index = leaf_parent_info.child_index;
            leaf = leaf_parent_info.child;
        }

        var leaf_node = (try self.pager.get_page(page.Page, leaf)).*;

        switch (leaf_node.search(key)) {
            .found => |insert_at_slot| {
                std.debug.print("found: {} \n", .{insert_at_slot});
                leaf_node.update(cell, insert_at_slot) catch {
                    try self.balance(
                        leaf,
                        leaf_parent,
                        leaf_index,
                        &path,
                        .{ .cell = cell, .should_be_inserted_at = insert_at_slot },
                    );
                };
            },
            .go_down => |insert_at_slot| {
                std.debug.print("go down: {} \n", .{insert_at_slot});
                leaf_node.insert(cell, insert_at_slot) catch {
                    try self.balance(
                        leaf,
                        leaf_parent,
                        leaf_index,
                        &path,
                        .{ .cell = cell, .should_be_inserted_at = insert_at_slot },
                    );
                };

                try self.pager.update_page(leaf, leaf_node.to_bytes());
            },
        }
    }

    fn balance(
        self: *BTree,
        child_page_number: muscle.PageNumber,
        // null for root page
        parent_page_number: ?muscle.PageNumber,
        child_index: ?u16,
        path: *std.ArrayList(PathDetail),
        // present when we can't insert cell inside child
        overflowing_cell_info: ?struct { cell: page.Cell, should_be_inserted_at: u16 },
    ) !void {
        _ = self;
        _ = child_page_number;
        _ = parent_page_number;
        _ = child_index;
        _ = path;
        _ = overflowing_cell_info;
    }
};

const UnbalancedPage = struct {
    main: muscle.PageNumber,
    overflow_map: []struct {},
};
