const std = @import("std");
const muscle = @import("muscle");
const Pager = @import("./pager.zig").Pager;
const page = @import("./page.zig");

const assert = std.debug.assert;

// Btree gets the root page and will do search, insert, delete operations
// execution engine will provide the root page information to btree
// that means execution engine needs to have access to file
pub const BTree = struct {
    metadata: *page.DBMetadataPage,
    allocator: std.mem.Allocator,
    pager: *Pager,

    pub fn init(pager: *Pager, metadata: *page.DBMetadataPage, allocator: std.mem.Allocator) BTree {
        return BTree{ .pager = pager, .metadata = metadata, .allocator = allocator };
    }

    pub fn deinit(self: *BTree) !void {
        _ = self;
        // TODO
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

        // while we don't reach to leaf node
        while (!node.is_leaf()) {
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
        cell: page.Cell,
    ) !void {
        const key = cell.get_keys_slice(false);
        var path = try self.search(root, key);
        defer path.deinit();

        var leaf = root;
        var leaf_index: ?u16 = null;
        var leaf_parent: ?muscle.PageNumber = null;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent = leaf_parent_info.parent;
            leaf_index = leaf_parent_info.child_index;
            leaf = leaf_parent_info.child;
        }

        var leaf_node = try self.pager.get_page(page.Page, leaf);

        switch (leaf_node.search(key)) {
            .found => |update_at_slot| {
                //std.debug.print("update_at_slot: {} \n", .{update_at_slot});
                // leaf_node.update(cell, update_at_slot) catch {};
                // try self.pager.update_page(leaf, &leaf_node);

                try self.balance(
                    leaf,
                    leaf_parent,
                    leaf_index,
                    &path,
                    .{ .FIRST_LEAF_OPERATION = .{
                        .UPDATE = .{
                            .cell = cell,
                            .update_at_slot = update_at_slot,
                        },
                    } },
                );
            },
            .go_down => |insert_at_slot| {
                //std.debug.print("btree.insert() insert_at_slot: {} insert_inside_page: {}\n", .{ insert_at_slot, leaf });
                // leaf_node.update(cell, update_at_slot) catch {};
                // try self.pager.update_page(leaf, &leaf_node);

                try self.balance(
                    leaf,
                    leaf_parent,
                    leaf_index,
                    &path,
                    .{ .FIRST_LEAF_OPERATION = .{
                        .INSERT = .{
                            .cell = cell,
                            .insert_at_slot = insert_at_slot,
                        },
                    } },
                );
            },
        }

        //std.debug.print("Saved updated page: {any}\n", .{leaf_node});
    }

    pub fn delete(
        self: *BTree,
        root: muscle.PageNumber,
        key: []const u8,
    ) !void {
        var path = try self.search(root, key);
        defer path.deinit();

        var leaf = root;
        var leaf_index: ?u16 = null;
        var leaf_parent: ?muscle.PageNumber = null;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent = leaf_parent_info.parent;
            leaf_index = leaf_parent_info.child_index;
            leaf = leaf_parent_info.child;
        }

        var leaf_node = try self.pager.get_page(page.Page, leaf);

        switch (leaf_node.search(key)) {
            .found => |delete_at_slot| {
                try self.balance(
                    leaf,
                    leaf_parent,
                    leaf_index,
                    &path,
                    .{ .FIRST_LEAF_OPERATION = .{
                        .DELETE = .{
                            .delete_at_slot = delete_at_slot,
                        },
                    } },
                );
            },
            .go_down => |_| {
                return error.NotFound;
            },
        }

        //std.debug.print("Saved updated page: {any}\n", .{leaf_node});
    }

    const CellInfoForBalance = struct {
        // raw cell bytes
        cell_bytes: std.ArrayList(u8),
        slot_index: u16,
        operation: enum { insert, update, delete },
    };
    const LeafOperation = union(enum) {
        INSERT: struct {
            cell: page.Cell,
            insert_at_slot: page.Page.SlotArrayIndex,
        },
        UPDATE: struct {
            cell: page.Cell,
            update_at_slot: page.Page.SlotArrayIndex,
        },
        DELETE: struct {
            delete_at_slot: page.Page.SlotArrayIndex,
        },
    };
    const DistributionInfo = std.BoundedArray(struct {
        sibling_slot_index: page.Page.SlotArrayIndex, // TODO rename
        // zero indicates we freed sibling page
        sibling_page_number: muscle.PageNumber, // TODO rename
    }, MAX_LOADED_SIBLINGS);
    const BTreeChangeInfo = struct {
        distribution_info: DistributionInfo,
        // new page should get attached after last sibling
        new_page_number: ?muscle.PageNumber = null,
    };
    fn balance(
        self: *BTree,
        child_page_number: muscle.PageNumber, // the page we are balancing
        parent_page_number: ?muscle.PageNumber, // null for root page
        child_index_inside_parent: ?u16, // null for root node
        path: *std.ArrayList(PathDetail),
        leaf_operation_or_prev_change_info: union(enum) {
            FIRST_LEAF_OPERATION: LeafOperation,
            CHANGE_INFO: BTreeChangeInfo,
        },
    ) !void {

        // TODO I don't like this name
        var child = try self.pager.get_page(page.Page, child_page_number);

        const is_leaf = child.is_leaf();
        const is_root = parent_page_number == null;

        if (is_root) {
            if (is_leaf) {
                var save = true;
                switch (leaf_operation_or_prev_change_info.FIRST_LEAF_OPERATION) {
                    .INSERT => |data| {
                        child.insert(data.cell, data.insert_at_slot) catch {
                            save = false;
                            try self.split_leaf_root(&child, child_page_number, true, data.cell, data.insert_at_slot);
                        };

                        if (save) try self.pager.update_page(child_page_number, &child);
                    },

                    .UPDATE => |data| {
                        child.update(data.cell, data.update_at_slot) catch {
                            save = false;
                            try self.split_leaf_root(&child, child_page_number, false, data.cell, data.update_at_slot);
                        };
                    },

                    .DELETE => |data| {
                        child.remove(data.delete_at_slot);
                        save = true;
                    },
                }

                if (save) try self.pager.update_page(child_page_number, &child);
            } else {
                std.debug.print("change_info new_page_number: {any}, distribution_info: {any}\n", .{ leaf_operation_or_prev_change_info.CHANGE_INFO.new_page_number, leaf_operation_or_prev_change_info.CHANGE_INFO.distribution_info.constSlice() });

                // we need to know if we can fit updated cells and the new page inside root
                // if we can fit them then we don't need to create new left and right.

                const firstElement = leaf_operation_or_prev_change_info.CHANGE_INFO.distribution_info.constSlice()[0];

                if (firstElement.sibling_page_number == 0) {
                    // TODO
                    // we may be freeing more than one sibling in one go, that case isn't handled yet
                    if (leaf_operation_or_prev_change_info.CHANGE_INFO.distribution_info.len > 1) {
                        unreachable;
                    }

                    // if this is right child
                    if (firstElement.sibling_slot_index == child.num_slots) {
                        // left_child of last cell will become new right child
                        const last_cell = child.cell_at_slot(child.num_slots - 1);
                        child.right_child = last_cell.left_child;
                        child.remove(child.num_slots - 1);
                    } else {
                        child.remove(firstElement.sibling_slot_index);
                    }

                    // if root becomes completely empty
                    if (child.num_slots == 0) {
                        // copy the right_child stuff inside root and free right child
                        const right_child = child.right_child;
                        child = try self.pager.get_page(page.Page, right_child);
                        try self.pager.free(self.metadata, right_child);
                        child.right_child = 0;
                    }

                    try self.pager.update_page(child_page_number, child);
                } else {
                    const space_needed = try self.get_needed_space(&child, &leaf_operation_or_prev_change_info.CHANGE_INFO);

                    // if we can fit everything
                    if (space_needed <= page.Page.CONTENT_MAX_SIZE) {
                        try self.update_internal_node_with_change_info(
                            &child,
                            child_page_number,
                            &leaf_operation_or_prev_change_info.CHANGE_INFO,
                        );
                    } else {
                        // we will come here when divider key updates cannot fit into root or we can't fit new divider cell for new page
                        try self.split_internal_root(
                            &child,
                            child_page_number,
                            leaf_operation_or_prev_change_info.CHANGE_INFO,
                        );
                    }
                }
            }

            return;
        }

        var next_change_info: ?BTreeChangeInfo = null;

        if (is_leaf) {
            var save = true;
            switch (leaf_operation_or_prev_change_info.FIRST_LEAF_OPERATION) {
                .INSERT => |data| {
                    child.insert(data.cell, data.insert_at_slot) catch {
                        save = false;

                        // 1. load siblings and distribute the cells
                        // 2. after distribution if we create new page or free some page then pass that info to parent.

                        var siblings = try Siblings.init(0);
                        try self.load_siblings(parent_page_number.?, child_index_inside_parent.?, &siblings);

                        // distribute
                        next_change_info = try self.distribute_leafs(&siblings, true, data.cell, child_page_number, data.insert_at_slot);
                    };
                },

                .UPDATE => |data| {
                    child.update(data.cell, data.update_at_slot) catch {
                        save = false;

                        var siblings = try Siblings.init(0);
                        try self.load_siblings(parent_page_number.?, child_index_inside_parent.?, &siblings);

                        // distribute
                        next_change_info = try self.distribute_leafs(&siblings, false, data.cell, child_page_number, data.update_at_slot);
                    };
                },

                .DELETE => |data| {
                    child.remove(data.delete_at_slot);

                    // need to check if the child becomes empty after deleting an key
                    // if it does then we will need to free this page and balance the tree
                    if (child.num_slots == 0) {
                        next_change_info = BTreeChangeInfo{
                            .distribution_info = try DistributionInfo.init(0),
                        };
                        try next_change_info.?.distribution_info.append(.{ .sibling_page_number = 0, .sibling_slot_index = child_index_inside_parent.? });
                        try self.free_page(child_page_number);
                        save = false;
                    }
                },
            }

            if (save) try self.pager.update_page(child_page_number, &child);
        } else {
            // internal nodes

            const firstElement = leaf_operation_or_prev_change_info.CHANGE_INFO.distribution_info.constSlice()[0];

            if (firstElement.sibling_page_number == 0) {
                // TODO
                // we may be freeing more than one sibling in one go, that case isn't handled yet
                if (leaf_operation_or_prev_change_info.CHANGE_INFO.distribution_info.len > 1) {
                    unreachable;
                }

                // if this is right child
                if (firstElement.sibling_slot_index == child.num_slots) {
                    // left_child of last cell will become new right child
                    const last_cell = child.cell_at_slot(child.num_slots - 1);
                    child.right_child = last_cell.left_child;
                    child.remove(child.num_slots - 1);
                } else {
                    child.remove(firstElement.sibling_slot_index);
                }

                if (child.num_slots == 0) {
                    // TODO
                    // here we need to rebalance siblings or try to bring parent key down?
                    unreachable;
                } else {
                    try self.pager.update_page(child_page_number, child);
                }
            } else {
                const space_needed = try self.get_needed_space(&child, &leaf_operation_or_prev_change_info.CHANGE_INFO);

                child.print("child before update");
                std.debug.print("space_needed: {any} curr_space: {any}\n", .{ space_needed, child.content_size });

                if (space_needed <= page.Page.CONTENT_MAX_SIZE) {
                    try self.update_internal_node_with_change_info(
                        &child,
                        child_page_number,
                        &leaf_operation_or_prev_change_info.CHANGE_INFO,
                    );
                } else {

                    // 1. load siblings
                    // 2.

                    var siblings = try Siblings.init(0);
                    try self.load_siblings(parent_page_number.?, child_index_inside_parent.?, &siblings);

                    next_change_info = try self.distribute_internal_node(
                        &siblings,
                        &leaf_operation_or_prev_change_info.CHANGE_INFO,
                    );
                }
            }
        }

        // no further balancing is needed
        if (next_change_info == null) {
            return;
        }

        // call balance on the parent
        if (path.pop()) |parent_info| {
            try self.balance(
                parent_info.child,
                parent_info.parent,
                parent_info.child_index,
                path,
                .{ .CHANGE_INFO = next_change_info.? },
            );
        } else {
            try self.balance(
                parent_page_number.?,
                null,
                null,
                path,
                .{ .CHANGE_INFO = next_change_info.? },
            );
        }
    }

    const MAX_LOADED_SIBLINGS = 3;
    const Siblings = std.BoundedArray(struct { page_number: muscle.PageNumber, page: page.Page, slot_index: page.Page.SlotArrayIndex }, MAX_LOADED_SIBLINGS);
    // TODO we can just use left and right pointers instead of using parent.child().
    // TODO do we need to return first child index?
    fn load_siblings(
        self: *BTree,
        parent_page_number: muscle.PageNumber,
        child_index: u16,
        siblings: *Siblings,
    ) !void {
        const parent = try self.pager.get_page(page.Page, parent_page_number);

        var num_siblings_per_side: u8 = 1;
        if (child_index == 0 or child_index == parent.num_slots) num_siblings_per_side = 2;

        for (child_index -| num_siblings_per_side..child_index + num_siblings_per_side + 1) |index| {
            if (index > parent.num_slots) break;

            const page_number = parent.child(@intCast(index));
            try siblings.append(.{ .page_number = page_number, .page = try self.pager.get_page(page.Page, page_number), .slot_index = @intCast(index) });
        }
    }

    fn free_page(self: *BTree, page_number: muscle.PageNumber) !void {
        const freeing = try self.pager.get_page(page.Page, page_number);
        if (freeing.left > 0) {
            var left = try self.pager.get_page(page.Page, freeing.left);
            left.right = freeing.right;
            try self.pager.update_page(freeing.left, &left);
        }

        if (freeing.right > 0) {
            var right = try self.pager.get_page(page.Page, freeing.right);
            right.left = freeing.left;
            try self.pager.update_page(freeing.right, &right);
        }

        try self.pager.free(self.metadata, page_number);
    }

    // this updates the node assuming the needed space is available
    fn update_internal_node_with_change_info(
        self: *BTree,
        node: *page.Page,
        node_page_number: muscle.PageNumber,
        change_info: *const BTreeChangeInfo,
    ) !void {
        for (change_info.distribution_info.constSlice()) |info| {
            if (info.sibling_page_number == 0) {
                // TODO
                // some siblings may have updated and some may have freed, this case is not handled yet
                unreachable;
            }

            // if sibling is last one we don't need to have divider key
            if (info.sibling_slot_index == node.num_slots) {
                break;
            }

            const sibling_node = try self.pager.get_page(page.Page, info.sibling_page_number);
            const last_cell = sibling_node.cell_at_slot(sibling_node.num_slots - 1);
            const divider_cell = page.Cell.init(last_cell.get_keys_slice(!sibling_node.is_leaf()), info.sibling_page_number);

            try node.update(divider_cell, info.sibling_slot_index);
        }

        // if we have new page then add new divider key after last sibling
        if (change_info.new_page_number) |new_page_number| {
            const last_sibling_info = change_info.distribution_info.get(change_info.distribution_info.len - 1);

            var divider_cell: page.Cell = undefined;

            // if the last sibling is the right child
            if (last_sibling_info.sibling_slot_index == node.num_slots) {
                assert(last_sibling_info.sibling_page_number == node.right_child);

                const curr_last_node = try self.pager.get_page(page.Page, node.right_child);
                const last_cell = curr_last_node.cell_at_slot(curr_last_node.num_slots - 1);

                divider_cell = page.Cell.init(
                    last_cell.get_keys_slice(!curr_last_node.is_leaf()),
                    node.right_child,
                );

                try node.append(divider_cell);
                node.right_child = new_page_number;
            } else {
                const new_page_node = try self.pager.get_page(page.Page, new_page_number);
                const last_cell = new_page_node.cell_at_slot(new_page_node.num_slots - 1);

                divider_cell = page.Cell.init(
                    last_cell.get_keys_slice(!new_page_node.is_leaf()),
                    new_page_number,
                );

                try node.insert(divider_cell, last_sibling_info.sibling_slot_index + 1);
            }
        }

        try self.pager.update_page(node_page_number, node);
    }

    fn get_needed_space(self: *BTree, curr_state: *page.Page, change_info: *const BTreeChangeInfo) !isize {
        var space_needed: isize = curr_state.content_size;

        // Below for loop just calculates the space needed
        for (change_info.distribution_info.constSlice()) |info| {
            if (info.sibling_page_number == 0) {
                // TODO
                // some siblings may have updated and some may have freed, this case is not handled yet
                unreachable;
            }

            // if the sibling is right_child then we don't have it's divider cell
            if (info.sibling_slot_index == curr_state.num_slots) {
                break;
            }

            const node = try self.pager.get_page(page.Page, info.sibling_page_number);
            const last_cell = node.cell_at_slot(node.num_slots - 1);
            const new_cell_size = last_cell.get_keys_slice(!node.is_leaf()).len + page.Cell.HEADER_SIZE;
            const old_cell_size = curr_state.cell_at_slot(info.sibling_slot_index).size;

            space_needed += @intCast(new_cell_size - old_cell_size);
        }

        if (change_info.new_page_number) |new_page_number| {
            const node = try self.pager.get_page(page.Page, new_page_number);
            const last_cell = node.cell_at_slot(node.num_slots - 1);
            const new_cell_size = last_cell.get_keys_slice(!node.is_leaf()).len + page.Cell.HEADER_SIZE +
                @sizeOf(page.Page.SlotArrayEntry);
            space_needed += @intCast(new_cell_size);
        }

        return space_needed;
    }

    fn distribute_leafs(
        self: *BTree,
        siblings: *Siblings,
        is_new_cell: bool,
        cell: page.Cell,
        cell_page_number: muscle.PageNumber,
        cell_slot_index: page.Page.SlotArrayIndex,
    ) !BTreeChangeInfo {
        //
        // @Speed
        // we can improve to first determine whether if we distribute would that make enough
        // space, If not then we don't need to distribute.

        var raw_cells = std.ArrayList([]u8).init(self.allocator);

        defer {
            for (raw_cells.items) |slice| {
                self.allocator.free(slice);
            }
            raw_cells.deinit();
        }

        // collect cells
        for (siblings.slice()) |*s| {
            for (0..s.page.num_slots) |slot| {
                var bytes_slice: []u8 = undefined;

                if (s.page_number == cell_page_number and slot == cell_slot_index) {
                    // if it's a new cell then we need to add this new cell before existing cell
                    if (is_new_cell) {
                        bytes_slice = try self.allocator.alloc(u8, cell.size);
                        cell.serialize(bytes_slice);
                        try raw_cells.append(bytes_slice);

                        // add existing cell
                        bytes_slice = try self.allocator.dupe(u8, s.page.raw_cell_slice_at_slot(@intCast(slot)));
                        try raw_cells.append(bytes_slice);
                    } else {
                        // skip adding existing cell and only add new cell
                        bytes_slice = try self.allocator.alloc(u8, cell.size);
                        cell.serialize(bytes_slice);
                        try raw_cells.append(bytes_slice);
                    }
                } else {
                    bytes_slice = try self.allocator.dupe(u8, s.page.raw_cell_slice_at_slot(@intCast(slot)));
                    try raw_cells.append(bytes_slice);
                }
            }

            // if we need to insert at last
            if (s.page_number == cell_page_number and cell_slot_index == s.page.num_slots) {
                const bytes_slice = try self.allocator.alloc(u8, cell.size);
                cell.serialize(bytes_slice);
                try raw_cells.append(bytes_slice);
            }

            // reset sibling
            s.page.reset();
        }

        var change_info = BTreeChangeInfo{
            .distribution_info = try DistributionInfo.init(0),
        };

        // distribute
        // TODO Before we distribute the cells it would be great if we make sure the pages are in ascending order
        var curr: u16 = 0;
        var last_filled_sibling_index: u16 = 0;

        for (siblings.slice(), 0..) |*sib, i| {
            if (curr == raw_cells.items.len) {
                try self.free_page(sib.page_number);
                // mark siblings as free
                try change_info.distribution_info.append(.{
                    .sibling_slot_index = sib.slot_index,
                    .sibling_page_number = 0,
                });

                continue;
            }

            last_filled_sibling_index = @intCast(i);

            while (curr < raw_cells.items.len) {
                var failed = false;
                sib.page.append(page.Cell.from_bytes(raw_cells.items[curr])) catch |err| {
                    assert(err == error.Overflow);
                    failed = true;
                };

                if (failed) {
                    try change_info.distribution_info.append(.{
                        .sibling_slot_index = sib.slot_index,
                        .sibling_page_number = sib.page_number,
                    });

                    // move to next sibling
                    break;
                }

                curr += 1;
            }

            // save this sibling
            try self.pager.update_page(sib.page_number, &sib.page);
        }

        // if we still have cells we need to create new page
        if (curr < raw_cells.items.len) {
            const new_page_number = try self.pager.alloc_free_page(self.metadata);
            var new_page = page.Page.init();

            while (curr < raw_cells.items.len) {
                try new_page.append(page.Cell.from_bytes(raw_cells.items[curr]));
                curr += 1;
            }

            var last_sibling = &siblings.slice()[siblings.len - 1];

            // update pointers
            new_page.right = last_sibling.page.right;
            last_sibling.page.right = new_page_number;
            new_page.left = last_sibling.page_number;

            // save both last sibling and new page
            try self.pager.update_page(last_sibling.page_number, &last_sibling.page);
            try self.pager.update_page(new_page_number, &new_page);

            change_info.new_page_number = new_page_number;
        } else {
            // when we have freed some siblings we need to adjust left, right pointers
            if (last_filled_sibling_index < siblings.len - 1) {
                //var last_filled_sibling = &siblings.get(last_filled_sibling_index);
                var last_filled_sibling = &siblings.buffer[last_filled_sibling_index];
                const last_sibling = siblings.get(siblings.len - 1);
                var page_after_last_sibling = try self.pager.get_page(page.Page, last_sibling.page.right);

                last_filled_sibling.page.right = last_sibling.page.right;
                page_after_last_sibling.left = last_filled_sibling.page_number;

                // save
                try self.pager.update_page(last_filled_sibling.page_number, &last_filled_sibling.page);
                try self.pager.update_page(last_sibling.page.right, &page_after_last_sibling);
            }
        }

        return change_info;
    }

    fn split_internal_root(
        self: *BTree,
        root: *page.Page,
        root_page_number: muscle.PageNumber,
        change_info: BTreeChangeInfo,
    ) !void {
        // when internal root can only hold one cell
        if (root.num_slots == 1) {
            unreachable;
        }

        var raw_cells = std.ArrayList([]u8).init(self.allocator);

        defer {
            for (raw_cells.items) |slice| {
                self.allocator.free(slice);
            }
            raw_cells.deinit();
        }

        // first collect all cells before siblings
        const first_sibling_index = change_info.distribution_info.get(0).sibling_slot_index;
        for (0..first_sibling_index) |slot| {
            var bytes_slice: []u8 = undefined;

            bytes_slice = try self.allocator.dupe(u8, root.raw_cell_slice_at_slot(@intCast(slot)));
            try raw_cells.append(bytes_slice);
        }

        var skip_new_page_cell = false;

        // siblings are updated so get the updated last cells
        for (change_info.distribution_info.constSlice()) |sibling_info| {
            // handle the last sibling
            if (sibling_info.sibling_slot_index == root.num_slots) {
                // last sibling is right_child so it doesn't need a cell
                // but if we do have a new page after last sibling we will create a cell here itself
                if (change_info.new_page_number) |new_page_number| {
                    root.right_child = new_page_number;
                    skip_new_page_cell = true;
                } else {
                    break;
                }
            }

            var cell: page.Cell = undefined;
            var bytes_slice: []u8 = undefined;

            const sibling = try self.pager.get_page(page.Page, sibling_info.sibling_page_number);
            cell = sibling.cell_at_slot(sibling.num_slots - 1);
            cell = page.Cell.init(cell.get_keys_slice(!sibling.is_leaf()), sibling_info.sibling_page_number);
            bytes_slice = try self.allocator.alloc(u8, cell.size);
            cell.serialize(bytes_slice);
            try raw_cells.append(bytes_slice);
        }

        // if new page is added need to add cell
        if (change_info.new_page_number) |new_page_number| {
            if (!skip_new_page_cell) {
                // cell content depends new page
                var cell: page.Cell = undefined;

                const new_page = try self.pager.get_page(page.Page, new_page_number);
                cell = new_page.cell_at_slot(new_page.num_slots - 1);
                cell = page.Cell.init(cell.get_keys_slice(!new_page.is_leaf()), new_page_number);
                const bytes_slice = try self.allocator.alloc(u8, cell.size);
                cell.serialize(bytes_slice);
                try raw_cells.append(bytes_slice);
            }
        }

        // collect remaning cells from last_sibling_index .. root.num_slots
        const last_sibling_index = change_info.distribution_info.get(change_info.distribution_info.len - 1).sibling_slot_index;
        if (last_sibling_index + 1 < root.num_slots) {
            for (last_sibling_index + 1..root.num_slots) |slot| {
                var bytes_slice: []u8 = undefined;

                bytes_slice = try self.allocator.dupe(u8, root.raw_cell_slice_at_slot(@intCast(slot)));
                try raw_cells.append(bytes_slice);
            }
        }

        // distribute
        {
            var left = page.Page.init();
            var right = page.Page.init();
            var new_root = page.Page.init();

            const left_page_number = try self.pager.alloc_free_page(self.metadata);
            const right_page_number = try self.pager.alloc_free_page(self.metadata);

            // we need to have atleast one cell inside right so our logic is designed for that
            // TODO: we can probably try to improve the logic and try to distribute such that difference between left's size and right's size is as minimal as possible

            assert(raw_cells.items.len > 2);

            var fill_right = false;
            for (raw_cells.items, 0..) |cell_bytes, i| {
                var cell = page.Cell.from_bytes(cell_bytes);

                if (raw_cells.items.len - i == 2) {
                    fill_right = true;
                }

                if (!fill_right) {
                    left.append(cell) catch |err| {
                        assert(err == error.Overflow);
                        fill_right = true;
                    };
                } else {
                    if (new_root.num_slots == 0) {
                        left.right_child = cell.left_child;
                        cell.left_child = left_page_number;
                        try new_root.append(cell);
                        new_root.right_child = right_page_number;
                    } else {
                        try right.append(cell);
                    }
                }
            }

            right.right_child = root.right_child;
            left.right = right_page_number;
            right.left = left_page_number;

            // After distribution we should have atleast one cell inside right
            assert(right.num_slots > 0);

            try self.pager.update_page(root_page_number, &new_root);
            try self.pager.update_page(left_page_number, &left);
            try self.pager.update_page(right_page_number, &right);
        }
    }

    // TODO can we merge with split_internal_node
    fn distribute_internal_node(
        self: *BTree,
        siblings: *Siblings,
        change_info: *const BTreeChangeInfo,
    ) !?BTreeChangeInfo {
        var next_change_info = BTreeChangeInfo{
            .distribution_info = try DistributionInfo.init(0),
        };
        var raw_cells = std.ArrayList([]u8).init(self.allocator);

        defer {
            for (raw_cells.items) |slice| {
                self.allocator.free(slice);
            }
            raw_cells.deinit();
        }

        // first collect all cells inside the siblings
        // for all right_child's we will create a cell so that it's easy when we start putting them
        for (siblings.slice()) |*sibling_info| {
            var sibling = &sibling_info.page;
            var bytes_slice: []u8 = undefined;

            for (0..sibling.num_slots) |slot| {
                bytes_slice = try self.allocator.dupe(u8, sibling.raw_cell_slice_at_slot(@intCast(slot)));
                try raw_cells.append(bytes_slice);
            }

            {
                // Note: this can be also obtained from just copying cell from parent if parent is available here we can skip loading right_child
                const right_child = try self.pager.get_page(page.Page, sibling.right_child);
                const cell = page.Cell.init(
                    right_child.cell_at_slot(right_child.num_slots - 1).get_keys_slice(!right_child.is_leaf()),
                    sibling.right_child,
                );
                bytes_slice = try self.allocator.alloc(u8, cell.size);
                cell.serialize(bytes_slice);
                try raw_cells.append(bytes_slice);
            }

            // reset
            sibling.reset();
        }

        if (change_info.new_page_number) |new_page_number| {
            const new_page = try self.pager.get_page(page.Page, new_page_number);
            const cell = page.Cell.init(
                new_page.cell_at_slot(new_page.num_slots - 1).get_keys_slice(!new_page.is_leaf()),
                new_page_number,
            );
            const bytes_slice = try self.allocator.alloc(u8, cell.size);
            cell.serialize(bytes_slice);
            try raw_cells.append(bytes_slice);
        }

        // put the cells starting from first sibling until sibling gets full
        // when sibling gets full we will attach right child and record change for parent
        // continue this until all cells are filled or we don't have any siblings left to fill
        // Maybe create new node or free

        // distribute
        // TODO Before we distribute the cells it would be great if we make sure the pages are in ascending order
        var curr: u16 = 0;
        var last_filled_sibling_index: u16 = 0;
        for (siblings.slice(), 0..) |*sib, i| {
            if (curr == raw_cells.items.len) {
                try self.free_page(sib.page_number);
                // mark siblings as free
                try next_change_info.distribution_info.append(.{
                    .sibling_slot_index = sib.slot_index,
                    .sibling_page_number = 0,
                });

                continue;
            }

            last_filled_sibling_index = @intCast(i);

            while (curr < raw_cells.items.len) {
                var attach_right_child_and_move_to_next_sibling = false;

                // if we have only three cells left and we can't fit all inside current sibling then we will need to
                // use one for right_child, and other two for next sibling
                if (raw_cells.items.len - curr == 3) {
                    attach_right_child_and_move_to_next_sibling = true;
                }

                // attach last one as right_child
                if (raw_cells.items.len - curr == 1) {
                    attach_right_child_and_move_to_next_sibling = true;
                }

                sib.page.append(page.Cell.from_bytes(raw_cells.items[curr])) catch |err| {
                    assert(err == error.Overflow);
                    attach_right_child_and_move_to_next_sibling = true;
                };

                if (attach_right_child_and_move_to_next_sibling) {
                    // attach right child and record change_info
                    const cell = page.Cell.from_bytes(raw_cells.items[curr]);
                    sib.page.right_child = cell.left_child;

                    try next_change_info.distribution_info.append(.{
                        .sibling_slot_index = sib.slot_index,
                        .sibling_page_number = sib.page_number,
                    });

                    curr += 1;
                    // move to next sibling
                    break;
                }

                curr += 1;
            }

            // save this sibling
            try self.pager.update_page(sib.page_number, &sib.page);
        }

        // if we still have cells we need to create new page
        if (curr < raw_cells.items.len) {
            // we can't have only have one cell at this point, number can be > 1 or 0
            assert(raw_cells.items.len - curr != 1);

            const new_page_number = try self.pager.alloc_free_page(self.metadata);
            var new_page = page.Page.init();

            for (curr..raw_cells.items.len - 1) |i| {
                try new_page.append(page.Cell.from_bytes(raw_cells.items[i]));
            }

            // set last one as right_child
            {
                const cell = page.Cell.from_bytes(raw_cells.items[raw_cells.items.len - 1]);
                new_page.right_child = cell.left_child;
            }

            var last_sibling = &siblings.slice()[siblings.len - 1];

            // update pointers
            new_page.right = last_sibling.page.right;
            last_sibling.page.right = new_page_number;
            new_page.left = last_sibling.page_number;

            // save both last sibling and new page
            try self.pager.update_page(last_sibling.page_number, &last_sibling.page);
            try self.pager.update_page(new_page_number, &new_page);

            next_change_info.new_page_number = new_page_number;
        } else {
            // when we have freed some siblings we need to adjust left, right pointers
            if (last_filled_sibling_index < siblings.len - 1) {
                //var last_filled_sibling = &siblings.get(last_filled_sibling_index);
                var last_filled_sibling = &siblings.buffer[last_filled_sibling_index];
                const last_sibling = siblings.get(siblings.len - 1);
                var page_after_last_sibling = try self.pager.get_page(page.Page, last_sibling.page.right);

                last_filled_sibling.page.right = last_sibling.page.right;
                page_after_last_sibling.left = last_filled_sibling.page_number;

                // save
                try self.pager.update_page(last_filled_sibling.page_number, &last_filled_sibling.page);
                try self.pager.update_page(last_sibling.page.right, &page_after_last_sibling);
            }
        }

        return next_change_info;
    }

    // TODO fixate on better name?
    fn split_leaf_root(
        self: *BTree,
        root: *page.Page,
        root_page_number: muscle.PageNumber,
        is_new_cell: bool,
        cell: page.Cell,
        slot_index: page.Page.SlotArrayIndex,
    ) !void {
        var left = page.Page.init();
        var right = page.Page.init();
        const left_page_number = try self.pager.alloc_free_page(self.metadata);
        const right_page_number = try self.pager.alloc_free_page(self.metadata);

        for (0..root.num_slots) |curr_slot| {
            if (curr_slot == slot_index) {
                left.append(cell) catch |err| {
                    assert(err == error.Overflow);
                    try right.append(cell);
                };

                if (!is_new_cell) continue;
            }

            const curr_cell = root.cell_at_slot(@intCast(curr_slot));
            left.append(curr_cell) catch |err| {
                assert(err == error.Overflow);
                try right.append(curr_cell);
            };
        }

        // if the cell is meant to be inserted at last
        if (slot_index == root.num_slots) {
            left.append(cell) catch |err| {
                assert(err == error.Overflow);
                try right.append(cell);
            };
        }

        // pivot is the last cell inside the left
        const pivot_cell = page.Cell.init(
            left.cell_at_slot(left.num_slots - 1).get_keys_slice(false),
            left_page_number,
        );

        // reset the root
        root.reset();
        // attach right page
        root.right_child = right_page_number;
        // attach left page
        try root.append(pivot_cell);

        // set left and right pointers
        left.right = right_page_number;
        right.left = left_page_number;

        // persist
        try self.pager.update_page(root_page_number, root);
        try self.pager.update_page(left_page_number, &left);
        try self.pager.update_page(right_page_number, &right);
    }
};

const UnbalancedPage = struct {
    main: muscle.PageNumber,
    overflow_map: []struct {},
};
