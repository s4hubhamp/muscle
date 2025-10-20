const std = @import("std");
const muscle = @import("muscle");
const Pager = @import("./pager.zig").Pager;
const page = @import("./page.zig");

const assert = std.debug.assert;

/// B-tree implementation for managing sorted data with search, insert, and delete operations.
/// The execution engine provides the root page number, allowing the B-tree to navigate the page hierarchy.
/// All operations maintain B-tree invariants through automatic balancing and splitting/merging of pages.
pub const BTree = struct {
    metadata: *page.DBMetadataPage,
    allocator: std.mem.Allocator,
    pager: *Pager,
    root_page_number: muscle.PageNumber,
    // data type of *key* used inside the btree
    primary_key_data_type: muscle.DataType,

    pub fn init(
        pager: *Pager,
        metadata: *page.DBMetadataPage,
        root_page_number: muscle.PageNumber,
        primary_key_data_type: muscle.DataType,
        allocator: std.mem.Allocator,
    ) BTree {
        return BTree{
            .pager = pager,
            .metadata = metadata,
            .allocator = allocator,
            .root_page_number = root_page_number,
            .primary_key_data_type = primary_key_data_type,
        };
    }

    pub fn deinit(self: *BTree) !void {
        _ = self;
    }

    const PathDetail = struct {
        parent: muscle.PageNumber,
        child: muscle.PageNumber, // this is here alongside the child_index to avoid array access
        child_index: u16, // this is the index in `.children` array.
    };

    pub fn search(self: *const BTree, key: []const u8) !std.ArrayList(PathDetail) {
        var path = std.ArrayList(PathDetail).init(self.allocator);
        var page_number = self.root_page_number;
        var node = try self.pager.get_page(page.Page, page_number);

        // while we don't reach to leaf node
        while (!node.is_leaf()) {
            const search_result = node.search(key, self.primary_key_data_type);

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
                .child = node.child_at_slot(child_index),
            };
            page_number = path_detail.child;
            node = try self.pager.get_page(page.Page, page_number);
            try path.append(path_detail);
        }

        return path;
    }

    pub fn insert(self: *BTree, key: []const u8, cell_bytes: []const u8) !void {
        var path = try self.search(key);
        defer path.deinit();

        var target_leaf_page = self.root_page_number;
        var leaf_index_in_parent: ?u16 = null;
        var leaf_parent_page: ?muscle.PageNumber = null;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent_page = leaf_parent_info.parent;
            leaf_index_in_parent = leaf_parent_info.child_index;
            target_leaf_page = leaf_parent_info.child;
        }

        var leaf_node = try self.pager.get_page(page.Page, target_leaf_page);
        const cell = page.Cell.init(cell_bytes, null);

        switch (leaf_node.search(key, self.primary_key_data_type)) {
            .found => |_| {
                return error.DuplicateKey;
            },
            .go_down => |insert_at_slot| {
                try self.balance(
                    target_leaf_page,
                    leaf_parent_page,
                    leaf_index_in_parent,
                    &path,
                    .{ .LEAF_OPERATION = .{
                        .INSERT = .{
                            .cell = cell,
                            .insert_at_slot = insert_at_slot,
                        },
                    } },
                );
            },
        }
    }

    pub fn update(self: *BTree, cell: page.Cell) !void {
        const key = cell.get_keys_slice(false);
        var path = try self.search(key);
        defer path.deinit();

        var target_leaf_page = self.root_page_number;
        var leaf_index_in_parent: ?u16 = null;
        var leaf_parent_page: ?muscle.PageNumber = null;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent_page = leaf_parent_info.parent;
            leaf_index_in_parent = leaf_parent_info.child_index;
            target_leaf_page = leaf_parent_info.child;
        }

        var leaf_node = try self.pager.get_page(page.Page, target_leaf_page);

        switch (leaf_node.search(key)) {
            .found => |update_at_slot| {
                try self.balance(
                    target_leaf_page,
                    leaf_parent_page,
                    leaf_index_in_parent,
                    &path,
                    .{ .LEAF_OPERATION = .{
                        .UPDATE = .{
                            .cell = cell,
                            .update_at_slot = update_at_slot,
                        },
                    } },
                );
            },
            .go_down => |_| {
                return error.NotFound;
            },
        }
    }

    pub fn delete(self: *BTree, key: []const u8) !void {
        var path = try self.search(key);
        defer path.deinit();

        var target_leaf_page = self.root_page_number;
        var leaf_index_in_parent: ?u16 = null;
        var leaf_parent_page: ?muscle.PageNumber = null;

        if (path.pop()) |leaf_parent_info| {
            leaf_parent_page = leaf_parent_info.parent;
            leaf_index_in_parent = leaf_parent_info.child_index;
            target_leaf_page = leaf_parent_info.child;
        }

        var leaf_node = try self.pager.get_page(page.Page, target_leaf_page);

        switch (leaf_node.search(key, self.primary_key_data_type)) {
            .found => |delete_at_slot| {
                try self.balance(
                    target_leaf_page,
                    leaf_parent_page,
                    leaf_index_in_parent,
                    &path,
                    .{ .LEAF_OPERATION = .{
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
    }

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
    const SiblingUpdateInfo = std.BoundedArray(struct {
        sibling_slot_index: page.Page.SlotArrayIndex,
        // zero indicates we freed sibling page
        sibling_page_number: muscle.PageNumber,
    }, MAX_LOADED_SIBLINGS);
    const TreeChangeInfo = struct {
        sibling_updates: SiblingUpdateInfo,
        // new page should get attached after last sibling
        newly_created_page: ?muscle.PageNumber = null,
    };
    fn balance(
        self: *BTree,
        target_page_number: muscle.PageNumber, // the page we are balancing
        parent_page_number: ?muscle.PageNumber, // null for root page
        target_page_slot_inside_parent: ?u16, // null for root node
        path: *std.ArrayList(PathDetail),
        modification_request: union(enum) {
            LEAF_OPERATION: LeafOperation,
            // @Todo
            PROPAGATED_CHANGES: TreeChangeInfo,
        },
    ) !void {
        var target_page = try self.pager.get_page(page.Page, target_page_number);
        const is_leaf = target_page.is_leaf();
        const is_root = parent_page_number == null;

        if (is_root) {
            if (is_leaf) {
                var save = true;
                switch (modification_request.LEAF_OPERATION) {
                    .INSERT => |data| {
                        target_page.insert(data.cell, data.insert_at_slot) catch {
                            save = false;
                            try self.split_leaf_root(&target_page, target_page_number, true, data.cell, data.insert_at_slot);
                        };
                    },

                    .UPDATE => |data| {
                        target_page.update(data.cell, data.update_at_slot) catch {
                            save = false;
                            try self.split_leaf_root(&target_page, target_page_number, false, data.cell, data.update_at_slot);
                        };
                    },

                    .DELETE => |data| {
                        target_page.remove(data.delete_at_slot);
                        save = true;
                    },
                }

                if (save) try self.pager.update_page(target_page_number, &target_page);
            } else {
                const space_needed = try self.calculate_space_after_modifications(
                    &target_page,
                    &modification_request.PROPAGATED_CHANGES,
                );

                if (space_needed <= page.Page.CONTENT_MAX_SIZE) {
                    try self.update_internal_node_with_change_info(
                        &target_page,
                        &modification_request.PROPAGATED_CHANGES,
                    );

                    // if root becomes empty after update, then right child will become new root
                    if (target_page.content_size == 0) {
                        // copy the right_child stuff inside root and free right child
                        const right_child = target_page.right_child;
                        assert(right_child > 0);
                        target_page = try self.pager.get_page(page.Page, right_child);
                        try self.pager.free(self.metadata, right_child);
                    }

                    // persist
                    try self.pager.update_page(target_page_number, target_page);
                } else {
                    try self.split_internal_root(
                        &target_page,
                        target_page_number,
                        modification_request.PROPAGATED_CHANGES,
                    );
                }
            }

            return;
        }

        // @Todo
        var next_change_info: ?TreeChangeInfo = null;

        if (is_leaf) {
            var save = true;
            switch (modification_request.LEAF_OPERATION) {
                .INSERT => |data| {
                    target_page.insert(data.cell, data.insert_at_slot) catch {
                        save = false;

                        var siblings = try LoadedSiblings.init(0);
                        try self.load_siblings(parent_page_number.?, target_page_slot_inside_parent.?, &siblings);

                        // distribute
                        next_change_info = try self.modify_leaf(&siblings, true, data.cell, target_page_number, data.insert_at_slot);
                    };
                },

                .UPDATE => |data| {
                    target_page.update(data.cell, data.update_at_slot) catch {
                        save = false;

                        var siblings = try LoadedSiblings.init(0);
                        try self.load_siblings(parent_page_number.?, target_page_slot_inside_parent.?, &siblings);

                        // distribute
                        next_change_info = try self.modify_leaf(&siblings, false, data.cell, target_page_number, data.update_at_slot);
                    };
                },

                .DELETE => |data| {
                    target_page.remove(data.delete_at_slot);

                    // need to check if the child becomes empty after deleting an key
                    // if it does then we will need to free this page and balance the tree
                    if (target_page.num_slots == 0) {
                        // @Todo
                        next_change_info = TreeChangeInfo{
                            .sibling_updates = try SiblingUpdateInfo.init(0),
                        };
                        try next_change_info.?.sibling_updates.append(
                            .{
                                .sibling_page_number = 0,
                                .sibling_slot_index = target_page_slot_inside_parent.?,
                            },
                        );
                        try self.free_page(target_page_number);
                        save = false;
                    }
                },
            }

            if (save) try self.pager.update_page(target_page_number, &target_page);
        } else {
            // internal nodes

            const space_needed = try self.calculate_space_after_modifications(
                &target_page,
                &modification_request.PROPAGATED_CHANGES,
            );

            if (space_needed == 0 or space_needed > page.Page.CONTENT_MAX_SIZE) {
                var siblings = try LoadedSiblings.init(0);
                try self.load_siblings(parent_page_number.?, target_page_slot_inside_parent.?, &siblings);

                next_change_info = try self.modify_internal(
                    &siblings,
                    target_page_number,
                    &modification_request.PROPAGATED_CHANGES,
                );
            } else {
                try self.update_internal_node_with_change_info(
                    &target_page,
                    &modification_request.PROPAGATED_CHANGES,
                );

                // assert that we should not end up removing all cells
                assert(target_page.num_slots > 0);

                // persist
                try self.pager.update_page(target_page_number, target_page);
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
                .{ .PROPAGATED_CHANGES = next_change_info.? },
            );
        } else {
            try self.balance(
                parent_page_number.?,
                null,
                null,
                path,
                .{ .PROPAGATED_CHANGES = next_change_info.? },
            );
        }
    }

    const MAX_LOADED_SIBLINGS = 3;
    const LoadedSiblings = std.BoundedArray(struct { page_number: muscle.PageNumber, page: page.Page, slot_index: page.Page.SlotArrayIndex }, MAX_LOADED_SIBLINGS);
    // @Perf we can just use left and right pointers instead of using parent
    fn load_siblings(
        self: *BTree,
        parent_page_number: muscle.PageNumber,
        child_index: u16,
        siblings: *LoadedSiblings,
    ) !void {
        const parent = try self.pager.get_page(page.Page, parent_page_number);

        var num_siblings_per_side: u8 = 1;
        if (child_index == 0 or child_index == parent.num_slots) num_siblings_per_side = 2;

        for (child_index -| num_siblings_per_side..child_index + num_siblings_per_side + 1) |index| {
            if (index > parent.num_slots) break;

            const page_number = parent.child_at_slot(@intCast(index));
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

    fn get_divider_key(self: *const BTree, node: *const page.Page) []const u8 {
        const cell = node.cell_at_slot(node.num_slots - 1);
        return cell.get_keys_slice(!node.is_leaf(), self.primary_key_data_type);
    }

    // this updates the node assuming the needed space is available
    fn update_internal_node_with_change_info(
        self: *BTree,
        node: *page.Page,
        // @Todo
        change_info: *const TreeChangeInfo,
    ) !void {
        for (change_info.sibling_updates.constSlice()) |info| {
            if (info.sibling_page_number == 0) {
                // we have freed child attached to this node
                // we should also let go of this divider key

                // if we have freed right child then attach left_child of last as new right child and free existing last slot
                if (info.sibling_slot_index == node.num_slots) {
                    const last_cell = node.cell_at_slot(node.num_slots - 1);
                    node.right_child = last_cell.left_child;
                    node.remove(node.num_slots - 1);
                } else {
                    node.remove(info.sibling_slot_index);
                }
            } else {

                // if sibling is last one we don't need to update divider key
                if (info.sibling_slot_index == node.num_slots) {
                    break;
                }

                const sibling_node = try self.pager.get_page(page.Page, info.sibling_page_number);
                const divider_cell = page.Cell.init(self.get_divider_key(&sibling_node), info.sibling_page_number);

                try node.update(divider_cell, info.sibling_slot_index);
            }
        }

        // if we have new page then add new divider key after last sibling
        if (change_info.newly_created_page) |new_page_number| {
            const last_sibling_info = change_info.sibling_updates.get(change_info.sibling_updates.len - 1);

            var divider_cell: page.Cell = undefined;

            // if the last sibling is the right child
            if (last_sibling_info.sibling_slot_index == node.num_slots) {
                assert(last_sibling_info.sibling_page_number == node.right_child);

                const curr_last_node = try self.pager.get_page(page.Page, node.right_child);
                divider_cell = page.Cell.init(self.get_divider_key(&curr_last_node), node.right_child);

                try node.append(divider_cell);
                node.right_child = new_page_number;
            } else {
                const new_page_node = try self.pager.get_page(page.Page, new_page_number);
                divider_cell = page.Cell.init(self.get_divider_key(&new_page_node), new_page_number);

                try node.insert(divider_cell, last_sibling_info.sibling_slot_index + 1);
            }
        }
    }

    // this tells how much resultant space will become after incorporating
    // prev distribution change with existing cells
    fn calculate_space_after_modifications(
        self: *BTree,
        curr_state: *page.Page,
        change_info: *const TreeChangeInfo,
    ) !usize {
        var space_needed: usize = 0;
        var last_increment: usize = 0;

        const first_updated_slot_index = change_info.sibling_updates.get(0).sibling_slot_index;
        const last_updated_slot_index =
            change_info.sibling_updates.get(change_info.sibling_updates.len - 1).sibling_slot_index;

        for (0..first_updated_slot_index) |slot_index| {
            const cell = curr_state.cell_at_slot(@intCast(slot_index));
            last_increment = cell.size + @sizeOf(page.Page.SlotArrayEntry);
            space_needed += last_increment;
        }

        for (change_info.sibling_updates.constSlice()) |info| {
            // for right child we don't need space
            // but if the page was previously freed then previous cell will give new right child and hence
            // we need to subtract space
            if (info.sibling_slot_index == curr_state.num_slots and info.sibling_page_number > 0)
                break;

            // if we have freed some siblings
            if (info.sibling_page_number == 0) {
                // if we have freed right child then previous key is not needed
                if (info.sibling_slot_index == curr_state.num_slots) {
                    assert(last_increment > 0);
                    space_needed -= last_increment;
                }
            } else {
                const node = try self.pager.get_page(page.Page, info.sibling_page_number);
                const cell_size = self.get_divider_key(&node).len + page.Cell.HEADER_SIZE;
                last_increment = cell_size + @sizeOf(page.Page.SlotArrayEntry);
                space_needed += last_increment;
            }
        }

        if (last_updated_slot_index + 1 < curr_state.num_slots) {
            for (last_updated_slot_index + 1..curr_state.num_slots) |slot_index| {
                const cell = curr_state.cell_at_slot(@intCast(slot_index));
                last_increment = cell.size + @sizeOf(page.Page.SlotArrayEntry);
                space_needed += last_increment;
            }
        }

        if (change_info.newly_created_page) |new_page_number| {
            const node = try self.pager.get_page(page.Page, new_page_number);
            const new_cell_size = self.get_divider_key(&node).len + page.Cell.HEADER_SIZE;
            space_needed += @intCast(new_cell_size);
            space_needed += @sizeOf(page.Page.SlotArrayEntry);
        }

        return space_needed;
    }

    fn modify_leaf(
        self: *BTree,
        siblings: *LoadedSiblings,
        is_new_cell: bool,
        cell: page.Cell,
        cell_page_number: muscle.PageNumber,
        cell_slot_index: page.Page.SlotArrayIndex,
    ) !TreeChangeInfo {
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
            s.page.reset_content();
        }

        var change_info = TreeChangeInfo{
            .sibling_updates = try SiblingUpdateInfo.init(0),
        };

        // distribute
        // @Perf Before we distribute the cells it would be great if we make sure the pages are in ascending order
        var curr: u16 = 0;
        var last_filled_sibling_index: u16 = 0;

        for (siblings.slice(), 0..) |*sib, i| {
            if (curr == raw_cells.items.len) {
                try self.free_page(sib.page_number);
                // mark siblings as free
                try change_info.sibling_updates.append(.{
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
                    try change_info.sibling_updates.append(.{
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

            change_info.newly_created_page = new_page_number;
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
        change_info: TreeChangeInfo,
    ) !void {
        var raw_cells = std.ArrayList([]u8).init(self.allocator);

        defer {
            for (raw_cells.items) |slice| {
                self.allocator.free(slice);
            }
            raw_cells.deinit();
        }

        // first collect all cells before siblings
        const first_sibling_index = change_info.sibling_updates.get(0).sibling_slot_index;
        for (0..first_sibling_index) |slot| {
            var bytes_slice: []u8 = undefined;

            bytes_slice = try self.allocator.dupe(u8, root.raw_cell_slice_at_slot(@intCast(slot)));
            try raw_cells.append(bytes_slice);
        }

        var skip_new_page_cell = false;

        // siblings are updated so get the updated last cells
        for (change_info.sibling_updates.constSlice()) |sibling_info| {
            // handle the last sibling
            if (sibling_info.sibling_slot_index == root.num_slots) {
                // last sibling is right_child so it doesn't need a cell
                // but if we do have a new page after last sibling we will create a cell here itself
                if (change_info.newly_created_page) |new_page_number| {
                    root.right_child = new_page_number;
                    skip_new_page_cell = true;
                } else {
                    break;
                }
            }

            var cell: page.Cell = undefined;
            var bytes_slice: []u8 = undefined;

            const sibling = try self.pager.get_page(page.Page, sibling_info.sibling_page_number);
            cell = page.Cell.init(
                self.get_divider_key(&sibling),
                sibling_info.sibling_page_number,
            );
            bytes_slice = try self.allocator.alloc(u8, cell.size);
            cell.serialize(bytes_slice);
            try raw_cells.append(bytes_slice);
        }

        // if new page is added need to add cell
        if (change_info.newly_created_page) |new_page_number| {
            if (!skip_new_page_cell) {
                // cell content depends new page
                var cell: page.Cell = undefined;

                const new_page = try self.pager.get_page(page.Page, new_page_number);
                cell = page.Cell.init(self.get_divider_key(&new_page), new_page_number);
                const bytes_slice = try self.allocator.alloc(u8, cell.size);
                cell.serialize(bytes_slice);
                try raw_cells.append(bytes_slice);
            }
        }

        // collect remaning cells from last_sibling_index .. root.num_slots
        const last_sibling_index = change_info.sibling_updates.get(change_info.sibling_updates.len - 1).sibling_slot_index;
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
            // @Perf: we can probably try to improve the logic and try to distribute such that difference between left's size and right's size is as minimal as possible

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

    fn modify_internal(
        self: *BTree,
        siblings: *LoadedSiblings,
        // for which page number below change info is for
        change_info_page_number: muscle.PageNumber,
        // previously done updates to page which is amongst loaded siblings
        change_info: *const TreeChangeInfo,
    ) !TreeChangeInfo {
        var next_change_info = TreeChangeInfo{
            .sibling_updates = try SiblingUpdateInfo.init(0),
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
            const sibling_page_number = sibling_info.page_number;
            var sibling = &sibling_info.page;
            var bytes_slice: []u8 = undefined;

            if (sibling_page_number == change_info_page_number) {
                const first_updated_slot_index = change_info.sibling_updates.get(0).sibling_slot_index;
                const last_updated_slot_index =
                    change_info.sibling_updates.get(change_info.sibling_updates.len - 1).sibling_slot_index;

                // collect cells before update
                for (0..first_updated_slot_index) |slot_index| {
                    bytes_slice = try self.allocator.dupe(u8, sibling.raw_cell_slice_at_slot(@intCast(slot_index)));
                    try raw_cells.append(bytes_slice);
                }

                // collect latest with change info
                for (change_info.sibling_updates.constSlice()) |info| {
                    // collect latest info for pages which we didn't free
                    if (info.sibling_page_number == 0) break;

                    const node = try self.pager.get_page(page.Page, info.sibling_page_number);
                    const cell = page.Cell.init(self.get_divider_key(&node), info.sibling_page_number);
                    bytes_slice = try self.allocator.alloc(u8, cell.size);
                    cell.serialize(bytes_slice);
                    try raw_cells.append(bytes_slice);
                }

                // new ones should be added before adding remaining
                if (change_info.newly_created_page) |new_page_number| {
                    const new_page = try self.pager.get_page(page.Page, new_page_number);
                    const cell = page.Cell.init(self.get_divider_key(&new_page), new_page_number);
                    bytes_slice = try self.allocator.alloc(u8, cell.size);
                    cell.serialize(bytes_slice);
                    try raw_cells.append(bytes_slice);
                }

                // collect remaining slots
                if (last_updated_slot_index + 1 < sibling.num_slots + 1) {
                    for (last_updated_slot_index + 1..sibling.num_slots + 1) |slot_index| {
                        // check if this is right child
                        if (slot_index == sibling.num_slots) {
                            const right_child = try self.pager.get_page(page.Page, sibling.right_child);
                            const cell = page.Cell.init(self.get_divider_key(&right_child), sibling.right_child);
                            bytes_slice = try self.allocator.alloc(u8, cell.size);
                            cell.serialize(bytes_slice);
                            try raw_cells.append(bytes_slice);
                        } else {
                            bytes_slice = try self.allocator.dupe(
                                u8,
                                sibling.raw_cell_slice_at_slot(@intCast(slot_index)),
                            );
                            try raw_cells.append(bytes_slice);
                        }
                    }
                }
            } else {
                for (0..sibling.num_slots) |slot| {
                    bytes_slice = try self.allocator.dupe(u8, sibling.raw_cell_slice_at_slot(@intCast(slot)));
                    try raw_cells.append(bytes_slice);
                }

                {
                    // Note: this can be also obtained from just copying cell from parent if parent is available here we can skip loading right_child
                    const right_child = try self.pager.get_page(page.Page, sibling.right_child);
                    const cell = page.Cell.init(self.get_divider_key(&right_child), sibling.right_child);
                    bytes_slice = try self.allocator.alloc(u8, cell.size);
                    cell.serialize(bytes_slice);
                    try raw_cells.append(bytes_slice);
                }
            }

            // reset
            sibling.reset_content();
        }

        // put the cells starting from first sibling until sibling gets full
        // when sibling gets full we will attach right child and record change for parent
        // continue this until all cells are filled or we don't have any siblings left to fill
        // Maybe create new node or free

        // distribute
        // @Perf Before we distribute the cells it would be great if we make sure the pages are in ascending order
        var curr: u16 = 0;
        var last_filled_sibling_index: u16 = 0;
        for (siblings.slice(), 0..) |*sib, i| {
            if (curr == raw_cells.items.len) {
                try self.free_page(sib.page_number);
                // mark siblings as free
                try next_change_info.sibling_updates.append(.{
                    .sibling_slot_index = sib.slot_index,
                    .sibling_page_number = 0,
                });

                continue;
            }

            last_filled_sibling_index = @intCast(i);

            while (curr < raw_cells.items.len) {
                var attach_right_child_and_move_to_next_sibling = false;

                // if we have only have three cells left and we can't fit two inside current sibling
                // then we will need to use one for right_child, and other two for next sibling
                if (raw_cells.items.len - curr == 3) {
                    // need to check if we can fit two in this node itself
                    var needed_space: usize = 0;
                    needed_space += page.Cell.from_bytes(raw_cells.items[0]).size;
                    needed_space += page.Cell.from_bytes(raw_cells.items[1]).size;
                    needed_space += 2 * @sizeOf(page.Page.SlotArrayEntry);

                    if (needed_space > sib.page.free_space()) {
                        attach_right_child_and_move_to_next_sibling = true;
                    }
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

                    try next_change_info.sibling_updates.append(.{
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
            // we shouldn't have only have one cell at this point
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

            next_change_info.newly_created_page = new_page_number;
        }
        // when we have freed some siblings we need to adjust left, right pointers
        else if (last_filled_sibling_index < siblings.len - 1) {
            var last_filled_sibling = &siblings.buffer[last_filled_sibling_index];
            const last_sibling = siblings.get(siblings.len - 1);
            var page_after_last_sibling = try self.pager.get_page(page.Page, last_sibling.page.right);

            last_filled_sibling.page.right = last_sibling.page.right;
            page_after_last_sibling.left = last_filled_sibling.page_number;

            // save
            try self.pager.update_page(last_filled_sibling.page_number, &last_filled_sibling.page);
            try self.pager.update_page(last_sibling.page.right, &page_after_last_sibling);
        }

        return next_change_info;
    }

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
        const pivot_cell = page.Cell.init(self.get_divider_key(&left), left_page_number);

        // reset the root
        root.reset_content();
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
