const std = @import("std");
const muscle = @import("muscle");
const serde = @import("../serialize_deserialize.zig");

const print = std.debug.print;
const assert = std.debug.assert;

pub const DBMetadataPage = extern struct {
    total_pages: u32,
    free_pages: u32,
    first_free_page: u32,

    // stringified tables length
    tables_len: u32,
    // stringified tables
    tables: [4080]u8,

    comptime {
        assert(@alignOf(DBMetadataPage) == 4);
        assert(@sizeOf(DBMetadataPage) == muscle.PAGE_SIZE);
    }

    pub fn init() DBMetadataPage {
        return DBMetadataPage{
            .total_pages = 1,
            .free_pages = 0,
            .first_free_page = 0,
            .tables_len = 2, // this is number of characters in json
            .tables = [_]u8{ '[', ']' } ++ [_]u8{0} ** 4078,
        };
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const DBMetadataPage) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }

    pub fn parse_tables(self: *const DBMetadataPage, allocator: std.mem.Allocator) !std.json.Parsed([]muscle.Table) {
        return std.json.parseFromSlice(
            []muscle.Table,
            allocator,
            self.tables[0..self.tables_len],
            .{ .allocate = .alloc_always },
        );
    }

    pub fn set_tables(self: *DBMetadataPage, allocator: std.mem.Allocator, tables: []muscle.Table) !void {
        const json = try std.json.stringifyAlloc(allocator, tables, .{});
        defer allocator.free(json);

        self.tables = [_]u8{0} ** 4080;
        self.tables_len = @intCast(json.len);
        for (json, 0..) |char, i| self.tables[i] = char;
    }
};

//
// ```text
//     count start        last_used_offset
//          |                    |
//          V                    V
// +--------+-------+------------+------+-----+------+
// | HEADER | SLOTS | FREE SPACE | CELL | DEL | CELL |
// +--------+-------+------------+------+-----+------+
//          ^                                        ^
//          |                                        |
//          +-----------------------------------------
//                         Page Content
//
pub const Page = extern struct {
    // All fields except content are part of the Page header
    const HEADER_SIZE = 18;
    pub const CONTENT_MAX_SIZE = 4078;

    // Max offset is CONTENT_MAX_SIZE
    const SlotArrayEntry = u16;
    const SlotArrayIndex = u16;

    // length of slot array
    num_slots: u16,
    // Offset of the last inserted cell counting from the start of the content
    last_used_offset: u16,
    // for internal btree node the rightmost child node
    right_child: muscle.PageNumber,
    // for leaf nodes sibling pointers
    // @space: for internal nodes we are wasting 8 bytes here
    left: muscle.PageNumber,
    right: muscle.PageNumber,
    // content size is size of slot array + cells
    // used space by the content
    // used to determine whether page is overflow or not
    // this tells about the size that is in use.
    // If we have some empty cells in the middle those cells will not account in the calculation of the size
    content_size: u16,
    // content is slot array + cells
    // slot array stores the offset to cells from start of the content.
    // for now they are all u16's.
    content: [4078]u8,

    comptime {
        assert(@alignOf(Page) == 4);
        assert(@sizeOf(Page) == muscle.PAGE_SIZE);
        assert(CONTENT_MAX_SIZE + HEADER_SIZE == muscle.PAGE_SIZE);
    }

    // when we insert some key inside the parent or initially inside the leaf it can
    // overflow before we call balance on it. For that case we actually don't insert it
    // inside the Page content, instead we keep it inside overflow map.
    // TODO I feel like this should not be part of the struct itself and can be kept inside pager OR
    // the btree balance algorithm function argument.
    //temp__overflow_map: std.hash_map.AutoHashMapUnmanaged(SlotIndex, []u8),

    pub fn init() Page {
        return Page{
            .num_slots = 0,
            .last_used_offset = CONTENT_MAX_SIZE,
            .content_size = 0,
            .right_child = 0, // page number
            .left = 0,
            .right = 0,
            .content = [_]u8{0} ** CONTENT_MAX_SIZE,
        };
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const Page) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }

    fn free_space(self: *const Page) u16 {
        return CONTENT_MAX_SIZE - self.content_size;
    }

    fn put_cell_at_offset(self: *Page, cell: Cell, offset: SlotArrayEntry) void {
        // @note: Order is important here as `cell_at_slot` assumes this order later to deserialize
        // palce the cell size
        std.mem.writeInt(
            @TypeOf(cell.size),
            self.content[offset..][0..2],
            cell.size,
            .little,
        );
        // place the left child
        std.mem.writeInt(
            @TypeOf(cell.left_child),
            self.content[offset + 2 ..][0..4],
            cell.left_child,
            .little,
        );
        //  place the cell content
        @memcpy(self.content[offset + 2 + 4 ..][0..cell.content.len], cell.content);
    }

    const OverflowError = error{Overflow};
    // try to insert the cell, if we can't insert it we will return Overflow error.
    pub fn insert(self: *Page, cell: Cell, slot_index: SlotArrayIndex) OverflowError!void {
        if (self.free_space() < cell.size) {
            return error.Overflow;
        }

        const sizeof_slot_array = @sizeOf(SlotArrayEntry) * self.num_slots;
        const free_space_before_last_used_offset = self.last_used_offset - sizeof_slot_array;

        if (free_space_before_last_used_offset < cell.size) {
            // this means that we have space but we need to re-adjust cells
            self.defragment(null);
        }

        // calculate new last_used_offset
        self.last_used_offset = self.last_used_offset - @as(u16, @intCast(cell.size));
        // update slot array
        // if we are inserting somewhere in middle then need to shift elements after the slot_index
        if (slot_index < self.num_slots) {
            var index = self.num_slots;
            while (index >= slot_index) {
                const dest_ptr: *[2]u8 = @ptrCast(&self.content[index]);
                const src_ptr: *[2]u8 = @ptrCast(&self.content[index - 1]);

                std.mem.swap([2]u8, dest_ptr, src_ptr);
                index -= 1;
            }
        }

        std.mem.writeInt(
            SlotArrayEntry,
            self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
            self.last_used_offset,
            .little,
        );

        // place the cell
        self.put_cell_at_offset(cell, self.last_used_offset);

        self.num_slots += 1;
        self.content_size += @sizeOf(SlotArrayEntry);
        self.content_size += @as(u16, @intCast(cell.size));
    }

    pub fn update(self: *Page, cell: Cell, slot_index: SlotArrayIndex) OverflowError!void {
        const old = self.cell_at_slot(slot_index);
        if (cell.size > old.size and self.free_space() < cell.size - old.size) {
            return error.Overflow;
        }

        // @speed: to make life simple we are calling defragment. Not sure this is optimal
        self.defragment(.{ .slot_index = slot_index, .old_size = old.size, .new_size = cell.size });

        // after degramentation, self.last_used_offset should point to the cell we are updating.
        // update slot array
        std.mem.writeInt(SlotArrayEntry, self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)], self.last_used_offset, .little);
        self.put_cell_at_offset(cell, self.last_used_offset);
        self.content_size -= old.size;
        self.content_size += @as(u16, @intCast(cell.size));

        //std.debug.print("After put_cell_at_offset: {any}", .{self});
    }

    // slot array stores offsets for cell headers
    // with slot array we reach to cell header and from their we know the cell size and thus
    // we can convert a slice into `Cell`
    pub fn cell_at_slot(self: *const Page, slot_index: SlotArrayIndex) Cell {
        assert(slot_index < self.num_slots);

        const cell_offset = std.mem.readInt(
            u16,
            self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
            .little,
        );

        // read cell size
        const cell_size = std.mem.readInt(
            u16,
            self.content[cell_offset..][0..@sizeOf(u16)],
            .little,
        );
        const left_child = std.mem.readInt(
            muscle.PageNumber,
            self.content[cell_offset + @sizeOf(u16) ..][0..@sizeOf(muscle.PageNumber)],
            .little,
        );

        const cell = Cell{
            .size = cell_size,
            .left_child = left_child,
            .content = self.content[cell_offset + @sizeOf(u16) + @sizeOf(muscle.PageNumber) ..],
        };

        return cell;
    }

    /// Returns the child at the given `index`.
    pub fn child(self: *const Page, slot_index: SlotArrayIndex) muscle.PageNumber {
        if (slot_index == self.num_slots) {
            return self.right_child;
        } else {
            return self.cell_at_slot(slot_index).left_child;
        }
    }

    const SearchResult = union(enum) {
        found: SlotArrayIndex,
        go_down: SlotArrayIndex,
    };
    pub fn search(
        self: *const Page,
        key: []const u8,
    ) SearchResult {
        if (self.num_slots == 0) {
            return SearchResult{
                .go_down = 0,
            };
        }

        var low: u16 = 0;
        var high: u16 = self.num_slots - 1;
        var mid: u16 = undefined;

        while (low <= high) {
            mid = (low + high) / 2;
            const cell = self.cell_at_slot(mid);

            // TODO
            // we are always comparing RowId
            const value = cell.get_keys_slice(.index);
            const ordering = std.mem.order(u8, key, value);
            switch (ordering) {
                .eq => return SearchResult{ .found = mid },
                .lt => {
                    high = mid - 1;
                },
                .gt => {
                    low = mid + 1;
                },
            }
        }

        return SearchResult{
            .go_down = low,
        };
    }

    //fn is_underflow(self: *const Page) bool {}
    //fn is_overflow(self: *const Page) void {}

    pub fn is_leaf(self: *const Page) bool {
        return self.right_child == 0;
    }

    // arrange cells towords the end
    fn defragment(
        self: *Page,
        updating_cell_info: ?struct { slot_index: SlotArrayEntry, old_size: u16, new_size: u16 },
    ) void {
        //print("before defragmenting {any} last_used_offset: {} updating_cell_info: {any}\n", .{
        //    self.content,
        //    self.last_used_offset,
        //    updating_cell_info,
        //});

        const sizeof_slot_array = @sizeOf(SlotArrayEntry) * self.num_slots;
        const sizeof_cells = self.content_size - sizeof_slot_array;

        var start: u16 = Page.CONTENT_MAX_SIZE - sizeof_cells;
        var updated_content = [_]u8{0} ** Page.CONTENT_MAX_SIZE;

        // if we get updating_cell_info reserve the space at start
        if (updating_cell_info) |info| {
            const delta: i32 = info.new_size - info.old_size;
            start = @intCast(start + delta);
            // update last_used_offset
            self.last_used_offset = start;
            // rest of the cells will be put after this cell
            start += info.new_size;
        } else {
            self.last_used_offset = start;
        }

        var slot_index: u16 = 0;
        var cell: Cell = undefined;
        var cell_offset: u16 = undefined;
        while (slot_index < self.num_slots) : (slot_index += 1) {
            if (updating_cell_info != null) {
                if (slot_index == updating_cell_info.?.slot_index) continue;
            }

            cell = self.cell_at_slot(slot_index);
            cell_offset = std.mem.readInt(
                u16,
                self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
                .little,
            );

            // copy cell and update slot array
            @memcpy(updated_content[start..][0..cell.size], self.content[cell_offset..][0..cell.size]);
            std.mem.writeInt(
                SlotArrayEntry,
                self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
                start,
                .little,
            );

            start += cell.size;
        }

        self.content = updated_content;
        //print("after defragmenting {any} last_used_offset: {}\n", .{ self.content, self.last_used_offset });
    }
};

pub const Cell = struct {
    // tatal size of cell HEADER_SIZE + content size
    // this must be placed at first
    size: u16,
    left_child: muscle.PageNumber,
    //
    // For main table cells, content is (RowId + Row).
    // For indexes cell content is (RowId + Indexed field value).
    // By always storing RowId first we get to actual mapped content by doing content[@sizeOf(RowId)..]
    //
    // slot array element stores the offsets from the start of the content
    content: []const u8,

    pub fn get_keys_slice(self: *const Cell, page_type: enum { index, table }) []const u8 {
        if (page_type == .index) {
            return self.content[0..@sizeOf(muscle.RowId)];
        } else {
            return self.content[@sizeOf(muscle.RowId)..];
        }
    }
};

pub const OverflowPage = extern struct {
    // size of content
    size: u16 = 0,
    // pointer to next free page
    next: u32 = 0,
    // btyes
    content: [4088]u8 = [_]u8{0} ** 4088,

    comptime {
        assert(@alignOf(Page) == 4);
        assert(@sizeOf(Page) == muscle.PAGE_SIZE);
    }

    fn init() OverflowPage {
        return OverflowPage{ .size = 0, .next = 0, .content = [_]u8{0} ** 4088 };
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const OverflowPage) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }
};

pub const FreePage = extern struct {
    next: muscle.PageNumber,
    padding: [4092]u8,

    comptime {
        assert(@alignOf(FreePage) == 4);
        assert(@sizeOf(FreePage) == muscle.PAGE_SIZE);
    }

    pub fn init() FreePage {
        return FreePage{ .next = 0, .padding = undefined };
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const FreePage) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }
};
