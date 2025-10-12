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
    pub const SlotArrayEntry = u16;
    pub const SlotArrayIndex = u16;

    // length of slot array
    num_slots: u16,
    // Offset of the last inserted cell counting from the start of the content
    last_used_offset: u16,
    // for internal btree node the rightmost child node
    right_child: muscle.PageNumber,

    // sibling pointers for both leaf and non leaf nodes
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

    // @Note: this is just to reset content. This does not changes *right_child*, *left* and *right*
    // @Name: Rename to something like reset_content_and_pointers?
    pub fn reset(self: *Page) void {
        self.num_slots = 0;
        self.last_used_offset = CONTENT_MAX_SIZE;
        self.content_size = 0;
    }

    pub fn print(self: *const Page, info: ?[]const u8) void {
        std.debug.print("\n{s}", .{info orelse ""});
        std.debug.print(
            "\t.num_slots = {any}\n\t.last_used_offset = {any}\n\t.content_size = {any}\n\t.free_space = {any}\n\t.right_child = {any}\n\t.left = {any}\n\t.right = {any}\n\n",
            .{ self.num_slots, self.last_used_offset, self.content_size, self.free_space(), self.right_child, self.left, self.right },
        );
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const Page) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }

    pub fn free_space(self: *const Page) u16 {
        return CONTENT_MAX_SIZE - self.content_size;
    }

    // A page is considered underflow if it's less than half full
    pub fn is_underflow(self: *const Page) bool {
        return self.free_space() < CONTENT_MAX_SIZE / 2;
    }

    fn put_cell_at_offset(self: *Page, cell: Cell, offset: SlotArrayEntry) void {
        cell.serialize(self.content[offset .. offset + cell.size]);
    }

    // append the cell at last
    pub fn append(self: *Page, cell: Cell) OverflowError!void {
        try self.insert(cell, self.num_slots);
    }

    const OverflowError = error{Overflow};
    // try to insert the cell, if we can't insert it we will return Overflow error.
    pub fn insert(self: *Page, cell: Cell, slot_index: SlotArrayIndex) OverflowError!void {
        if (self.free_space() < (cell.size + @sizeOf(SlotArrayEntry))) {
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
            while (index > slot_index) {
                const offset1 = (index - 1) * 2;
                const offset2 = index * 2;

                // Swap the two slot indexes
                std.mem.swap(u8, &self.content[offset1], &self.content[offset2]);
                std.mem.swap(u8, &self.content[offset1 + 1], &self.content[offset2 + 1]);
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

    // TODO check if we are using it or not
    pub fn remove(self: *Page, slot_index: SlotArrayIndex) void {
        assert(slot_index < self.num_slots);

        const cell_offset = std.mem.readInt(
            u16,
            self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
            .little,
        );
        const cell_size = std.mem.readInt(
            u16,
            self.content[cell_offset..][0..@sizeOf(u16)],
            .little,
        );

        // update self.content_size
        self.content_size -= cell_size;
        self.content_size -= @sizeOf(SlotArrayEntry);

        // update self.last_used_offset
        if (cell_offset == self.last_used_offset) {
            self.last_used_offset += cell_size;
        }

        // update num slots
        self.num_slots -= 1;

        // update slot array
        for (slot_index..self.num_slots) |index| {
            const offset1 = index * 2;
            const offset2 = (index + 1) * 2;

            // Swap the two slot indexes
            std.mem.swap(u8, &self.content[offset1], &self.content[offset2]);
            std.mem.swap(u8, &self.content[offset1 + 1], &self.content[offset2 + 1]);
        }
    }

    pub fn update(self: *Page, cell: Cell, slot_index: SlotArrayIndex) OverflowError!void {
        const old = self.cell_at_slot(slot_index);
        if (cell.size > old.size and self.free_space() < cell.size - old.size) {
            return error.Overflow;
        }

        // @speed: to make life simple we are calling defragment. this feels non optimal.
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

        return Cell.from_bytes(self.content[cell_offset..]);
    }

    pub fn raw_cell_slice_at_slot(self: *const Page, slot_index: SlotArrayIndex) []const u8 {
        assert(slot_index < self.num_slots);

        const cell_offset = std.mem.readInt(
            u16,
            self.content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
            .little,
        );

        const cell_size = std.mem.readInt(
            u16,
            self.content[cell_offset..][0..@sizeOf(u16)],
            .little,
        );

        return self.content[cell_offset .. cell_offset + cell_size];
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

        var low: i32 = 0;
        var high: i32 = self.num_slots - 1;
        var mid: i32 = undefined;

        while (low <= high) {
            mid = @divFloor(low + high, 2);
            const cell = self.cell_at_slot(@intCast(mid));

            // compare needs to know how each value is serialized
            // for different types comparison will change
            // for now we are always assuming and comparing rowIds
            // we can't use std.mem.order because it compares lexicographically and hence yields incorrect
            // results for little endian byte slices comparisons.
            // we will deserialize and then compare.
            // todo std.mem.order can be used to compare exact equality

            const value_le_slice = cell.get_keys_slice(!self.is_leaf());
            assert(value_le_slice.len == 8);
            const deserialized_value = std.mem.readInt(muscle.RowId, @ptrCast(value_le_slice), .little);
            const deserialized_key = std.mem.readInt(muscle.RowId, @ptrCast(key), .little);

            const ordering = std.math.order(deserialized_key, deserialized_value);
            switch (ordering) {
                .eq => return SearchResult{ .found = @intCast(mid) },
                .lt => {
                    high = mid - 1;
                },
                .gt => {
                    low = mid + 1;
                },
            }
        }

        return SearchResult{
            .go_down = @intCast(low),
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
        //std.debug.print("before defragmenting last_used_offset: {} updating_cell_info: {any}\n", .{
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
            if (delta > 0) {
                // need to leave extra space
                // decrease start
                start = @intCast(start - delta);
            } else {
                // new size is lesser and hence start can move ahead
                start = @intCast(start + delta);
            }
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
            if (updating_cell_info != null and slot_index == updating_cell_info.?.slot_index) continue;

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
                updated_content[slot_index * @sizeOf(SlotArrayEntry) ..][0..@sizeOf(SlotArrayEntry)],
                start,
                .little,
            );

            start += cell.size;
        }

        self.content = updated_content;
        //std.debug.print("after defragmenting last_used_offset: {}\n", .{self.last_used_offset});
    }
};

// TODO start using this limit
// we need to limit size of divider key inside parent in a way so that we have space for atleast two internal cells
pub const INTERNAL_CELL_SIZE_LIMIT = blk: {
    break :blk Page.CONTENT_MAX_SIZE / 2 - @sizeOf(Page.SlotArrayEntry);
};

// Cell struct gives a view over the raw bytes stored inside the `Page.content`
pub const Cell = struct {
    pub const HEADER_SIZE = 6;
    // tatal size of cell. HEADER_SIZE + content size
    // this must be placed at first, see serialize() below.
    size: u16,
    //
    left_child: muscle.PageNumber,
    //
    // For leaf nodes cell content is key and value.
    //  So, For main table cells, content is (RowId + Row).
    //      For index cells, content is (RowId + Indexed field value).
    // By always storing RowId first we get to actual mapped content by doing content[@sizeOf(RowId)..]
    //
    // For internal nodes all cell content is key only.
    //  So, For main table cells, content will be RowId only.
    //      For index cells, content will be Indexed field value.
    // For internal nodes cell content is only the key. And it's simply equal to content size.
    //
    content: []const u8,

    pub fn init(content: []const u8, left_child: ?muscle.PageNumber) Cell {
        return Cell{
            .size = @intCast(HEADER_SIZE + content.len),
            .content = content,
            .left_child = left_child orelse 0,
        };
    }

    // serializes Cell attributes and writes those bytes inside provided slice
    // This does not serialize cell content
    pub fn serialize(self: *const Cell, slice: []u8) void {
        assert(slice.len == self.size);

        std.mem.writeInt(
            @TypeOf(self.size),
            slice[0..2],
            self.size,
            .little,
        );
        // place the left child
        std.mem.writeInt(
            @TypeOf(self.left_child),
            slice[2..6],
            self.left_child,
            .little,
        );

        //  place the cell content
        @memcpy(slice[6..slice.len], self.content);
    }

    // TODO: rename to init_from_bytes
    pub fn from_bytes(slice: []const u8) Cell {
        // read cell size
        const cell_size = std.mem.readInt(
            u16,
            slice[0..@sizeOf(u16)],
            .little,
        );

        const left_child = std.mem.readInt(
            muscle.PageNumber,
            slice[@sizeOf(u16)..][0..@sizeOf(muscle.PageNumber)],
            .little,
        );

        // TODO we have to do -6 below every time. Can the size be only about the cell.content and not header + content?
        return Cell{ .size = cell_size, .left_child = left_child, .content = slice[6..][0 .. cell_size - 6] };
    }

    pub fn get_keys_slice(
        self: *const Cell,
        // whether this cell belongs to internal page or leaf page
        is_internal_page_cell: bool,
        // whether this cell is for index page or main table data page
        // is_index_page_cell: bool,
    ) []const u8 {
        // for internal nodes all cell content is key for both data and index pages
        if (is_internal_page_cell) {
            return self.content;
        }

        // for index page, key is indexed field value
        // if (is_index_page_cell) {
        //     return self.content[@sizeOf(muscle.RowId)..];
        // }

        // for data page(leaf nodes), key is rowId which gets stored at start
        return self.content[0..@sizeOf(muscle.RowId)];
    }
};

pub const OverflowPage = extern struct {
    // size of content
    size: u16 = 0,
    // pointer to next free page
    next: muscle.PageNumber = 0,
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
        return FreePage{ .next = 0, .padding = [_]u8{0} ** 4092 };
    }

    // return a serialized bytes
    pub fn to_bytes(self: *const FreePage) ![muscle.PAGE_SIZE]u8 {
        return try serde.serialize_page(self.*);
    }
};
