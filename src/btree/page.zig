const std = @import("std");
const muscle = @import("muscle");
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

    pub fn cast_from(bytes: *[muscle.PAGE_SIZE]u8) *const DBMetadataPage {
        const ptr: *DBMetadataPage = @constCast(@ptrCast(@alignCast(bytes)));
        return ptr;
    }

    pub fn to_bytes(self: *const DBMetadataPage) [muscle.PAGE_SIZE]u8 {
        return std.mem.toBytes(self.*);
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
    const HEADER_SIZE = 10;
    const CONTENT_MAX_SIZE = 4086;

    // length of slot array
    num_slots: u16,
    // Offset of the last inserted cell counting from the start of the content
    last_used_offset: u16,
    // for internal btree node the rightmost child node
    right_child: muscle.PageNumber,
    // used space by the content
    // used to determine whether page is overflow or not
    // this tells about the size that is in use.
    // If we have some empty cells in the middle those cells will not account in the calculation of the size
    content_size: u16,
    // content is slot array + cells
    content: [4086]u8,

    // TODO instead of content being fixed length if we have padding parameter here
    // we don't have to work with entire content every time we need to do updates
    // or comparisons.

    comptime {
        assert(@alignOf(Page) == 4);
        assert(@sizeOf(Page) == muscle.PAGE_SIZE);
        assert(HEADER_SIZE == 10);
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
            .content = [_]u8{0} ** CONTENT_MAX_SIZE,
        };
    }

    pub fn cast_from(bytes: *[muscle.PAGE_SIZE]u8) *const Page {
        const ptr: *Page = @constCast(@ptrCast(@alignCast(bytes)));
        return ptr;
    }

    pub fn to_bytes(self: *const Page) [muscle.PAGE_SIZE]u8 {
        return std.mem.toBytes(self.*);
    }

    fn free_space(self: *const Page) u16 {
        return CONTENT_MAX_SIZE - self.content_size;
    }

    const OverflowError = error.Overflow;
    // try to push the cell, if we can't push it we will return Overflow error.
    pub fn push_cell(self: *Page, cell: Cell) OverflowError!void {
        // New cell gets inserted at an last_used_offset
        // 1. if the free_space can't contain sizeOf(u32) + sizeOf(cell) then return
        //      overflow error.
        // 1. If we have enough space to keep the cell we will be placed before
        //      last_used_offset. content_start + cell_size() -> last_used_offset;
        // 2. If we have enough space but we need to defragment, then we will
        //      defragment first and then insert.

        if (self.free_space() < cell.size) {
            return OverflowError;
        }

        const sizeof_slot_array = @sizeOf(u32) * self.num_slots;
        const free_space_before_last_used_offset =
            CONTENT_MAX_SIZE - self.last_used_offset - sizeof_slot_array;

        if (free_space_before_last_used_offset < cell.size) {
            // defragment first and then push
            // not implemented
            unreachable;
        }

        // push
        const slot_index = self.num_slots * @sizeOf(u32);
        // calculate new last_used_offset
        self.last_used_offset = self.last_used_offset - cell.size;
        // keep the cell start at slot_index
        self.content[slot_index] = self.last_used_offset;
        // keep the cell
        const cell_bytes = std.mem.toBytes(cell);
        for (0..cell_bytes.len) |i| {
            self.content[self.last_used_offset + i] = cell_bytes[i];
        }

        self.num_slots += 1;
        self.content_size += cell.size;
    }

    fn cell_header_at_offset() void {}
    fn cell_at_offset() void {}
    // slot array stores offsets for cell headers
    // with slot array we reach to cell header and from their we know the cell size and thus
    // we can convert a slice into `Cell`
    fn cell_at_slot() void {}

    /// Returns the child at the given `index`.
    fn child() void {}

    //fn is_underflow(self: *const Page) bool {}
    //fn is_overflow(self: *const Page) void {}

    fn is_leaf(self: *const Page) bool {
        return self.right_child == 0;
    }

    // arrange cells towords the end
    fn defragment() void {}

    // parse `content` and iterate over `SlotArray` and `Cells`
    pub fn iterate_cells(self: *Page) void {
        _ = self;
    }
};

const Cell = struct {
    // tatal size of cell HEADER_SIZE + content size
    size: u16,
    left_page: muscle.PageNumber,
    is_overflow: bool,
    content: []u8,
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

    pub fn cast_from(bytes: *[muscle.PAGE_SIZE]u8) *const OverflowPage {
        const ptr: *OverflowPage = @constCast(@ptrCast(@alignCast(bytes)));
        return ptr;
    }

    pub fn to_bytes(self: *const OverflowPage) [muscle.PAGE_SIZE]u8 {
        return std.mem.toBytes(self.*);
    }
};

pub const FreePage = extern struct {
    next: muscle.PageNumber,
    padding: [4092]u8,

    comptime {
        assert(@alignOf(FreePage) == 4);
        assert(@sizeOf(FreePage) == muscle.PAGE_SIZE);
    }

    pub fn cast_from(bytes: *const [muscle.PAGE_SIZE]u8) *const FreePage {
        const ptr: *FreePage = @constCast(@ptrCast(@alignCast(bytes)));
        return ptr;
    }

    pub fn to_bytes(self: *const FreePage) [muscle.PAGE_SIZE]u8 {
        return std.mem.toBytes(self.*);
    }
};
