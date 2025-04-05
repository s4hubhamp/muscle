const std = @import("std");
const print = std.debug.print;
const assert = std.debug.assert;

const muscle = @import("../muscle.zig");
const page = @import("./pager.zig");
const PageNumber = page.PageNumber;
const SlotIndex = page.SlotIndex;
const PAGE_SIZE = page.PAGE_SIZE;

// The whole db metadata is stringified json
// TODO this can be packed to avoid heap references
pub const DBMetadataPage = extern struct {
    total_pages: u32,
    free_pages: u32,
    first_free_page: u32,

    tables_len: u32,
    // stringified tables
    tables: [4080]u8,

    comptime {
        assert(@alignOf(DBMetadataPage) == 4);
        assert(@sizeOf(DBMetadataPage) == 4096);
    }

    pub fn init() DBMetadataPage {
        return DBMetadataPage{
            .total_pages = 1,
            .free_pages = 0,
            .first_free_page = 0,
            .tables_len = 2,
            .tables = [_]u8{ '[', ']' } ++ [_]u8{0} ** 4078,
        };
    }

    pub fn parse_tables(self: *DBMetadataPage, allocator: std.mem.Allocator) !std.json.Parsed([]muscle.Table) {
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

    pub fn remove_table() void {}
    pub fn add_index() void {}
    pub fn remove_index() void {}
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
    const HEADER_SIZE = 12;

    // length of slot array
    num_slots: u16,
    // Offset of the last inserted cell counting from the start
    last_used_offset: u16,
    // free space is 4096 - size - (size of header fields = 16)
    // used to determine whether page is underflow or not
    free_space: u16,
    // size of the content only
    // used to determine whether page is overflow or not
    // this tells about the size that is in use.
    // If we have some empty cells in the middle those cells will not account in the calculation of the size
    size: u16,
    // for internal btree node the rightmost child node
    right_child: u32, // page number
    // content is slot array + cells
    content: [4084]u8,

    // TODO instead of content being fixed length if we have padding parameter here
    // we don't have to work with entire content every time we need to do updates
    // or comparisons.

    comptime {
        assert(@alignOf(Page) == 4);
        assert(@sizeOf(Page) == 4096);
        assert(HEADER_SIZE == 12);
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
            .last_used_offset = 0,
            .size = 0,
            .free_space = 4084,
            .right_child = 0, // page number
            .content = [_]u8{0} ** 4084,
        };
    }

    fn cell_header_at_offset() void {}
    fn cell_at_offset() void {}
    // slot array stores offsets for cell headers
    // with slot array we reach to cell header and from their we know the cell size and thus
    // we can convert a slice into `Cell`
    fn cell_at_slot() void {}

    // insert a cell
    fn insert() void {}

    /// Returns the child at the given `index`.
    fn child() void {}

    fn is_underflow() void {}
    fn is_overflow() void {}

    fn is_leaf(self: *Page) bool {
        return self.right_child == 0;
    }

    // arrange cells towords the end
    fn defragment() void {}

    // parse `content` and iterate over `SlotArray` and `Cells`
    pub fn iterate_cells(self: *Page) void {
        _ = self;
    }

    pub fn push_cell() void {}
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
        assert(@sizeOf(Page) == 4096);
    }

    fn init() OverflowPage {
        return OverflowPage{ .size = 0, .next = 0, .content = [_]u8{0} ** 4088 };
    }
};

const PageTag = enum { DBMetadataPage, Page, OverflowPage };
pub const PageType = union(PageTag) { DBMetadataPage: DBMetadataPage, Page: Page, OverflowPage: OverflowPage };
