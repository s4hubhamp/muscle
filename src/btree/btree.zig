const pager = @import("./pager.zig");
const PageNumber = pager.PageNumber;

// Btree gets the root page and will do search, insert, delete operations
// execution engine will provide the root page information to btree
// that means execution engine needs to have access to file
const BTree = struct {
    fn search(root_page_number: PageNumber) void {
        _ = root_page_number;
    }

    fn insert(root_page_number: PageNumber) void {
        _ = root_page_number;
    }
};
