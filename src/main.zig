const std = @import("std");
const muscle = @import("muscle.zig");
const ExecutionEngine = @import("./execution_engine.zig").ExecutionEngine;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var engine = try ExecutionEngine.init(allocator, "/Users/shupawar/x/muscledb/muscle");
    var columns_constraints = [_]muscle.ColumnConstraint{ .PrimaryKey, .Unique };
    var columns = [_]muscle.Column{
        muscle.Column{
            .name = "column 1",
            .data_type = .Varchar,
            .constraints = &columns_constraints,
        },
    };
    try engine.create_table("users", &columns);

    defer {
        const deinit_status = gpa.deinit();
        //fail test; can't try in defer as defer is executed after we return
        if (deinit_status == .leak) std.testing.expect(false) catch @panic("Memory leak while deiniting");
    }

    //{
    //    // TODO add use cases

    //    std.debug.print("\tAlignment for structs\n", .{});

    //    const S = extern struct {
    //        // length of slot array
    //        num_slots: u16 = 0,
    //        // Offset of the last inserted cell counting from the start
    //        last_used_offset: u16 = 0,
    //        // free space is 4096 - size - (size of header fields = 16)
    //        // used to determine whether page is underflow or not
    //        free_space: u16 = 0,
    //        // size of the content only
    //        // used to determine whether page is overflow or not
    //        // this tells about the size that is in use.
    //        // If we have some empty cells in the middle those cells will not account in the calculation of the size
    //        size: u16 = 0,
    //        // for internal btree node the rightmost child node
    //        right_child: u32 = 0, // page number
    //        // content is slot array + cells
    //        content: [4084]u8 = [_]u8{0} ** 4084,
    //    };

    //    // packed structs alignment is equal to alignment of backing integer so it is @alignOf(u160) = 16
    //    std.debug.print("\tstructs alignment:   {}\n", .{@alignOf(S)});
    //    //
    //    // |_____________________   ____________________________     ________________________|
    //    // 0                   15   16                        19     20                     31
    //    // -  u128 (16 bytes)  -    -  (u31 + bool) (4 bytes) -      -   PADDING (12 bytes)  -
    //    // Tatal size -- 32 bytes
    //    //
    //    std.debug.print("\tstructs size:        {}\n", .{@sizeOf(S)});

    //    print_struct_info(S);
    //}
}

fn print_struct_info(S: type) void {
    const info = @typeInfo(S);
    const s = S{};

    inline for (info.@"struct".fields) |field| {
        std.debug.print("\t\tfield:       {s}\n", .{field.name});
        std.debug.print("\t\tsize:        {}\n", .{@sizeOf(field.type)});
        std.debug.print("\t\toffset:      {}\n", .{@offsetOf(S, field.name)});
        std.debug.print("\t\talignment:   {}\n", .{field.alignment});
        std.debug.print("\t\taddress:     {*}\n", .{&@field(s, field.name)});
        std.debug.print("\n", .{});
    }

    std.debug.print("\n", .{});
}
