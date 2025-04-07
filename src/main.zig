const std = @import("std");
const muscle = @import("muscle");
const ExecutionEngine = @import("./execution_engine.zig").ExecutionEngine;
const Query = @import("./execution_engine.zig").Query;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var engine = try ExecutionEngine.init(allocator, "/Users/shupawar/x/muscle/muscle");

    const columns_constraints = [_]muscle.ColumnConstraint{ .PrimaryKey, .Unique };
    const columns = [_]muscle.Column{
        muscle.Column{
            .name = "column 1",
            .data_type = .Varchar,
            .constraints = &columns_constraints,
        },
    };
    const create_table_query: Query = Query{ .CreateTable = .{ .table_name = "users", .columns = &columns } };
    try engine.execute_query(create_table_query);

    //const delete_table_query = Query{ .DropTable = .{ .table_name = "sometable" } };
    //try engine.execute_query(delete_table_query);

    defer {
        const deinit_status = gpa.deinit();
        //fail test; can't try in defer as defer is executed after we return
        if (deinit_status == .leak) std.testing.expect(false) catch @panic("Memory leak while deiniting");
    }

    //{
    //    // TODO add use cases

    //    std.debug.print("\tAlignment for structs\n", .{});

    //    const S = extern struct {
    //        n_pages: u32 = 0,
    //        pages: [1023]u32 = [_]u32{0} ** 1023,
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
