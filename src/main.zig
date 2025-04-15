const std = @import("std");
const muscle = @import("muscle");
const ExecutionEngine = @import("./execution_engine.zig").ExecutionEngine;
const Query = @import("./execution_engine.zig").Query;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var engine = try ExecutionEngine.init(allocator, "/Users/shupawar/x/muscle/muscle");
    const columns = [_]muscle.Column{
        muscle.Column{
            .name = "name",
            .data_type = muscle.DataType{ .TEXT = 20 },
        },
        muscle.Column{
            .name = "email",
            .data_type = muscle.DataType{ .TEXT = 20 },
        },
        muscle.Column{ .name = "age", .data_type = muscle.DataType{ .INT = {} }, .not_null = true },
    };

    const create_table_query: Query = Query{ .CreateTable = .{ .table_name = "users", .columns = &columns } };
    try engine.execute_query(create_table_query);

    const values = [_]muscle.Value{
        muscle.Value{ .TEXT = "shubham" },
        muscle.Value{ .TEXT = "foo@fuck.com" },
        muscle.Value{ .INT = 100 },
    };
    const insert_query = Query{ .Insert = .{ .table_name = "users", .columns = &columns, .values = &values } };
    try engine.execute_query(insert_query);

    const select_query = Query{ .Select = .{ .table_name = "users" } };
    try engine.execute_query(select_query);

    //const delete_table_query = Query{ .DropTable = .{ .table_name = "sometable" } };
    //try engine.execute_query(delete_table_query);

    defer {
        engine.deinit();
        const deinit_status = gpa.deinit();
        //fail test; can't try in defer as defer is executed after we return
        if (deinit_status == .leak) std.testing.expect(false) catch @panic("Memory leak while deiniting");
    }

    //{
    //    // TODO add use cases

    //    std.debug.print("\tAlignment for structs\n", .{});

    //    const S = struct {
    //        a: u8 = 1,
    //        b: u32 = 1,
    //        c: u16 = 1,
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

//const MyData = struct {
//    number: u32,
//    flag: bool,
//    name: []const u8,
//};

//fn serialize(data: MyData, buffer: *std.ArrayList(u8)) !void {
//    var num_buf: [4]u8 = undefined;
//    std.mem.writeInt(u32, &num_buf, data.number, .little);
//    try buffer.appendSlice(&num_buf);

//    try buffer.append(if (data.flag) 1 else 0);

//    std.mem.writeInt(u32, &num_buf, @intCast(data.name.len), .little);
//    try buffer.appendSlice(&num_buf);
//    try buffer.appendSlice(data.name);
//}

//fn deserialize(bytes: []const u8, allocator: std.mem.Allocator) !MyData {
//    var index: usize = 0;

//    const number = std.mem.readInt(u32, bytes[index..][0..4], .little);
//    index += 4;

//    const flag = bytes[index] != 0;
//    index += 1;

//    const name_len = std.mem.readInt(u32, bytes[index..][0..4], .little);
//    index += 4;

//    const name_slice = try allocator.alloc(u8, name_len);
//    std.mem.copyForwards(u8, name_slice, bytes[index .. index + name_len]);

//    return MyData{
//        .number = number,
//        .flag = flag,
//        .name = name_slice,
//    };
//}
