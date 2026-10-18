const std = @import("std");
const muscle = @import("muscle");

const max_query_bytes = 1024 * 1024;

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var database_path: []const u8 = "muscle.db";
    var host: []const u8 = "127.0.0.1";
    var port: u16 = 8080;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (std.mem.eql(u8, args[i], "--help")) {
            try std.Io.File.stdout().writeStreamingAll(init.io, "Usage: zig build run -- [--db PATH] [--host IP] [--port PORT]\n" ++
                "Defaults: muscle.db, 127.0.0.1, 8080. POST raw SQL to /query.\n");
            return;
        }
        if (i + 1 >= args.len) return error.MissingArgument;
        if (std.mem.eql(u8, args[i], "--db")) {
            database_path = args[i + 1];
        } else if (std.mem.eql(u8, args[i], "--host")) {
            host = args[i + 1];
        } else if (std.mem.eql(u8, args[i], "--port")) {
            port = try std.fmt.parseInt(u16, args[i + 1], 10);
            if (port == 0) return error.InvalidPort;
        } else return error.UnknownArgument;
    }

    const cwd = try std.Io.Dir.cwd().realPathFileAlloc(init.io, ".", allocator);
    const absolute_path = try std.fs.path.resolve(allocator, &.{ cwd, database_path });
    var db = try muscle.database.Muscle.init(init.gpa, absolute_path);
    defer db.deinit();

    const address = try std.Io.net.IpAddress.parse(host, port);
    var listener = try address.listen(init.io, .{ .reuse_address = true });
    defer listener.deinit(init.io);
    std.debug.print("MuscleDB listening on {f}; POST /query (database: {s})\n", .{ address, absolute_path });

    // One database owner serializes queries and commits.
    while (true) {
        const stream = try listener.accept(init.io);
        serveConnection(init.io, init.gpa, stream, &db) catch |err| {
            std.log.warn("Query connection failed: {s}", .{@errorName(err)});
        };
    }
}

fn serveConnection(io: std.Io, allocator: std.mem.Allocator, stream: std.Io.net.Stream, db: *muscle.database.Muscle) !void {
    defer stream.close(io);
    var read_buffer: [8192]u8 = undefined;
    var write_buffer: [8192]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);
    var server = std.http.Server.init(&reader.interface, &writer.interface);
    var request = try server.receiveHead();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    try handleRequest(&request, db, arena.allocator());
}

fn respond(request: *std.http.Server.Request, status: std.http.Status, body: []const u8) !void {
    try request.respond(body, .{
        .status = status,
        .keep_alive = false,
        .extra_headers = &.{.{ .name = "content-type", .value = "text/plain; charset=utf-8" }},
    });
}

fn handleRequest(request: *std.http.Server.Request, db: *muscle.database.Muscle, allocator: std.mem.Allocator) !void {
    if (!std.mem.eql(u8, request.head.target, "/query"))
        return respond(request, .not_found, "Not found. POST SQL to /query.\n");
    if (request.head.method != .POST) {
        return request.respond("Use POST /query with SQL in the request body.\n", .{
            .status = .method_not_allowed,
            .keep_alive = false,
            .extra_headers = &.{
                .{ .name = "allow", .value = "POST" },
                .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
            },
        });
    }
    if (request.head.content_length) |length| {
        if (length > max_query_bytes)
            return respond(request, .payload_too_large, "Query exceeds 1 MiB.\n");
    }
    var body_buffer: [4096]u8 = undefined;
    const body_reader = try request.readerExpectContinue(&body_buffer);
    const query = body_reader.allocRemaining(allocator, .limited(max_query_bytes)) catch |err| {
        return respond(request, if (err == error.StreamTooLong) .payload_too_large else .bad_request, "Invalid or oversized query body.\n");
    };
    if (std.mem.trim(u8, query, " \t\r\n").len == 0)
        return respond(request, .bad_request, "Query must not be empty.\n");

    const result = db.execute(query, allocator) catch |err| {
        std.log.err("Database query failed: {s}", .{@errorName(err)});
        return respond(request, .internal_server_error, "Database query failed.\n");
    };
    const output = try std.fmt.allocPrint(allocator, "{f}", .{result});
    try respond(request, if (result == .err) .bad_request else .ok, output);
}

test "HTTP queries return database output and reject invalid requests" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    const path = try std.fs.path.join(allocator, &.{ directory, "server.db" });
    var db = try muscle.database.Muscle.init(std.testing.allocator, path);
    defer db.deinit();
    const cases = [_]struct { method: []const u8 = "POST", target: []const u8 = "/query", sql: []const u8, status: []const u8, output: []const u8 }{
        .{ .sql = "create table records (id int primary key, name text(32))", .status = "200", .output = "(0 rows)\n" },
        .{ .sql = "insert into records (id, name) values (1, 'Alice')", .status = "200", .output = "rows_created" },
        .{ .sql = "select id, name from records", .status = "200", .output = "Alice" },
        .{ .sql = "select * from missing", .status = "400", .output = "Error (" },
        .{ .sql = " \n", .status = "400", .output = "Query must not be empty" },
        .{ .method = "GET", .sql = "", .status = "405", .output = "Use POST" },
        .{ .target = "/missing", .sql = "", .status = "404", .output = "Not found" },
    };
    for (cases) |case| {
        const wire = try std.fmt.allocPrint(allocator, "{s} {s} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\n\r\n{s}", .{ case.method, case.target, case.sql.len, case.sql });
        var reader = std.Io.Reader.fixed(wire);
        var writer = std.Io.Writer.Allocating.init(allocator);
        var server = std.http.Server.init(&reader, &writer.writer);
        var request = try server.receiveHead();
        try handleRequest(&request, &db, allocator);
        const response = writer.written();
        try std.testing.expect(std.mem.startsWith(u8, response, try std.fmt.allocPrint(allocator, "HTTP/1.1 {s}", .{case.status})));
        try std.testing.expect(std.mem.indexOf(u8, response, case.output) != null);
    }
}
