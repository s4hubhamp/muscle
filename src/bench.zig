const std = @import("std");
const builtin = @import("builtin");
const muscle = @import("muscle");

const Config = struct {
    rows: usize = 10_000,
    samples: usize = 15,
    warmup: usize = 3,
    label: []const u8 = "",
};
const Query = struct { sql: []const u8, count: usize, first: i64, last: i64 };
const Workload = struct { name: []const u8, iterations: usize, queries: []const Query };
const Measurement = struct {
    name: []const u8,
    iterations: usize,
    queries: []const Query,
    samples_ns_per_query: []const f64,
    median_ns_per_query: f64,
    min_ns_per_query: f64,
    max_ns_per_query: f64,
    checksum: u64,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var config: Config = .{};
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (std.mem.eql(u8, args[i], "--help")) {
            try std.Io.File.stdout().writeStreamingAll(init.io, "Usage: zig build bench -Doptimize=ReleaseFast -- [--rows N] [--samples N] [--warmup N] [--label TEXT]\n" ++
                "Defaults: 10000 rows, 15 samples, 3 warmup batches. JSON goes to stdout.\n");
            return;
        }
        if (i + 1 == args.len) return error.MissingArgument;
        if (std.mem.eql(u8, args[i], "--label")) {
            config.label = args[i + 1];
            continue;
        }
        const value = try std.fmt.parseInt(usize, args[i + 1], 10);
        if (std.mem.eql(u8, args[i], "--rows")) {
            config.rows = value;
        } else if (std.mem.eql(u8, args[i], "--samples")) {
            config.samples = value;
        } else if (std.mem.eql(u8, args[i], "--warmup")) {
            config.warmup = value;
        } else return error.UnknownArgument;
    }
    // Bounds keep integer keys and fixture construction manageable.
    if (config.rows < 100 or config.rows > 1_000_000 or config.samples < 3 or config.samples > 1000 or config.warmup < 1 or config.warmup > 1000)
        return error.InvalidConfiguration;

    var random_bytes: [16]u8 = undefined;
    init.io.random(&random_bytes);
    const temp_root = if (builtin.os.tag == .windows)
        init.environ_map.get("TEMP") orelse init.environ_map.get("TMP") orelse "."
    else
        init.environ_map.get("TMPDIR") orelse "/tmp";
    const directory_name = try std.fmt.allocPrint(allocator, "muscle-bench-{x}", .{std.mem.readInt(u128, &random_bytes, .little)});
    const directory_path = try std.fs.path.join(allocator, &.{ temp_root, directory_name });
    try std.Io.Dir.cwd().createDir(init.io, directory_path, .default_dir);
    defer std.Io.Dir.cwd().deleteTree(init.io, directory_path) catch |err| {
        std.debug.print("Failed to remove benchmark directory {s}: {s}\n", .{ directory_path, @errorName(err) });
    };
    const directory = try std.Io.Dir.cwd().realPathFileAlloc(init.io, directory_path, allocator);
    const path = try std.fs.path.join(allocator, &.{ directory, "bench.db" });
    var db = try muscle.database.Muscle.init(init.gpa, path);
    defer db.deinit();
    var query_arena = std.heap.ArenaAllocator.init(init.gpa);
    defer query_arena.deinit();
    std.debug.print("Preparing {d} rows (outside timing)...\n", .{config.rows});
    _ = try execute(&db, &query_arena, "create table records (id int primary key, score int, payload text(64))");
    var inserts: std.ArrayList(u8) = .empty;
    defer inserts.deinit(init.gpa);
    for (0..config.rows) |row| {
        var buffer: [256]u8 = undefined;
        const sql = try std.fmt.bufPrint(&buffer, "insert into records (id, score, payload) values ({d}, {d}, 'abcdefghijklmnopqrstuvwxyz0123456789');", .{ row + 1, row % 100 });
        try inserts.appendSlice(init.gpa, sql);
        // Seed in batches to avoid one commit (and storage diagnostic) per row.
        if ((row + 1) % 100 == 0 or row + 1 == config.rows) {
            _ = try execute(&db, &query_arena, inserts.items);
            inserts.clearRetainingCapacity();
        }
    }

    const lookups = try allocator.alloc(Query, 128);
    for (lookups, 0..) |*query, index| {
        // Fixed integer mixing spreads successful lookups across the tree.
        const key = 1 + ((index * 2654435761 + 17) % config.rows);
        query.* = .{ .sql = try std.fmt.allocPrint(allocator, "select id, score from records where id = {d}", .{key}), .count = 1, .first = @intCast(key), .last = @intCast(key) };
    }
    const start = config.rows / 2;
    const narrow_end = start + 10;
    const wide_end = start + config.rows / 10;
    const workloads = [_]Workload{
        .{ .name = "primary_key_hit", .iterations = 1024, .queries = lookups },
        .{ .name = "primary_key_miss", .iterations = 1024, .queries = &.{.{ .sql = try std.fmt.allocPrint(allocator, "select id, score from records where id = {d}", .{config.rows + 1}), .count = 0, .first = 0, .last = 0 }} },
        .{ .name = "sequential_scan", .iterations = 10, .queries = &.{.{ .sql = "select id, score from records", .count = config.rows, .first = 1, .last = @intCast(config.rows) }} },
        .{ .name = "filtered_sequential_scan", .iterations = 10, .queries = &.{.{ .sql = "select id, score from records where score = 42", .count = (config.rows + 57) / 100, .first = 43, .last = @intCast(43 + ((config.rows - 43) / 100) * 100) }} },
        .{ .name = "range_scan_narrow", .iterations = 100, .queries = &.{.{ .sql = try std.fmt.allocPrint(allocator, "select id, score from records where id >= {d} and id < {d}", .{ start, narrow_end }), .count = narrow_end - start, .first = @intCast(start), .last = @intCast(narrow_end - 1) }} },
        .{ .name = "range_scan_wide", .iterations = 30, .queries = &.{.{ .sql = try std.fmt.allocPrint(allocator, "select id, score from records where id >= {d} and id < {d}", .{ start, wide_end }), .count = wide_end - start, .first = @intCast(start), .last = @intCast(wide_end - 1) }} },
    };
    var measurements: [workloads.len]Measurement = undefined;
    for (workloads, &measurements) |workload, *measurement| {
        // Validate every query before timing; fail instead of benchmarking errors.
        for (workload.queries) |query| {
            const result = try execute(&db, &query_arena, query.sql);
            if (result.rowCount() != query.count) return error.UnexpectedRowCount;
            if (query.count > 0) {
                const rows = result.data.rows.items;
                if (std.mem.readInt(i64, rows[0][1..9], .little) != query.first or
                    std.mem.readInt(i64, rows[rows.len - 1][1..9], .little) != query.last)
                    return error.UnexpectedRowContents;
            }
        }
        var checksum: u64 = 0;
        for (0..config.warmup) |_| checksum +%= try batch(&db, &query_arena, workload);
        const samples = try allocator.alloc(f64, config.samples);
        for (samples) |*sample| {
            const before = std.Io.Clock.awake.now(init.io);
            checksum +%= try batch(&db, &query_arena, workload);
            const elapsed = before.durationTo(std.Io.Clock.awake.now(init.io)).toNanoseconds();
            sample.* = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(workload.iterations));
        }
        const sorted = try allocator.dupe(f64, samples);
        std.mem.sort(f64, sorted, {}, std.sort.asc(f64));
        const median = if (sorted.len % 2 == 0) (sorted[sorted.len / 2 - 1] + sorted[sorted.len / 2]) / 2 else sorted[sorted.len / 2];
        measurement.* = .{ .name = workload.name, .iterations = workload.iterations, .queries = workload.queries, .samples_ns_per_query = samples, .median_ns_per_query = median, .min_ns_per_query = sorted[0], .max_ns_per_query = sorted[sorted.len - 1], .checksum = checksum };
        std.debug.print("{s}: {d:.2} us/query (median)\n", .{ workload.name, median / 1000 });
    }
    const json = try std.json.Stringify.valueAlloc(allocator, .{
        .schema_version = 1,
        .suite_version = 1,
        .zig_version = builtin.zig_version_string,
        .optimize = @tagName(builtin.mode),
        .arch = @tagName(builtin.cpu.arch),
        .os = @tagName(builtin.os.tag),
        .cpu = builtin.cpu.model.name,
        .cache_mode = "warmed_reads",
        .allocator = "process_gpa_with_reused_query_arena",
        .config = config,
        .benchmarks = measurements,
    }, .{ .whitespace = .indent_2 });
    try std.Io.File.stdout().writeStreamingAll(init.io, json);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}

fn execute(db: *muscle.database.Muscle, arena: *std.heap.ArenaAllocator, sql: []const u8) !muscle.query_result.QueryResultV2 {
    _ = arena.reset(.retain_capacity);
    const result = try db.execute(sql, arena.allocator());
    if (result == .err) {
        std.debug.print("Query failed: {s}\n{s}: {s}\n", .{ sql, @errorName(result.err.code), result.err.message });
        return error.QueryFailed;
    }
    return result;
}

fn batch(db: *muscle.database.Muscle, arena: *std.heap.ArenaAllocator, workload: Workload) !u64 {
    var checksum: u64 = 0;
    for (0..workload.iterations) |index| {
        const query = workload.queries[index % workload.queries.len];
        const result = try execute(db, arena, query.sql);
        if (result.rowCount() != query.count) return error.UnexpectedRowCount;
        // Consume every returned key, keeping query work observable.
        for (result.data.rows.items) |row| checksum +%= @bitCast(std.mem.readInt(i64, row[1..9], .little));
    }
    return checksum;
}
