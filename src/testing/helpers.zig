const std = @import("std");
const muscle = @import("../muscle.zig");

/// Owns query allocations and a context for tests that do not need storage.
/// Fixtures live at a stable address because allocators and plans reference them.
pub const QueryFixture = struct {
    arena: std.heap.ArenaAllocator,
    context: muscle.QueryContext,

    pub fn init() !*QueryFixture {
        const self = try std.testing.allocator.create(QueryFixture);
        self.arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        self.context = .{
            .input = "",
            .arena = self.arena.allocator(),
            .result = .{ .data = .__void },
            .pager = undefined,
            .catalog = undefined,
        };
        return self;
    }

    pub fn allocator(self: *QueryFixture) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn deinit(self: *QueryFixture) void {
        self.arena.deinit();
        std.testing.allocator.destroy(self);
    }
};

/// Owns an isolated temporary database and all its query allocations.
pub const DatabaseFixture = struct {
    query: *QueryFixture,
    tmp_dir: std.testing.TmpDir,
    file_path: []const u8,
    db: muscle.database.Muscle,

    pub fn init() !*DatabaseFixture {
        const self = try std.testing.allocator.create(DatabaseFixture);
        errdefer std.testing.allocator.destroy(self);
        self.query = try QueryFixture.init();
        errdefer self.query.deinit();
        self.tmp_dir = std.testing.tmpDir(.{});
        errdefer self.tmp_dir.cleanup();

        const alloc = self.allocator();
        const directory = try self.tmp_dir.dir.realPathFileAlloc(std.testing.io, ".", alloc);
        self.file_path = try std.fs.path.join(alloc, &.{ directory, "test.db" });
        self.db = try muscle.database.Muscle.init(alloc, self.file_path);
        self.query.context = muscle.QueryContext.init(alloc, "", &self.db.pager, &self.db.catalog);
        return self;
    }

    pub fn allocator(self: *DatabaseFixture) std.mem.Allocator {
        return self.query.allocator();
    }

    pub fn deinit(self: *DatabaseFixture) void {
        self.db.deinit();
        self.query.deinit();
        self.tmp_dir.cleanup();
        std.testing.allocator.destroy(self);
    }
};

test "DatabaseFixture isolates databases and cleans up temporary files" {
    const file_path = blk: {
        const first = try DatabaseFixture.init();
        defer first.deinit();
        const second = try DatabaseFixture.init();
        defer second.deinit();

        const created = try first.db.execute("create table records (id int primary key)", first.allocator());
        try std.testing.expect(created == .data);
        try std.testing.expect(first.db.catalog.find_table("records") != null);
        try std.testing.expect(second.db.catalog.find_table("records") == null);
        break :blk try std.testing.allocator.dupe(u8, first.file_path);
    };
    defer std.testing.allocator.free(file_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(std.testing.io, file_path, .{}));
}
