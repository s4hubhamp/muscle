const std = @import("std");
const muscle = @import("../../muscle.zig");

const Io = std.Io;
const posix = std.posix;
const File = Io.File;
const PAGE_SIZE = muscle.PAGE_SIZE;

pub const IO = struct {
    io: std.Io.Threaded,
    file: std.Io.File,

    // if the provided path is incorrect init may fail
    pub fn init(allocator: std.mem.Allocator, absolute_path: []const u8) !IO {
        var io = Io.Threaded.init(allocator, .{});
        errdefer io.deinit();

        // first try to check if the database file exists or not
        // if it does not exists we need to create it and also we need to create metadata page
        const file = Io.Dir.openFileAbsolute(
            io.io(),
            absolute_path,
            .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true },
        ) catch |err| sw: switch (err) {
            error.FileNotFound => {
                std.debug.print("File not found.\n", .{});
                std.debug.print("Creating new file at {s}.\n", .{absolute_path});
                const file = Io.Dir.createFileAbsolute(io.io(), absolute_path, .{ .read = true, .lock = .exclusive, .lock_nonblocking = true }) catch |w_err| {
                    std.debug.print(
                        "Error occurred while trying to create file at: {s} error: {any}\n",
                        .{ absolute_path, w_err },
                    );
                    return err;
                };
                break :sw file;
            },
            else => return err,
        };

        return IO{ .io = io, .file = file };
    }

    // read page and return number of bytes read into buffer
    pub fn read(self: *IO, page_number: u32, buffer: []u8) !usize {
        return try Io.File.readPositional(self.file, self.io.io(), &.{buffer}, page_offset(page_number));
    }

    pub fn write(self: *IO, page_number: u32, buffer: []const u8) !usize {
        const written = try Io.File.writePositional(self.file, self.io.io(), &.{buffer}, page_offset(page_number));
        // for now we are not doing retries so asserting
        std.debug.assert(written == buffer.len);
        return written;
    }

    pub fn truncate(self: *IO, page_number: u32) !void {
        std.debug.print("Truncated file to position: {}\n", .{page_offset(page_number)});
        try Io.File.setLength(self.file, self.io.io(), page_offset(page_number));
    }

    pub fn deinit(self: *IO) void {
        self.file.close(self.io.io());
        self.io.deinit();
    }

    pub fn page_offset(page_number: usize) usize {
        return page_number * PAGE_SIZE;
    }
};
