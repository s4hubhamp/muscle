const std = @import("std");
const muscle = @import("../../muscle.zig");

const Io = std.Io;
const PAGE_SIZE = muscle.PAGE_SIZE;

const logger = std.log.scoped(.io);

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
                logger.info("File not found.", .{});
                logger.info("Creating new file at {s}.", .{absolute_path});
                // Preserve contents if another opener creates the file first.
                const file = Io.Dir.createFileAbsolute(io.io(), absolute_path, .{ .read = true, .truncate = false, .lock = .exclusive, .lock_nonblocking = true }) catch |w_err| {
                    logger.err(
                        "Error occurred while trying to create file at: {s} error: {any}",
                        .{ absolute_path, w_err },
                    );
                    return w_err;
                };
                break :sw file;
            },
            else => return err,
        };

        return IO{ .io = io, .file = file };
    }

    // read page and return number of bytes read into buffer
    pub fn read(self: *IO, page_number: u32, buffer: []u8) !usize {
        var total: usize = 0;
        while (total < buffer.len) {
            const n = try self.file.readPositional(self.io.io(), &.{buffer[total..]}, page_offset(page_number) + total);
            if (n == 0) break; // EOF, including a newly created database.
            total += n;
        }
        return total;
    }

    pub fn write(self: *IO, page_number: u32, buffer: []const u8) !usize {
        var total: usize = 0;
        while (total < buffer.len) {
            const n = try self.file.writePositional(self.io.io(), &.{buffer[total..]}, page_offset(page_number) + total);
            if (n == 0) return error.WriteZero;
            total += n;
        }
        return total;
    }

    pub fn truncate(self: *IO, page_number: u32) !void {
        logger.info("Truncated file to position: {}", .{page_offset(page_number)});
        try Io.File.setLength(self.file, self.io.io(), page_offset(page_number));
    }

    pub fn deinit(self: *IO) void {
        self.file.close(self.io.io());
        self.io.deinit();
    }

    pub fn page_offset(page_number: u32) u64 {
        return @as(u64, page_number) * PAGE_SIZE;
    }
};
