const std = @import("std");
const fs = std.fs;
const File = fs.File;
const pager = @import("../btree/pager.zig");

pub const IO = struct {
    file: File,

    // if the provided path is incorrect init may fail
    pub fn init(absolute_path: []const u8) !IO {
        // first try to check if the database file exists or not
        // if it does not exists we need to create it and also we need to create metadata page
        const file = fs.openFileAbsolute(
            absolute_path,
            .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true },
        ) catch |err| sw: switch (err) {
            error.FileNotFound => {
                std.debug.print("File not found.\n", .{});
                std.debug.print("Creating new file at {s}.\n", .{absolute_path});
                const file = fs.createFileAbsolute(absolute_path, .{ .read = true, .lock = .exclusive, .lock_nonblocking = true }) catch |w_err| {
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

        return IO{ .file = file };
    }

    // read page and return number of bytes read into buffer
    pub fn read(self: *IO, page_number: u32, buffer: []u8) !usize {
        // since every page size is supposed to be same as 8Kb
        try self.file.seekTo(page_number * pager.PAGE_SIZE);
        return try self.file.read(buffer);
    }

    pub fn write(self: *IO, page_number: u32, buffer: []u8) !usize {
        try self.file.seekTo(page_number * pager.PAGE_SIZE);
        return try self.file.write(buffer);
    }

    pub fn deinit(self: IO) void {
        self.file.close();
    }
};
