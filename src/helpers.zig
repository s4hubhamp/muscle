const std = @import("std");

const TmpFile = struct {
    tmp_dir: std.testing.TmpDir,
    file_path: []const u8,

    pub fn deinit(self: *TmpFile) void {
        //self.tmp_dir.cleanup();
        std.testing.allocator.free(self.file_path);
    }
};
pub fn get_temp_file_path(file_name: []const u8) !TmpFile {
    var tmp_dir = std.testing.tmpDir(.{});
    // Get temp directory path
    const temp_dir_path = try tmp_dir.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(temp_dir_path);

    // Build database file path
    const file_path = try std.fs.path.join(
        std.testing.allocator,
        &[_][]const u8{
            temp_dir_path,
            file_name,
        },
    );

    return TmpFile{ .tmp_dir = tmp_dir, .file_path = file_path };
}
