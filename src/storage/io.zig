const builtin = @import("builtin");
const IO_Darwin = @import("io/darwin.zig").IO;
const IO_Windows = @import("io/windows.zig").IO;

pub const IO = switch (builtin.target.os.tag) {
    .macos => IO_Darwin,
    .windows => IO_Windows,
    else => @compileError("IO is not supported for platform"),
};

test {
    _ = @import("../testing/io.zig");
}
