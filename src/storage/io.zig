const builtin = @import("builtin");
const IO_Darwin = @import("io/darwin.zig").IO;

pub const IO = switch (builtin.target.os.tag) {
    .macos => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};
