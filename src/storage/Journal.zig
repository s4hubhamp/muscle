const std = @import("std");
const muscle = @import("../muscle.zig");
const constants = @import("constants.zig");
const IO = @import("io.zig").IO;

const assert = std.debug.assert;
const print = std.debug.print;
const PAGE_SIZE = muscle.PAGE_SIZE;
const MAX_JOURNAL_UNSAVED_ENTRIES = constants.MAX_JOURNAL_UNSAVED_ENTRIES;

// Journal stores the original state of pages before update/delete operations
// If update/delete fails in the middle we can rollback to original state
// If update/delete complets we clear the journal
// Note: create is just a update for some page
//
const Journal = @This();

io: IO,
// unsaved entries
entries: std.BoundedArray(JournalEntry, MAX_JOURNAL_UNSAVED_ENTRIES),
// this metadata does not contain newer stuff from entries. This is just an already saved stuff
metadata: JournalMetadataPage,

const JournalEntry = struct {
    page_number: u32,
    entry: [muscle.PAGE_SIZE]u8,
};

const JournalMetadataPage = extern struct {
    // zero indicates no allocation
    first_new_alloced_page: u32,
    n_pages: u32,
    // Inherently array len here becomes max pages one query can modify
    pages: [1022]u32,

    comptime {
        assert(@alignOf(JournalMetadataPage) == 4);
        assert(@sizeOf(JournalMetadataPage) == muscle.PAGE_SIZE);
    }

    fn init() JournalMetadataPage {
        return JournalMetadataPage{
            .first_new_alloced_page = 0,
            .n_pages = 0,
            .pages = [_]u32{0} ** 1022,
        };
    }
};

pub fn init(database_file_path: []const u8) !Journal {
    var journal_file_path = [_]u8{0} ** 128;

    for (database_file_path, 0..) |char, index| journal_file_path[index] = char;
    const JOURNAL_FILE_EXTENSION = ".journal";
    for (JOURNAL_FILE_EXTENSION, database_file_path.len..) |char, index| journal_file_path[index] = char;

    // in posix path should not be null terminated
    print("Opening Journal file.\n", .{});
    var io = try IO.init(journal_file_path[0 .. database_file_path.len + JOURNAL_FILE_EXTENSION.len]);

    // load metadata
    var metadata: JournalMetadataPage = undefined;
    var buffer = [_]u8{0} ** muscle.PAGE_SIZE;
    const bytes_read = try io.read(0, &buffer);

    if (bytes_read > 0) {
        assert(bytes_read == muscle.PAGE_SIZE);
        metadata = @as(*JournalMetadataPage, @ptrCast(@alignCast(&buffer))).*;
    } else {
        metadata = JournalMetadataPage.init();
    }

    return Journal{
        .io = io,
        .entries = try std.BoundedArray(JournalEntry, MAX_JOURNAL_UNSAVED_ENTRIES).init(0),
        .metadata = metadata,
    };
}

const OriginalPage = struct { page_number: u32, page: [muscle.PAGE_SIZE]u8 };
const BATCH_GET_SIZE = 16;

pub fn get_first_newly_alloced_page(self: *const Journal) ?u32 {
    if (self.metadata.first_new_alloced_page == 0) {
        return null;
    }
    return self.metadata.first_new_alloced_page;
}

pub fn maybe_set_first_newly_alloced_page(self: *Journal, page_number: u32) void {
    if (self.metadata.first_new_alloced_page == 0) {
        self.metadata.first_new_alloced_page = page_number;
    }
}

pub fn batch_get_original_pages(self: *Journal, offset: u32) !struct {
    pages: [BATCH_GET_SIZE]OriginalPage,
    n_read: u32,
} {
    const metadata = &self.metadata;
    var pages = [_]OriginalPage{OriginalPage{ .page_number = 0, .page = undefined }} ** BATCH_GET_SIZE;

    var i: u32 = offset;
    var j: u8 = 0;
    while (i < metadata.n_pages and j < BATCH_GET_SIZE) {
        const original_page_number = self.metadata.pages[i];
        pages[j].page_number = original_page_number;
        _ = try self.io.read(original_page_number, &pages[j].page);
        i += 1;
        j += 1;
    }

    if (j < BATCH_GET_SIZE) {
        // total collected - pages from metadata
        var already_collected_pages_from_entries = i - metadata.n_pages;

        while (already_collected_pages_from_entries < self.entries.len and j < BATCH_GET_SIZE) {
            pages[j] = .{
                .page_number = self.entries.buffer[already_collected_pages_from_entries].page_number,
                .page = self.entries.buffer[already_collected_pages_from_entries].entry,
            };
            j += 1;
            already_collected_pages_from_entries += 1;
        }
    }

    return .{ .pages = pages, .n_read = j };
}

// record original state of the page
pub fn record(self: *Journal, page_number: u32, entry: [muscle.PAGE_SIZE]u8) !void {
    // check if record already exists
    for (0..self.metadata.n_pages) |i| {
        if (self.metadata.pages[i] == page_number) {
            return;
        }
    }

    // check if it exists inside entries
    for (self.entries.constSlice()) |*e| {
        if (e.page_number == page_number) {
            return;
        }
    }

    // if unsaved entries are full then persist first
    if (self.entries.len == self.entries.capacity()) {
        try self.persist();
    }

    try self.entries.append(JournalEntry{ .page_number = page_number, .entry = entry });
}

// Save all the original unsaved pages to the journal file.
// After we write all the pages then we update the header page saving the
// metadata of journal file.
pub fn persist(self: *Journal) !void {
    if (self.entries.len == 0) return;

    var metadata = &self.metadata;

    // write unsaved pages
    for (self.entries.constSlice()) |*entry| {
        const page_number = metadata.n_pages + 1;
        _ = try self.io.write(page_number, &entry.entry);
        metadata.pages[metadata.n_pages] = entry.page_number;
        metadata.n_pages += 1;
    }

    // write metadata page
    // NOTE: It's important that we write metadata page only after saving
    // all the entries.
    _ = try self.io.write(0, std.mem.asBytes(metadata));

    // reset entries
    self.entries.clear();
}

// clear the journal file
// this gets called only when whole query execution is completed
pub fn clear(self: *Journal) !void {
    self.entries.clear();
    self.metadata = JournalMetadataPage.init();
    try self.io.truncate(null);
}
