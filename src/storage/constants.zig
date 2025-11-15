const assert = @import("std").debug.assert;

// number of pages we can have at a time inside our cache
pub const MAX_CACHE_SIZE = 1024;

// When this count is hit, we will commit before proceeding to next query
// various data structures below are optmized based on this number. Since this is a
// small and well known number we've choosen Arrays instead of HashMaps since for smaller
// number of elements arrays perform better or same as hash maps and we can have
// *static* allocations.
pub const MAX_DIRTY_COUNT_BEFORE_COMMIT = 1000;

comptime {
    // This is important.
    // cache.put should always be able to evict non dirty pages.
    // Hence we need to make sure that number of dirty pages are always less than max cached capacity.
    assert(MAX_DIRTY_COUNT_BEFORE_COMMIT < MAX_CACHE_SIZE);
}

pub const MAX_JOURNAL_UNSAVED_ENTRIES = 64;

comptime {
    // Journal unsaved entries will not go over dirty pages
    assert(MAX_JOURNAL_UNSAVED_ENTRIES < MAX_DIRTY_COUNT_BEFORE_COMMIT);
}
