const std = @import("std");
const assert = std.debug.assert;

// @copypasta from tigerbeetle
pub fn BoundedArrayType(comptime T: type, comptime buffer_capacity: usize) type {
    return struct {
        buffer: [buffer_capacity]T align(@alignOf(T)) = undefined,
        len: u32 = 0,

        const BoundedArray = @This();

        pub inline fn from_slice(items: []const T) error{Overflow}!BoundedArray {
            if (items.len <= buffer_capacity) {
                var result: BoundedArray = .{};
                result.push_slice(items);
                return result;
            } else {
                return error.Overflow;
            }
        }

        /// Returns count of elements in this BoundedArray in the specified integer types,
        /// checking at compile time that it indeed can represent the length.
        pub inline fn count_as(array: *const BoundedArray, comptime Int: type) Int {
            comptime assert(buffer_capacity <= std.math.maxInt(Int));
            return @intCast(array.len);
        }

        pub inline fn full(array: BoundedArray) bool {
            return array.len == buffer_capacity;
        }

        pub inline fn empty(array: BoundedArray) bool {
            return array.len == 0;
        }

        pub inline fn get(array: *const BoundedArray, index: usize) T {
            assert(index < array.len);
            return array.buffer[index];
        }

        pub inline fn slice(array: *BoundedArray) []T {
            return array.buffer[0..array.len];
        }

        pub inline fn const_slice(array: *const BoundedArray) []const T {
            return array.buffer[0..array.len];
        }

        pub inline fn unused_capacity_slice(array: *BoundedArray) []T {
            return array.buffer[array.len..];
        }

        pub fn push(array: *BoundedArray, item: T) void {
            assert(!array.full());
            array.buffer[array.len] = item;
            array.len += 1;
        }

        pub fn push_slice(array: *BoundedArray, items: []const T) void {
            assert(array.len + items.len <= array.capacity());
            @memcpy(array.buffer[array.len..][0..items.len], items); // Copy first
            array.len += @intCast(items.len); // Then update count
        }

        pub inline fn swap_remove(array: *BoundedArray, index: usize) T {
            assert(array.len > 0);
            assert(index < array.len);
            const result = array.buffer[index];
            array.len -= 1;
            array.buffer[index] = array.buffer[array.len];
            return result;
        }

        pub fn resize(array: *BoundedArray, count_new: usize) error{Overflow}!void {
            if (count_new <= buffer_capacity) {
                array.len = @intCast(count_new);
            } else {
                return error.Overflow;
            }
        }

        pub inline fn truncate(array: *BoundedArray, count_new: usize) void {
            assert(count_new <= array.len);
            array.len = @intCast(count_new); // can't overflow due to check above.
        }

        pub inline fn clear(array: *BoundedArray) void {
            array.len = 0;
        }

        pub inline fn pop(array: *BoundedArray) ?T {
            if (array.len == 0) return null;
            array.len -= 1;
            return array.buffer[array.len];
        }

        pub inline fn capacity(_: *BoundedArray) usize {
            return buffer_capacity;
        }
    };
}
