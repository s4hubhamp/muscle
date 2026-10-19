const std = @import("std");
const muscle = @import("./muscle.zig");

const Statement = muscle.statement.Statement;
const InsertStatement = muscle.statement.Insert;
const UpdateStatement = muscle.statement.Update;
const DeleteStatement = muscle.statement.Delete;
const CreateTableStatement = muscle.statement.CreateTable;
const SelectStatement = muscle.statement.Select;
const Assignment = muscle.statement.Assignment;
const Expression = muscle.expression.Expression;
const BinaryOperator = muscle.expression.BinaryOperator;
const serde = muscle.common.serde;
const BoundedArray = muscle.common.BoundedArray;

pub const Plan = union(enum) {
    project: ProjectPlan,
    sort,

    // seq scan comparisons are on each table row
    // select * from table;
    seq_scan: SeqentialScanPlan,

    // exact_match and range_scan are both on the primary keys
    // select * from table where primary_key = 5;
    exact_match: ExactMatchPlan,
    // select * from table where primary_key > 10 AND primary_key < 20 ;
    range_scan: RangeScanPlan,

    // external index btree scan
    index_scan,

    // for queries that need comparisons other than primary_keys
    // scanner will produce a row and filter will optionally select it
    filter: FilterPlan,
    // filter is logical_and
    logical_or_scan: LogicalOrScan,

    insert: InsertPlan,
    update: UpdatePlan,
    delete: DeletePlan,

    // create table, drop table
    unplanned: Statement,

    fn next(self: *@This()) anyerror!?muscle.page_types.EncodedCellContent {
        return switch (self.*) {
            .seq_scan => |*seq_scan| seq_scan.next(),
            .exact_match => |*exact_match| exact_match.next(),
            .range_scan => |*range_scan| range_scan.next(),
            .filter => |*filter| filter.next(),
            .logical_or_scan => |*logical_or| logical_or.next(),
            .insert => |*insert| insert.next(),
            .update => |*update| update.next(),
            .delete => |*delete| delete.next(),
            .project => |*project| project.next(),
            .sort,
            .index_scan,
            .unplanned,
            => {
                unreachable;
            },
        };
    }
};

const InsertPlan = struct {
    context: *muscle.QueryContext,
    into: []const u8,
    columns: [][]const u8,
    values: []Expression,

    fn find_value(self: *@This(), col: []const u8) ?Expression {
        for (self.columns, 0..) |column, idx| {
            if (std.mem.eql(u8, column, col)) {
                return self.values[idx];
            }
        }

        return null;
    }

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        const table = try (self.context.catalog.find_table(self.into).?.clone(self.context.arena));

        // If the passed payload size is greater than max content that a single page can hold
        // this will overflow.
        // so the max size of a single row for now is equal to `page_types.Page.CONTENT_MAX_SIZE`
        var buffer = BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE){};
        var primary_key_bytes: []const u8 = undefined;
        var primary_key_type: muscle.DataType = undefined;

        // for each column find value
        for (table.columns.values()) |*column| {
            const value: ?muscle.Value = if (self.find_value(column.name)) |expr|
                try resolve_expression(self.context, &table, &.{}, expr)
            else
                null;
            var final_value_to_serialize: muscle.Value = .{ .null = {} };

            // if value is not provided or it's provided and null
            if (value == null or value.? == .null) {
                if (column.auto_increment) {
                    // increment value and serialize
                    column.max_int_value += 1;
                    final_value_to_serialize = muscle.Value{ .int = column.max_int_value };
                } else if (column.not_null) {
                    return error.MissingValue;
                } else {
                    // A nullable column with no supplied value stores NULL.
                    final_value_to_serialize = .null;
                }
            } else {
                final_value_to_serialize = value.?;

                if (column.auto_increment) {
                    if (final_value_to_serialize != .int) return error.TypeMismatch;
                    // here we have auto_increment but we are still getting value
                    // need to adjust max value if current value is bigger
                    if (column.max_int_value < final_value_to_serialize.int) {
                        column.max_int_value = final_value_to_serialize.int;
                    } else if (column.max_int_value >= final_value_to_serialize.int) {
                        try self.context.set_err(
                            error.AutoIncrementConstraintViolation,
                            "Value is less than current maximum for auto incremented column. Current Value: {d} Provided: {d}",
                            .{ column.max_int_value, final_value_to_serialize.int },
                        );
                        return error.AutoIncrementConstraintViolation;
                    }
                }

                // validate the type of value
                switch (column.data_type) {
                    .int => if (final_value_to_serialize != .int and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .real => if (final_value_to_serialize != .real and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .bool => if (final_value_to_serialize != .bool and final_value_to_serialize != .null)
                        return error.TypeMismatch,
                    .txt => |len| switch (final_value_to_serialize) {
                        .txt => |text| {
                            if (text.len > len) return error.TextTooLong;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                    .bin => |len| switch (final_value_to_serialize) {
                        .bin => |bin| {
                            if (bin.len > len) return error.BinaryTooLarge;
                        },
                        .null => {},
                        else => return error.TypeMismatch,
                    },
                }
            }

            try serde.serialize_value(&buffer, final_value_to_serialize);
            if (std.mem.eql(u8, column.name, table.primary_key)) {
                primary_key_bytes = buffer.const_slice();
                primary_key_type = column.data_type;
                if (primary_key_bytes.len > muscle.page_types.INTERNAL_CELL_CONTENT_SIZE_LIMIT) {
                    return error.KeyTooLong;
                }
            }
        }

        var btree = muscle.BTree.init(self.context, table.root, primary_key_type);
        try btree.insert(primary_key_bytes, buffer.const_slice());

        // update metadata
        try self.context.catalog.update_table(self.context.pager, table);
        return try serde.serialize(muscle.Value{ .int = 1 }, self.context.arena);
    }
};

const UpdatePlan = struct {
    context: *muscle.QueryContext,
    table_name: []const u8,
    assignments: []Assignment,
    where: ?Expression,
    source: *Plan,
    done: bool = false,

    const Change = struct {
        key: []const u8,
        row: muscle.page_types.EncodedCellContent,
    };

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.done) return null;
        self.done = true;
        const arena = self.context.arena;
        var table = try self.context.catalog.find_table(self.table_name).?.clone(arena);
        const key_type = table.columns.get(table.primary_key).?.data_type;
        var changes: std.ArrayList(Change) = .empty;

        // Read and validate the complete result before modifying any pages.
        while (try self.source.next()) |original| {
            const old_value = try serde.deserialize_value(self.context, &table, original, table.primary_key);
            const old_key = try serde.serialize(old_value, arena);

            var buffer = BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE){};
            for (table.columns.values()) |*column| {
                var value = try serde.deserialize_value(self.context, &table, original, column.name);
                for (self.assignments) |assignment| {
                    if (std.mem.eql(u8, assignment.column, column.name)) {
                        // All assignments use the original row, including swaps.
                        value = try resolve_expression(self.context, &table, original, assignment.value);
                        value = try update_value(column.*, value);
                        if (column.auto_increment and value == .int and value.int > column.max_int_value) column.max_int_value = value.int;
                        break;
                    }
                }
                try serde.serialize_value(&buffer, value);
            }
            try changes.append(arena, .{
                .key = old_key,
                .row = try arena.dupe(u8, buffer.const_slice()),
            });
        }

        var btree = muscle.BTree.init(self.context, table.root, key_type);
        for (changes.items) |change| try btree.update(change.key, change.row);
        if (changes.items.len > 0) try self.context.catalog.update_table(self.context.pager, table);
        return try serde.serialize(.{ .int = @intCast(changes.items.len) }, arena);
    }
};

fn update_value(column: muscle.Column, original: muscle.Value) !muscle.Value {
    var value = original;
    if (value == .null) return if (column.not_null) error.MissingValue else value;
    switch (column.data_type) {
        .int => {
            // Arithmetic expressions return REAL; only exact integers fit INT.
            if (value == .real) {
                const number = value.real;
                if (!std.math.isFinite(number) or @trunc(number) != number or
                    number < -9223372036854775808.0 or number >= 9223372036854775808.0) return error.TypeMismatch;
                value = .{ .int = @intFromFloat(number) };
            }
            if (value != .int) return error.TypeMismatch;
        },
        .real => {
            if (value == .int) {
                const number = value.int;
                value = .{ .real = @floatFromInt(number) };
            }
            if (value != .real) return error.TypeMismatch;
        },
        .bool => if (value != .bool) return error.TypeMismatch,
        .txt => |len| {
            if (value != .txt) return error.TypeMismatch;
            if (value.txt.len > len) return error.TextTooLong;
        },
        .bin => |len| {
            if (value != .bin) return error.TypeMismatch;
            if (value.bin.len > len) return error.BinaryTooLarge;
        },
    }
    return value;
}

const DeletePlan = struct {
    context: *muscle.QueryContext,
    table_name: []const u8,
    where: ?Expression,
    source: *Plan,
    done: bool = false,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.done) return null;
        self.done = true;
        const arena = self.context.arena;
        var table = try self.context.catalog.find_table(self.table_name).?.clone(arena);
        const key_type = table.columns.get(table.primary_key).?.data_type;

        var keys_to_delete: std.ArrayList([]const u8) = .empty;

        while (try self.source.next()) |content| {
            const key = try serde.get_serialized_slice(self.context, &table, content, table.primary_key);
            try keys_to_delete.append(arena, try arena.dupe(u8, key));
        }

        var btree = muscle.BTree.init(self.context, table.root, key_type);
        for (keys_to_delete.items) |key| try btree.delete(key);

        return try serde.serialize(.{ .int = @intCast(keys_to_delete.items.len) }, arena);
    }
};

const ProjectPlan = struct {
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    source: *Plan,
    projection: []Expression,
    columns: []const muscle.query_result.ResultColumn,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        const row = try self.source.next() orelse return null;
        var buffer = BoundedArray(u8, muscle.page_types.Page.CONTENT_MAX_SIZE){};
        for (self.projection) |expression| {
            const value = try resolve_expression(self.context, self.table, row, expression);
            try serde.serialize_value(&buffer, value);
        }
        return try self.context.arena.dupe(u8, buffer.const_slice());
    }
};

const SeqentialScanPlan = struct {
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    init: bool = false,
    done: bool = false,
    curr_page: muscle.PageNumber = undefined,
    curr_slot: muscle.page_types.Page.SlotArrayIndex = undefined,
    limit: ?usize = null,
    collected: usize = 0,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.done) return null;

        const table = self.table;
        const allocator = self.context.arena;

        if (!self.init) {
            var btree = muscle.BTree.init(
                self.context,
                self.table.root,
                table.columns.get(table.primary_key).?.data_type,
            );
            self.curr_page = try btree.find_first_leaf();
            self.curr_slot = 0;
            self.init = true;
        }

        var page = try self.context.pager.get_page(muscle.page_types.Page, self.curr_page);

        if (self.curr_slot >= page.num_slots) {
            if (page.right == 0) {
                self.done = true;
                return null;
            }

            self.curr_page = page.right;
            page = try self.context.pager.get_page(muscle.page_types.Page, self.curr_page);
            std.debug.assert(page.num_slots > 0); // If there are more than one leaf pages in btree then they must be non empty
            self.curr_slot = 0;
        }

        self.collected += 1;
        if (self.limit == self.collected) {
            self.done = true;
        }

        const cell = page.cell_at_slot(self.curr_slot);
        self.curr_slot += 1;
        return try allocator.dupe(u8, cell.content);
    }
};

const ExactMatchPlan = struct {
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    key_expression: Expression,
    done: bool = false,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.done) return null;
        self.done = true;
        var key = try resolve_expression(self.context, self.table, &.{}, self.key_expression);
        if (key == .null) return null;
        const key_type = self.table.columns.get(self.table.primary_key).?.data_type;
        // Numeric equality allows INT and REAL operands; fractional INT keys cannot match.
        if (key_type == .int and key == .real) {
            const number = key.real;
            if (!std.math.isFinite(number) or @trunc(number) != number or
                number < -9223372036854775808.0 or number >= 9223372036854775808.0) return null;
            key = .{ .int = @intFromFloat(number) };
        } else if (key_type == .real and key == .int) {
            const number = key.int;
            key = .{ .real = @floatFromInt(number) };
        }
        if (!std.mem.eql(u8, @tagName(key), @tagName(key_type))) return error.IncompatibleTypes;
        switch (key_type) {
            .txt => |len| if (key.txt.len > len) return null,
            .bin => |len| if (key.bin.len > len) return null,
            else => {},
        }
        const bytes = try serde.serialize(key, self.context.arena);
        var tree = muscle.BTree.init(self.context, self.table.root, key_type);
        return tree.find(bytes);
    }
};

const RangeConstraint = struct {
    operator: BinaryOperator,
    expression: Expression,
};

const RangeScanPlan = struct {
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    constraints: []RangeConstraint,
    limit: ?usize = null,
    collected: usize = 0,
    initialized: bool = false,
    done: bool = false,
    curr_page: muscle.PageNumber = undefined,
    curr_slot: muscle.page_types.Page.SlotArrayIndex = undefined,
    lower: ?Bound = null,
    upper: ?Bound = null,

    const Bound = struct { key: []const u8, inclusive: bool };

    fn add_bound(self: *@This(), bound: Bound, lower: bool) void {
        const destination = if (lower) &self.lower else &self.upper;
        if (destination.*) |current| {
            const key_type = self.table.columns.get(self.table.primary_key).?.data_type;
            const order = serde.compare_serialized_bytes(key_type, bound.key, current.key);
            if (order == .eq) {
                destination.* = .{ .key = current.key, .inclusive = current.inclusive and bound.inclusive };
            } else if (order == (if (lower) std.math.Order.gt else std.math.Order.lt)) {
                destination.* = bound;
            }
        } else destination.* = bound;
    }

    fn initialize(self: *@This()) !void {
        self.initialized = true;
        const key_type = self.table.columns.get(self.table.primary_key).?.data_type;
        for (self.constraints) |constraint| {
            var value = try resolve_expression(self.context, self.table, &.{}, constraint.expression);
            if (value == .null) {
                self.done = true;
                return;
            }
            var operator = constraint.operator;
            if (key_type == .int and value == .real) {
                const number = value.real;
                // Out-of-domain bounds either exclude every INT or impose no restriction.
                if (number >= 9223372036854775808.0) {
                    if (operator != .lt and operator != .lte) self.done = true;
                    continue;
                }
                if (number < -9223372036854775808.0) {
                    if (operator != .gt and operator != .gte) self.done = true;
                    continue;
                }
                if (!std.math.isFinite(number)) return error.IncompatibleTypes;
                if (@trunc(number) != number) {
                    switch (operator) {
                        .eq => {
                            self.done = true;
                            continue;
                        },
                        .gt, .gte => {
                            value = .{ .int = @intFromFloat(@ceil(number)) };
                            operator = .gte;
                        },
                        .lt, .lte => {
                            value = .{ .int = @intFromFloat(@floor(number)) };
                            operator = .lte;
                        },
                        else => unreachable,
                    }
                } else value = .{ .int = @intFromFloat(number) };
            } else if (key_type == .real and value == .int) {
                const number = value.int;
                value = .{ .real = @floatFromInt(number) };
            }
            if (!std.mem.eql(u8, @tagName(value), @tagName(key_type))) return error.IncompatibleTypes;
            const bytes = try serde.serialize(value, self.context.arena);
            const inclusive = operator == .eq or operator == .gte or operator == .lte;
            if (operator == .eq or operator == .gt or operator == .gte) self.add_bound(.{ .key = bytes, .inclusive = inclusive }, true);
            if (operator == .eq or operator == .lt or operator == .lte) self.add_bound(.{ .key = bytes, .inclusive = inclusive }, false);
        }
        if (self.lower) |lower| if (self.upper) |upper| {
            const order = serde.compare_serialized_bytes(key_type, lower.key, upper.key);
            if (order == .gt or (order == .eq and (!lower.inclusive or !upper.inclusive))) self.done = true;
        };
        if (self.done) return;
        var tree = muscle.BTree.init(self.context, self.table.root, key_type);
        if (self.lower) |lower| {
            const position = try tree.seek(lower.key, lower.inclusive);
            self.curr_page = position.page;
            self.curr_slot = position.slot;
        } else {
            self.curr_page = try tree.find_first_leaf();
            self.curr_slot = 0;
        }
    }

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.done) return null;
        if (self.limit) |limit| if (self.collected >= limit) return null;
        if (!self.initialized) try self.initialize();
        if (self.done) return null;
        var page = try self.context.pager.get_page(muscle.page_types.Page, self.curr_page);
        while (self.curr_slot >= page.num_slots) {
            if (page.right == 0) {
                self.done = true;
                return null;
            }
            self.curr_page = page.right;
            self.curr_slot = 0;
            page = try self.context.pager.get_page(muscle.page_types.Page, self.curr_page);
        }
        const cell = page.cell_at_slot(self.curr_slot);
        if (self.upper) |upper| {
            const key_type = self.table.columns.get(self.table.primary_key).?.data_type;
            const order = serde.compare_serialized_bytes(key_type, cell.get_keys_slice(false, key_type), upper.key);
            if (order == .gt or (order == .eq and !upper.inclusive)) {
                self.done = true;
                return null;
            }
        }
        self.curr_slot += 1;
        self.collected += 1;
        return try self.context.arena.dupe(u8, cell.content);
    }
};

const FilterPlan = struct {
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    source: *Plan,
    predicate: Expression,
    limit: ?usize = null,
    collected: usize = 0,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.limit) |limit| if (self.collected >= limit) return null;
        while (try self.source.next()) |row| {
            const matches = try resolve_expression(self.context, self.table, row, self.predicate);
            if (matches == .null) continue;
            if (matches != .bool) return error.TypeMismatch;
            if (matches.bool) {
                self.collected += 1;
                return row;
            }
        }
        return null;
    }
};
const LogicalOrScan = struct {
    table: *const muscle.Table,
    left: *Plan,
    right: *Plan,
    limit: ?usize = null,
    collected: usize = 0,
    left_row: ?muscle.page_types.EncodedCellContent = null,
    right_row: ?muscle.page_types.EncodedCellContent = null,
    left_done: bool = false,
    right_done: bool = false,

    pub fn next(self: *@This()) !?muscle.page_types.EncodedCellContent {
        if (self.limit) |limit| if (self.collected >= limit) return null;
        if (self.left_row == null and !self.left_done) {
            self.left_row = try self.left.next();
            self.left_done = self.left_row == null;
        }
        if (self.right_row == null and !self.right_done) {
            self.right_row = try self.right.next();
            self.right_done = self.right_row == null;
        }
        // Every source emits primary-key order. Merge by key to retain distinct
        // table rows even when their projected values are identical.
        const row = if (self.left_row) |left| blk: {
            if (self.right_row) |right| {
                const key_type = self.table.columns.get(self.table.primary_key).?.data_type;
                const left_cell = muscle.page_types.Cell.init(left, null);
                const right_cell = muscle.page_types.Cell.init(right, null);
                const order = serde.compare_serialized_bytes(key_type, left_cell.get_keys_slice(false, key_type), right_cell.get_keys_slice(false, key_type));
                if (order == .gt) {
                    self.right_row = null;
                    break :blk right;
                }
                if (order == .eq) self.right_row = null;
            }
            self.left_row = null;
            break :blk left;
        } else if (self.right_row) |right| blk: {
            self.right_row = null;
            break :blk right;
        } else return null;
        self.collected += 1;
        return row;
    }
};
const CreateTablePlan = struct {};

pub fn make_plans(context: *muscle.QueryContext, statements: []Statement) ![]Plan {
    var plans = try std.ArrayList(Plan).initCapacity(context.arena, statements.len);

    for (statements) |stmt| {
        switch (stmt) {
            .insert => |insert| {
                plans.appendAssumeCapacity(try plan_insert(context, insert));
            },
            .update => |update| {
                plans.appendAssumeCapacity(try plan_update(context, update));
            },
            .create_table, .drop_table => {
                plans.appendAssumeCapacity(Plan{ .unplanned = stmt });
            },
            .select => |select| {
                plans.appendAssumeCapacity(try plan_select(context, select));
            },
            .delete => |delete| {
                plans.appendAssumeCapacity(try plan_delete(context, delete));
            },
            else => unreachable,
        }
    }

    return plans.toOwnedSlice(context.arena);
}

fn plan_insert(context: *muscle.QueryContext, payload: InsertStatement) !Plan {
    return Plan{ .insert = .{
        .context = context,
        .into = payload.into,
        .columns = payload.columns,
        .values = payload.values,
    } };
}

fn plan_update(context: *muscle.QueryContext, payload: UpdateStatement) !Plan {
    const table = context.catalog.find_table(payload.table).?;
    for (payload.assignments) |assignment| {
        if (std.mem.eql(u8, assignment.column, table.primary_key)) {
            try context.set_err(error.PrimaryKeyUpdateNotAllowed, "Primary key column \"{s}\" cannot be updated.", .{table.primary_key});
            return error.PrimaryKeyUpdateNotAllowed;
        }
        if (table.columns.get(assignment.column).?.unique) {
            try context.set_err(error.UniqueColumnUpdateNotAllowed, "Unique column \"{s}\" cannot be updated.", .{assignment.column});
            return error.UniqueColumnUpdateNotAllowed;
        }
    }
    const source = try plan_source(context, table, payload.where, null);
    return .{ .update = .{
        .context = context,
        .table_name = payload.table,
        .assignments = payload.assignments,
        .where = payload.where,
        .source = source,
    } };
}

fn plan_delete(context: *muscle.QueryContext, payload: DeleteStatement) !Plan {
    const table = context.catalog.find_table(payload.from).?;
    const source = try plan_source(context, table, payload.where, null);
    return .{ .delete = .{
        .context = context,
        .table_name = payload.from,
        .where = payload.where,
        .source = source,
    } };
}

fn plan_select(context: *muscle.QueryContext, payload: SelectStatement) !Plan {
    const table = context.catalog.find_table(payload.table).?;

    var projection = payload.columns;
    if (projection.len == 1 and projection[0] == .star) {
        projection = try context.arena.alloc(Expression, table.columns.count());
        for (table.columns.keys(), projection) |name, *expression| {
            expression.* = .{ .identifier = name };
        }
    }
    const columns = try context.arena.alloc(muscle.query_result.ResultColumn, projection.len);
    for (projection, columns, 0..) |expression, *column, i| {
        column.* = .{
            .name = if (expression == .identifier) expression.identifier else try std.fmt.allocPrint(context.arena, "column_{d}", .{i + 1}),
            .data_type = try expression_data_type(context, table, expression),
        };
    }
    const source = try plan_source(context, table, payload.where, payload.limit);
    return Plan{
        .project = .{
            .context = context,
            .table = table,
            .projection = projection,
            .columns = columns,
            .source = source,
        },
    };
}

fn plan_source(context: *muscle.QueryContext, table: *const muscle.Table, predicate_expression: ?Expression, limit: ?usize) anyerror!*Plan {
    const source = try context.arena.create(Plan);
    if (predicate_expression) |predicate| {
        if (try expression_data_type(context, table, predicate) != .bool) return error.TypeMismatch;
        const expression = unwrap_expression(predicate);
        if (expression == .binary_operation and expression.binary_operation.operator == .logical_or) {
            const binary = expression.binary_operation;
            source.* = .{ .logical_or_scan = .{
                .table = table,
                .left = try plan_source(context, table, binary.left.*, null),
                .right = try plan_source(context, table, binary.right.*, null),
                .limit = limit,
            } };
        } else if (exact_match_key(table, predicate)) |key_expression| {
            source.* = .{ .exact_match = .{ .context = context, .table = table, .key_expression = key_expression } };
        } else if (try range_constraints(context, table, predicate)) |constraints| {
            source.* = .{ .range_scan = .{ .context = context, .table = table, .constraints = constraints, .limit = limit } };
        } else {
            const scan = try context.arena.create(Plan);
            scan.* = .{ .seq_scan = .{ .context = context, .table = table } };
            source.* = .{ .filter = .{ .context = context, .table = table, .source = scan, .predicate = predicate, .limit = limit } };
        }
    } else {
        source.* = .{ .seq_scan = .{ .table = table, .context = context, .limit = limit } };
    }
    return source;
}

fn unwrap_expression(expression: Expression) Expression {
    var result = expression;
    while (result == .nested) result = (result.nested orelse return result).*;
    return result;
}

fn constant_expression(expression: Expression) bool {
    return switch (expression) {
        .value => true,
        .nested => |nested| if (nested) |inner| constant_expression(inner.*) else false,
        .unary_operation => |unary| constant_expression(unary.operand.*),
        .binary_operation => |binary| constant_expression(binary.left.*) and constant_expression(binary.right.*),
        .identifier, .star => false,
    };
}

fn exact_match_key(table: *const muscle.Table, expression: Expression) ?Expression {
    const predicate = unwrap_expression(expression);
    if (predicate != .binary_operation or predicate.binary_operation.operator != .eq) return null;
    const left = unwrap_expression(predicate.binary_operation.left.*);
    const right = unwrap_expression(predicate.binary_operation.right.*);
    if (left == .identifier and std.mem.eql(u8, left.identifier, table.primary_key) and constant_expression(right)) return right;
    if (right == .identifier and std.mem.eql(u8, right.identifier, table.primary_key) and constant_expression(left)) return left;
    return null;
}

// Only conjunctions of primary-key comparisons describe a single contiguous range.
fn collect_range_constraints(table: *const muscle.Table, expression: Expression, constraints: *std.ArrayList(RangeConstraint), allocator: std.mem.Allocator) !bool {
    const predicate = unwrap_expression(expression);
    if (predicate != .binary_operation) return false;
    const binary = predicate.binary_operation;
    if (binary.operator == .logical_and) {
        return try collect_range_constraints(table, binary.left.*, constraints, allocator) and
            try collect_range_constraints(table, binary.right.*, constraints, allocator);
    }
    switch (binary.operator) {
        .eq, .gt, .gte, .lt, .lte => {},
        else => return false,
    }
    const left = unwrap_expression(binary.left.*);
    const right = unwrap_expression(binary.right.*);
    if (left == .identifier and std.mem.eql(u8, left.identifier, table.primary_key) and constant_expression(right)) {
        try constraints.append(allocator, .{ .operator = binary.operator, .expression = right });
        return true;
    }
    if (right == .identifier and std.mem.eql(u8, right.identifier, table.primary_key) and constant_expression(left)) {
        const reversed: BinaryOperator = switch (binary.operator) {
            .gt => .lt,
            .gte => .lte,
            .lt => .gt,
            .lte => .gte,
            .eq => .eq,
            else => unreachable,
        };
        try constraints.append(allocator, .{ .operator = reversed, .expression = left });
        return true;
    }
    return false;
}

fn range_constraints(context: *muscle.QueryContext, table: *const muscle.Table, expression: Expression) !?[]RangeConstraint {
    var constraints: std.ArrayList(RangeConstraint) = .empty;
    if (!try collect_range_constraints(table, expression, &constraints, context.arena)) return null;
    return try constraints.toOwnedSlice(context.arena);
}

// Infer the output type before scanning so empty results still have a schema.
fn expression_data_type(context: *muscle.QueryContext, table: *const muscle.Table, expression: Expression) anyerror!muscle.DataType {
    return try infer_expression_data_type(context, table, expression) orelse error.InvalidExpression;
}

fn infer_expression_data_type(context: *muscle.QueryContext, table: *const muscle.Table, expression: Expression) anyerror!?muscle.DataType {
    return switch (expression) {
        .identifier => |name| if (table.columns.get(name)) |column| column.data_type else blk: {
            try context.set_err(error.ColumnDoesNotExist, "Referenced column does not exist {s}.", .{name});
            break :blk error.ColumnDoesNotExist;
        },
        .value => |value| switch (value) {
            .int => .int,
            .real => .real,
            .bool => .bool,
            .txt => |text| .{ .txt = std.math.cast(u16, text.len) orelse return error.TextTooLong },
            .bin => |bytes| .{ .bin = std.math.cast(u16, bytes.len) orelse return error.BinaryTooLarge },
            .null => null,
        },
        .binary_operation => |binary| blk: {
            _ = try infer_expression_data_type(context, table, binary.left.*);
            _ = try infer_expression_data_type(context, table, binary.right.*);
            break :blk switch (binary.operator) {
                .plus, .minus, .mul, .div => .real,
                .eq, .neq, .lt, .lte, .gt, .gte, .logical_and, .logical_or => .bool,
            };
        },
        .unary_operation => |unary| blk: {
            _ = try infer_expression_data_type(context, table, unary.operand.*);
            break :blk switch (unary.operator) {
                .plus, .minus => .real,
                .not => .bool,
            };
        },
        .nested => |nested| if (nested) |inner| try infer_expression_data_type(context, table, inner.*) else error.InvalidExpression,
        .star => error.InvalidExpression,
    };
}

pub fn resolve_expression(
    context: *muscle.QueryContext,
    table: *const muscle.Table,
    content: muscle.page_types.EncodedCellContent,
    expr: Expression,
) !muscle.Value {
    return switch (expr) {
        .value => |value| value,
        .identifier => |identifier| muscle.common.serde.deserialize_value(context, table, content, identifier),
        .binary_operation => |binary| {
            const left = try resolve_expression(context, table, content, binary.left.*);
            const right = try resolve_expression(context, table, content, binary.right.*);
            switch (binary.operator) {
                .eq, .neq, .lt, .lte, .gt, .gte => {
                    if (left == .null or right == .null) return .null;
                    const left_tag = std.meta.activeTag(left);
                    const right_tag = std.meta.activeTag(right);
                    const operands_are_numeric = (left_tag == .int or left_tag == .real) and
                        (right_tag == .int or right_tag == .real);

                    if (left_tag != right_tag and !operands_are_numeric) {
                        try context.set_err(
                            error.IncompatibleTypes,
                            "Incompatible types for {s} operation. left {s} right {s}",
                            .{ @tagName(binary.operator), @tagName(left), @tagName(right) },
                        );
                        return error.IncompatibleTypes;
                    }

                    const order: std.math.Order = switch (left) {
                        .int => |left_value| switch (right) {
                            .int => |right_value| std.math.order(left_value, right_value),
                            .real => |right_value| std.math.order(@as(f64, @floatFromInt(left_value)), right_value),
                            else => unreachable,
                        },
                        .real => |left_value| switch (right) {
                            .int => |right_value| std.math.order(left_value, @as(f64, @floatFromInt(right_value))),
                            .real => |right_value| std.math.order(left_value, right_value),
                            else => unreachable,
                        },
                        .bool => |left_value| std.math.order(@intFromBool(left_value), @intFromBool(right.bool)),
                        .txt => |left_value| std.mem.order(u8, left_value, right.txt),
                        .bin => |left_value| std.mem.order(u8, left_value, right.bin),
                        .null => .eq,
                    };

                    return .{ .bool = switch (binary.operator) {
                        .eq => order == .eq,
                        .neq => order != .eq,
                        .lt => order == .lt,
                        .lte => order != .gt,
                        .gt => order == .gt,
                        .gte => order != .lt,
                        else => unreachable,
                    } };
                },
                .plus, .minus, .mul, .div => {
                    if (left == .null or right == .null) return .null;
                    const left_tag = std.meta.activeTag(left);
                    const right_tag = std.meta.activeTag(right);
                    if ((left_tag != .int and left_tag != .real) or
                        (right_tag != .int and right_tag != .real))
                    {
                        try context.set_err(
                            error.IncompatibleTypes,
                            "Incompatible types for {s} operation. left {s} right {s}",
                            .{ @tagName(binary.operator), @tagName(left), @tagName(right) },
                        );
                        return error.IncompatibleTypes;
                    }

                    const left_value = left.to_f64();
                    const right_value = right.to_f64();
                    if (binary.operator == .div and right_value == 0) {
                        try context.set_err(error.DivisionByZero, "Cannot divide by zero.", .{});
                        return error.DivisionByZero;
                    }

                    return .{ .real = switch (binary.operator) {
                        .plus => left_value + right_value,
                        .minus => left_value - right_value,
                        .mul => left_value * right_value,
                        .div => left_value / right_value,
                        else => unreachable,
                    } };
                },
                .logical_and, .logical_or => {
                    if ((left != .bool and left != .null) or (right != .bool and right != .null)) {
                        try context.set_err(
                            error.IncompatibleTypes,
                            "Incompatible types for {s} operation. left {s} right {s}",
                            .{ @tagName(binary.operator), @tagName(left), @tagName(right) },
                        );
                        return error.IncompatibleTypes;
                    }

                    // SQL three-valued logic: FALSE AND NULL is FALSE;
                    // TRUE OR NULL is TRUE; other unknown results remain NULL.
                    if (binary.operator == .logical_and) {
                        if ((left == .bool and !left.bool) or (right == .bool and !right.bool)) return .{ .bool = false };
                        if (left == .null or right == .null) return .null;
                        return .{ .bool = true };
                    }
                    if ((left == .bool and left.bool) or (right == .bool and right.bool)) return .{ .bool = true };
                    if (left == .null or right == .null) return .null;
                    return .{ .bool = false };
                },
            }
        },
        .unary_operation => |unary| {
            const operand = try resolve_expression(context, table, content, unary.operand.*);
            return switch (unary.operator) {
                .not => switch (operand) {
                    .bool => |value| .{ .bool = !value },
                    .null => .null,
                    else => {
                        try context.set_err(
                            error.IncompatibleTypes,
                            "Incompatible type for {s} operation. operand {s}",
                            .{ @tagName(unary.operator), @tagName(operand) },
                        );
                        return error.IncompatibleTypes;
                    },
                },
                .plus, .minus => switch (operand) {
                    .null => .null,
                    .int, .real => .{ .real = switch (unary.operator) {
                        .plus => operand.to_f64(),
                        .minus => -operand.to_f64(),
                        else => unreachable,
                    } },
                    else => {
                        try context.set_err(
                            error.IncompatibleTypes,
                            "Incompatible type for {s} operation. operand {s}",
                            .{ @tagName(unary.operator), @tagName(operand) },
                        );
                        return error.IncompatibleTypes;
                    },
                },
            };
        },
        .nested => |nested| {
            const expression = nested orelse {
                try context.set_err(error.InvalidExpression, "Nested expression cannot be empty.", .{});
                return error.InvalidExpression;
            };
            return resolve_expression(context, table, content, expression.*);
        },
        .star => {
            unreachable;
        },
    };
}

test {
    _ = @import("testing/planner.zig");
}
