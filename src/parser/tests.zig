const std = @import("std");
const muscle = @import("../muscle.zig");
const Parser = @import("Parser.zig");

const helpers = muscle.common.helpers;

test "parseSelect" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    {
        context.input = "invalid";
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select a,";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select *, a";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select *";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * col";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from duck dd";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from duck where a < 3 && b > 43";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from duck where a < limit";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from duck where a < 3 and b > 43 limit -12";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "select * from duck where a < 3 and b > 43 limit 12";
        parser.position = 0;
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const select = statements[0];
        try std.testing.expect(select.select.columns.len == 1);
        try std.testing.expectEqualStrings(select.select.table, "duck");
        try std.testing.expect(select.select.limit == 12);
    }
}

test "parseInsert" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    {
        context.input = "insert";
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col2)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col2) values";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck () values ()";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1) values ()";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col1) values (val)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col2) values (val)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col2) values (val, val) &";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "insert into duck (col1, col2) values (val, val) ;";
        parser.position = 0;
        context.result = .{ .data = .__void };
        const statements = try parser.parse();

        try std.testing.expect(!context.result.is_error_result());
        const insert = statements[0].insert;
        try std.testing.expectEqualStrings(insert.into, "duck");
        try std.testing.expectEqualStrings(insert.columns[0], "col1");
        try std.testing.expectEqualStrings(insert.columns[1], "col2");
        try std.testing.expect(insert.values.len == 2);
    }
}

test "parseUpdate" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    {
        context.input = "update";
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name =";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name = 'John', name = 'Jane'";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name = 'John', age = 25,";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name = 'John' where";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name = 'John' &";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    {
        context.input = "update users set name = 'John';";
        parser.position = 0;
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const update = statements[0];
        try std.testing.expect(update == .update);
        try std.testing.expectEqualStrings(update.update.table, "users");
        try std.testing.expect(update.update.assignments.len == 1);
        try std.testing.expectEqualStrings(update.update.assignments[0].column, "name");
        try std.testing.expect(update.update.where == null);
        try std.testing.expect(!context.result.is_error_result());
    }

    {
        context.input = "update products set price = 99.99, stock = 50 where id = 1";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const update = statements[0];
        try std.testing.expect(update == .update);
        try std.testing.expectEqualStrings(update.update.table, "products");
        try std.testing.expect(update.update.assignments.len == 2);
        try std.testing.expectEqualStrings(update.update.assignments[0].column, "price");
        try std.testing.expectEqualStrings(update.update.assignments[1].column, "stock");
        try std.testing.expect(update.update.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }
}

test "parseCreateTable" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    // Test: Missing TABLE keyword
    {
        context.input = "create";
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing table name
    {
        context.input = "create table";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing opening parenthesis
    {
        context.input = "create table users";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Empty column list
    {
        context.input = "create table users ()";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing column type
    {
        context.input = "create table users (id)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid column type
    {
        context.input = "create table users (id varchar)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Duplicate column names
    {
        context.input = "create table users (id int, id text)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Multiple primary keys
    {
        context.input = "create table users (id int primary key, email text primary key)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Duplicate constraints on same column
    {
        context.input = "create table users (id int primary key primary key)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Duplicate UNIQUE constraints on same column
    {
        context.input = "create table users (email text unique unique)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing closing parenthesis
    {
        context.input = "create table users (id int";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid TEXT size (zero)
    {
        context.input = "create table users (name text(0))";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid TEXT size (non-numeric)
    {
        context.input = "create table users (name text(abc))";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid BINARY size (zero)
    {
        context.input = "create table files (data binary(0))";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing closing parenthesis for TEXT size
    {
        context.input = "create table users (name text(255)";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Unexpected token after table definition
    {
        context.input = "create table users (id int) &";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Simple table with basic types
    {
        context.input = "create table users (id int, name text, active bool);";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const create = statements[0];
        try std.testing.expect(create == .create_table);
        try std.testing.expectEqualStrings(create.create_table.table, "users");
        try std.testing.expect(create.create_table.columns.len == 3);

        // Check first column
        try std.testing.expectEqualStrings(create.create_table.columns[0].name, "id");
        try std.testing.expect(create.create_table.columns[0].data_type == .int);
        try std.testing.expect(!create.create_table.columns[0].unique);

        // Check second column
        try std.testing.expectEqualStrings(create.create_table.columns[1].name, "name");
        try std.testing.expect(create.create_table.columns[1].data_type.txt == std.math.maxInt(u16));

        // Check third column
        try std.testing.expectEqualStrings(create.create_table.columns[2].name, "active");
        try std.testing.expect(create.create_table.columns[2].data_type == .bool);

        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Table with primary key and constraints
    {
        context.input = "create table products (id int primary key, name text(100) unique, price real, description text)";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const create = statements[0];
        try std.testing.expect(create == .create_table);
        try std.testing.expectEqualStrings(create.create_table.table, "products");
        try std.testing.expect(create.create_table.columns.len == 4);

        // Check primary key column
        try std.testing.expectEqualStrings(create.create_table.columns[0].name, "id");
        try std.testing.expect(create.create_table.columns[0].data_type == .int);
        try std.testing.expect(!create.create_table.columns[0].unique);

        // Check unique column with size
        try std.testing.expectEqualStrings(create.create_table.columns[1].name, "name");
        try std.testing.expect(create.create_table.columns[1].data_type.txt == 100);
        try std.testing.expect(create.create_table.columns[1].unique);

        // Check real column
        try std.testing.expectEqualStrings(create.create_table.columns[2].name, "price");
        try std.testing.expect(create.create_table.columns[2].data_type == .real);

        // Check text column without size
        try std.testing.expectEqualStrings(create.create_table.columns[3].name, "description");
        try std.testing.expect(create.create_table.columns[3].data_type.txt == std.math.maxInt(u16));

        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Table with binary data and various sizes
    {
        context.input = "create table files (id int primary key, filename text(255), data binary(1024), thumbnail binary)";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const create = statements[0];
        try std.testing.expect(create == .create_table);
        try std.testing.expectEqualStrings(create.create_table.table, "files");
        try std.testing.expect(create.create_table.columns.len == 4);

        // Check binary column with size
        try std.testing.expectEqualStrings(create.create_table.columns[2].name, "data");
        try std.testing.expect(create.create_table.columns[2].data_type.bin == 1024);

        // Check binary column without size
        try std.testing.expectEqualStrings(create.create_table.columns[3].name, "thumbnail");
        try std.testing.expect(create.create_table.columns[3].data_type.bin == std.math.maxInt(u16));

        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Table with all data types
    {
        context.input = "create table test_types (id int, score real, name text, data binary, active bool);";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const create = statements[0];
        try std.testing.expect(create == .create_table);
        try std.testing.expect(create.create_table.columns.len == 5);

        try std.testing.expect(create.create_table.columns[0].data_type == .int);
        try std.testing.expect(create.create_table.columns[1].data_type == .real);
        try std.testing.expect(create.create_table.columns[2].data_type.txt == std.math.maxInt(u16));
        try std.testing.expect(create.create_table.columns[3].data_type.bin == std.math.maxInt(u16));
        try std.testing.expect(create.create_table.columns[4].data_type == .bool);

        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Table without semicolon (EOF termination)
    {
        context.input = "create table simple (id int)";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const create = statements[0];
        try std.testing.expect(create == .create_table);
        try std.testing.expectEqualStrings(create.create_table.table, "simple");
        try std.testing.expect(create.create_table.columns.len == 1);
        try std.testing.expect(!context.result.is_error_result());
    }
}

test "parseDropTable" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    // Test: Missing TABLE keyword
    {
        context.input = "drop";
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing table name
    {
        context.input = "drop table";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid token after table name
    {
        context.input = "drop table users &";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Multiple table names (not supported)
    {
        context.input = "drop table users, products";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Keyword as table name (should fail)
    {
        context.input = "drop table select";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Number as table name (should fail)
    {
        context.input = "drop table 123";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: String as table name (should fail)
    {
        context.input = "drop table 'users'";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Extra tokens after valid statement
    {
        context.input = "drop table users cascade";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Valid drop table with semicolon
    {
        context.input = "drop table users;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "users");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid drop table without semicolon (EOF termination)
    {
        context.input = "drop table products";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "products");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid drop table with underscore in name
    {
        context.input = "drop table user_profiles;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "user_profiles");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid drop table with numbers in name
    {
        context.input = "drop table table123;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "table123");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid drop table with mixed case
    {
        context.input = "DROP TABLE MyTable;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "MyTable");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Long table name
    {
        context.input = "drop table very_long_table_name_with_many_underscores_and_characters;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "very_long_table_name_with_many_underscores_and_characters");
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Table name starting with underscore
    {
        context.input = "drop table _private_table;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const drop = statements[0];
        try std.testing.expect(drop == .drop_table);
        try std.testing.expectEqualStrings(drop.drop_table.table, "_private_table");
        try std.testing.expect(!context.result.is_error_result());
    }
}

test "parseDelete" {
    var file = try helpers.get_temp_file_path("test_tree_operations");
    defer file.deinit();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var pager = try muscle.PageManager.init(file.file_path, std.testing.allocator);
    var catalog = try muscle.Catalog_Manager.init(std.testing.allocator, &pager);
    defer {
        pager.deinit();
        catalog.deinit();
    }

    var context = muscle.QueryContext.init(arena.allocator(), "", &pager, &catalog);
    var parser = Parser.init(&context);

    // Test: Missing FROM keyword
    {
        context.input = "delete";
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Missing table name
    {
        context.input = "delete from";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid token after table name
    {
        context.input = "delete from users &";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Incomplete WHERE clause
    {
        context.input = "delete from users where";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Invalid WHERE expression
    {
        context.input = "delete from users where id =";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Keyword as table name (should fail)
    {
        context.input = "delete from select";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Number as table name (should fail)
    {
        context.input = "delete from 123";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: String as table name (should fail)
    {
        context.input = "delete from 'users'";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Extra tokens after valid statement
    {
        context.input = "delete from users cascade";
        parser.position = 0;
        context.result = .{ .data = .__void };
        _ = parser.parse() catch {};
        try std.testing.expect(context.result.is_error_result());
    }

    // Test: Valid delete without WHERE clause (with semicolon)
    {
        context.input = "delete from users;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "users");
        try std.testing.expect(delete.delete.where == null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete without WHERE clause (EOF termination)
    {
        context.input = "delete from products";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "products");
        try std.testing.expect(delete.delete.where == null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with simple WHERE clause
    {
        context.input = "delete from users where id = 1;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "users");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with complex WHERE clause
    {
        context.input = "delete from orders where status = 'cancelled' and created_at < '2023-01-01'";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "orders");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with numeric comparison
    {
        context.input = "delete from products where price > 100.50;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "products");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with boolean WHERE clause
    {
        context.input = "delete from users where active = false";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "users");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with table name containing underscores
    {
        context.input = "delete from user_profiles where user_id = 42;";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "user_profiles");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with OR condition
    {
        context.input = "delete from logs where level = 'debug' or level = 'trace'";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "logs");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }

    // Test: Valid delete with parenthesized WHERE expression
    {
        context.input = "delete from items where (category = 'electronics' and price < 50)";
        parser.position = 0;
        parser.statements.clearAndFree(arena.allocator());
        context.result = .{ .data = .__void };
        const statements = try parser.parse();
        const delete = statements[0];
        try std.testing.expect(delete == .delete);
        try std.testing.expectEqualStrings(delete.delete.from, "items");
        try std.testing.expect(delete.delete.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }
}
