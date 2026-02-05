const std = @import("std");
const muscle = @import("../muscle.zig");
pub const types = @import("types.zig");
pub const Expression = types.Expression;
pub const BinaryOperator = types.BinaryOperator;
pub const UnaryOperator = types.UnaryOperator;
pub const Statement = types.Statement;
pub const Assignment = types.Assignment;

const assert = std.debug.assert;
const Self = @This();

context: *muscle.QueryContext,
statements: std.ArrayList(Statement),
position: usize,

pub fn init(context: *muscle.QueryContext) Self {
    return Self{
        .context = context,
        .statements = std.ArrayList(Statement){},
        .position = 0,
    };
}

pub fn parse(self: *Self) ![]Statement {
    self.skip_whitespace_and_comments();
    if (self.is_at_end()) {
        try self.context.set_err(error.ParserError, "Invalid sql query.", .{});
        return error.ParserError;
    }

    while (!self.is_at_end()) {
        self.skip_whitespace_and_comments();
        if (self.is_at_end()) break;

        const stmt = try self.parse_statement();
        try self.statements.append(self.context.arena, stmt);
    }

    return self.statements.items;
}

fn parse_statement(self: *Self) !Statement {
    const token = try self.next_token();

    return switch (token) {
        .keyword => |kw| {
            return switch (kw) {
                .create => switch (try self.peek_token()) {
                    Token{ .keyword = .table } => try self.parse_create_table(),
                    else => return self.set_err("Unsupported create statement {s}\n", .{
                        self.token_to_string(try self.peek_token()),
                    }),
                },

                .drop => switch (try self.peek_token()) {
                    Token{ .keyword = .table } => try self.parse_drop_table(),
                    else => return self.set_err("Unsupported drop statement {s}\n", .{
                        self.token_to_string(try self.peek_token()),
                    }),
                },
                .select => try self.parse_select(),
                .insert => try self.parse_insert(),
                .update => try self.parse_update(),
                .delete => try self.parse_delete(),

                .explain => try self.parse_explain(),
                .start => try self.parse_start(),
                .rollback => try self.parse_rollback(),
                .commit => try self.parseCommit(),

                else => return self.set_err("Unsupported statement {s}\n", .{@tagName(kw)}),
            };
        },

        else => return self.set_err("Unexpected token `{s}`", .{@tagName(token)}),
    };
}

fn set_err(self: *Self, comptime message: []const u8, args: anytype) Error {
    var context_start = if (self.position >= 10) self.position - 10 else 0;
    var context_end = @min(self.position + 10, self.context.input.len - 1);

    while (!std.ascii.isWhitespace(self.context.input[context_start]) and context_start > 0) {
        context_start -= 1;
    }

    while (!std.ascii.isWhitespace(self.context.input[context_end]) and context_end < self.context.input.len - 1) {
        context_end += 1;
    }

    const context_str = self.context.input[context_start .. context_end + 1];

    try self.context.set_err(
        error.ParserError,
        message ++ ", Parser error. --> " ++ " `{s}`\n",
        args ++ .{context_str},
    );

    return error.ParserError;
}

fn advance(self: *Self) void {
    self.position += 1;
}

fn parse_number(self: *Self) !u64 {
    const start = self.position;

    while (self.position < self.context.input.len and std.ascii.isDigit(self.peek())) {
        self.advance();
    }
    return std.fmt.parseInt(u64, self.context.input[start..self.position], 10);
}

fn skip_whitespace_and_comments(self: *Self) void {
    while (self.position < self.context.input.len) {
        const c = self.peek();
        if (std.ascii.isWhitespace(c)) {
            self.advance();
        } else if (c == '-' and self.position + 1 < self.context.input.len and self.context.input[self.position + 1] == '-') {
            // Skip line comment
            while (self.position < self.context.input.len and self.peek() != '\n') {
                self.advance();
            }
        } else {
            break;
        }
    }
}

fn is_at_end(self: *const Self) bool {
    return self.position >= self.context.input.len;
}

fn peek(self: *const Self) u8 {
    if (self.is_at_end()) return 0;
    return self.context.input[self.position];
}

fn parse_select(self: *Self) !Statement {
    const columns = try self.parse_select_list();
    var order_by: []Expression = &.{};
    var where_clause: ?Expression = null;
    var limit: usize = 0;

    if (columns.len == 0) {
        return error.ParserError;
    }

    try self.expect_keyword(.from);

    const table_name = try self.expect_identifier("Expected identifier for table name");

    if (try self.consume_optional_keyword(.where)) {
        where_clause = try self.parse_expression();
    }

    if (try self.consume_optional_keyword(.order)) {
        try self.expect_keyword(.by);
        order_by = try self.parse_comma_separated_expressions();
    }

    if (try self.consume_optional_keyword(.limit)) {
        limit = try self.parse_limit();
    }

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .select = .{
            .columns = columns,
            .table = table_name,
            .order_by = order_by,
            .where = where_clause,
            .limit = limit,
        } };
    }

    return self.set_err("Unexepcted token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_select_list(self: *Self) ![]Expression {
    var columns = std.ArrayList(Expression){};

    var star_is_found = false;
    var expr = try self.parse_expression();
    while (true) {
        if (star_is_found) return self.set_err("'*' must be the only column in select list", .{});

        switch (expr) {
            .star => {
                star_is_found = true;
            },
            else => {},
        }

        try columns.append(self.context.arena, expr);
        self.skip_whitespace_and_comments();
        if (try self.peek_token() == .comma) {
            self.advance();
            expr = try self.parse_expression();
        } else {
            break;
        }
    }

    return try columns.toOwnedSlice(self.context.arena);
}

fn parse_limit(self: *Self) !usize {
    const token = try self.next_token();
    switch (token) {
        .number => |num_str| {
            const num = try parse_num(num_str);
            switch (num.value) {
                .int => |limit| {
                    if (limit < 0) {
                        return self.set_err("{s} value must be non-negative, got {}", .{
                            self.token_to_string(.{ .keyword = .limit }),
                            limit,
                        });
                    }
                    return @intCast(limit);
                },
                .real => return self.set_err("{s} value must be an integer, not a decimal", .{
                    self.token_to_string(.{ .keyword = .limit }),
                }),
                else => unreachable,
            }
        },

        else => return self.set_err("Expected non-negative integer for {s}", .{self.token_to_string(.{ .keyword = .limit })}),
    }
}

// Used to parse the expressions after `SELECT`, `WHERE`, `SET` or `ORDER BY`.
fn parse_comma_separated_expressions(self: *Self) ![]Expression {
    return self.parse_comma_separated(Self.parse_expression, false);
}
fn parse_comma_separated_expressions_with_parantheses(self: *Self) ![]Expression {
    return self.parse_comma_separated(Self.parse_expression, true);
}

// Used to parse column names in "insert into ()"
// Expects parantheses and checks for duplicates by default
fn parse_comma_separated_identifiers(self: *Self) ![][]const u8 {
    try self.expect_token(.left_paren);

    var values = std.ArrayList([]const u8){};
    var seen = std.StringHashMap(void).init(self.context.arena);
    defer seen.deinit();

    const first = try self.expect_identifier("Expected identifier");
    try values.append(self.context.arena, first);
    try seen.put(first, {});

    while (try self.consume_optional_token(.comma)) {
        const val = try self.expect_identifier("Expected identifier");

        if (seen.contains(val)) {
            return self.set_err("Duplicate identifier name '{s}' in list", .{val});
        }

        try values.append(self.context.arena, val);
        try seen.put(val, {});
    }

    try self.expect_token(.right_paren);
    return try values.toOwnedSlice(self.context.arena);
}

// Takes a `subparser` as input and calls it after every instance of
// [`Token::Comma`].
fn parse_comma_separated(
    self: *Self,
    comptime subparser: fn (self: *Self) Error!Expression,
    required_parenthesis: bool,
) ![]Expression {
    if (required_parenthesis) {
        try self.expect_token(.left_paren);
    }

    var results = std.ArrayList(Expression){};
    try results.append(self.context.arena, try subparser(self));
    while (try self.consume_optional_token(.comma)) {
        try results.append(self.context.arena, try subparser(self));
    }

    if (required_parenthesis) {
        try self.expect_token(.right_paren);
    }

    return try results.toOwnedSlice(self.context.arena);
}

// SQL tokens.
const Token = union(enum) {
    keyword: Keyword,
    identifier: []const u8,
    string: []const u8,
    number: []const u8,
    newline,
    eq,
    neq,
    lt,
    gt,
    lte,
    gte,
    star,
    div,
    plus,
    minus,
    left_paren,
    right_paren,
    comma,
    semicolon,
    // Not a real token, used to mark the end of a token stream.
    // @Todo Use nulls insted
    eof,
};

// SQL keywords.
const Keyword = enum {
    select,
    create,
    update,
    delete,
    insert,
    into,
    values,
    set,
    drop,
    from,
    where,
    logical_and,
    logical_or,
    primary,
    key,
    unique,
    not,
    null,
    auto_increment,
    increment,
    table,
    database,
    int,
    real,
    txt,
    bin,
    bool,
    true,
    false,
    order,
    by,
    limit,
    index,
    on,
    start,
    transaction,
    rollback,
    commit,
    explain,
};

const Error = error{
    ParserError,
} || std.mem.Allocator.Error;

const UNARY_ARITHMETIC_OPERATOR_PRECEDENCE: u8 = 50;
// Expression parsing using Pratt parsing
// [tutorial]: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing
fn parse_expression(self: *Self) Error!Expression {
    return self.parse_expr(0);
}

fn parse_expr(self: *Self, precedence: u8) Error!Expression {
    var expr = try self.parse_prefix();
    var next_precedence = try self.get_next_precedence();

    while (precedence < next_precedence) {
        expr = try self.parse_infix(expr, next_precedence);
        next_precedence = try self.get_next_precedence();
    }

    return expr;
}

// Parses the beginning of an expression.
fn parse_prefix(self: *Self) !Expression {
    const token = try self.next_token();

    switch (token) {
        .identifier => |ident| return Expression{ .identifier = ident },
        .star => return Expression.star,
        .string => |string| return Expression{ .value = .{ .txt = string } },
        .keyword => |kw| switch (kw) {
            .true => return Expression{ .value = .{ .bool = true } },
            .false => return Expression{ .value = .{ .bool = false } },
            else => return self.set_err("Unexpcted keyword `{s}` while parsing expression", .{@tagName(kw)}),
        },
        .number => |num_str| {
            return try parse_num(num_str);
        },
        .minus, .plus => {
            const operator: UnaryOperator = switch (token) {
                .plus => .plus,
                .minus => .minus,
                else => unreachable,
            };

            const expr_box = try self.context.arena.create(Expression);
            expr_box.* = try self.parse_expr(UNARY_ARITHMETIC_OPERATOR_PRECEDENCE);

            return Expression{ .unary_operation = .{ .operator = operator, .operand = expr_box } };
        },
        .left_paren => {
            const expr = try self.parse_expression();
            try self.expect_token(.right_paren);
            const expr_box = try self.context.arena.create(Expression);
            expr_box.* = expr;
            return Expression{ .nested = expr_box };
        },
        .eof => {
            return self.set_err("Unexpected end of input, expected expression", .{});
        },

        else => {
            return self.set_err("Unexpected token {s}", .{self.token_to_string(token)});
        },
    }
}

fn parse_num(num_str: []const u8) !Expression {
    // Check if the number contains a decimal point
    if (std.mem.indexOf(u8, num_str, ".")) |_| {
        // It's a float
        const parsed_float = std.fmt.parseFloat(f64, num_str) catch {
            return error.ParserError;
        };
        return Expression{ .value = .{ .real = parsed_float } };
    } else {
        // It's an integer
        const parsed_int = std.fmt.parseInt(i64, num_str, 10) catch {
            return error.ParserError;
        };
        return Expression{ .value = .{ .int = parsed_int } };
    }
}

// Parses an infix expression in the form of
// (left expr | operator | right expr).
fn parse_infix(self: *Self, left: Expression, precedence: u8) !Expression {
    const token = try self.next_token();

    const operator: BinaryOperator = switch (token) {
        .plus => .plus,
        .minus => .minus,
        .div => .div,
        .star => .mul,
        .eq => .eq,
        .neq => .neq,
        .gt => .gt,
        .gte => .gte,
        .lt => .lt,
        .lte => .lte,
        .keyword => |kw| switch (kw) {
            .logical_and => .logical_and,
            .logical_or => .logical_or,
            else => return error.ParserError,
        },
        else => return error.ParserError,
    };

    const left_box = try self.context.arena.create(Expression);
    left_box.* = left;

    const right_box = try self.context.arena.create(Expression);
    right_box.* = try self.parse_expr(precedence);

    return Expression{ .binary_operation = .{ .left = left_box, .operator = operator, .right = right_box } };
}

// Returns the precedence value of the next operator in the stream.
fn get_next_precedence(self: *Self) !u8 {
    const token = try self.peek_token();

    return switch (token) {
        .keyword => |kw| switch (kw) {
            .logical_or => 5,
            .logical_and => 10,
            else => 0,
        },
        .eq, .neq, .gt, .gte, .lt, .lte => 20,
        .plus, .minus => 30,
        .star, .div => 40,
        else => 0,
    };
}

fn next_token(self: *Self) !Token {
    self.skip_whitespace_and_comments();

    if (self.is_at_end()) {
        return Token.eof;
    }

    const chr = self.peek();

    switch (chr) {
        '\n' => {
            self.advance();
            return Token.newline;
        },
        '<' => {
            self.advance();
            if (self.peek() == '=') {
                self.advance();
                return Token.lte;
            }
            return Token.lt;
        },
        '>' => {
            self.advance();
            if (self.peek() == '=') {
                self.advance();
                return Token.gte;
            }
            return Token.gt;
        },
        '*' => {
            self.advance();
            return Token.star;
        },
        '/' => {
            self.advance();
            return Token.div;
        },
        '+' => {
            self.advance();
            return Token.plus;
        },
        '-' => {
            self.advance();
            return Token.minus;
        },
        '=' => {
            self.advance();
            return Token.eq;
        },
        '!' => {
            self.advance();
            if (self.peek() == '=') {
                self.advance();
                return Token.neq;
            }
            return error.ParserError;
        },
        '(' => {
            self.advance();
            return Token.left_paren;
        },
        ')' => {
            self.advance();
            return Token.right_paren;
        },
        ',' => {
            self.advance();
            return Token.comma;
        },
        ';' => {
            self.advance();
            return Token.semicolon;
        },
        '"', '\'' => return self.tokenize_string(),
        '0'...'9' => return self.tokenize_number(),
        else => {
            if (is_part_of_ident_or_keyword(chr)) return self.tokenize_keyword_or_identifier();
            return self.set_err("Unexpected character '{c}' at position {}", .{ chr, self.position });
        },
    }
}

fn peek_token(self: *Self) !Token {
    const saved_position = self.position;
    const token = try self.next_token();
    self.position = saved_position;
    return token;
}
// Parses a single quoted or double quoted string like `"this one"` into Token.string.
fn tokenize_string(self: *Self) !Token {
    const quote = self.peek();
    self.advance();

    const start = self.position;
    while (!self.is_at_end() and self.peek() != quote) {
        self.advance();
    }

    if (self.is_at_end()) {
        return error.ParserError;
    }

    const string_value = self.context.input[start..self.position];
    self.advance(); // consume closing quote

    return Token{ .string = string_value };
}

// Tokenizes numbers like `1234`. Floats are not supported.
fn tokenize_number(self: *Self) !Token {
    const start = self.position;
    var has_decimal = false;

    // Parse integer part
    while (!self.is_at_end() and std.ascii.isDigit(self.peek())) {
        self.advance();
    }

    // Check for decimal point
    if (!self.is_at_end() and self.peek() == '.') {
        // Look ahead to see if there's a digit after the decimal point
        if (self.position + 1 < self.context.input.len and std.ascii.isDigit(self.context.input[self.position + 1])) {
            has_decimal = true;
            self.advance(); // consume the '.'

            // Parse fractional part
            while (!self.is_at_end() and std.ascii.isDigit(self.peek())) {
                self.advance();
            }
        }
    }

    const number_value = self.context.input[start..self.position];

    return Token{ .number = number_value };
}

// Attempts to parse an instance of Token.keyword or Token.identifier.
fn tokenize_keyword_or_identifier(self: *Self) !Token {
    const start = self.position;
    while (!self.is_at_end() and is_part_of_ident_or_keyword(self.peek())) {
        self.advance();
    }

    const value = self.context.input[start..self.position];
    const keyword = try get_keyword(value);

    if (keyword) |k| {
        return Token{ .keyword = k };
    }

    return Token{ .identifier = value };
}

fn get_keyword(value: []const u8) !?Keyword {
    var uppercase_buf: [256]u8 = undefined;
    if (value.len > uppercase_buf.len) {
        return error.ParserError;
    }

    for (value, 0..) |c, i| {
        uppercase_buf[i] = std.ascii.toUpper(c);
    }
    const uppercase_value = uppercase_buf[0..value.len];

    const KeywordMap = std.StaticStringMap(Keyword);
    const map = KeywordMap.initComptime(.{
        .{ "SELECT", .select },
        .{ "CREATE", .create },
        .{ "UPDATE", .update },
        .{ "DELETE", .delete },
        .{ "INSERT", .insert },
        .{ "VALUES", .values },
        .{ "INTO", .into },
        .{ "SET", .set },
        .{ "DROP", .drop },
        .{ "FROM", .from },
        .{ "WHERE", .where },
        .{ "AND", .logical_and },
        .{ "OR", .logical_or },
        .{ "PRIMARY", .primary },
        .{ "KEY", .key },
        .{ "UNIQUE", .unique },
        .{ "NOT", .not },
        .{ "NULL", .null },
        .{ "AUTO_INCREMENT", .auto_increment },
        .{ "TABLE", .table },
        .{ "DATABASE", .database },
        .{ "INT", .int },
        .{ "REAL", .real },
        .{ "BOOL", .bool },
        .{ "TEXT", .txt },
        .{ "BINARY", .bin },
        .{ "TRUE", .true },
        .{ "FALSE", .false },
        .{ "ORDER", .order },
        .{ "BY", .by },
        .{ "LIMIT", .limit },
        .{ "INDEX", .index },
        .{ "ON", .on },
        .{ "START", .start },
        .{ "TRANSACTION", .transaction },
        .{ "ROLLBACK", .rollback },
        .{ "COMMIT", .commit },
        .{ "EXPLAIN", .explain },
    });

    return map.get(uppercase_value);
}

fn consume_optional_keyword(self: *Self, keyword: Keyword) !bool {
    const start = self.position;
    const token = try self.next_token();

    return switch (token) {
        .keyword => |kw| if (kw == keyword) {
            return true;
        } else {
            self.position = start;
            return false;
        },
        else => {
            self.position = start;
            return false;
        },
    };
}

fn expect_keyword(self: *Self, keyword: Keyword) !void {
    return try self.expect_token(.{ .keyword = keyword });
}

fn consume_optional_token(self: *Self, optional: Token) !bool {
    if (std.meta.eql(try self.peek_token(), optional)) {
        _ = try self.next_token();
        return true;
    }

    return false;
}

fn expect_token(self: *Self, expected: Token) !void {
    const current = try self.next_token();
    if (!std.meta.eql(current, expected)) {
        return self.set_err(
            "Expected `{s}`, but got `{s}`",
            .{ self.token_to_string(expected), self.token_to_string(current) },
        );
    }
}

fn expect_identifier(self: *Self, comptime expected_msg: []const u8) ![]const u8 {
    const token = try self.next_token();
    switch (token) {
        .identifier => |name| return name,
        else => return self.set_err(expected_msg ++ ", but got `{s}`", .{self.token_to_string(token)}),
    }
}

fn token_to_string(self: *Self, token: Token) []const u8 {
    return switch (token) {
        .keyword => |kw| std.fmt.allocPrint(self.context.arena, "keyword '{s}'", .{@tagName(kw)}) catch @tagName(kw),
        .identifier => |name| std.fmt.allocPrint(self.context.arena, "identifier '{s}'", .{name}) catch "identifier",
        .string => |str| std.fmt.allocPrint(self.context.arena, "string '{s}'", .{str}) catch "string",
        .number => |num| std.fmt.allocPrint(self.context.arena, "number '{s}'", .{num}) catch "number",
        .newline => "newline",
        .eq => "'='",
        .neq => "'!='",
        .lt => "'<'",
        .gt => "'>'",
        .lte => "'<='",
        .gte => "'>='",
        .star => "'*'",
        .div => "'/'",
        .plus => "'+'",
        .minus => "'-'",
        .left_paren => "'('",
        .right_paren => "')'",
        .comma => "','",
        .semicolon => "';'",
        .eof => "end of input",
    };
}

fn is_part_of_ident_or_keyword(chr: u8) bool {
    return std.ascii.isAlphanumeric(chr) or chr == '_';
}

fn parse_insert(self: *Self) !Statement {
    try self.expect_keyword(.into);
    const into = try self.expect_identifier("Expected identifier for table name");
    var columns: [][]const u8 = &.{};
    var values: []Expression = &.{};

    if (try self.peek_token() == .left_paren) {
        columns = try self.parse_comma_separated_identifiers();

        if (columns.len == 0) {
            return self.set_err("Column list cannot be empty. Expected at least one column name", .{});
        }
    }

    try self.expect_keyword(.values);
    values = try self.parse_comma_separated_expressions_with_parantheses();
    if (values.len == 0) {
        return self.set_err("Values list cannot be empty. Expected at least one value", .{});
    }

    if (columns.len > 0 and columns.len != values.len) {
        return self.set_err("Column count ({}) does not match value count ({})", .{ columns.len, values.len });
    }

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .insert = .{
            .into = into,
            .columns = columns,
            .values = values,
        } };
    }

    return self.set_err("Unexepcted token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_update(self: *Self) !Statement {
    const table_name = try self.expect_identifier("Expected identifier for table name");

    try self.expect_keyword(.set);

    var assignments = std.ArrayList(Assignment){};
    var seen_columns = std.StringHashMap(void).init(self.context.arena);

    // Parse first assignment
    const first_column = try self.expect_identifier("Expected column name after SET");
    try self.expect_token(.eq);
    const first_value = try self.parse_expression();

    try assignments.append(self.context.arena, .{ .column = first_column, .value = first_value });
    try seen_columns.put(first_column, {});

    // Parse additional assignments
    while (try self.consume_optional_token(.comma)) {
        const column = try self.expect_identifier("Expected column name");

        // Check for duplicate column assignments
        if (seen_columns.contains(column)) {
            return self.set_err("Duplicate column assignment '{s}' in SET clause", .{column});
        }

        try self.expect_token(.eq);
        const value = try self.parse_expression();

        try assignments.append(self.context.arena, .{ .column = column, .value = value });
        try seen_columns.put(column, {});
    }

    // Parse optional WHERE clause
    var where_clause: ?Expression = null;
    if (try self.consume_optional_keyword(.where)) {
        where_clause = try self.parse_expression();
    }

    // Expect statement termination
    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .update = .{
            .table = table_name,
            .assignments = try assignments.toOwnedSlice(self.context.arena),
            .where = where_clause,
        } };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_create_table(self: *Self) !Statement {
    try self.expect_keyword(.table);

    const table_name = try self.expect_identifier("Expected table name after CREATE TABLE");

    try self.expect_token(.left_paren);

    var columns = std.ArrayList(muscle.Column){};
    var seen_columns = std.StringHashMap(void).init(self.context.arena);
    var primary_key_column_index: ?usize = null;

    const first_column = try self.parse_column_definition(&seen_columns);
    if (first_column.is_primary_key) primary_key_column_index = 0;
    try columns.append(self.context.arena, first_column.column);

    while (try self.consume_optional_token(.comma)) {
        const column = try self.parse_column_definition(&seen_columns);
        if (column.is_primary_key) {
            if (primary_key_column_index) |index| {
                return self.set_err(
                    "Column {s} already marked as PRIMARY KEY, Only one column can be marked as primary key.",
                    .{columns.items[index].name},
                );
            }

            primary_key_column_index = columns.items.len;
        }

        try columns.append(self.context.arena, column.column);
    }

    try self.expect_token(.right_paren);

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{
            .create_table = .{
                .table = table_name,
                .columns = try columns.toOwnedSlice(self.context.arena),
                .primary_key_column_index = primary_key_column_index,
            },
        };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_column_definition(self: *Self, seen_columns: *std.StringHashMap(void)) !struct {
    column: muscle.Column,
    is_primary_key: bool,
} {
    const column_name = try self.expect_identifier("Expected column name");

    if (seen_columns.contains(column_name)) {
        return self.set_err("Duplicate column name '{s}' in table definition", .{column_name});
    }
    try seen_columns.put(column_name, {});

    const column_type = try self.parse_column_type();

    var is_primary_key = false;
    var unique = false;
    var not_null = false;
    var auto_increment = false;

    // constraints
    while (true) {
        if (try self.consume_optional_keyword(.primary)) {
            try self.expect_keyword(.key);

            if (is_primary_key) {
                return self.set_err("Column '{s}' already marked as PRIMARY KEY", .{column_name});
            }

            is_primary_key = true;
        } else if (try self.consume_optional_keyword(.unique)) {
            if (unique) {
                return self.set_err("Column '{s}' already marked as UNIQUE", .{column_name});
            }
            unique = true;
        } else if (try self.consume_optional_keyword(.not)) {
            try self.expect_keyword(.null);
            if (not_null) {
                return self.set_err("Column '{s}' already marked as NOT NULL", .{column_name});
            }
            not_null = true;
        } else if (try self.consume_optional_keyword(.auto_increment)) {
            if (auto_increment) {
                return self.set_err("Column '{s}' already marked as AUTO_INCREMENT", .{column_name});
            }
            auto_increment = true;
        } else {
            break;
        }
    }

    return .{
        .column = muscle.Column{
            .name = column_name,
            .data_type = column_type,
            .unique = unique,
            .not_null = not_null,
            .auto_increment = auto_increment,
        },
        .is_primary_key = is_primary_key,
    };
}

fn parse_column_type(self: *Self) !muscle.DataType {
    const token = try self.next_token();

    switch (token) {
        .keyword => |kw| switch (kw) {
            .int => return .int,
            .bool => return .bool,
            .txt => {
                // Check for optional size specification: TEXT(size)
                if (try self.consume_optional_token(.left_paren)) {
                    const size_token = try self.next_token();
                    switch (size_token) {
                        .number => |num_str| {
                            const size = std.fmt.parseInt(u16, num_str, 10) catch {
                                return self.set_err("Invalid size '{s}' for TEXT type. Size must be a positive integer", .{num_str});
                            };

                            if (size == 0) {
                                return self.set_err("TEXT size must be greater than 0", .{});
                            }

                            try self.expect_token(.right_paren);
                            return muscle.DataType{ .txt = size };
                        },
                        else => return self.set_err("Expected size (positive integer) after TEXT(, but got `{s}`", .{self.token_to_string(size_token)}),
                    }
                } else {
                    // Default TEXT without size specification
                    return muscle.DataType{ .txt = std.math.maxInt(u16) };
                }
            },
            .bin => {
                // Check for optional size specification: BINARY(size)
                if (try self.consume_optional_token(.left_paren)) {
                    const size_token = try self.next_token();
                    switch (size_token) {
                        .number => |num_str| {
                            const size = std.fmt.parseInt(u16, num_str, 10) catch {
                                return self.set_err("Invalid size '{s}' for BINARY type. Size must be a positive integer", .{num_str});
                            };

                            if (size == 0) {
                                return self.set_err("BINARY size must be greater than 0", .{});
                            }

                            try self.expect_token(.right_paren);
                            return muscle.DataType{ .bin = size };
                        },
                        else => return self.set_err("Expected size (positive integer) after BINARY(, but got `{s}`", .{self.token_to_string(size_token)}),
                    }
                } else {
                    // Default BINARY without size specification
                    return muscle.DataType{ .bin = std.math.maxInt(u16) };
                }
            },
            .real => return .real,
            else => return self.set_err("Invalid column type '{s}'", .{@tagName(kw)}),
        },
        else => return self.set_err("Expected column type (INT, TEXT, BINARY, REAL, or BOOL), but got `{s}`", .{self.token_to_string(token)}),
    }
}

fn parse_drop_table(self: *Self) !Statement {
    try self.expect_keyword(.table);
    const table_name = try self.expect_identifier("Expected table name after DROP TABLE");

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .drop_table = .{ .table = table_name } };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_delete(self: *Self) !Statement {
    try self.expect_keyword(.from);
    const from = try self.expect_identifier("Expected table name after DELETE FROM");

    var where_clause: ?Expression = null;
    if (try self.consume_optional_keyword(.where)) {
        where_clause = try self.parse_expression();
    }

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .delete = .{
            .from = from,
            .where = where_clause,
        } };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_explain(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_start(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parseCommit(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_rollback(self: *Self) !Statement {
    _ = self;
    unreachable;
}

test {
    _ = @import("tests.zig");
}

// TODO
// Current error handling does not report exact error position.
// This can be improved by:
// store the start position before starting to parse next token/expression in some variable inside parse_expression, expect_keyword, expect_token and if we don't get what we want we know exact start position from where the problem began.
//
