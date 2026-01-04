const std = @import("std");
const muscle = @import("../muscle.zig");

const Expression = @import("expression.zig").Expression;
const BinaryOperator = @import("expression.zig").BinaryOperator;
const UnaryOperator = @import("expression.zig").UnaryOperator;
const statement = @import("statement.zig");

const Statement = statement.Statement;
const Assignment = statement.Assignment;

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
    var order_by = std.ArrayList(Expression){};
    var where_clause: ?Expression = null;
    var limit: usize = 0;

    if (columns.items.len == 0) {
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

fn parse_select_list(self: *Self) !std.ArrayList(Expression) {
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

    return columns;
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
fn parse_comma_separated_expressions(self: *Self) !std.ArrayList(Expression) {
    return self.parse_comma_separated(Self.parse_expression, false);
}
fn parse_comma_separated_expressions_with_parantheses(self: *Self) !std.ArrayList(Expression) {
    return self.parse_comma_separated(Self.parse_expression, true);
}

// Used to parse column names in "insert into ()"
// Expects parantheses and checks for duplicates by default
fn parse_comma_separated_identifiers(self: *Self) !std.ArrayList([]const u8) {
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
    return values;
}

// Takes a `subparser` as input and calls it after every instance of
// [`Token::Comma`].
fn parse_comma_separated(
    self: *Self,
    comptime subparser: fn (self: *Self) Error!Expression,
    required_parenthesis: bool,
) !std.ArrayList(Expression) {
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

    return results;
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

    // Validate that we have at least one digit
    if (number_value.len == 0 or (has_decimal and number_value.len == 1)) {
        return error.ParserError;
    }

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
    var columns = std.ArrayList([]const u8){};
    var values = std.ArrayList(Expression){};

    if (try self.peek_token() == .left_paren) {
        columns = try self.parse_comma_separated_identifiers();

        if (columns.items.len == 0) {
            return self.set_err("Column list cannot be empty. Expected at least one column name", .{});
        }
    }

    try self.expect_keyword(.values);
    values = try self.parse_comma_separated_expressions_with_parantheses();
    if (values.items.len == 0) {
        return self.set_err("Values list cannot be empty. Expected at least one value", .{});
    }

    if (columns.items.len > 0 and columns.items.len != values.items.len) {
        return self.set_err("Column count ({}) does not match value count ({})", .{ columns.items.len, values.items.len });
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
            .assignments = assignments,
            .where = where_clause,
        } };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_create_table(self: *Self) !Statement {
    try self.expect_keyword(.table);

    const table_name = try self.expect_identifier("Expected table name after CREATE TABLE");

    try self.expect_token(.left_paren);

    var columns = std.ArrayList(statement.ColumnDefinition){};
    var seen_columns = std.StringHashMap(void).init(self.context.arena);
    var primary_key_count: u32 = 0;

    const first_column = try self.parse_column_definition(&seen_columns, &primary_key_count);
    try columns.append(self.context.arena, first_column);

    while (try self.consume_optional_token(.comma)) {
        const column = try self.parse_column_definition(&seen_columns, &primary_key_count);
        try columns.append(self.context.arena, column);
    }

    try self.expect_token(.right_paren);

    if (try self.consume_optional_token(.semicolon) or
        try self.consume_optional_token(.eof))
    {
        return Statement{ .create_table = .{ .table = table_name, .columns = columns } };
    }

    return self.set_err("Unexpected token `{s}`", .{self.token_to_string(try self.next_token())});
}

fn parse_column_definition(
    self: *Self,
    seen_columns: *std.StringHashMap(void),
    primary_key_count: *u32,
) !statement.ColumnDefinition {
    const column_name = try self.expect_identifier("Expected column name");

    if (seen_columns.contains(column_name)) {
        return self.set_err("Duplicate column name '{s}' in table definition", .{column_name});
    }
    try seen_columns.put(column_name, {});

    const column_type = try self.parse_column_type();

    var is_primary_key = false;
    var is_unique = false;

    // constraints
    while (true) {
        if (try self.consume_optional_keyword(.primary)) {
            try self.expect_keyword(.key);
            if (is_primary_key) {
                return self.set_err("Column '{s}' already marked as PRIMARY KEY", .{column_name});
            }
            is_primary_key = true;
            primary_key_count.* += 1;

            if (primary_key_count.* > 1) {
                return self.set_err("Multiple PRIMARY KEY constraints are not allowed", .{});
            }
        } else if (try self.consume_optional_keyword(.unique)) {
            if (is_unique) {
                return self.set_err("Column '{s}' already marked as UNIQUE", .{column_name});
            }
            is_unique = true;
        } else {
            break;
        }
    }

    return statement.ColumnDefinition{
        .name = column_name,
        .column_type = column_type,
        .is_primary_key = is_primary_key,
        .is_unique = is_unique,
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
    std.testing.refAllDecls(@This());
}

test "parseSelect" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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
        try std.testing.expect(select.select.columns.items.len == 1);
        try std.testing.expectEqualStrings(select.select.table, "duck");
        try std.testing.expect(select.select.limit == 12);
    }
}

test "parseInsert" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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
        try std.testing.expectEqualStrings(insert.columns.items[0], "col1");
        try std.testing.expectEqualStrings(insert.columns.items[1], "col2");
        try std.testing.expect(insert.values.items.len == 2);
    }
}

test "parseUpdate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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
        try std.testing.expect(update.update.assignments.items.len == 1);
        try std.testing.expectEqualStrings(update.update.assignments.items[0].column, "name");
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
        try std.testing.expect(update.update.assignments.items.len == 2);
        try std.testing.expectEqualStrings(update.update.assignments.items[0].column, "price");
        try std.testing.expectEqualStrings(update.update.assignments.items[1].column, "stock");
        try std.testing.expect(update.update.where != null);
        try std.testing.expect(!context.result.is_error_result());
    }
}

test "parseCreateTable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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
        try std.testing.expect(create.create_table.columns.items.len == 3);

        // Check first column
        try std.testing.expectEqualStrings(create.create_table.columns.items[0].name, "id");
        try std.testing.expect(create.create_table.columns.items[0].column_type == .int);
        try std.testing.expect(!create.create_table.columns.items[0].is_primary_key);
        try std.testing.expect(!create.create_table.columns.items[0].is_unique);

        // Check second column
        try std.testing.expectEqualStrings(create.create_table.columns.items[1].name, "name");
        try std.testing.expect(create.create_table.columns.items[1].column_type.txt == std.math.maxInt(u16));

        // Check third column
        try std.testing.expectEqualStrings(create.create_table.columns.items[2].name, "active");
        try std.testing.expect(create.create_table.columns.items[2].column_type == .bool);

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
        try std.testing.expect(create.create_table.columns.items.len == 4);

        // Check primary key column
        try std.testing.expectEqualStrings(create.create_table.columns.items[0].name, "id");
        try std.testing.expect(create.create_table.columns.items[0].column_type == .int);
        try std.testing.expect(create.create_table.columns.items[0].is_primary_key);
        try std.testing.expect(!create.create_table.columns.items[0].is_unique);

        // Check unique column with size
        try std.testing.expectEqualStrings(create.create_table.columns.items[1].name, "name");
        try std.testing.expect(create.create_table.columns.items[1].column_type.txt == 100);
        try std.testing.expect(!create.create_table.columns.items[1].is_primary_key);
        try std.testing.expect(create.create_table.columns.items[1].is_unique);

        // Check real column
        try std.testing.expectEqualStrings(create.create_table.columns.items[2].name, "price");
        try std.testing.expect(create.create_table.columns.items[2].column_type == .real);

        // Check text column without size
        try std.testing.expectEqualStrings(create.create_table.columns.items[3].name, "description");
        try std.testing.expect(create.create_table.columns.items[3].column_type.txt == std.math.maxInt(u16));

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
        try std.testing.expect(create.create_table.columns.items.len == 4);

        // Check binary column with size
        try std.testing.expectEqualStrings(create.create_table.columns.items[2].name, "data");
        try std.testing.expect(create.create_table.columns.items[2].column_type.bin == 1024);

        // Check binary column without size
        try std.testing.expectEqualStrings(create.create_table.columns.items[3].name, "thumbnail");
        try std.testing.expect(create.create_table.columns.items[3].column_type.bin == std.math.maxInt(u16));

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
        try std.testing.expect(create.create_table.columns.items.len == 5);

        try std.testing.expect(create.create_table.columns.items[0].column_type == .int);
        try std.testing.expect(create.create_table.columns.items[1].column_type == .real);
        try std.testing.expect(create.create_table.columns.items[2].column_type.txt == std.math.maxInt(u16));
        try std.testing.expect(create.create_table.columns.items[3].column_type.bin == std.math.maxInt(u16));
        try std.testing.expect(create.create_table.columns.items[4].column_type == .bool);

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
        try std.testing.expect(create.create_table.columns.items.len == 1);
        try std.testing.expect(!context.result.is_error_result());
    }
}

test "parseDropTable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var context = muscle.QueryContext.init(arena.allocator(), "");
    var parser = Self.init(&context);

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

// TODO
// Current error handling does not report exact error position.
// This can be improved by:
// store the start position before starting to parse next token/expression in some variable inside parse_expression, expect_keyword, expect_token and if we don't get what we want we know exact start position from where the problem began.
//
