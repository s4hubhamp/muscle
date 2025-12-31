const std = @import("std");
const muscle = @import("../muscle.zig");

const Expression = @import("expression.zig").Expression;
const Statement = @import("statement.zig").Statement;
const BinaryOperator = @import("expression.zig").BinaryOperator;
const UnaryOperator = @import("expression.zig").UnaryOperator;

const assert = std.debug.assert;
const Self = @This();

allocator: std.mem.Allocator,
statements: std.ArrayList(Statement),
input: []const u8,
position: usize,

pub fn init(allocator: std.mem.Allocator, input: []const u8) Self {
    return Self{
        .input = input,
        .allocator = allocator,
        .statements = std.ArrayList(Statement).init(allocator),
        .position = 0,
    };
}

pub fn parse(self: *Self) ![]Statement {
    while (!self.is_at_end()) {
        self.skip_whitespace_and_comments();
        if (self.is_at_end()) break;

        const stmt = try self.parse_statement();
        try self.statements.append(stmt);
    }

    return self.statements.items;
}

fn advance(self: *Self) void {
    self.position += 1;
}

// @Todo error type
fn parse_statement(self: *Self) !Statement {
    const token = try self.next_token();

    return switch (token) {
        .keyword => |kw| {
            return switch (kw) {
                .create => try self.parse_create(),
                .drop => try self.parse_drop(),

                .select => try self.parse_select(),
                .insert => try self.parse_insert(),
                .update => try self.parse_update(),
                .delete => try self.parse_delete(),

                .explain => try self.parse_explain(),

                .start => try self.parse_start(),
                .rollback => try self.parse_rollback(),
                .commit => try self.parseCommit(),

                else => error.UnknownStatement,
            };
        },
        else => return error.UnknownStatement,
    };
}

fn parse_number(self: *Self) !u64 {
    const start = self.position;
    while (self.position < self.input.len and std.ascii.isDigit(self.peek())) {
        self.advance();
    }
    return std.fmt.parseInt(u64, self.input[start..self.position], 10);
}

fn skip_whitespace_and_comments(self: *Self) void {
    while (self.position < self.input.len) {
        const c = self.peek();
        if (std.ascii.isWhitespace(c)) {
            self.advance();
        } else if (c == '-' and self.position + 1 < self.input.len and self.input[self.position + 1] == '-') {
            // Skip line comment
            while (self.position < self.input.len and self.peek() != '\n') {
                self.advance();
            }
        } else {
            break;
        }
    }
}

fn is_at_end(self: *const Self) bool {
    return self.position >= self.input.len;
}

fn peek(self: *const Self) u8 {
    if (self.is_at_end()) return 0;
    return self.input[self.position];
}

fn parse_select(self: *Self) !Statement {
    const columns = try self.parse_select_list();
    var order_by = std.ArrayList(Expression).init(self.allocator);
    var where_clause: ?Expression = null;
    var limit: usize = 0;

    if (columns.items.len == 0) {
        return error.InvalidStatement;
    }

    try self.expect_keyword(.from);

    const table_name: []const u8 = sw: switch (try self.next_token()) {
        .identifier => |name| {
            break :sw name;
        },
        else => return error.UnexpectedToken,
    };

    if (try self.consume_optional_keyword(.where)) {
        where_clause = try self.parse_expression();
    }

    if (try self.consume_optional_keyword(.order)) {
        try self.expect_keyword(.by);
        order_by = try self.parse_comma_separated_expressions();
    }

    // if there is a token still left it must be limit otherwise it's a invalid token
    if (try self.peek_token() != .eof) {
        try self.expect_keyword(.limit);
        limit = try self.parse_limit();
    }

    std.debug.print("columns: {any} where_clause: {any} \n", .{ columns.items, where_clause });
    std.debug.print("position: {any} \n", .{self.position});
    std.debug.print("order_by: {any} \n", .{order_by.items});
    std.debug.print("limit: {any} \n", .{limit});
    std.debug.print("\n", .{});

    return Statement{ .select = .{
        .columns = columns,
        .table = table_name,
        .order_by = order_by,
        .where = where_clause,
        .limit = limit,
    } };
}

fn parse_select_list(self: *Self) !std.ArrayList(Expression) {
    var columns = std.ArrayList(Expression).init(self.allocator);

    var star_is_found = false;
    var expr = try self.parse_expression();
    while (true) {
        if (star_is_found) return error.InvalidStatement;
        switch (expr) {
            .star => {
                star_is_found = true;
            },
            else => {},
        }

        try columns.append(expr);
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
                        return error.UnexpectedToken;
                    }

                    return @intCast(limit);
                },
                .real => {
                    return error.UnexpectedToken;
                },
                else => {
                    unreachable;
                },
            }
        },
        else => {
            return error.UnexpectedToken;
        },
    }
}

// Used to parse the expressions after `SELECT`, `WHERE`, `SET` or `ORDER BY`.
fn parse_comma_separated_expressions(self: *Self) !std.ArrayList(Expression) {
    return self.parse_comma_separated(Self.parse_expression, false);
}

// Takes a `subparser` as input and calls it after every instance of
// [`Token::Comma`].
fn parse_comma_separated(
    self: *Self,
    comptime subparser: fn (self: *Self) ParseError!Expression,
    required_parenthesis: bool,
) !std.ArrayList(Expression) {
    if (required_parenthesis) {
        try self.expect_token(.left_paren);
    }

    var results = std.ArrayList(Expression).init(self.allocator);
    try results.append(try subparser(self));
    while (try self.consume_optional_token(.comma)) {
        try results.append(try subparser(self));
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
    newline,
    string: []const u8,
    number: []const u8,
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
    txt,
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

const ParseError = error{
    IntegerOutOfRange,
    ExpectedToken,
    InvalidExpression,
} || TokenError || std.mem.Allocator.Error;

const UNARY_ARITHMETIC_OPERATOR_PRECEDENCE: u8 = 50;
// Expression parsing using Pratt parsing
// [tutorial]: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing
fn parse_expression(self: *Self) ParseError!Expression {
    return self.parse_expr(0);
}

fn parse_expr(self: *Self, precedence: u8) ParseError!Expression {
    var expr = try self.parse_prefix();
    var next_precedence = try self.get_next_precedence();

    while (precedence < next_precedence) {
        expr = try self.parse_infix(expr, next_precedence);
        next_precedence = try self.get_next_precedence();
    }

    return expr;
}

// Parses the beginning of an expression.
fn parse_prefix(self: *Self) ParseError!Expression {
    const token = try self.next_token();

    switch (token) {
        .identifier => |ident| return Expression{ .identifier = ident },
        .star => return Expression.star,
        .string => |string| return Expression{ .value = .{ .txt = string } },
        .keyword => |kw| switch (kw) {
            .true => return Expression{ .value = .{ .bool = true } },
            .false => return Expression{ .value = .{ .bool = false } },
            else => {
                std.debug.print("While parsing expression prefix received unexpected token {any} at position {}", .{
                    kw,
                    self.position,
                });
                std.debug.print("keyword {any} is not allowed inside expressions.", .{kw});
                return ParseError.UnexpectedToken;
            },
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

            const expr_box = try self.allocator.create(Expression);
            expr_box.* = try self.parse_expr(UNARY_ARITHMETIC_OPERATOR_PRECEDENCE);

            return Expression{ .unary_operation = .{ .operator = operator, .operand = expr_box } };
        },
        .left_paren => {
            const expr = try self.parse_expression();
            try self.expect_token(.right_paren);
            const expr_box = try self.allocator.create(Expression);
            expr_box.* = expr;
            return Expression{ .nested = expr_box };
        },
        .eof => {
            @panic("Maybe control should not have reached here??");
        },
        else => return ParseError.UnexpectedToken,
    }
}

fn parse_num(num_str: []const u8) ParseError!Expression {
    // Check if the number contains a decimal point
    if (std.mem.indexOf(u8, num_str, ".")) |_| {
        // It's a float
        const parsed_float = std.fmt.parseFloat(f64, num_str) catch {
            return ParseError.IntegerOutOfRange;
        };
        return Expression{ .value = .{ .real = parsed_float } };
    } else {
        // It's an integer
        const parsed_int = std.fmt.parseInt(i64, num_str, 10) catch {
            return ParseError.IntegerOutOfRange;
        };
        return Expression{ .value = .{ .int = parsed_int } };
    }
}

// Parses an infix expression in the form of
// (left expr | operator | right expr).
fn parse_infix(self: *Self, left: Expression, precedence: u8) ParseError!Expression {
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
            else => return ParseError.UnexpectedToken,
        },
        else => return ParseError.UnexpectedToken,
    };

    const left_box = try self.allocator.create(Expression);
    left_box.* = left;

    const right_box = try self.allocator.create(Expression);
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

// @Todo this to be error union that has error messages and position. Also have more types.
const TokenError = error{
    StringNotClosed,
    UnexpectedToken,
};
fn next_token(self: *Self) TokenError!Token {
    // This should not consume whitespaces automatically.
    self.skip_whitespace_and_comments();

    // @Todo instead of sending eof can we use optional if there are no more tokens left?
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
            return TokenError.UnexpectedToken;
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
            if (is_part_of_ident_or_keyword(chr)) {
                return self.tokenize_keyword_or_identifier();
            }

            return TokenError.UnexpectedToken;
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
fn tokenize_string(self: *Self) TokenError!Token {
    const quote = self.peek();
    self.advance();

    const start = self.position;
    while (!self.is_at_end() and self.peek() != quote) {
        self.advance();
    }

    if (self.is_at_end()) {
        return TokenError.StringNotClosed;
    }

    const string_value = self.input[start..self.position];
    self.advance(); // consume closing quote

    return Token{ .string = string_value };
}

// Tokenizes numbers like `1234`. Floats are not supported.
fn tokenize_number(self: *Self) TokenError!Token {
    const start = self.position;
    var has_decimal = false;

    // Parse integer part
    while (!self.is_at_end() and std.ascii.isDigit(self.peek())) {
        self.advance();
    }

    // Check for decimal point
    if (!self.is_at_end() and self.peek() == '.') {
        // Look ahead to see if there's a digit after the decimal point
        if (self.position + 1 < self.input.len and std.ascii.isDigit(self.input[self.position + 1])) {
            has_decimal = true;
            self.advance(); // consume the '.'

            // Parse fractional part
            while (!self.is_at_end() and std.ascii.isDigit(self.peek())) {
                self.advance();
            }
        }
    }

    const number_value = self.input[start..self.position];

    // Validate that we have at least one digit
    if (number_value.len == 0 or (has_decimal and number_value.len == 1)) {
        return TokenError.UnexpectedToken;
    }

    return Token{ .number = number_value };
}

// Attempts to parse an instance of Token.keyword or Token.identifier.
fn tokenize_keyword_or_identifier(self: *Self) TokenError!Token {
    const start = self.position;
    while (!self.is_at_end() and is_part_of_ident_or_keyword(self.peek())) {
        self.advance();
    }

    const value = self.input[start..self.position];
    const keyword = try get_keyword(value);

    if (keyword) |k| {
        return Token{ .keyword = k };
    }

    return Token{ .identifier = value };
}

fn get_keyword(value: []const u8) TokenError!?Keyword {
    var uppercase_buf: [256]u8 = undefined;
    if (value.len > uppercase_buf.len) {
        return TokenError.UnexpectedToken;
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
        .{ "TEXT", .txt },
        .{ "BOOL", .bool },
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
        std.debug.print("Expected token {}, but got {}\n", .{ expected, current });
        return error.UnexpectedToken;
    }
}

fn is_part_of_ident_or_keyword(chr: u8) bool {
    return std.ascii.isAlphanumeric(chr) or chr == '_';
}

// Placeholder implementations for other statement types
fn parse_insert(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_update(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_delete(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_drop(self: *Self) !Statement {
    _ = self;
    unreachable;
}

fn parse_create(self: *Self) !Statement {
    _ = self;
    unreachable;
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

test "select" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    //var parser = Self.init("select a, sdf, from shubham where a = b order by a", arena.allocator());
    //var parser = Self.init("select a * 3, sdf, from shubham where a = b = c", arena.allocator());
    //var parser = Self.init("select  sdf from shubham where a = b order by \"shubham\"", arena.allocator());
    var parser = Self.init(arena.allocator(), "select column_name from shubham limit 12");
    _ = try parser.parse();
}
