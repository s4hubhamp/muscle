const std = @import("std");
const muscle = @import("../muscle.zig");

pub const Binder = struct {
    catalog: *muscle.Catalog,
    allocator: std.mem.Allocator,

    pub fn bind_query(self: *Binder, query: muscle.Query) !BoundQuery {
        return switch (query) {
            .select => |select| try self.bind_select(select),
            .insert => |insert| try self.bind_insert(insert),
            .update => |update| try self.bind_update(update),
            .delete => |delete| try self.bind_delete(delete),
            .create_table => |create| try self.bind_create_table(create),
            // ... other query types
        };
    }

    fn bind_select(self: *Binder, select: SelectQuery) !BoundSelectQuery {
        // Resolve table name
        // Validate column references
        // Type check WHERE clause
        // Prepare execution plan
    }

    fn validate_column_reference(self: *Binder, table_name: []const u8, column_name: []const u8) !muscle.Column {
        // Look up table in catalog
        // Find column in table schema
        // Return column metadata
    }

    fn type_check_expression(self: *Binder, expr: Expression, table_schema: []const muscle.Column) !muscle.DataType {
        // Validate expression types
        // Check operator compatibility
        // Return result type
    }
};

// Note: BoundQuery is just validated Statements with Expressions.
pub const BoundQuery = union(enum) {
    select: BoundSelectQuery,
    insert: BoundInsertQuery,
    update: BoundUpdateQuery,
    delete: BoundDeleteQuery,
    create_table: BoundCreateTableQuery,
};

//1. Name Resolution
// Resolve table names against catalog
// Resolve column names against table schemas
// Handle aliases and qualified names (table.column)
// Validate that referenced tables and columns exist
//2. Type Checking & Validation
// Validate data types in expressions and comparisons
// Check type compatibility for operations (e.g., can't add text + int)
// Validate literal values against column constraints
// Ensure primary key constraints are respected
//3. Schema Validation
// Validate INSERT statements have all required columns
// Check that UPDATE statements target valid columns
// Ensure SELECT columns exist in referenced tables
// Validate CREATE TABLE column definitions
//4. Constraint Checking
// Primary key constraint validation
// Data type size constraints (e.g., your txt fields with max lengths)
// NOT NULL constraints
// Unique constraints
//5. Query Structure Validation
// Validate SQL syntax is semantically correct
// Check that WHERE clauses reference valid columns
// Ensure GROUP BY, ORDER BY reference valid columns
// Validate subquery structure
//6. Expression Binding
// Bind column references in WHERE clauses
// Validate function calls and operators
// Type check arithmetic and comparison operations
// Handle literal value conversion
//7. Permission/Security Checks
// Validate user has permissions for requested operations
// Check table-level and column-level access rights
// Validate schema modification permissions
//Muscle-Specific Considerations
//Based on your codebase structure:
//8. B-tree Key Validation
// Ensure primary key operations are valid for B-tree structure
// Validate key sizes don't exceed page limits
// Check key ordering for range operations
//9. Storage Layout Validation
// Validate row size fits within page constraints
// Check that variable-length fields (txt, bin) don't exceed limits
// Ensure serialization constraints are met
//10. Query Plan Preparation
// Prepare bound query for execution engine
// Optimize column access patterns
// Determine index usage strategies
