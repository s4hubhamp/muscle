// Add these error types at the top of your file
pub const ClientError = error{
    // Table/Schema errors
    TableNotFound,
    DuplicateTableName,
    ColumnDoesNotExist,
    DuplicateColumnName,
    DuplicateColumns,
    KeyNotFound,

    // Data validation errors
    TypeMismatch,
    MissingValue,
    TextTooLong,
    BinaryTooLarge,
    RowTooBig,
    KeyTooLong,

    // Constraint violations
    AutoIncrementColumnMustBeInteger,
    BadPrimaryKeyType,
    PrimaryKeyMaxLengthExceeded,

    // Business logic errors
    MaxValueReached,
    UniqueConstraintViolation,
};

pub const SystemError = error{
    // Memory issues
    OutOfMemory,

    // Corruption/Internal consistency
    CorruptedData,
    InvalidPageType,
    InternalConsistencyError,

    // Unexpected states
    UnexpectedState,
};

pub const DatabaseError = ClientError || SystemError;

pub const ErrorClassification = enum {
    Client,
    System,
};

pub fn classify_error(err: anyerror) ErrorClassification {
    return switch (err) {
        error.TableNotFound,
        error.DuplicateTableName,
        error.ColumnDoesNotExist,
        error.DuplicateColumnName,
        error.DuplicateColumns,
        error.TypeMismatch,
        error.MissingValue,
        error.TextTooLong,
        error.BinaryTooLarge,
        error.RowTooBig,
        error.KeyTooLong,
        error.AutoIncrementColumnMustBeInteger,
        error.BadPrimaryKeyType,
        error.PrimaryKeyMaxLengthExceeded,
        error.MaxValueReached,
        error.UniqueConstraintViolation,
        => .Client,

        // System errors - should crash or require special handling
        error.OutOfMemory,
        error.FileNotFound,
        error.AccessDenied,
        error.DiskFull,
        error.CorruptedData,
        error.InvalidPageType,
        error.InternalConsistencyError,
        error.UnexpectedState,
        => .System,

        // Default to system error for unknown errors
        else => .System,
    };
}
