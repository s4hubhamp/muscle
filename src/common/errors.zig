pub const SchemaError = error{
    TableNotFound,
    DuplicateTableName,
    ColumnDoesNotExist,
    ColumnNotFound,
    DuplicateColumnName,
    DuplicateColumns,
    DuplicateKey,
    KeyNotFound,
};

pub const ConstraintError = error{
    AutoIncrementColumnMustBeInteger,
    BadPrimaryKeyType,
    PrimaryKeyMaxLengthExceeded,
    PrimaryKeyUpdateNotAllowed,
    UniqueColumnUpdateNotAllowed,
};

//pub const ClientError =
//    SchemaError ||
//    ConstraintError ||
//    DomainViolationError;

pub const ClientError = error{
    ParserError,

    // Table/Schema errors
    TableNotFound,
    DuplicateTableName,
    ColumnDoesNotExist,
    DuplicateColumnName,
    DuplicateColumns,
    ColumnNotFound,
    DuplicateKey,
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
    PrimaryKeyUpdateNotAllowed,
    UniqueColumnUpdateNotAllowed,

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
    client,
    system,
};

pub fn classify_error(err: anyerror) ErrorClassification {
    return switch (err) {
        error.ParserError,
        error.AnalyzerError,
        error.InvalidExpression,
        error.IncompatibleTypes,
        error.DivisionByZero,

        error.TableNotFound,
        error.DuplicateTableName,
        error.ColumnDoesNotExist,
        error.DuplicateColumnName,
        error.DuplicateColumns,
        error.ColumnNotFound,
        error.DuplicateKey,
        error.KeyNotFound,
        error.TypeMismatch,
        error.MissingValue,
        error.TextTooLong,
        error.BinaryTooLarge,
        error.RowTooBig,
        error.KeyTooLong,
        error.AutoIncrementConstraintViolation,
        error.BadPrimaryKeyType,
        error.PrimaryKeyMaxLengthExceeded,
        error.PrimaryKeyUpdateNotAllowed,
        error.UniqueColumnUpdateNotAllowed,
        error.MaxValueReached,
        error.UniqueConstraintViolation,
        => .client,

        else => .system,
    };
}
