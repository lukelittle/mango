//! Server errors, shaped like MongoDB command errors so drivers understand them.

use bson::{Document, doc};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub code: i32,
    pub code_name: &'static str,
    pub msg: String,
}

pub type Result<T> = std::result::Result<T, Error>;

macro_rules! codes {
    ($($fn_name:ident => ($code:expr, $name:expr)),* $(,)?) => {
        impl Error {
            $(
                pub fn $fn_name(msg: impl Into<String>) -> Error {
                    Error { code: $code, code_name: $name, msg: msg.into() }
                }
            )*

            /// Rebuilds an error from its code (e.g. one received from a peer).
            pub fn from_code(code: i32, msg: impl Into<String>) -> Error {
                let code_name = match code {
                    $($code => $name,)*
                    _ => "Error",
                };
                Error { code, code_name, msg: msg.into() }
            }
        }
    };
}

codes! {
    internal => (1, "InternalError"),
    bad_value => (2, "BadValue"),
    no_such_key => (4, "NoSuchKey"),
    host_unreachable => (6, "HostUnreachable"),
    failed_to_parse => (9, "FailedToParse"),
    unauthorized => (13, "Unauthorized"),
    type_mismatch => (14, "TypeMismatch"),
    overflow => (15, "Overflow"),
    auth_failed => (18, "AuthenticationFailed"),
    illegal_operation => (20, "IllegalOperation"),
    namespace_not_found => (26, "NamespaceNotFound"),
    index_not_found => (27, "IndexNotFound"),
    path_not_viable => (28, "PathNotViable"),
    conflicting_update => (40, "ConflictingUpdateOperators"),
    cursor_not_found => (43, "CursorNotFound"),
    namespace_exists => (48, "NamespaceExists"),
    dollar_prefixed_field => (52, "DollarPrefixedFieldName"),
    invalid_id_field => (53, "InvalidIdField"),
    not_single_value_field => (54, "NotSingleValueField"),
    empty_field_name => (56, "EmptyFieldName"),
    command_not_found => (59, "CommandNotFound"),
    immutable_field => (66, "ImmutableField"),
    cannot_create_index => (67, "CannotCreateIndex"),
    index_already_exists => (68, "IndexAlreadyExists"),
    invalid_options => (72, "InvalidOptions"),
    invalid_namespace => (73, "InvalidNamespace"),
    index_options_conflict => (85, "IndexOptionsConflict"),
    index_key_specs_conflict => (86, "IndexKeySpecsConflict"),
    network_timeout => (89, "NetworkTimeout"),
    shutdown_in_progress => (91, "ShutdownInProgress"),
    operation_failed => (96, "OperationFailed"),
    cannot_index_parallel_arrays => (171, "CannotIndexParallelArrays"),
    primary_stepped_down => (189, "PrimarySteppedDown"),
    not_implemented => (238, "NotImplemented"),
    no_such_transaction => (251, "NoSuchTransaction"),
    location => (16020, "Location16020"),
    bson_too_large => (10334, "BSONObjectTooLarge"),
    not_writable_primary => (10107, "NotWritablePrimary"),
    duplicate_key => (11000, "DuplicateKey"),
    interrupted_repl_change => (11602, "InterruptedDueToReplStateChange"),
    not_primary_no_secondary_ok => (13435, "NotPrimaryNoSecondaryOk"),
}

impl Error {
    /// Errors that a driver may safely retry for a retryable write.
    pub fn is_retryable_write(&self) -> bool {
        matches!(self.code, 6 | 89 | 91 | 189 | 10107 | 11602 | 13435)
    }

    pub fn to_doc(&self) -> Document {
        doc! { "ok": 0.0, "errmsg": self.msg.clone(), "code": self.code, "codeName": self.code_name }
    }

    /// Shape used inside `writeErrors`.
    pub fn to_write_error(&self, index: usize) -> Document {
        doc! { "index": index as i32, "code": self.code, "errmsg": self.msg.clone() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.code_name, self.code, self.msg)
    }
}

impl std::error::Error for Error {}

impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        Error::internal(format!("storage error: {e}"))
    }
}

macro_rules! from_redb {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(e: $t) -> Self { Error::internal(format!("storage error: {e}")) }
        }
    )*};
}
from_redb!(
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
    redb::CommitError,
    redb::DatabaseError
);

impl From<bson::error::Error> for Error {
    fn from(e: bson::error::Error) -> Self {
        Error::failed_to_parse(format!("invalid BSON: {e}"))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::internal(format!("io error: {e}"))
    }
}
