/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */
//! Error construction macros (all call #[track_caller] constructors)

#[macro_export]
macro_rules! invalid_sql_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::invalid_sql($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::invalid_sql(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! interceptor_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::interceptor_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::interceptor_error(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! security_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::security_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::security_error(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! invalid_field_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::invalid_field($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::invalid_field(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! missing_ident_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::missing_ident($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::missing_ident(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! missing_table_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::missing_table($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::missing_table(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! missing_field_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::missing_field($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::missing_field(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! tokio_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::tokio_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::tokio_error(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! execute_sql_err {
    ($sql:expr, $msg:expr) => {
        $crate::errors::AkitaError::execute_sql_error($sql, $msg)
    };
    ($sql:expr, $fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::execute_sql_error($sql, format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! data_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::data_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::data_error(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! database_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::database_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::database_error(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! redundant_field_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::redundant_field($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::redundant_field(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! unsupported_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::unsupported_operation($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::unsupported_operation(format!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! connection_valid_err {
    () => {
        $crate::errors::AkitaError::connection_valid_error()
    };
}

#[macro_export]
macro_rules! empty_data_err {
    () => {
        $crate::errors::AkitaError::empty_data_error()
    };
}

#[macro_export]
macro_rules! unknown_err {
    () => {
        $crate::errors::AkitaError::unknown_error()
    };
}

// Feature-gated database error macros
#[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))]
#[macro_export]
macro_rules! deadpool_err {
    ($msg:expr) => {
        $crate::errors::AkitaError::deadpool_error($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::errors::AkitaError::deadpool_error(format!($fmt, $($arg)*))
    };
}

#[cfg(any(
    feature = "mysql-sync",
    feature = "postgres-sync",
    feature = "sqlite-sync",
    feature = "oracle-sync",
    feature = "mssql-sync"
))]
#[macro_export]
macro_rules! r2d2_err {
    ($err:expr) => {
        $crate::errors::AkitaError::r2d2_error($err)
    };
}

#[cfg(feature = "mysql-sync")]
#[macro_export]
macro_rules! mysql_err {
    ($err:expr) => {
        $crate::errors::AkitaError::mysql_error($err)
    };
}

#[cfg(feature = "mysql-async")]
#[macro_export]
macro_rules! mysql_async_err {
    ($err:expr) => {
        $crate::errors::AkitaError::mysql_async_error($err)
    };
}

#[cfg(any(feature = "oracle-async", feature = "oracle-sync"))]
#[macro_export]
macro_rules! oracle_err {
    ($err:expr) => {
        $crate::errors::AkitaError::oracle_error($err)
    };
}

#[cfg(any(feature = "mssql-async", feature = "mssql-sync"))]
#[macro_export]
macro_rules! mssql_err {
    ($err:expr) => {
        $crate::errors::AkitaError::mssql_error($err)
    };
}

#[cfg(feature = "postgres-sync")]
#[macro_export]
macro_rules! postgres_err {
    ($err:expr) => {
        $crate::errors::AkitaError::postgres_error($err)
    };
}

#[cfg(feature = "postgres-async")]
#[macro_export]
macro_rules! tokio_postgres_err {
    ($err:expr) => {
        $crate::errors::AkitaError::tokio_postgres_error($err)
    };
}

#[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))]
#[macro_export]
macro_rules! sqlite_err {
    ($err:expr) => {
        $crate::errors::AkitaError::sqlite_error($err)
    };
}

#[macro_export]
macro_rules! akita_data_err {
    ($err:expr) => {
        $crate::errors::AkitaError::akita_data_error($err)
    };
}

#[macro_export]
macro_rules! sql_loader_err {
    ($err:expr) => {
        $crate::errors::AkitaError::sql_loader_error($err)
    };
}
