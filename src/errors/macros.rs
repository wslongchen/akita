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
#[macro_export]
macro_rules! invalid_sql_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InvalidSQL($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InvalidSQL(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! interceptor_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InterceptorError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InterceptorError(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! security_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::SecurityError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::SecurityError(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! invalid_field_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InvalidField($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::InvalidField(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! missing_ident_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingIdent($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingIdent(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! missing_table_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingTable($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingTable(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! missing_field_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingField($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MissingField(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! tokio_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::TokioError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::TokioError(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! execute_sql_err {
    ($sql:expr, $msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::ExecuteSqlError {
            message: $msg.into(),
            sql: $sql.into(),
            backtrace,
        }
    }};
    ($sql:expr, $fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::ExecuteSqlError {
            message: format!($fmt, $($arg)*),
            sql: $sql.into(),
            backtrace,
        }
    }};
}

#[macro_export]
macro_rules! data_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DataError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DataError(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! database_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DatabaseError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DatabaseError(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! redundant_field_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::RedundantField($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::RedundantField(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! unsupported_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::UnsupportedOperation($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::UnsupportedOperation(format!($fmt, $($arg)*), backtrace)
    }};
}

#[macro_export]
macro_rules! connection_valid_err {
    () => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::ConnectionValidError(backtrace)
    }};
}

#[macro_export]
macro_rules! empty_data_err {
    () => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::EmptyData(backtrace)
    }};
}

#[macro_export]
macro_rules! unknown_err {
    () => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::Unknown(backtrace)
    }};
}

#[macro_export]
#[cfg(any(
    feature = "mysql-async",
    feature = "postgres-async",
    feature = "sqlite-async",
    feature = "oracle-async",
    feature = "mssql-async"
))]
macro_rules! deadpool_err {
    ($msg:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DeadPoolError($msg.into(), backtrace)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::DeadPoolError(format!($fmt, $($arg)*), backtrace)
    }};
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
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::R2D2Error($err, backtrace)
    }};
}

#[cfg(feature = "mysql-sync")]
#[macro_export]
macro_rules! mysql_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MySQLError($err, backtrace)
    }};
}

#[cfg(any(
    feature = "oracle-sync",
    feature = "oracle-async"
))]
#[macro_export]
macro_rules! oracle_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::OracleError($err, backtrace)
    }};
}

#[cfg(any(
    feature = "mssql-sync",
    feature = "mssql-async"
))]
#[macro_export]
macro_rules! mssql_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MssqlError($err, backtrace)
    }};
}

#[cfg(feature = "mysql-async")]
#[macro_export]
macro_rules! mysql_async_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::MySQLAsyncError($err, backtrace)
    }};
}

#[macro_export]
macro_rules! akita_data_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::AkitaDataError($err.into(), backtrace)
    }};
}

#[macro_export]
macro_rules! sql_loader_err {
    ($err:expr) => {{
        use $crate::errors::SmartBacktrace;
        let backtrace = match $crate::errors::current_backtrace_mode() {
            $crate::errors::BacktraceMode::Full => SmartBacktrace::full(),
            $crate::errors::BacktraceMode::Light => SmartBacktrace::light(file!(), line!(), column!()),
            $crate::errors::BacktraceMode::None => SmartBacktrace::none(),
        };
        $crate::errors::AkitaError::SqlLoaderError($err.into(), backtrace)
    }};
}

#[macro_export]
macro_rules! bail {
    ($err:expr) => {
        return Err($err.into())
    };
    
    (data: $msg:expr) => {
        return Err($crate::data_err!($msg).into())
    };
    (data: $fmt:expr, $($arg:tt)*) => {
        return Err($crate::data_err!($fmt, $($arg)*).into())
    };
    
    (db: $msg:expr) => {
        return Err($crate::database_err!($msg).into())
    };
    (db: $fmt:expr, $($arg:tt)*) => {
        return Err($crate::database_err!($fmt, $($arg)*).into())
    };
    
    (sql: $msg:expr) => {
        return Err($crate::invalid_sql_err!($msg).into())
    };
    (sql: $fmt:expr, $($arg:tt)*) => {
        return Err($crate::invalid_sql_err!($fmt, $($arg)*).into())
    };
    
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::invalid_sql_err!($fmt, $($arg)*).into())
    };
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
    ($cond:expr, data: $msg:expr) => {
        if !$cond {
            return Err($crate::data_err!($msg).into());
        }
    };
    ($cond:expr, db: $msg:expr) => {
        if !$cond {
            return Err($crate::database_err!($msg).into());
        }
    };
    ($cond:expr, sql: $msg:expr) => {
        if !$cond {
            return Err($crate::invalid_sql_err!($msg).into());
        }
    };
}