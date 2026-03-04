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
        $crate::errors::AkitaError::InvalidSQL(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::InvalidSQL(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! interceptor_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::InterceptorError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::InterceptorError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! security_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::SecurityError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::SecurityError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! invalid_field_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::InvalidField(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::InvalidField(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! missing_ident_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::MissingIdent(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::MissingIdent(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! missing_table_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::MissingTable(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::MissingTable(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! missing_field_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::MissingField(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::MissingField(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! tokio_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::TokioError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::TokioError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! execute_sql_err {
    ($sql:expr, $msg:expr) => {{
        $crate::errors::AkitaError::ExecuteSqlError {
            message: $msg.into(),
            sql: $sql.into(),
            backtrace: $crate::errors::SmartBacktrace::capture(),
        }
    }};
    ($sql:expr, $fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::ExecuteSqlError {
            message: format!($fmt, $($arg)*),
            sql: $sql.into(),
            backtrace: $crate::errors::SmartBacktrace::capture(),
        }
    }};
}


#[macro_export]
macro_rules! data_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::DataError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::DataError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! database_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::DatabaseError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::DatabaseError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! redundant_field_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::RedundantField(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::RedundantField(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! unsupported_err {
    ($msg:expr) => {{
        $crate::errors::AkitaError::UnsupportedOperation(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::UnsupportedOperation(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! connection_valid_err {
    () => {{
        $crate::errors::AkitaError::ConnectionValidError(
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! empty_data_err {
    () => {{
        $crate::errors::AkitaError::EmptyData(
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! unknown_err {
    () => {{
        $crate::errors::AkitaError::Unknown(
            $crate::errors::SmartBacktrace::capture()
        )
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
        $crate::errors::AkitaError::DeadPoolError(
            $msg.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::errors::AkitaError::DeadPoolError(
            format!($fmt, $($arg)*),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[cfg(feature = "mysql-sync")]
#[macro_export]
macro_rules! mysql_err {
    ($err:expr) => {{
        $crate::errors::AkitaError::MySQLError(
            $err,
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[cfg(feature = "mysql-async")]
#[macro_export]
macro_rules! mysql_async_err {
    ($err:expr) => {{
        $crate::errors::AkitaError::MySQLAsyncError(
            $err,
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! akita_data_err {
    ($err:expr) => {{
        $crate::errors::AkitaError::AkitaDataError(
            $err.into(),
            $crate::errors::SmartBacktrace::capture()
        )
    }};
}

#[macro_export]
macro_rules! sql_loader_err {
    ($err:expr) => {{
        $crate::errors::AkitaError::SqlLoaderError(
            $err.into(),
            $crate::errors::SmartBacktrace::capture()
        )
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