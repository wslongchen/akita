use std::{convert::TryFrom, ops::Deref};

use url::Url;

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::SqliteDatabase;
}}

cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::mysql::MysqlDatabase;
}}

use crate::{AkitaError, cfg_if, data::Rows, information::{DatabaseName, TableDef, TableName}, value::Value};


pub trait Database {
    fn start_transaction(&mut self) -> Result<(), AkitaError>;

    fn commit_transaction(&mut self) -> Result<(), AkitaError>;

    fn rollback_transaction(&mut self) -> Result<(), AkitaError>;

    fn execute_result(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, AkitaError>;

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError>;

    fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>, AkitaError>;

    fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>, AkitaError>;

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError>;

}


pub enum DatabasePlatform {
    #[cfg(feature = "akita-mysql")]
    Mysql(Box<MysqlDatabase>),
    #[cfg(feature = "akita-sqlite")]
    Sqlite(Box<SqliteDatabase>),
}

impl Deref for DatabasePlatform {
    type Target = dyn Database;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(ref mysql) => mysql.deref(),
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(ref sqlite) => sqlite.deref(),
        }
    }
}

impl std::ops::DerefMut for DatabasePlatform {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(ref mut mysql) => mysql.deref_mut(),
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(ref mut sqlite) => sqlite.deref_mut(),
        }
    }
}

pub(crate) enum Platform {
    #[cfg(feature = "akita-mysql")]
    Mysql,
    #[cfg(feature = "akita-sqlite")]
    Sqlite(String),
    Unsupported(String),
}

impl<'a> TryFrom<&'a str> for Platform {
    type Error = AkitaError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let url = Url::parse(s);
        match url {
            Ok(url) => {
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "akita-mysql")]
                    "mysql" => Ok(Platform::Mysql),
                    #[cfg(feature = "akita-sqlite")]
                    "sqlite" => {
                        let host = url.host_str().unwrap_or_default();
                        let path = url.path();
                        let path = if path == "/" { "" } else { path };
                        let db_file = format!("{}{}", host, path);
                        Ok(Platform::Sqlite(db_file))
                    },
                    _ => Ok(Platform::Unsupported(scheme.to_string())),
                }
            }
            Err(e) => Err(AkitaError::UrlParseError(e.to_string())),
        }
    }
}
