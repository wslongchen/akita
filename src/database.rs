use std::{convert::TryFrom, ops::Deref, string::ParseError};

use url::Url;

use crate::{AkitaError, DatabaseName, MysqlDatabase, TableDef, TableName, data::Rows, value::Value};


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
    Mysql(Box<MysqlDatabase>),
}

impl Deref for DatabasePlatform {
    type Target = dyn Database;

    fn deref(&self) -> &Self::Target {
        match *self {
            DatabasePlatform::Mysql(ref mysql) => mysql.deref(),
        }
    }
}

impl std::ops::DerefMut for DatabasePlatform {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            DatabasePlatform::Mysql(ref mut mysql) => mysql.deref_mut(),
        }
    }
}

pub(crate) enum Platform {
    Mysql,
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
                    "mysql" => Ok(Platform::Mysql),
                    _ => Ok(Platform::Unsupported(scheme.to_string())),
                }
            }
            Err(e) => Err(AkitaError::UrlParseError(e.to_string())),
        }
    }
}
