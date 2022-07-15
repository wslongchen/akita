use std::{convert::TryFrom, ops::Deref};

use crate::{cfg_if, Params, TableName, DatabaseName, SchemaContent, TableDef, Rows};
use url::Url;

cfg_if! {if #[cfg(feature = "akita-sqlite")]{
    use crate::platform::sqlite::SqliteDatabase;
}}
cfg_if! {if #[cfg(feature = "akita-mysql")]{
    use crate::platform::mysql::MysqlDatabase;
}}

cfg_if! {if #[cfg(feature = "akita-auth")]{
    use crate::auth::{GrantUserPrivilege, Role, UserInfo, DataBaseUser};
}}

use crate::{AkitaError};


pub trait Database {
    fn start_transaction(&mut self) -> Result<(), AkitaError>;

    fn commit_transaction(&mut self) -> Result<(), AkitaError>;

    fn rollback_transaction(&mut self) -> Result<(), AkitaError>;

    fn execute_result(&mut self, sql: &str, param: Params) -> Result<Rows, AkitaError>;

    fn execute_drop(&mut self, sql: &str, param: Params) -> Result<(), AkitaError>;

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError>;

    fn exist_table(&mut self, table_name: &TableName) -> Result<bool, AkitaError>;

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, AkitaError>;

    fn get_all_tables(&mut self, shema: &str) -> Result<Vec<TableDef>, AkitaError>;

    fn get_tablenames(&mut self, schema: &str) -> Result<Vec<TableName>, AkitaError>;

    fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>, AkitaError>;

    fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>, AkitaError>;

    fn affected_rows(&self) -> u64;

    fn last_insert_id(&self) -> u64;

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError>;

    fn create_database(&mut self, database: &str) -> Result<(), AkitaError>;

    fn exist_databse(&mut self, database: &str) -> Result<bool, AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn get_users(&mut self) -> Result<Vec<DataBaseUser>, AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn exist_user(&mut self, user: &UserInfo) -> Result<bool, AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn get_user_detail(&mut self, username: &str) -> Result<Vec<DataBaseUser>, AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn get_roles(&mut self, username: &str) -> Result<Vec<Role>, AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn create_user(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn drop_user(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn update_user_password(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn lock_user(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn unlock_user(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn expire_user_password(&mut self, user: &UserInfo) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn grant_privileges(&mut self, user: &GrantUserPrivilege) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn revoke_privileges(&mut self, user: &GrantUserPrivilege) -> Result<(), AkitaError>;

    #[cfg(feature = "akita-auth")]
    fn flush_privileges(&mut self) -> Result<(), AkitaError>;
}

#[derive(Debug)]
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


#[derive(Debug, Clone)]
pub enum Platform {
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
