use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::{FieldName, TableName, self as akita};


#[derive(Debug, Serialize, Deserialize, FromValue)]
pub struct DataBaseUser {
    pub sysid: Option<i32>,
    pub username: String,
    pub is_superuser: bool,
    pub is_inherit: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_login: bool,
    pub can_do_replication: bool,
    pub can_bypass_rls: bool,
    pub valid_until: Option<DateTime<Utc>>,
    pub conn_limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, FromValue)]
pub struct Role {
    pub role_name: String,
}

/// User can have previlege to tables, to columns
/// The table models can be filtered depending on how much
///  and which columns it has privilege
#[allow(unused)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Truncate,
    Connect,
    Execute,
}

impl ToString for Privilege {
    fn to_string(&self) -> String {
        match self {
            Privilege::Select => String::from("Select"),
            Privilege::Insert => String::from("Insert"),
            Privilege::Update => String::from("Update"),
            Privilege::Delete => String::from("Delete"),
            Privilege::Create => String::from("Create"),
            Privilege::Drop => String::from("Drop"),
            Privilege::Truncate => String::from("Truncate"),
            Privilege::Connect => String::from("Connect"),
            Privilege::Execute => String::from("Execute"),
        }
    }
}

///
///  CREATE TABLE user_privilege(
///     user_id int,
///     schema text,
///     table_name text,
///     columns text[], -- if no column mentioned, then the user has priviledge to all of the table columns
///     privilege text[],
///  )
/// User privileges for each tables
#[allow(unused)]
struct UserPrivilege {
    user: DataBaseUser,
    table_name: TableName,
    column_names: Vec<FieldName>,
    privilege: Vec<Privilege>,
}

#[allow(unused)]
pub struct UserInfo {
    pub username: String,
    pub password: Option<String>,
    pub host: Option<String>,
    pub privileges: Option<Vec<Privilege>>,
}

impl UserInfo {
    pub fn new(username: String, password: Option<String>, host: Option<String>, privileges: Option<Vec<Privilege>>) -> Self {
        UserInfo {
            username,
            password,
            host,
            privileges,
        }
    }
}

#[allow(unused)]
pub struct GrantUserPrivilege {
    pub username: String,
    pub schema: String,
    pub table: String,
    pub host: Option<String>,
    pub privileges: Vec<Privilege>,
}


impl GrantUserPrivilege {
    pub fn new(username: String, schema: String, table: String, host: Option<String>, privileges: Vec<Privilege>) -> Self {
        GrantUserPrivilege {
            username,
            schema,
            table,
            host,
            privileges,
        }
    }
}