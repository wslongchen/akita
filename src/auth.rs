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
    Unknown,
    Alter,
    AlterRoutine,
    Create,
    CreateRoutine,
    CreateTemporaryTables,
    CreateUser,
    CreateView,
    Delete,
    Drop,
    Event,
    Execute,
    File,
    GrantOption,
    Index,
    Insert,
    LockTables,
    Process,
    References,
    Reload,
    ReplicationClient,
    ReplicationSalve,
    Select,
    ShowDatabases,
    ShowView,
    Shutdown,
    Super,
    Trigger,
    Update,
    Truncate,
    Connect,
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
            Privilege::Unknown => String::from(""),
            Privilege::Alter => String::from("Alter"),
            Privilege::AlterRoutine => String::from("AlterRoutine"),
            Privilege::CreateRoutine => String::from("CreateRoutine"),
            Privilege::CreateTemporaryTables => String::from("CreateTemporaryTables"),
            Privilege::CreateUser => String::from("CreateUser"),
            Privilege::CreateView => String::from("CreateView"),
            Privilege::Event => String::from("Event"),
            Privilege::File => String::from("File"),
            Privilege::GrantOption => String::from("GrantOption"),
            Privilege::Index => String::from("Index"),
            Privilege::LockTables => String::from("LockTables"),
            Privilege::Process => String::from("Process"),
            Privilege::References => String::from("References"),
            Privilege::Reload => String::from("Reload"),
            Privilege::ReplicationClient => String::from("ReplicationClient"),
            Privilege::ReplicationSalve => String::from("ReplicationSalve"),
            Privilege::ShowDatabases => String::from("ShowDatabases"),
            Privilege::ShowView => String::from("ShowView"),
            Privilege::Shutdown => String::from("Shutdown"),
            Privilege::Super => String::from("Super"),
            Privilege::Trigger => String::from("Trigger"),
            
        }
    }
}

impl From<String> for Privilege {
    fn from(privilege: String) -> Self {
        match privilege.as_str() {
            "Select" => Self::Select,
            "Insert" => Self::Insert,
            "Update" => Self::Update,
            "Delete" => Self::Delete,
            "Create" => Self::Create,
            "Drop" => Self::Drop,
            "Truncate" => Self::Truncate,
            "Connect" => Self::Connect,
            "Execute" => Self::Execute,
            "Alter" => Self::Alter,
            "AlterRoutine" => Self::AlterRoutine,
            "CreateRoutine" => Self::CreateRoutine,
            "CreateTemporaryTables" => Self::CreateTemporaryTables,
            "CreateUser" => Self::CreateUser,
            "CreateView" => Self::CreateView,
            "Event" => Self::Event,
            "File" => Self::File,
            "GrantOption" => Self::GrantOption,
            "Index" => Self::Index,
            "LockTables" => Self::LockTables,
            "Process" => Self::Process,
            "References" => Self::References,
            "Reload" => Self::Reload,
            "ReplicationClient" => Self::ReplicationClient,
            "ReplicationSalve" => Self::ReplicationSalve,
            "ShowDatabases" => Self::ShowDatabases,
            "ShowView" => Self::ShowView,
            "Shutdown" => Self::Shutdown,
            "Super" => Self::Super,
            "Trigger" => Self::Trigger,
            _=> Self::Unknown
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
    pub is_lock: Option<bool>,
    pub privileges: Option<Vec<Privilege>>,
}

impl UserInfo {
    pub fn new(username: String, password: Option<String>, host: Option<String>, privileges: Option<Vec<Privilege>>, is_lock: Option<bool>) -> Self {
        UserInfo {
            username,
            password,
            host,
            privileges,
            is_lock,
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