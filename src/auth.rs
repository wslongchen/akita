use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::{FieldName, TableName, self as akita};


#[derive(Debug, Serialize, Deserialize, FromAkita)]
pub struct User {
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

#[derive(Debug, Serialize, Deserialize, FromAkita)]
pub struct Role {
    pub role_name: String,
}

/// User can have previlege to tables, to columns
/// The table models can be filtered depending on how much
///  and which columns it has privilege
#[allow(unused)]
enum Privilege {
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
    user: User,
    table_name: TableName,
    column_names: Vec<FieldName>,
    privilege: Vec<Privilege>,
}