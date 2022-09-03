//! 
//! MySQL modules.
//!
use mysql::prelude::Protocol;
use mysql::{Conn, Error, Opts, OptsBuilder, Row, prelude::Queryable};
use r2d2::{ManageConnection, Pool};

use std::result::Result;
use akita_core::Array;

use crate::{AkitaConfig, Params, self as akita};

cfg_if! {if #[cfg(feature = "akita-auth")]{
    use crate::auth::{GrantUserPrivilege, Role, UserInfo, DataBaseUser};
}}
use crate::database::Database;
use crate::pool::LogLevel;
use serde_json::Map;
use crate::{ToValue, Value, FromValue, Rows, SqlType, cfg_if, AkitaError, ColumnDef, FieldName, ColumnSpecification, DatabaseName, TableDef, TableName, SchemaContent, comm};
type R2d2Pool = Pool<MysqlConnectionManager>;

#[derive(Debug)]
pub struct MysqlDatabase(r2d2::PooledConnection<MysqlConnectionManager>, AkitaConfig);

impl MysqlDatabase {
    pub fn new(pool: r2d2::PooledConnection<MysqlConnectionManager>, cfg: AkitaConfig) -> Self {
        MysqlDatabase(pool, cfg)
    }

    pub fn log(&self, _fmt: String) {
        if let Some(log_level) = &self.1.log_level() {
            match log_level {
                LogLevel::Debug => {
                    #[cfg(feature = "akita-logging")]
                    log::debug!("[Akita]: {}", &_fmt);
                    #[cfg(feature = "akita-tracing")]
                    tracing::debug!("[Akita]: {}", &_fmt);
                },
                LogLevel::Info => {
                    #[cfg(feature = "akita-logging")]
                    log::info!("[Akita]: {}", &_fmt);
                    #[cfg(feature = "akita-tracing")]
                    tracing::info!("[Akita]: {}", &_fmt);
                },
                LogLevel::Error => {
                    #[cfg(feature = "akita-logging")]
                    log::error!("[Akita]: {}", &_fmt);
                    #[cfg(feature = "akita-tracing")]
                    tracing::error!("[Akita]: {}", &_fmt);
                },
            }
        }
    }
}

/// MYSQL数据操作
impl Database for MysqlDatabase {
    fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("BEGIN", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }

    fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("COMMIT", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }

    fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("ROLLBACK", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }
    
    fn execute_result(&mut self, sql: &str, param: Params) -> Result<Rows, AkitaError> {
        self.log(format!("Prepare SQL: {} params: {:?}", &sql, param));
        fn collect<T: Protocol>(mut rows: mysql::QueryResult<T>) -> Result<Rows, AkitaError> {
            let column_types: Vec<_> = rows.columns().as_ref().iter().map(|c| c.column_type()).collect();
            let _fields = rows
                .columns().as_ref()
                .iter()
                .map(|c| std::str::from_utf8(c.name_ref()).map(ToString::to_string))
                .collect::<Result<Vec<String>, _>>()
                .map_err(|e| AkitaError::from(e))?;

            let mut records = Rows::new();
            // while rows.next().is_some() {
            //     for r in rows.by_ref() {
            //         records.push(into_record(r.map_err(AkitaError::from)?, &column_types)?);
            //     }
            // }
            for r in rows.by_ref() {
                records.push(into_record(r.map_err(AkitaError::from)?, &column_types)?);
            }
            Ok(records)
        }
        match param {
            Params::Nil => {
                let rows = self
                .0
                .query_iter(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let rows = collect(rows)?;
                self.log(format!("AffectRows: {}", self.affected_rows()));
                Ok(rows)
            },
            Params::Vector(param) => {
                let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let params: mysql::Params = param
                    .iter()
                    .map(|v| MySQLValue(v))
                    .map(|v| mysql::prelude::ToValue::to_value(&v))
                    .collect::<Vec<_>>()
                    .into();
                let rows = self.0.exec_iter(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let rows = collect(rows)?;
                self.log(format!("AffectRows: {} records: {:?}", self.affected_rows(), rows));
                Ok(rows)
            },
            Params::Custom(param) => {
                let mut format_sql = sql.to_owned();
                let len = format_sql.len();
                let mut values = param.iter().map(|param| {
                    let key = format!(":{}", param.0);
                    let index = format_sql.find(&key).unwrap_or(len);
                    format_sql = format_sql.replace(&key, "?");
                    (index, &param.1)
                }).collect::<Vec<_>>();
                values.sort_by(|a, b| a.0.cmp(&b.0));
                let param = values.iter().map(|v| v.1).collect::<Vec<_>>();
                let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let params: mysql::Params = param
                    .iter()
                    .map(|v| MySQLValue(v))
                    .map(|v| mysql::prelude::ToValue::to_value(&v))
                    .collect::<Vec<_>>()
                    .into();
                let rows = self.0.exec_iter(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let rows = collect(rows)?;
                self.log(format!("AffectRows: {} records: {:?}", self.0.affected_rows(), rows));
                Ok(rows)
            },
        }
    }
    
    fn execute_drop(&mut self, sql: &str, param: Params) -> Result<(), AkitaError> {
        self.log(format!("Prepare SQL: {} params: {:?}", &sql, param));
        match param {
            Params::Nil => {
                self
                .0
                .exec_drop(&sql, ())
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))
            },
            Params::Vector(param) => {
                let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let params: mysql::Params = param
                    .iter()
                    .map(|v| MySQLValue(v))
                    .map(|v| mysql::prelude::ToValue::to_value(&v))
                    .collect::<Vec<_>>()
                    .into();
                self.0.exec_drop(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))
            },
            Params::Custom(param) => {
                let mut format_sql = sql.to_owned();
                let len = format_sql.len();
                let mut values = param.iter().map(|param| {
                    let key = format!(":{}", param.0);
                    let index = format_sql.find(&key).unwrap_or(len);
                    format_sql = format_sql.replace(&key, "?");
                    (index, &param.1)
                }).collect::<Vec<_>>();
                values.sort_by(|a, b| a.0.cmp(&b.0));
                let param = values.iter().map(|v| v.1).collect::<Vec<_>>();
                let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
                let params: mysql::Params = param
                    .iter()
                    .map(|v| MySQLValue(v))
                    .map(|v| mysql::prelude::ToValue::to_value(&v))
                    .collect::<Vec<_>>()
                    .into();
                self.0.exec_drop(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))
            },
        }
    }

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError> {
        #[derive(Debug, FromValue)]
        struct TableSpec {
            schema: String,
            name: String,
            comment: String,
            is_view: i32,
        }
        let schema: &Value = &table_name
            .schema
            .as_ref()
            .map(String::as_str)
            .unwrap_or("__DUMMY__")
            .into();
        let table_name: Value = table_name.name.clone().into();

        let mut tables: Vec<TableSpec> = self
        .execute_result(
            r#"
            SELECT TABLE_SCHEMA AS `schema`,
                   TABLE_NAME AS name,
                   TABLE_COMMENT AS comment,
                   CASE TABLE_TYPE WHEN 'VIEW' THEN TRUE ELSE FALSE END AS is_view
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_SCHEMA = CASE ? WHEN '__DUMMY__' THEN DATABASE() ELSE ? END AND TABLE_NAME = ?"#,
             (
                schema, schema,
                &table_name,
            ).into(),
        )?
        .iter()
        .map(|data| FromValue::from_value(&data))
        .collect();
        let table_spec = match tables.len() {
            0 => return Err(AkitaError::DataError("Unknown table finded.".to_string())),
            _ => tables.remove(0),
        };

        #[derive(Debug, FromValue)]
        struct ColumnSpec {
            schema: String,
            table_name: String,
            name: String,
            comment: String,
            type_: String,
        }
        let table_schema: Value =  table_spec.schema.clone().into();
        let columns: Vec<ColumnDef> = self
            .execute_result(
                r#"
                SELECT TABLE_SCHEMA AS `schema`,
                       TABLE_NAME AS table_name,
                       COLUMN_NAME AS name,
                       COLUMN_COMMENT AS comment,
                       CAST(COLUMN_TYPE as CHAR(255)) AS type_
                  FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"#,
                 (&table_schema, &table_name).into(),
            )?
            .iter()
            .map(|data| FromValue::from_value(&data))
            .map(|spec: ColumnSpec| {
                let (sql_type, capacity) =
                    if spec.type_.starts_with("enum(") || spec.type_.starts_with("set(") {
                        let start = spec.type_.find('(');
                        let end = spec.type_.find(')');
                        if let (Some(start), Some(end)) = (start, end) {
                            let dtype = &spec.type_[0..start];
                            let range = &spec.type_[start + 1..end];
                            let choices = range
                                .split(',')
                                .map(|v| v.to_owned())
                                .collect::<Vec<String>>();

                            match dtype {
                                "enum" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                "set" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                _ => panic!("not yet handled: {}", dtype),
                            }
                        } else {
                            panic!("not yet handled spec_type: {:?}", spec.type_)
                        }
                    } else {
                        let (dtype, capacity) = comm::extract_datatype_with_capacity(&spec.type_);
                        let sql_type = match &*dtype {
                            "tinyint" | "tinyint unsigned" => SqlType::Tinyint,
                            "smallint" | "smallint unsigned" | "year" => SqlType::Smallint,
                            "mediumint" | "mediumint unsigned" => SqlType::Int,
                            "int" | "int unsigned" => SqlType::Int,
                            "bigint" | "bigin unsigned" => SqlType::Bigint,
                            "float" | "float unsigned" => SqlType::Float,
                            "double" | "double unsigned" => SqlType::Double,
                            "decimal" => SqlType::Numeric,
                            "tinyblob" => SqlType::Tinyblob,
                            "mediumblob" => SqlType::Mediumblob,
                            "blob" => SqlType::Blob,
                            "longblob" => SqlType::Longblob,
                            "binary" | "varbinary" => SqlType::Varbinary,
                            "char" => SqlType::Char,
                            "varchar" => SqlType::Varchar,
                            "tinytext" => SqlType::Tinytext,
                            "mediumtext" => SqlType::Mediumtext,
                            "text" | "longtext" => SqlType::Text,
                            "date" => SqlType::Date,
                            "datetime" | "timestamp" => SqlType::Timestamp,
                            "time" => SqlType::Time,
                            "json" => SqlType::Json,
                            _ => panic!("not yet handled: {}", dtype),
                        };

                        (sql_type, capacity)
                    };

                ColumnDef {
                    table: TableName::from(&format!("{}.{}", spec.schema, spec.table_name)),
                    name: FieldName::from(&spec.name),
                    comment: Some(spec.comment),
                    specification: ColumnSpecification {
                        capacity,
                        // TODO: implementation
                        constraints: vec![],
                        sql_type,
                    },
                    stat: None,
                }
            })
            .collect();

        Ok(Some(TableDef {
            name: TableName {
                name: table_spec.name,
                schema: Some(table_spec.schema),
                alias: None,
            },
            comment: Some(table_spec.comment),
            columns,
            is_view: table_spec.is_view == 1,
            // TODO: implementation
            table_key: vec![],
        }))
    }

    fn exist_table(&mut self, table_name: &TableName) -> Result<bool, AkitaError> {
        let sql = "SELECT count(1) as count FROM information_schema.tables WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?";
        self.execute_result(&sql, (&table_name.name, &table_name.schema).into()).map(|rows| {
            rows.iter().next()
                .map(|row| {
                    row.get_obj_opt::<i32>("count")
                        .expect("must not error")
                }).unwrap_or_default().unwrap_or_default() > 0
        })
    }

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, AkitaError> {
        let table_names = get_table_names(&mut *self, &"BASE TABLE".to_string())?;
        let view_names = get_table_names(&mut *self, &"VIEW".to_string())?;
        let mut schema_contents: Vec<SchemaContent> = Vec::new();
        for table in table_names.iter() {
            let schema = table.schema.to_owned().unwrap_or_default();
            if let Some(t) = schema_contents.iter_mut().find(|data| data.to_owned().schema.to_owned().eq(&schema)) {
                t.tablenames.push(table.to_owned());
            } else {
                schema_contents.push(SchemaContent {
                    schema,
                    tablenames: vec![table.to_owned()],
                    views: vec![],
                });
            }
        }
        
        for table in view_names.iter() {
            let schema = table.schema.to_owned().unwrap_or_default();
            if let Some(t) = schema_contents.iter_mut().find(|data| data.to_owned().schema.to_owned().eq(&schema)) {
                t.tablenames.push(table.to_owned());
            } else {
                schema_contents.push(SchemaContent {
                    schema,
                    tablenames: vec![table.to_owned()],
                    views: vec![],
                });
            }
        }
            
        Ok(schema_contents)
    }

    fn get_all_tables(&mut self, schema: &str) -> Result<Vec<TableDef>, AkitaError> {
        let tablenames = self.get_tablenames(schema)?;
        Ok(tablenames
            .iter()
            .filter_map(|tablename| self.get_table(tablename).ok().flatten())
            .collect())
    }

    fn get_tablenames(&mut self, schema: &str) -> Result<Vec<TableName>, AkitaError> {
        #[derive(Debug, FromValue)]
        struct TableNameSimple {
            table_name: String,
        }
        let sql =
            "SELECT TABLE_NAME as table_name FROM information_schema.tables WHERE TABLE_SCHEMA = ?";

        let result: Vec<TableNameSimple> = self
            .execute_result(sql, (schema, ).into())?
            .iter()
            .map(|row| TableNameSimple {
                table_name: row.get_obj("table_name").expect("must have a table name"),
            })
            .collect();
        let tablenames = result
            .iter()
            .map(|r| TableName::from(&r.table_name))
            .collect();
        Ok(tablenames)
    }

    fn set_autoincrement_value(
        &mut self,
        _table_name: &TableName,
        _sequence_value: i64,
    ) -> Result<Option<i64>, AkitaError> {
        todo!()
    }

    fn get_autoincrement_last_value(
        &mut self,
        _table_name: &TableName,
    ) -> Result<Option<i64>, AkitaError> {
        todo!()
    }

    fn affected_rows(&self) -> u64 {
        self.0.affected_rows()
    }

    fn last_insert_id(&self) -> u64 {
        self.0.last_insert_id()
    }

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError> {
        let sql = "SELECT database() AS name";
        let mut database_names: Vec<Option<DatabaseName>> =
            self.execute_result(&sql, Params::Nil).map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.get_obj_opt("name")
                            .expect("must not error")
                            .map(|name| DatabaseName {
                                name,
                                description: None,
                            })
                    })
                    .collect()
            })?;

        if database_names.len() > 0 {
            Ok(database_names.remove(0))
        } else {
            Ok(None)
        }
    }

    fn create_database(&mut self, database: &str) -> Result<(), AkitaError> {
        let sql = format!("CREATE DATABASE {}", database);
        self.execute_drop(&sql, ().into())
    }

    fn exist_databse(&mut self, database: &str) -> Result<bool, AkitaError> {
        let sql = "SELECT count(1) as count FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?";
        self.execute_result(&sql, (database,).into()).map(|rows| {
            rows.iter().next()
                .map(|row| {
                    row.get_obj_opt::<i32>("count")
                        .expect("must not error")
                }).unwrap_or_default().unwrap_or_default() > 0
        })
    }

    #[cfg(feature = "akita-auth")]
    fn get_users(&mut self) -> Result<Vec<DataBaseUser>, AkitaError> {
        let sql = "SELECT USER as username FROM information_schema.user_attributes";
        let rows: Result<Rows, AkitaError> = self.execute_result(&sql, Params::Nil);

        rows.map(|rows| {
            rows.iter()
                .map(|row| DataBaseUser {
                    sysid: None,
                    username: row.get_obj("username").expect("username"),
                    //TODO: join to the user_privileges tables
                    is_superuser: false,
                    is_inherit: false,
                    can_create_db: false,
                    can_create_role: false,
                    can_login: false,
                    can_do_replication: false,
                    can_bypass_rls: false,
                    valid_until: None,
                    conn_limit: None,
                })
                .collect()
        })
    }

    #[cfg(feature = "akita-auth")]
    fn exist_user(&mut self, user: &UserInfo) -> Result<bool, AkitaError> {
        let sql = "SELECT count(1) as count FROM mysql.user where User = ? and Host = ?";
        self.execute_result(&sql, (&user.username, user.host.as_ref().unwrap_or(&"localhost".to_owned())).into()).map(|rows| {
            rows.iter().next()
                .map(|row| {
                    row.get_obj_opt::<i32>("count")
                        .expect("must not error")
                }).unwrap_or_default().unwrap_or_default() > 0
        })
    }

    #[cfg(feature = "akita-auth")]
    fn get_user_detail(&mut self, _username: &str) -> Result<Vec<DataBaseUser>, AkitaError> {
        todo!()
    }

    #[cfg(feature = "akita-auth")]
    fn get_roles(&mut self, _username: &str) -> Result<Vec<Role>, AkitaError> {
        todo!()
    }

    #[cfg(feature = "akita-auth")]
    fn create_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        let mut sql = format!("create user '{}'@'{}'", &user.username, &user.host.to_owned().unwrap_or("localhost".to_string()));
        if let Some(password) = user.password.to_owned() { 
            sql.push_str(&format!("identified by '{}'", password));
        }
        if let Some(is_lock) = user.is_lock {
            if is_lock {
                sql.push_str("account lock")
            }
        }
        sql.push_str(";");
        // 创建用户
        self.execute_drop(&sql, ().into())
        
    }

    #[cfg(feature = "akita-auth")]
    fn drop_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("drop user '{}'@'{}';", &user.username, &user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }



    #[cfg(feature = "akita-auth")]
    fn update_user_password(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        if user.username.is_empty() || user.host.is_none() || user.password.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' identified by '{}'", user.username, user.host.to_owned().unwrap_or("localhost".to_string()), user.password.to_owned().unwrap_or_default());
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn lock_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' account lock;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn unlock_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        if user.username.is_empty() || user.host.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' account unlock;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn expire_user_password(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        if user.username.is_empty() || user.host.is_none() || user.password.is_none() {
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let sql = format!("alter user '{}'@'{}' password expire;", user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn grant_privileges(&mut self, user: &GrantUserPrivilege) -> Result<(), AkitaError> {
        // 分配权限
        if user.schema.is_empty() || user.table.is_empty() || user.username.is_empty() || user.host.is_none(){
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let privileges = if user.privileges.len() > 0 { 
            user.privileges.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",") 
        } else {
            "all".to_string()
        };
        if user.schema.eq("*") {
            return Err(AkitaError::UnsupportedOperation(
                "You are not allow this operation to use schema with *".to_string(),
            ))
        }
        let sql = format!("grant {} on {}.{} to '{}'@'{}';", privileges, user.schema, user.table, user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn revoke_privileges(&mut self, user: &GrantUserPrivilege) -> Result<(), AkitaError> {
        // 回收权限
        if user.schema.is_empty() || user.table.is_empty() || user.username.is_empty() || user.host.is_none(){
            return Err(AkitaError::UnsupportedOperation(
                "Some param is empty.".to_string(),
            ))
        }
        let privileges = if user.privileges.len() > 0 { 
            user.privileges.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",") 
        } else {
            "all".to_string()
        };
        if user.schema.eq("*") {
            return Err(AkitaError::UnsupportedOperation(
                "You are not allow this operation to use schema with *".to_string(),
            ))
        }
        let sql = format!("revoke {} on {}.{} from '{}'@'{}';", privileges, user.schema, user.table, user.username, user.host.to_owned().unwrap_or("localhost".to_string()));
        self.execute_drop(&sql, ().into())
    }

    #[cfg(feature = "akita-auth")]
    fn flush_privileges(&mut self) -> Result<(), AkitaError> {
        let sql = "flush privileges;";
        self.execute_drop(&sql, ().into())
    }
}

#[allow(unused)]
fn get_table_names(db: &mut dyn Database, kind: &str) -> Result<Vec<TableName>, AkitaError> {
    #[derive(Debug, FromValue)]
    struct TableNameSimple {
        table_name: String,
        schema_name: String,
    }
    let sql = "SELECT TABLE_NAME as table_name, TABLE_SCHEMA as schema_name FROM information_schema.tables WHERE table_type= ?";
    let result: Vec<TableNameSimple> = db
        .execute_result(sql, (kind.to_value(),).into())?
        .iter()
        .map(|row| TableNameSimple {
            table_name: row.get_obj("table_name").expect("must have a table name"),
            schema_name: row.get_obj("schema_name").expect("must have a schema name"),
        })
        .collect();
    let mut table_names = vec![];
    for r in result {
        let mut table_name = TableName::from(&r.table_name);
        table_name.schema = r.schema_name.into();
        table_names.push(table_name);
    }
    Ok(table_names)
}

#[derive(Debug)]
pub struct MySQLValue<'a>(&'a Value);


impl mysql::prelude::ToValue for MySQLValue<'_> {
    fn to_value(&self) -> mysql::Value {
        match self.0 {
            Value::Bool(ref v) => v.into(),
            Value::Tinyint(ref v) => v.into(),
            Value::Smallint(ref v) => v.into(),
            Value::Int(ref v) => v.into(),
            Value::Bigint(ref v) => v.into(),
            Value::Float(ref v) => v.into(),
            Value::Double(ref v) => v.into(),
            Value::Blob(ref v) => v.into(),
            Value::Char(ref v) => v.to_string().into(),
            Value::Text(ref v) => v.into(),
            Value::Uuid(ref v) => v.as_bytes().into(),
            Value::Date(ref v) => v.into(),
            Value::Timestamp(ref v) => v.naive_utc().into(),
            Value::DateTime(ref v) => v.into(),
            Value::Time(ref v) => v.into(),
            Value::Interval(ref _v) => panic!("storing interval in DB is not supported"),
            Value::Json(ref v) => v.into(),
            Value::Nil => mysql::Value::NULL,
            Value::Array(ref v) => {
                match v {
                    Array::Int(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Float(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Text(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Bool(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Tinyint(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Smallint(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Bigint(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Double(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::BigDecimal(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Char(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Uuid(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Date(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                    Array::Timestamp(vv) => {
                        let value = serde_json::to_string(vv).unwrap_or_default();
                        value.into()
                    }
                }
            },
            // Value::SerdeJson(ref v) => v.into(),
            Value::Object(ref v) => {
                let mut data = Map::new();
                for (k, v) in v.into_iter() {
                    data.insert(k.to_owned(), serde_json::Value::from_value(&v));
                }
                // serde_json::Value::Object(data).into()
                let value = serde_json::to_string(&data).unwrap_or_default();
                value.into()
            },
            Value::BigDecimal(_) => unimplemented!("we need to upgrade bigdecimal crate"),
            // Value::Point(_) | Value::Array(_) => unimplemented!("unsupported type"),
        }
    }
}

fn into_record(
    mut row: mysql::Row,
    column_types: &[mysql::consts::ColumnType],
) -> Result<crate::Row, AkitaError> {
    use mysql::{consts::ColumnType, from_value_opt as fvo};
    let cols = row.columns().iter().map(|v| v.name_str().to_string()).collect::<Vec<_>>();
    let values = column_types
        .iter()
        .enumerate()
        .map(|(i, column_type)| {
            let cell: mysql::Value = row
                .take_opt(i)
                .unwrap_or_else(|| unreachable!("column length does not enough"))
                .unwrap_or_else(|_| unreachable!("could not convert as `mysql::Value`"));

            if cell == mysql::Value::NULL {
                return Ok(Value::Nil);
            }

            match column_type {
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => fvo(cell)
                    .and_then(|v: Vec<u8>| {
                        bigdecimal::BigDecimal::parse_bytes(&v, 10)
                            .ok_or(mysql::FromValueError(mysql::Value::Bytes(v)))
                    })
                    .map(Value::BigDecimal),
                ColumnType::MYSQL_TYPE_TINY => fvo(cell).map(Value::Tinyint),
                ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                    fvo(cell).map(Value::Smallint)
                }
                ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                    fvo(cell).map(Value::Int)
                }
                ColumnType::MYSQL_TYPE_LONGLONG => fvo(cell).map(Value::Bigint),
                ColumnType::MYSQL_TYPE_FLOAT => fvo(cell).map(Value::Float),
                ColumnType::MYSQL_TYPE_DOUBLE => fvo(cell).map(Value::Double),
                ColumnType::MYSQL_TYPE_NULL => fvo(cell).map(|_: mysql::Value| Value::Nil),
                ColumnType::MYSQL_TYPE_TIMESTAMP => fvo(cell).map(|v: chrono::NaiveDateTime| {
                    Value::Timestamp(chrono::DateTime::from_utc(v, chrono::Utc))
                }),
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
                    fvo(cell).map(Value::Date)
                }
                ColumnType::MYSQL_TYPE_TIME => fvo(cell).map(Value::Time),
                ColumnType::MYSQL_TYPE_DATETIME => fvo(cell).map(Value::DateTime),
                ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_STRING => fvo(cell).map(Value::Text),
                ColumnType::MYSQL_TYPE_JSON => fvo(cell).map(Value::Json),
                ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BLOB => fvo(cell).map(Value::Blob),
                ColumnType::MYSQL_TYPE_TIMESTAMP2
                | ColumnType::MYSQL_TYPE_DATETIME2
                | ColumnType::MYSQL_TYPE_TIME2 => {
                    panic!("only used in server side: {:?}", column_type)
                }
                ColumnType::MYSQL_TYPE_BIT
                | ColumnType::MYSQL_TYPE_ENUM
                | ColumnType::MYSQL_TYPE_SET
                | ColumnType::MYSQL_TYPE_GEOMETRY => {
                    panic!("not yet handling this kind: {:?}", column_type)
                }
            }
            .map_err(AkitaError::from)
        }).map(|v| v.unwrap_or(Value::Nil))
        .collect::<Vec<_>>();
    Ok(crate::Row{
        columns: cols,
        data: values
    })
}



pub trait FromRowExt {
    fn from_long_row(row: mysql::Row) -> Self where
    Self: Sized + Default;
    fn from_long_row_opt(row: mysql::Row) -> Result<Self, mysql::FromRowError>
    where
        Self: Sized + Default;
}

#[inline]
#[allow(unused)]
pub fn from_long_row<T: FromRowExt + Default>(row: Row) -> T {
    FromRowExt::from_long_row(row)
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct MysqlConnectionManager {
    params: Opts,
    cfg: AkitaConfig,
}

impl MysqlConnectionManager {
    pub fn new(params: OptsBuilder, cfg: AkitaConfig) -> MysqlConnectionManager {
        MysqlConnectionManager {
            params: Opts::from(params),
            cfg,
        }
    }
}

impl r2d2::ManageConnection for MysqlConnectionManager {
    type Connection = Conn;
    type Error = Error;

    fn connect(&self) -> Result<Conn, Error> {
        Conn::new(self.params.to_owned())
    }

    fn is_valid(&self, conn: &mut Conn) -> Result<(), Error> {
        conn.query_drop("SELECT version()")
    }

    fn has_broken(&self, conn: &mut Conn) -> bool {
        self.is_valid(conn).is_err()
    }
}

///
/// 创建连接池
/// cfg 配置信息
/// 
pub fn init_pool(cfg: &AkitaConfig) -> Result<R2d2Pool, AkitaError> {
    test_connection(cfg)?;
    let manager = MysqlConnectionManager::new(cfg.into(), cfg.to_owned());
    let pool = Pool::builder().connection_timeout(cfg.connection_timeout()).min_idle(cfg.min_idle()).max_size(cfg.max_size()).build(manager)?;
    Ok(pool)
}

/// 测试连接池连接
fn test_connection(cfg: &AkitaConfig) -> Result<(), AkitaError> {
    let manager = MysqlConnectionManager::new(cfg.into(), cfg.to_owned());
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}