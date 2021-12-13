//! 
//! SQLite modules.
//! 
use bigdecimal::ToPrimitive;
use log::{debug, error, info};
use r2d2::{ManageConnection, Pool};
use rusqlite::{Connection, Error, OpenFlags};
use uuid::Uuid;
use std::fmt;
use std::path::{Path, PathBuf};
use std::result::Result;


cfg_if! {if #[cfg(feature = "akita-auth")]{
    use crate::auth::{GrantUserPrivilege, Role, UserInfo, DataBaseUser};
}}

use crate::{AkitaConfig, Params, ToValue};
use crate::database::Database;
use crate::pool::LogLevel;
use crate::{self as akita, comm::{extract_datatype_with_capacity, maybe_trim_parenthesis}, Rows, Value, SqlType, cfg_if, Capacity, ColumnConstraint, ForeignKey, Key, Literal, TableKey, AkitaError, ColumnDef, FieldName, ColumnSpecification, DatabaseName, TableDef, TableName, SchemaContent};
type R2d2Pool = Pool<SqliteConnectionManager>;

pub struct SqliteDatabase(r2d2::PooledConnection<SqliteConnectionManager>, AkitaConfig);

impl SqliteDatabase {
    pub fn new(pool: r2d2::PooledConnection<SqliteConnectionManager>, cfg: AkitaConfig) -> Self {
        SqliteDatabase(pool, cfg)
    }
}

/// SQLite数据操作
#[allow(unused)]
impl Database for SqliteDatabase {
    fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("BEGIN TRANSACTION", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }

    fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("COMMIT TRANSACTION", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }

    fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("ROLLBACK TRANSACTION", Params::Nil).map(|_| ()).map_err(AkitaError::from)
    }
    
    fn execute_result(&mut self, sql: &str, params: Params) -> Result<Rows, AkitaError> {
        if let Some(log_level) = &self.1.log_level() {
            match log_level {
                LogLevel::Debug => debug!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
                LogLevel::Info => info!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
                LogLevel::Error => error!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
            }
        }
        let stmt = self.0.prepare(&sql);
        let column_names = if let Ok(ref stmt) = stmt {
            stmt.column_names()
        } else {
            vec![]
        };
        let column_names: Vec<String> = column_names.iter().map(ToString::to_string).collect();
        match stmt {
            Ok(mut stmt) => {
                let column_count = stmt.column_count();
                let mut records = Rows::new(column_names);
                let sql_values = match params {
                    Params::Nil => {
                        vec![]
                    },
                    Params::Vector(param) => {
                        param
                            .iter()
                            .map(|v| to_sq_value(v))
                            .collect::<Vec<_>>()
                    },
                    Params::Custom(param) => {
                        let mut format_sql = sql.to_owned();
                        let len = format_sql.len();
                        let mut values = param.iter().map(|param| {
                            let key = format!(":{}", param.0);
                            let index = format_sql.find(&key).unwrap_or(len);
                            (index, key, &param.1)
                        }).collect::<Vec<_>>();
                        values.sort_by(|a, b| a.0.cmp(&b.0));
                        values.iter().map(|v| {
                            format_sql = format_sql.replace(&v.1, &format!("${}", v.0 + 1));
                            to_sq_value(v.2)
                        }).collect::<Vec<_>>()
                    },
                };
                // let v = sq_values.iter().map(|v| v.to_sql().unwrap()).collect::<Vec<_>>();
                if let Ok(mut rows) = stmt.query(sql_values) {
                    while let Some(row) = rows.next()? {
                        let mut record: Vec<Value> = vec![];
                        for i in 0..column_count {
                            let raw = row.get(i);
                            if let Ok(raw) = raw {
                                let value = match raw {
                                    rusqlite::types::Value::Blob(v) => Value::Blob(v),
                                    rusqlite::types::Value::Real(v) => Value::Double(v),
                                    rusqlite::types::Value::Integer(v) => Value::Bigint(v),
                                    rusqlite::types::Value::Text(v) => Value::Text(v),
                                    rusqlite::types::Value::Null => Value::Nil,
                                };
                                record.push(value);
                            }
                        }
                        records.push(record);
                    }
                }
                Ok(records)
            }
            Err(e) => Err(AkitaError::from(e)),
        }
    }

    fn execute_drop(&mut self, sql: &str, params: Params) -> Result<(), AkitaError> {
        if let Some(log_level) = &self.1.log_level() {
            match log_level {
                LogLevel::Debug => debug!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
                LogLevel::Info => info!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
                LogLevel::Error => error!("[Akita]: Prepare SQL: {} params: {:?}", &sql, params),
            }
        }
        let stmt = self.0.prepare(&sql);
        match stmt {
            Ok(mut stmt) => {
                let sql_values = match params {
                    Params::Nil => {
                        vec![]
                    },
                    Params::Vector(param) => {
                        param
                            .iter()
                            .map(|v| to_sq_value(v))
                            .collect::<Vec<_>>()
                    },
                    Params::Custom(param) => {
                        let mut format_sql = sql.to_owned();
                        let len = format_sql.len();
                        let mut values = param.iter().map(|param| {
                            let key = format!(":{}", param.0);
                            let index = format_sql.find(&key).unwrap_or(len);
                            (index, key, &param.1)
                        }).collect::<Vec<_>>();
                        values.sort_by(|a, b| a.0.cmp(&b.0));
                        values.iter().map(|v| {
                            format_sql = format_sql.replace(&v.1, &format!("${}", v.0 + 1));
                            to_sq_value(v.2)
                        }).collect::<Vec<_>>()
                    },
                };
                stmt.execute(sql_values).map(|_| ()).map_err(AkitaError::from)
            }
            Err(e) => Err(AkitaError::from(e)),
        }
    }

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError> {
        #[derive(Debug)]
        struct ColumnSimple {
            name: String,
            data_type: String,
            not_null: bool,
            default: Option<String>,
            pk: bool,
        }
        impl ColumnSimple {
            fn to_column(&self, table_name: &TableName) -> ColumnDef {
                ColumnDef {
                    table: table_name.clone(),
                    name: FieldName::from(&self.name),
                    comment: None,
                    specification: self.to_column_specification(),
                    stat: None,
                }
            }

            fn to_column_specification(&self) -> ColumnSpecification {
                let (sql_type, capacity) = self.get_sql_type_capacity();
                ColumnSpecification {
                    sql_type,
                    capacity,
                    constraints: self.to_column_constraints(),
                }
            }

            fn to_column_constraints(&self) -> Vec<ColumnConstraint> {
                let (sql_type, _) = self.get_sql_type_capacity();
                let mut constraints = vec![];
                if self.not_null {
                    constraints.push(ColumnConstraint::NotNull);
                }
                if let Some(ref default) = self.default {
                    let ic_default = default.to_lowercase();
                    let constraint = if ic_default == "null" {
                        ColumnConstraint::DefaultValue(Literal::Null)
                    } else if ic_default.starts_with("nextval") {
                        ColumnConstraint::AutoIncrement(None)
                    } else {
                        let literal = match sql_type {
                            SqlType::Bool => {
                                let v: bool = default.parse().unwrap();
                                Literal::Bool(v)
                            }
                            SqlType::Int
                            | SqlType::Smallint
                            | SqlType::Tinyint
                            | SqlType::Bigint => {
                                let v: Result<i64, _> = default.parse();
                                match v {
                                    Ok(v) => Literal::Integer(v),
                                    Err(e) => {
                                        panic!("error parsing to integer: {} error: {}", default, e)
                                    }
                                }
                            }
                            SqlType::Float | SqlType::Double | SqlType::Real | SqlType::Numeric => {
                                // some defaults have cast type example: (0)::numeric
                                let splinters = maybe_trim_parenthesis(&default)
                                    .split("::")
                                    .collect::<Vec<&str>>();
                                let default_value = maybe_trim_parenthesis(splinters[0]);
                                if default_value.to_lowercase() == "null" {
                                    Literal::Null
                                } else {
                                    match default.parse::<f64>() {
                                        Ok(val) => Literal::Double(val),
                                        Err(e) => {
                                            panic!(
                                                "unable to evaluate default value expression: {}, error: {}",
                                                default, e
                                            )
                                        }
                                    }
                                }
                            }
                            SqlType::Uuid => {
                                if ic_default == "uuid_generate_v4()" {
                                    Literal::UuidGenerateV4
                                } else {
                                    let v: Result<Uuid, _> = Uuid::parse_str(&default);
                                    match v {
                                        Ok(v) => Literal::Uuid(v),
                                        Err(e) => panic!(
                                            "error parsing to uuid: {} error: {}",
                                            default, e
                                        ),
                                    }
                                }
                            }
                            SqlType::Timestamp | SqlType::TimestampTz => {
                                if ic_default == "now()"
                                    || ic_default == "timezone('utc'::text, now())"
                                    || ic_default == "current_timestamp"
                                {
                                    Literal::CurrentTimestamp
                                } else {
                                    panic!(
                                        "timestamp other than now is not covered, got: {}",
                                        ic_default
                                    )
                                }
                            }
                            SqlType::Date => {
                                // timestamp converted to text then converted to date
                                // is equivalent to today()
                                if ic_default == "today()"
                                    || ic_default == "now()"
                                    || ic_default == "('now'::text)::date"
                                {
                                    Literal::CurrentDate
                                } else {
                                    panic!(
                                        "date other than today, now is not covered in {:?}",
                                        self
                                    )
                                }
                            }
                            SqlType::Varchar
                            | SqlType::Char
                            | SqlType::Tinytext
                            | SqlType::Mediumtext
                            | SqlType::Text => Literal::String(default.to_owned()),
                            SqlType::Enum(_name, _choices) => Literal::String(default.to_owned()),
                            _ => panic!("not convered: {:?}", sql_type),
                        };
                        ColumnConstraint::DefaultValue(literal)
                    };
                    constraints.push(constraint);
                }
                constraints
            }

            fn get_sql_type_capacity(&self) -> (SqlType, Option<Capacity>) {
                let (dtype, capacity) = extract_datatype_with_capacity(&self.data_type);
                let sql_type = match &*dtype {
                    "int" | "integer" => SqlType::Int,
                    "smallint" => SqlType::Smallint,
                    "varchar" => SqlType::Text,
                    "character varying" => SqlType::Text,
                    "decimal" => SqlType::Double,
                    "timestamp" => SqlType::Timestamp,
                    "numeric" => SqlType::Numeric,
                    "char" => match capacity {
                        None => SqlType::Char,
                        Some(Capacity::Limit(1)) => SqlType::Char,
                        Some(_) => SqlType::Varchar,
                    },
                    "blob" => SqlType::Blob,
                    "" => SqlType::Text,
                    _ => {
                        if dtype.contains("text") {
                            SqlType::Text
                        } else {
                            panic!("not yet handled: {:?}", dtype)
                        }
                    }
                };
                (sql_type, capacity)
            }
        }
        macro_rules! unwrap_ok_some {
            ($var:ident) => {
                match $var {
                    Ok($var) => match $var {
                        Some($var) => $var,
                        None => panic!("expecting {} to have a value", stringify!($var)),
                    },
                    Err(_e) => panic!("expecting {} to be not error", stringify!($var)),
                }
            };
        }
        let sql = format!("PRAGMA table_info({});", table_name.complete_name());
        let result = self.execute_result(&sql, ().into())?;
        let mut primary_columns = vec![];
        let mut columns = vec![];
        for data in result.iter() {
            let name: Result<Option<String>, _> = data.get_obj("name");
            let name = unwrap_ok_some!(name);
            let data_type: Result<Option<String>, _> = data.get_obj("type");
            let data_type = unwrap_ok_some!(data_type).to_lowercase();
            let not_null: Result<Option<i64>, _> = data.get_obj("notnull");
            let not_null = unwrap_ok_some!(not_null) != 0;
            let pk: Result<Option<i64>, _> = data.get_obj("pk");
            let pk = unwrap_ok_some!(pk) != 0;
            if pk {
                primary_columns.push(FieldName::from(&name));
            }
            let default = data.get_obj_value("dflt_value").map(|v| match *v {
                Value::Text(ref v) => v.to_owned(),
                Value::Nil => "null".to_string(),
                _ => panic!("Expecting a text value, got: {:?}", v),
            });
            let simple = ColumnSimple {
                name,
                data_type,
                default,
                pk,
                not_null,
            };
            columns.push(simple.to_column(table_name));
        }
        let primary_key = Key {
            name: None,
            columns: primary_columns,
        };
        let foreign_keys = get_foreign_keys(&mut *self, table_name)?;
        let table_key_foreign: Vec<TableKey> =
            foreign_keys.into_iter().map(TableKey::ForeignKey).collect();
        let mut table_keys = vec![TableKey::PrimaryKey(primary_key)];
        table_keys.extend(table_key_foreign);
        let table = TableDef {
            name: table_name.clone(),
            comment: None, // TODO: need to extract comment from the create_sql
            columns,
            is_view: false,
            table_key: table_keys,
        };
        Ok(Some(table))
    }

    fn exist_table(&mut self, table_name: &TableName) -> Result<bool, AkitaError> {
        todo!()
    }

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, AkitaError> {
        let table_names = get_table_names(&mut *self, &"table".to_string())?;
        let view_names = get_table_names(&mut *self, &"view".to_string())?;
        let schema_content = SchemaContent {
            schema: "".to_string(),
            tablenames: table_names,
            views: view_names,
        };
        Ok(vec![schema_content])
    }

    fn get_all_tables(&mut self, shema: &str) -> Result<Vec<TableDef>, AkitaError> {
        let tablenames = self.get_tablenames(shema)?;
        Ok(tablenames
            .iter()
            .filter_map(|tablename| self.get_table(tablename).ok().flatten())
            .collect())
    }

    fn get_tablenames(&mut self, _shema: &str) -> Result<Vec<TableName>, AkitaError> {
        #[derive(Debug, FromValue)]
        struct TableNameSimple {
            tbl_name: String,
        }
        let sql = "SELECT tbl_name FROM sqlite_master WHERE type IN ('table', 'view')";
        let result: Vec<TableNameSimple> = self
            .execute_result(sql, ().into())?
            .iter()
            .map(|row| TableNameSimple {
                tbl_name: row.get_obj("tbl_name").expect("tbl_name"),
            })
            .collect();
        let tablenames = result
            .iter()
            .map(|r| TableName::from(&r.tbl_name))
            .collect();
        Ok(tablenames)
    }

    fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>, AkitaError> {
        let sql = "UPDATE sqlite_sequence SET seq = $2 WHERE name = $1";
        self.execute_result(
            sql,
            (&table_name.complete_name(), &sequence_value).into()
        )?;

        Ok(None)
    }

    fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>, AkitaError> {
        let sql = "SELECT seq FROM sqlite_sequence where name = $1";
        let result: Vec<Option<i64>> = self
            .execute_result(sql, (table_name.complete_name(),).into())?
            .iter()
            .filter_map(|row| row.get_obj("seq").ok())
            .collect();

        if let Some(first) = result.get(0) {
            Ok(*first)
        } else {
            Ok(None)
        }
    }

    fn create_database(&mut self, _database: &str) -> Result<(), AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't need to created database".to_string(),
        ))
    }

    fn exist_databse(&mut self, database: &str) -> Result<bool, AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't need to exist databse".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn get_users(&mut self) -> Result<Vec<DataBaseUser>, AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operatio to extract users".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn exist_user(&mut self, user: &UserInfo) -> Result<bool, AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operatio to exist user".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn get_user_detail(&mut self, _username: &str) -> Result<Vec<DataBaseUser>, AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operatio to user details".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn get_roles(&mut self, _username: &str) -> Result<Vec<Role>, AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operation to extract roles".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn create_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operation to create_user".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn drop_user(&mut self, user: &UserInfo) -> Result<(), AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operation to drop_user".to_string(),
        ))
    }
    
    #[cfg(feature = "akita-auth")]
    fn grant_privileges(&mut self, user: &GrantUserPrivilege) -> Result<(), AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operation to grant_privileges".to_string(),
        ))
    }

    #[cfg(feature = "akita-auth")]
    fn flush_privileges(&mut self) -> Result<(), AkitaError> {
        Err(AkitaError::UnsupportedOperation(
            "sqlite doesn't have operation to flush_privileges".to_string(),
        ))
    }

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError> {
        let sql = "SELECT database() AS name";
        let mut database_names: Vec<Option<DatabaseName>> =
            self.execute_result(&sql, ().into()).map(|rows| {
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
}

#[allow(unused)]
fn get_table_names(db: &mut dyn Database, kind: &str) -> Result<Vec<TableName>, AkitaError> {
    #[derive(Debug, FromValue)]
    struct TableNameSimple {
        tbl_name: String,
    }
    let sql = "SELECT tbl_name FROM sqlite_master WHERE type = ?";
    let result: Vec<TableNameSimple> = db
        .execute_result(sql, kind.to_value().into())?
        .iter()
        .map(|row| TableNameSimple {
            tbl_name: row.get_obj("tbl_name").expect("tbl_name"),
        })
        .collect();
    let mut table_names = vec![];
    for r in result {
        let table_name = TableName::from(&r.tbl_name);
        table_names.push(table_name);
    }
    Ok(table_names)
}

/// get the foreign keys of table
fn get_foreign_keys(db: &mut dyn Database, table: &TableName) -> Result<Vec<ForeignKey>, AkitaError> {
    let sql = format!("PRAGMA foreign_key_list({});", table.complete_name());
    #[derive(Debug, FromValue)]
    struct ForeignSimple {
        id: i64,
        table: String,
        from: String,
        to: String,
    }
    let result: Vec<ForeignSimple> = db
        .execute_result(&sql, ().into())?
        .iter()
        .map(|row| ForeignSimple {
            id: row.get_obj("id").expect("id"),
            table: row.get_obj("table").expect("table"),
            from: row.get_obj("from").expect("from"),
            to: row.get_obj("to").expect("to"),
        })
        .collect();
    let mut foreign_tables: Vec<(i64, TableName)> = result
        .iter()
        .map(|f| (f.id, TableName::from(&f.table)))
        .collect();
    foreign_tables.dedup();
    let mut foreign_keys = Vec::with_capacity(foreign_tables.len());
    for (id, foreign_table) in foreign_tables {
        let foreigns: Vec<&ForeignSimple> = result.iter().filter(|f| f.id == id).collect();
        let (local_columns, referred_columns): (Vec<FieldName>, Vec<FieldName>) = foreigns
            .iter()
            .map(|f| (FieldName::from(&f.from), FieldName::from(&f.to)))
            .unzip();
        let foreign_key = ForeignKey {
            name: None,
            columns: local_columns,
            foreign_table,
            referred_columns,
        };
        foreign_keys.push(foreign_key);
    }
    Ok(foreign_keys)
}


fn to_sq_value(val: &Value) -> rusqlite::types::Value {
    match *val {
        Value::Text(ref v) => rusqlite::types::Value::Text(v.to_owned()),
        Value::Bool(v) => rusqlite::types::Value::Integer(if v { 1 } else { 0 }),
        Value::Tinyint(v) => rusqlite::types::Value::Integer(i64::from(v)),
        Value::Smallint(v) => rusqlite::types::Value::Integer(i64::from(v)),
        Value::Int(v) => rusqlite::types::Value::Integer(i64::from(v)),
        Value::Bigint(v) => rusqlite::types::Value::Integer(v),

        Value::Float(v) => rusqlite::types::Value::Real(f64::from(v)),
        Value::Double(v) => rusqlite::types::Value::Real(v),
        Value::BigDecimal(ref v) => match v.to_f64() {
            Some(v) => rusqlite::types::Value::Real(v as f64),
            None => panic!("unable to convert bigdecimal"),
        },
        Value::Blob(ref v) => rusqlite::types::Value::Blob(v.clone()),
        Value::Char(v) => rusqlite::types::Value::Text(format!("{}", v)),
        Value::Json(ref v) => rusqlite::types::Value::Text(v.clone()),
        Value::Uuid(ref v) => rusqlite::types::Value::Text(v.to_string()),
        Value::Date(ref v) => rusqlite::types::Value::Text(v.to_string()),
        Value::DateTime(ref v) => rusqlite::types::Value::Text(v.to_string()),
        Value::Nil => rusqlite::types::Value::Null,
        _ => panic!("not yet handled: {:?}", val),
    }
}


#[derive(Debug)]
enum Source {
    File(PathBuf),
    Memory,
}

type InitFn = dyn Fn(&mut Connection) -> Result<(), rusqlite::Error> + Send + Sync + 'static;

pub struct SqliteConnectionManager {
    source: Source,
    flags: OpenFlags,
    init: Option<Box<InitFn>>,
}

impl fmt::Debug for SqliteConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("SqliteConnectionManager");
        let _ = builder.field("source", &self.source);
        let _ = builder.field("flags", &self.source);
        let _ = builder.field("init", &self.init.as_ref().map(|_| "InitFn"));
        builder.finish()
    }
}

impl SqliteConnectionManager {
    /// Creates a new `SqliteConnectionManager` from file.
    ///
    /// See `rusqlite::Connection::open`
    pub fn file<P: AsRef<Path>>(path: P) -> Self {
        Self {
            source: Source::File(path.as_ref().to_path_buf()),
            flags: OpenFlags::default(),
            init: None,
        }
    }

    /// Creates a new `SqliteConnectionManager` from memory.
    pub fn memory() -> Self {
        Self {
            source: Source::Memory,
            flags: OpenFlags::default(),
            init: None,
        }
    }

    /// Converts `SqliteConnectionManager` into one that sets OpenFlags upon
    /// connection creation.
    ///
    /// See `rustqlite::OpenFlags` for a list of available flags.
    pub fn with_flags(self, flags: OpenFlags) -> Self {
        Self { flags, ..self }
    }

    /// Converts `SqliteConnectionManager` into one that calls an initialization
    /// function upon connection creation. Could be used to set PRAGMAs, for
    /// example.
    ///
    /// ### Example
    ///
    /// Make a `SqliteConnectionManager` that sets the `foreign_keys` pragma to
    /// true for every connection.
    ///
    /// ```rust,no_run
    /// # use r2d2_sqlite::{SqliteConnectionManager};
    /// let manager = SqliteConnectionManager::file("app.db")
    ///     .with_init(|c| c.execute_batch("PRAGMA foreign_keys=1;"));
    /// ```
    pub fn with_init<F>(self, init: F) -> Self
    where
        F: Fn(&mut Connection) -> Result<(), rusqlite::Error> + Send + Sync + 'static,
    {
        let init: Option<Box<InitFn>> = Some(Box::new(init));
        Self { init, ..self }
    }
}

impl r2d2::ManageConnection for SqliteConnectionManager {
    type Connection = Connection;
    type Error = rusqlite::Error;

    fn connect(&self) -> Result<Connection, Error> {
        match self.source {
            Source::File(ref path) => Connection::open_with_flags(path, self.flags),
            Source::Memory => Connection::open_in_memory_with_flags(self.flags),
        }
        .map_err(Into::into)
        .and_then(|mut c| match self.init {
            None => Ok(c),
            Some(ref init) => init(&mut c).map(|_| c),
        })
    }

    fn is_valid(&self, conn: &mut Connection) -> Result<(), Error> {
        conn.execute_batch("").map_err(Into::into)
    }

    fn has_broken(&self, _: &mut Connection) -> bool {
        false
    }
}

///
/// 创建连接池
/// cfg 配置信息
/// 
pub fn init_pool(cfg: &AkitaConfig) -> Result<R2d2Pool, AkitaError> {
    let database_url = &cfg.url().to_owned();
    test_connection(&database_url)?;
    let manager = SqliteConnectionManager::file(database_url);
    let pool = Pool::builder().connection_timeout(cfg.to_owned().connection_timeout()).min_idle(cfg.min_idle()).max_size(cfg.max_size()).build(manager)?;
    Ok(pool)
}

/// 测试连接池连接
fn test_connection(database_url: &str) -> Result<(), AkitaError> {
    let database_url: String = database_url.into();
    let manager = SqliteConnectionManager::file(database_url);
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}


#[cfg(test)]
mod test {
    use crate::{AkitaConfig, AkitaMapper, FromValue, Pool, QueryWrapper, AkitaTable, ToValue, types::SqlType::{Int, Text, Timestamp}};

    #[derive(Debug, FromValue, ToValue, AkitaTable, Clone)]
    #[table(name="test")]
    struct TestSqlite {
        #[table_id]
        id: i32,
        name: String
    }

    #[test]
    fn test_conn() {
        let db_url = "sqlite://./../../example/akita.sqlite3";
        let mut pool = Pool::new(AkitaConfig::new(db_url.to_string())).unwrap();
        let result = pool.connect();
        assert!(result.is_ok());
    }

    #[test]
    fn test_list() {
        let db_url = "sqlite://./../../example/akita.sqlite3";
        let mut pool = Pool::new(AkitaConfig::new(db_url.to_string())).unwrap();
        let mut em = pool.entity_manager().unwrap();
        let datas = em.list::<TestSqlite, QueryWrapper>(&mut QueryWrapper::new()).unwrap();
        println!("{:?}", datas);
    }
}