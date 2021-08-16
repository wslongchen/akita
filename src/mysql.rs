//! 
//! MySQL modules.
//! 
use mysql::prelude::Protocol;
use mysql::{Conn, Error, Opts, OptsBuilder, Row, prelude::Queryable};
use r2d2::{ManageConnection, Pool};

use std::result::Result;

use crate::database::Database;
use crate::pool::LogLevel;
use crate::types::SqlType;
use crate::value::{ToValue, Value};
use crate::{AkitaError, information::{ColumnDef, FieldName, ColumnSpecification, DatabaseName, TableDef, TableName}, comm};
use crate::data::{FromAkita, Rows, AkitaData};
type R2d2Pool = Pool<MysqlConnectionManager>;

pub struct MysqlDatabase(pub r2d2::PooledConnection<MysqlConnectionManager>);

/// TODO: 补全MYSQL数据操作
impl Database for MysqlDatabase {
    fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("START TRANSACTION", &[])?;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("COMMIT TRANSACTION", &[])?;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.execute_result("ROLLBACK TRANSACTION", &[])?;
        Ok(())
    }
    fn execute_result_log(&mut self, sql: &str, param: &[&Value], log_level: &LogLevel) -> Result<Rows, AkitaError> {
        match log_level {
            LogLevel::Debug => debug!("[sql]: {}", &sql),
            LogLevel::Info => info!("[sql]: {}", &sql),
            LogLevel::Error => error!("[sql]: {}", &sql),
        }
        fn collect<T: Protocol>(mut rows: mysql::QueryResult<T>) -> Result<Rows, AkitaError> {
            let column_types: Vec<_> = rows.columns().as_ref().iter().map(|c| c.column_type()).collect();

            let fields = rows
                .columns().as_ref()
                .iter()
                .map(|c| std::str::from_utf8(c.name_ref()).map(ToString::to_string))
                .collect::<Result<Vec<String>, _>>()
                .map_err(|e| AkitaError::from(e))?;

            let mut records = Rows::new(fields);
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

        if param.is_empty() {
            let rows = self
                .0
                .query_iter(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;

            collect(rows)
        } else {
            let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
            let params: mysql::Params = param
                .iter()
                .map(|v| MyValue(v))
                .map(|v| mysql::prelude::ToValue::to_value(&v))
                .collect::<Vec<_>>()
                .into();
            let rows = self.0.exec_iter(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
            
            collect(rows)
        }
    }

    fn execute_result(&mut self, sql: &str, param: &[&crate::value::Value]) -> Result<Rows, AkitaError> {
        fn collect<T: Protocol>(mut rows: mysql::QueryResult<T>) -> Result<Rows, AkitaError> {
            let column_types: Vec<_> = rows.columns().as_ref().iter().map(|c| c.column_type()).collect();
            let fields = rows
                .columns().as_ref()
                .iter()
                .map(|c| std::str::from_utf8(c.name_ref()).map(ToString::to_string))
                .collect::<Result<Vec<String>, _>>()
                .map_err(|e| AkitaError::from(e))?;

            let mut records = Rows::new(fields);
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

        if param.is_empty() {
            let rows = self
                .0
                .query_iter(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;

            collect(rows)
        } else {
            let stmt = self
                .0
                .prep(&sql)
                .map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
            let params: mysql::Params = param
                .iter()
                .map(|v| MyValue(v))
                .map(|v| mysql::prelude::ToValue::to_value(&v))
                .collect::<Vec<_>>()
                .into();
            let rows = self.0.exec_iter(stmt, &params).map_err(|e| AkitaError::ExcuteSqlError(e.to_string(), sql.to_string()))?;
            
            collect(rows)
        }
    }

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError> {
        #[derive(Debug, FromAkita)]
        struct TableSpec {
            schema: String,
            name: String,
            comment: String,
            is_view: i32,
        }

        let schema: Value = table_name
            .schema
            .as_ref()
            .map(String::as_str)
            .unwrap_or("__DUMMY__")
            .into();
        let table_name: &Value = &table_name.name.clone().into();

        let mut tables: Vec<TableSpec> = self
        .execute_result(
            r#"
            SELECT TABLE_SCHEMA AS `schema`,
                   TABLE_NAME AS name,
                   TABLE_COMMENT AS comment,
                   CASE TABLE_TYPE WHEN 'VIEW' THEN TRUE ELSE FALSE END AS is_view
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_SCHEMA = CASE ? WHEN '__DUMMY__' THEN DATABASE() ELSE ? END AND TABLE_NAME = ?"#,
            &[
                &schema, &schema,
                &table_name,
            ],
        )?
        .iter()
        .map(|data| FromAkita::from_data(&data))
        .collect();

        let table_spec = match tables.len() {
            0 => return Err(AkitaError::DataError("Zero record returned.".to_string())),
            _ => tables.remove(0),
        };

        #[derive(Debug, FromAkita)]
        struct ColumnSpec {
            schema: String,
            table_name: String,
            name: String,
            comment: String,
            type_: String,
        }
        let table_schema: &Value =  &table_spec.schema.clone().into();
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
                &[table_schema, &table_name],
            )?
            .iter()
            .map(|data| FromAkita::from_data(&data))
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

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError> {
        let sql = "SELECT database() AS name";
        let mut database_names: Vec<Option<DatabaseName>> =
            self.execute_result(&sql, &[]).map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.get_opt("name")
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
    #[derive(Debug, FromAkita)]
    struct TableNameSimple {
        table_name: String,
    }
    let sql = "SELECT TABLE_NAME as table_name FROM information_schema.tables WHERE table_type= ?";
    let result: Vec<TableNameSimple> = db
        .execute_result(sql, &[&kind.to_value()])?
        .iter()
        .map(|row| TableNameSimple {
            table_name: row.get("table_name").expect("must have a table name"),
        })
        .collect();
    let mut table_names = vec![];
    for r in result {
        let table_name = TableName::from(&r.table_name);
        table_names.push(table_name);
    }
    Ok(table_names)
}

#[derive(Debug)]
pub struct MyValue<'a>(&'a Value);


impl mysql::prelude::ToValue for MyValue<'_> {
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
            Value::Array(_) => unimplemented!("unsupported type"),
            Value::SerdeJson(ref v) => v.into(),
            // Value::BigDecimal(_) => unimplemented!("we need to upgrade bigdecimal crate"),
            // Value::Point(_) | Value::Array(_) => unimplemented!("unsupported type"),
        }
    }
}

fn into_record(
    mut row: mysql::Row,
    column_types: &[mysql::consts::ColumnType],
) -> Result<Vec<Value>, AkitaError> {
    use mysql::{consts::ColumnType, from_value_opt as fvo};

    column_types
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
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => fvo(cell).map(Value::Float),
                    // .and_then(|v: Vec<u8>| {
                    //     bigdecimal::BigDecimal::parse_bytes(&v, 10)
                    //         .ok_or(mysql::FromValueError(mysql::Value::Bytes(v)))
                    // })
                    // .map(Value::BigDecimal),
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
        })
        .collect()
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

#[derive(Clone, Debug)]
pub struct MysqlConnectionManager {
    params: Opts,
}

impl MysqlConnectionManager {
    pub fn new(params: OptsBuilder) -> MysqlConnectionManager {
        MysqlConnectionManager {
            params: Opts::from(params),
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
/// database_url 连接地址
/// max_size 最大连接数量
/// 
pub fn init_pool<S: Into<String>>(database_url: S, max_size: u32) -> Result<R2d2Pool, AkitaError> {
    let database_url: String = database_url.into();
    test_connection(&database_url)?;
    let opts = Opts::from_url(&database_url)?;
    let builder = OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let pool = Pool::builder().max_size(max_size).build(manager)?;
    Ok(pool)
}

/// 测试连接池连接
fn test_connection(database_url: &str) -> Result<(), AkitaError> {
    let opts = mysql::Opts::from_url(&database_url)?;
    let builder = mysql::OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}