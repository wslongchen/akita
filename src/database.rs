use std::{convert::TryFrom, ops::Deref};

use crate::{cfg_if, Params, TableName, DatabaseName, SchemaContent, TableDef, Rows, AkitaMapper, Wrapper, IPage, ISegment};
use url::Url;
use akita_core::{AkitaDataError, FieldType, from_value, from_value_opt, FromValue, GetFields, GetTableName, ToValue, Value};

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
use crate::manager::{build_insert_clause, build_update_clause};


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


impl DatabasePlatform {
    /// Get all the table of records
    pub fn list<T>(&mut self, mut wrapper:Wrapper) -> Result<Vec<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter().filter(|f| f.exist)
            .map(|c| format!("`{}`", &c.alias.to_owned().unwrap_or(c.name.to_string())))
            .collect::<Vec<_>>()
            .join(", ");
        let select_fields = wrapper.get_select_sql();
        let enumerated_columns = if select_fields.eq("*") {
            enumerated_columns
        } else {
            select_fields
        };
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &table.complete_name(),where_condition);
        let rows = self.execute_result(&sql, Params::Nil)?;
        let mut entities = vec![];
        for data in rows.iter() {
            let entity = T::from_value(&data);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    pub fn select_one<T>(&mut self, mut wrapper:Wrapper) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter().filter(|f| f.exist)
            .map(|c| format!("`{}`", &c.alias.to_owned().unwrap_or(c.name.to_string())))
            .collect::<Vec<_>>()
            .join(", ");
        let select_fields = wrapper.get_select_sql();
        let enumerated_columns = if select_fields.eq("*") {
            enumerated_columns
        } else {
            select_fields
        };
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &table.complete_name(), where_condition);
        let rows = self.execute_result(&sql, Params::Nil)?;
        Ok(rows.iter().next().map(|data| T::from_value(&data)))
    }

    /// Get one the table of records by id
    pub fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,
            I: ToValue
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let col_len = columns.len();
        let enumerated_columns = columns
            .iter().filter(|f| f.exist)
            .map(|c| format!("`{}`", c.alias.to_owned().unwrap_or(c.name.to_string())))
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(field) = columns.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match self {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("SELECT {} FROM {} WHERE `{}` = ? limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };

            let rows = self.execute_result(&sql, (id.to_value(),).into())?;
            Ok(rows.iter().next().map(|data| T::from_value(&data)))
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }

    /// Get table of records with page
    pub fn page<T>(&mut self, page: usize, size: usize, mut wrapper:Wrapper) -> Result<IPage<T>, AkitaError>
        where
            T: GetTableName + GetFields + FromValue,

    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter().filter(|f| f.exist)
            .map(|c| format!("`{}`", c.alias.to_owned().unwrap_or(c.name.to_string())))
            .collect::<Vec<_>>()
            .join(", ");
        let select_fields = wrapper.get_select_sql();
        let enumerated_columns = if select_fields.eq("*") {
            enumerated_columns
        } else {
            select_fields
        };
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let mut sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &table.complete_name(), where_condition);
        let count_sql = format!("select count(*) from ({}) TOTAL", &sql);
        let count: i64 = self.exec_first(&count_sql, ())?;
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &table.complete_name(), where_condition,page.offset(),  page.size);
            let rows = self.execute_result(&sql, Params::Nil)?;
            let mut entities = vec![];
            for dao in rows.iter() {
                let entity = T::from_value(&dao);
                entities.push(entity)
            }
            page.records = entities;
        }
        Ok(page)
    }

    /// Get the total count of records
    pub fn count<T>(&mut self, mut wrapper:Wrapper) -> Result<usize, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!(
            "SELECT COUNT(1) AS count FROM {} {}",
            table.complete_name(),
            where_condition
        );
        self.exec_first(&sql, ())
    }

    /// Remove the records by wrapper.
    pub fn remove<T>(&mut self, mut wrapper:Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("delete from {} {}", &table.complete_name(), where_condition);
        let _rows = self.execute_result(&sql, Params::Nil)?;
        Ok(self.affected_rows())
    }

    /// Remove the records by id.
    pub fn remove_by_id<T, I>(&mut self, id: I) -> Result<u64, AkitaError>
        where
            I: ToValue,
            T: GetTableName + GetFields {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let cols = T::fields();
        let col_len = cols.len();
        if let Some(field) = cols.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match self {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` = ?", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let _rows = self.execute_result(&sql, (id.to_value(),).into())?;
            Ok(self.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }


    /// Remove the records by wrapper.
    pub fn remove_by_ids<T, I>(&mut self, ids: Vec<I>) -> Result<u64, AkitaError>
        where
            I: ToValue,
            T: GetTableName + GetFields {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let cols = T::fields();
        let col_len = cols.len();
        if let Some(field) = cols.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match self {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` in (?)", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` in (${})", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let ids = ids.iter().map(|v| v.to_value().to_string()).collect::<Vec<String>>().join(",");
            let _rows = self.execute_result(&sql, (ids,).into())?;
            Ok(self.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }


    /// Update the records by wrapper.
    pub fn update<T>(&mut self, entity: &T, mut wrapper: Wrapper) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + ToValue {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let mut sql = build_update_clause(&self, entity, &mut wrapper);
        let update_fields = wrapper.fields_set.to_owned();
        let is_set = wrapper.get_set_sql().is_none();
        if update_fields.is_empty() && !is_set {
            sql = wrapper.table(&table.complete_name()).get_update_sql().unwrap_or_default();
        }
        let _bvalues: Vec<&Value> = Vec::new();
        if update_fields.is_empty() && is_set {
            let data = entity.to_value();
            let mut values: Vec<Value> = Vec::with_capacity(columns.len());
            for col in columns.iter() {
                if !col.exist || col.field_type.ne(&FieldType::TableField) {
                    continue;
                }
                let col_name = &col.name.to_string();
                let mut value = data.get_obj_value(&col_name);
                match &col.fill {
                    None => {}
                    Some(v) => {
                        match v.mode.as_ref() {
                            "update" | "default" => {
                                value = v.value.as_ref();
                            }
                            _=> {}
                        }
                    }
                }
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }

            let _rows = self.execute_result(&sql, values.into())?;
        } else {
            let _rows = self.execute_result(&sql, Params::Nil)?;
        }
        Ok(self.affected_rows())
    }

    /// Update the records by id.
    pub fn update_by_id<T>(&mut self, entity: &T) -> Result<u64, AkitaError>
        where
            T: GetTableName + GetFields + ToValue {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let data = entity.to_value();
        let columns = T::fields();
        let col_len = columns.len();
        if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let set_fields = columns
                .iter().filter(|col| col.exist && col.field_type == FieldType::TableField)
                .enumerate()
                .map(|(x, col)| {
                    #[allow(unreachable_patterns)]
                    match self {
                        #[cfg(feature = "akita-mysql")]
                        DatabasePlatform::Mysql(_) => format!("`{}` = ?", &col.alias.to_owned().unwrap_or(col.name.to_string())),
                        #[cfg(feature = "akita-sqlite")]
                        DatabasePlatform::Sqlite(_) => format!("`{}` = ${}",&col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                        _ => format!("`{}` = ${}", &col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = match self {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("update {} set {} where `{}` = ?", &table.name, &set_fields, &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("update {} set {} where `{}` = ${}", &table.name, &set_fields, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("update {} set {} where `{}` = ${}", &table.name, &set_fields, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let mut values: Vec<Value> = Vec::with_capacity(columns.len());
            let id = data.get_obj_value(&field.name.to_string());
            for col in columns.iter() {
                if !col.exist || col.field_type.ne(&FieldType::TableField) {
                    continue;
                }
                let col_name = &col.name.to_string();
                let mut value = data.get_obj_value(col_name);
                match &col.fill {
                    None => {}
                    Some(v) => {
                        match v.mode.as_ref() {
                            "update" | "default" => {
                                value = v.value.as_ref();
                            }
                            _=> {}
                        }
                    }
                }
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }
            match id {
                Some(id) => values.push(id.clone()),
                None => {
                    return Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident value...", &table.name)));
                }
            }
            let _ = self.execute_result(&sql, values.into())?;
            Ok(self.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }

    }

    #[allow(unused_variables)]
    pub fn save_batch<T>(&mut self, entities: &[&T]) -> Result<(), AkitaError>
        where
            T: GetTableName + GetFields + ToValue
    {
        let columns = T::fields();
        let sql = build_insert_clause(&self, entities);

        let mut values: Vec<Value> = Vec::with_capacity(entities.len() * columns.len());
        for entity in entities.iter() {
            for col in columns.iter().filter(|col| col.exist ) {
                let data = entity.to_value();
                let mut value = data.get_obj_value(&col.name.to_string());
                match &col.fill {
                    None => {}
                    Some(v) => {
                        match v.mode.as_ref() {
                            "insert" | "default" => {
                                value = v.value.as_ref();
                            }
                            _ => {}
                        }
                    }
                }
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        self.execute_result(&sql,values.into())?;
        Ok(())
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue,
    {
        let columns = T::fields();
        let sql = build_insert_clause(&self, &[entity]);
        let data = entity.to_value();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter().filter(|col| col.exist ) {
            let mut value = data.get_obj_value(&col.name.to_string());
            match &col.fill {
                None => {}
                Some(v) => {
                    match v.mode.as_ref() {
                        "insert" | "default" => {
                            value = v.value.as_ref();
                        }
                        _=> {}
                    }
                }
            }
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let _bvalues: Vec<&Value> = values.iter().collect();

        self.execute_result(&sql,values.into())?;
        let last_insert_id = match self {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(_) => {
                // conn.execute_result("SELECT LAST_INSERT_ID();", Params::Nil)?
                Some(I::from_value(&Value::Bigint(self.last_insert_id() as i64)))
            },
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(_) => {
                let rows = self.execute_result("SELECT LAST_INSERT_ROWID();", Params::Nil)?;
                rows.iter().next().map(|data| I::from_value(&data))
            },
            _ => return Err(AkitaError::UnknownDatabase("database must be init.".to_string()))
        };

        // let last_insert_id = _rows.iter().next().map(|data| I::from_value(&data));
        Ok(last_insert_id)
    }

    /// save or update
    pub fn save_or_update<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue {
        let data = entity.to_value();
        let id = if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            data.get_obj_value(&field.name.to_string()).unwrap_or(&Value::Nil)
        } else { &Value::Nil };
        match id {
            Value::Nil => {
                self.save(entity)
            },
            _ => {
                self.update_by_id(entity)?;
                Ok(I::from_value(id).into())
            }
        }
    }

    pub fn exec_iter<S: Into<String>, P: Into<Params>>(&mut self, sql: S, params: P) -> Result<Rows, AkitaError> {
        let rows = self.execute_result(&sql.into(), params.into())?;
        Ok(rows)
    }

    pub fn query<T, Q>(&mut self, query: Q) -> Result<Vec<T>, AkitaError>
        where
            Q: Into<String>,
            T: FromValue,
    {
        self.query_map(query, from_value)
    }

    pub fn query_opt<T, Q>(&mut self, query: Q) -> Result<Vec<Result<T, AkitaDataError>>, AkitaError>
        where
            Q: Into<String>,
            T: FromValue,
    {
        self.query_map(query, from_value_opt)
    }

    pub fn query_first<S: Into<String>, R>(
        &mut self, sql: S
    ) -> Result<R, AkitaError>
        where
            R: FromValue,
    {
        self.exec_first(sql, ())
    }

    fn query_first_opt<R, S: Into<String>>(
        &mut self, sql: S,
    ) -> Result<Option<R>, AkitaError>
        where
            R: FromValue,
    {
        self.exec_first_opt(sql, ())
    }


    pub fn query_map<T, F, Q, U>(&mut self, query: Q, mut f: F) -> Result<Vec<U>, AkitaError>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    pub fn query_fold<T, F, Q, U>(&mut self, query: Q, init: U, mut f: F) -> Result<U, AkitaError>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(U, T) -> U,
    {
        self.exec_iter::<_, _>(query, ()).map(|r| r.iter().map(|data| T::from_value(&data))
            .fold(init, |acc, row| f(acc, row)))
    }


    pub fn query_drop<Q>(&mut self, query: Q) -> Result<(), AkitaError>
        where
            Q: Into<String>,
    {
        self.query_iter(query).map(drop)
    }

    pub fn exec_map<T, F, Q, U>(&mut self, query: Q, mut f: F) -> Result<Vec<U>, AkitaError>
        where
            Q: Into<String>,
            T: FromValue,
            F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    pub fn query_iter<S: Into<String>>(
        &mut self,
        sql: S,
    ) -> Result<Rows, AkitaError>
    {
        self.exec_iter(sql, ())
    }

    #[allow(clippy::redundant_closure)]
    pub fn exec_raw<R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
        where
            R: FromValue,
    {
        let rows = self.exec_iter(&sql.into(), params.into())?;
        Ok(rows.iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    pub fn exec_first<R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<R, AkitaError>
        where
            R: FromValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.exec_raw(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn exec_drop<S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>
    {
        let sql: String = sql.into();
        let _result: Result<Vec<()>, AkitaError> = self.exec_raw(&sql, params);
        Ok(())
    }

    pub fn exec_first_opt<R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<R>, AkitaError>
        where
            R: FromValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.exec_raw(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Ok(None),
                1 => Ok(Some(result.remove(0))),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
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
