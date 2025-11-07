/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */

use std::cell::RefCell;
use std::rc::Rc;
use akita_core::IdentifierType;
use crate::{AkitaError, IPage, Wrapper, database::{DatabasePlatform}, mapper::AkitaMapper, GetFields, GetTableName, FromValue, ToValue, Rows, TableName, DatabaseName, FieldName, Params, Value, FieldType, TableInfo, segment::ISegment, IdentifierGenerator};
use crate::akita::GLOBAL_GENERATOR;
use crate::pool::PlatformPool;
use crate::errors::Result;

/// an interface executing sql statement and getting the results as generic Akita values
/// without any further conversion.

#[allow(unused)]
pub struct AkitaEntityManager(PlatformPool);

pub struct AkitaTransaction {
    pub(crate) conn: Rc<RefCell<DatabasePlatform>>,
    pub(crate) committed: bool,
    pub(crate) rolled_back: bool,
}

#[allow(unused)]
impl AkitaTransaction {
    pub fn commit(mut self) -> Result<()> {
        let mut conn = self.conn.borrow_mut();
        conn.commit_transaction()?;
        self.committed = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        let mut conn = self.conn.borrow_mut();
        conn.rollback_transaction()?;
        self.rolled_back = true;
        Ok(())
    }
}

impl<'a> Drop for AkitaTransaction {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            let mut conn = self.conn.borrow_mut();
            conn.rollback_transaction().unwrap_or_default();
        }
    }
}

#[allow(unused)]
impl AkitaMapper for AkitaTransaction {

    /// Get all the table of records
    fn list<T>(&self, wrapper:Wrapper) -> Result<Vec<T>>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        let mut conn = self.conn.borrow_mut();
        conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&self, wrapper:Wrapper) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        let mut conn = self.conn.borrow_mut();
        conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
    where
        T: GetTableName + GetFields + FromValue,
        I: ToValue
    {
        let mut conn = self.conn.borrow_mut();
        conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T>(&self, page: usize, size: usize, wrapper:Wrapper) -> Result<IPage<T>>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        let mut conn = self.conn.borrow_mut();
        conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&self, wrapper:Wrapper) -> Result<usize>
    where
        T: GetTableName + GetFields,
         {
        let mut conn = self.conn.borrow_mut();
        conn.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T>(&self, wrapper:Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields,
         {
            let mut conn = self.conn.borrow_mut();
        conn.remove::<T>(wrapper)
    }

    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64> where I: ToValue, T: GetTableName + GetFields {
        let mut conn = self.conn.borrow_mut();
        conn.remove_by_ids::<T,I>(ids)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
    where
        I: ToValue,
        T: GetTableName + GetFields {
            let mut conn = self.conn.borrow_mut();
        conn.remove_by_id::<T, I>(id)
        
    }

    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue {
            let mut conn = self.conn.borrow_mut();
        conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue {
        let mut conn = self.conn.borrow_mut();
        conn.update_by_id(entity)
        
    }

    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.conn.borrow_mut();
        conn.update_batch_by_id(entities)
    }

    #[allow(unused_variables)]
    fn save_batch<T>(&self, entities: &Vec<T>) -> Result<()>
    where
        T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.conn.borrow_mut();
        conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue,
    {
        let mut conn = self.conn.borrow_mut();
        conn.save(entity)
    }

    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>> where T: GetTableName + GetFields + ToValue, I: FromValue {
        let mut conn = self.conn.borrow_mut();
        conn.save_or_update(entity)
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> Result<Rows> {
        let mut conn = self.conn.borrow_mut();
        conn.exec_iter(sql, params)
    }
}


#[allow(unused)]
impl AkitaEntityManager{

    pub fn new(db: PlatformPool) -> Self {
        AkitaEntityManager(db)
    }

    pub fn acquire(&self) -> Result<DatabasePlatform> {
       self.0.database()
    }

    pub fn set_session_user(&mut self, username: &str) -> Result<()> {
        let sql = format!("SET SESSION ROLE '{}'", username);
        let mut conn = self.acquire()?;
        conn.execute_result(&sql, Params::Null)?;
        Ok(())
    }

    /// get the table from database based on this column name
    pub fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableInfo>> {
        let mut conn = self.acquire()?;
        conn.get_table(table_name)
    }

    /// set the autoincrement value of the primary column(if present) of this table.
    /// If the primary column of this table is not an autoincrement, returns Ok(None).
    pub fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>> {
        let mut conn = self.acquire()?;
        conn.set_autoincrement_value(table_name, sequence_value)
    }

    pub fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>> {
        let mut conn = self.acquire()?;
        conn.get_autoincrement_last_value(table_name)
    }

    pub fn last_insert_id(&self) -> u64 {
        match self.acquire() {
            Ok(conn) => {
                conn.last_insert_id()
            }
            Err(_) => {
                0
            }
        }

    }

    pub fn get_database_name(&mut self) -> Result<Option<DatabaseName>> {
        let mut conn = self.acquire()?;
        conn.get_database_name()
    }

    fn save_batch_inner<T>(&self, entities: &Vec<T>) -> Result<()>
    where
        T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.acquire()?;
        let columns = T::fields();
        let sql = build_insert_clause(&conn, &entities.iter().collect::<Vec<_>>());

        let mut values: Vec<Value> = Vec::with_capacity(entities.len() * columns.len());
        for entity in entities.iter() {
            let data = entity.to_value();
            for col in columns.iter() {
                let mut value = data.get_obj_value(&col.alias.to_owned().unwrap_or(col.name.to_string())).map(Clone::clone).unwrap_or(Value::Null);
                match &col.fill {
                    None => {}
                    Some(v) => {
                        match v.mode.as_ref() {
                            "insert" | "default" => {
                                value = v.value.clone().unwrap_or_default();
                            }
                            _=> {}
                        }
                    }
                }
                let (_, v) = identifier_generator_value(col,value);
                values.push(v)
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        conn.execute_result(&sql,values.into())?;
        Ok(())
    }

    /// build the returning clause
    fn build_returning_clause(&self, return_columns: Vec<FieldName>) -> String {
        format!(
            "\nRETURNING \n{}",
            return_columns
                .iter()
                .map(|rc| rc.name.to_owned())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

}

/// build an insert clause
pub fn build_insert_clause<T>(platform: &DatabasePlatform, entities: &[&T]) -> String
    where
        T: GetTableName + GetFields + ToValue,
{
    let table = T::table_name();
    let columns = T::fields();
    let columns_len = columns.len();
    let mut sql = String::new();
    sql += &format!("INSERT INTO {} ", table.complete_name());
    sql += &format!(
        "({})\n",
        columns
            .iter().filter(|f| f.exist)
            .map(|c| format!("`{}`", c.alias.to_owned().unwrap_or(c.name.to_string())))
            .collect::<Vec<_>>()
            .join(", ")
    );
    sql += "VALUES ";
    sql += &entities
        .iter()
        .enumerate()
        .map(|(y, _)| {
            format!(
                "\n\t({})",
                columns
                    .iter().filter(|f| f.exist)
                    .enumerate()
                    .map(|(x, _)| {
                        #[allow(unreachable_patterns)]
                        match platform {
                            #[cfg(feature = "akita-sqlite")]
                            DatabasePlatform::Sqlite(_) => format!("${}", y * columns_len + x + 1),
                            #[cfg(feature = "akita-mysql")]
                            DatabasePlatform::Mysql(_) => "?".to_string(),
                            _ => format!("${}", y * columns_len + x + 1),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql
}

/// 处理自动生成值
pub fn identifier_generator_value(field_name: &FieldName, mut value: Value) -> (bool, Value) {
    let mut is_auto = false;
    if let Some(id_type) = field_name.get_table_id_type() {
        if let Some(id) = match id_type {
            IdentifierType::Auto => {
                is_auto = true;
                None
            },
            IdentifierType::Input => None,
            IdentifierType::None => None,
            IdentifierType::AssignId => {
                let id = GLOBAL_GENERATOR.next_id();
                Some(id.to_string())
            }
            IdentifierType::AssignUuid => {
                let id = GLOBAL_GENERATOR.next_uuid();
                Some(id)
            },
        } {
            // 判断当前主键value值类型，以原类型返回,目前仅支持字符串和数字
            match value {
                Value::Text(_v) => {
                    value = Value::from(id);
                }
                Value::Bigint(_v) => {
                    value = Value::from(id.parse::<i64>().unwrap_or_default());
                }
                Value::Int(_vv) => {
                    value = Value::from(id.parse::<i32>().unwrap_or_default());
                }
                _ => {

                }
            }
        }
    }

    (is_auto, value)
}

/// build an update clause
pub fn build_update_clause<T>(platform: &DatabasePlatform, _entity: &T, wrapper: &mut Wrapper) -> String
    where
        T: GetTableName + GetFields + ToValue
{
    let table = T::table_name();
    let columns = T::fields();
    let set_fields = &mut wrapper.fields_set;
    let mut sql = String::new();
    sql += &format!("update {} ", table.complete_name());
    if set_fields.is_empty() {
        sql += &format!(
            "set {}",
            columns.iter().filter(|col| col.exist && col.field_type == FieldType::TableField).collect::<Vec<_>>()
                .iter()
                .enumerate()
                .map(|(x, col)| {
                    #[allow(unreachable_patterns)]
                    match platform {
                        #[cfg(feature = "akita-mysql")]
                        DatabasePlatform::Mysql(_) => format!("`{}` = ?", &col.alias.to_owned().unwrap_or(col.name.to_string())),
                        #[cfg(feature = "akita-sqlite")]
                        DatabasePlatform::Sqlite(_) => format!("`{}` = ${}", &col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                        _ => format!("`{}` = ${}", &col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    } else {
        sql += &format!(
            "set {}",
            set_fields
                .iter_mut()
                .enumerate()
                .map(|(x, (col, _value))| {
                    #[allow(unreachable_patterns)]
                    match platform {
                        #[cfg(feature = "akita-mysql")]
                        DatabasePlatform::Mysql(_) => format!("`{}` = {}", col, _value.get_sql_segment()),
                        #[cfg(feature = "akita-sqlite")]
                        DatabasePlatform::Sqlite(_) => format!("`{}` = ${}", col, x + 1),
                        _ => format!("`{}` = ${}", col, x + 1),
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let where_condition = wrapper.get_sql_segment();
    if !where_condition.is_empty() {
        sql += &format!(" where {} ", where_condition);
    }

    sql
}


#[allow(unused)]
impl AkitaMapper for AkitaEntityManager {

    /// Get all the table of records
    fn list<T>(&self, mut wrapper:Wrapper) -> Result<Vec<T>>
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
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &table.complete_name(),where_condition);
        let mut conn = self.acquire()?;
        let rows = conn.execute_result(&sql, Params::Null)?;
        let mut entities = vec![];
        for data in rows.iter() {
            let entity = T::from_value(&data);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    fn select_one<T>(&self, mut wrapper:Wrapper) -> Result<Option<T>>
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
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &table.complete_name(), where_condition);
        let mut conn = self.acquire()?;
        let rows = conn.execute_result(&sql, Params::Null)?;
        match rows.len() {
            0 => Err(AkitaError::DataError("Zero record returned".to_string())),
            1 => Ok(
                rows.iter().next().map(|data| T::from_value(&data))
            ),
            _ => Err(AkitaError::DataError("More than one record returned".to_string())),
        }
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&self, id: I) -> Result<Option<T>>
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
        let mut conn = self.acquire()?;
        if let Some(field) = columns.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match conn {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("SELECT {} FROM {} WHERE `{}` = ? limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let rows = conn.execute_result(&sql, (id.to_value(),).into())?;
            Ok(rows.iter().next().map(|data| T::from_value(&data)))
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }

    /// Get table of records with page
    fn page<T>(&self, page: usize, size: usize, mut wrapper:Wrapper) -> Result<IPage<T>>
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
        let count_sql = format!("select count(1) as count from {} {}", &table.complete_name(), where_condition);
        let count: i64 = self.exec_first(&count_sql, ())?;
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &table.complete_name(), where_condition,page.offset(),  page.size);
            let mut conn = self.acquire()?;
            let rows = conn.execute_result(&sql, Params::Null)?;
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
    fn count<T>(&self, mut wrapper:Wrapper) -> Result<usize>
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
    fn remove<T>(&self, mut wrapper:Wrapper) -> Result<u64>
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
        let mut conn = self.acquire()?;
        let _ = conn.execute_result(&sql, Params::Null)?;
        Ok(conn.affected_rows())
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&self, id: I) -> Result<u64>
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
            let mut conn = self.acquire()?;
            let sql = match conn {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` = ?", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let _ = conn.execute_result(&sql, (id.to_value(),).into())?;
            Ok(conn.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }


    /// Remove the records by wrapper.
    fn remove_by_ids<T, I>(&self, ids: Vec<I>) -> Result<u64>
        where
            I: ToValue,
            T: GetTableName + GetFields {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let mut conn = self.acquire()?;
        let cols = T::fields();
        let col_len = cols.len();
        if let Some(field) = cols.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match conn {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` in (?)", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string())),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` in (${})", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.alias.to_owned().unwrap_or(field.name.to_string()), col_len + 1),
            };
            let ids = ids.iter().map(|v| v.to_value().to_string()).collect::<Vec<String>>().join(",");
            let _ = conn.execute_result(&sql, (ids,).into())?;
            Ok(conn.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }


    /// Update the records by wrapper.
    fn update<T>(&self, entity: &T, mut wrapper: Wrapper) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let mut conn = self.acquire()?;
        let columns = T::fields();
        let sql = build_update_clause(&conn, entity, &mut wrapper);
        let update_fields = wrapper.fields_set;
        let mut bvalues: Vec<&Value> = Vec::new();
        if update_fields.is_empty() {
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
                    None => values.push(Value::Null),
                }
            }
            conn.execute_result(&sql, values.into())?;
        } else {
            conn.execute_result(&sql, Params::Null)?;
        }
        Ok(conn.affected_rows())
    }


    fn update_batch_by_id<T>(&self, entities: &Vec<T>) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.acquire()?;
        if entities.is_empty() {
            return Ok(0);
        }

        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()));
        }

        let columns = T::fields();
        let id_field = columns
            .iter()
            .find(|f| matches!(f.field_type, FieldType::TableId(_)))
            .ok_or_else(|| AkitaError::MissingIdent("Missing primary key field".to_string()))?;

        let mut sql = format!("UPDATE {} SET ", table.name);

        // 构建每个字段的 CASE WHEN 子句
        let mut sets = Vec::new();
        for col in columns.iter().filter(|c| matches!(c.field_type, FieldType::TableField) && c.exist) {
            let col_name = col.alias.as_ref().unwrap_or(&col.name);
            let mut case_stmt = format!("`{}` = CASE", col_name);

            for entity in entities.iter() {
                let data = entity.to_value();
                let id_value = data
                    .get_obj_value(id_field.alias.as_ref().unwrap_or(&id_field.name))
                    .ok_or_else(|| AkitaError::MissingIdent("Missing id value".to_string()))?;

                let mut field_value = data.get_obj_value(col_name);
                if let Some(fill) = &col.fill {
                    match fill.mode.as_str() {
                        "update" | "default" => field_value = fill.value.as_ref(),
                        _ => {}
                    }
                }

                let value_sql = match field_value {
                    Some(Value::Text(s)) => format!("'{}'", s.replace("'", "''")),
                    Some(Value::Null) => "NULL".to_string(),
                    Some(v) => v.to_string(),
                    None => "NULL".to_string(),
                };

                case_stmt.push_str(&format!(" WHEN `{}` = {} THEN {}", id_field.name, id_value, value_sql));
            }

            case_stmt.push_str(&format!(" ELSE `{}` END", col_name));
            sets.push(case_stmt);
        }

        sql.push_str(&sets.join(", "));

        // WHERE id IN (...)
        let ids: Vec<String> = entities
            .iter()
            .map(|e| {
                let data = e.to_value();
                let id_val = data
                    .get_obj_value(id_field.alias.as_ref().unwrap_or(&id_field.name))
                    .unwrap();
                id_val.to_string()
            })
            .collect();

        sql.push_str(&format!(" WHERE `{}` IN ({})", id_field.name, ids.join(",")));

        // 执行 SQL
        conn.execute_result(&sql, ().into())?;
        Ok(conn.affected_rows())
    }

    /// Update the records by id.
    fn update_by_id<T>(&self, entity: &T) -> Result<u64>
    where
        T: GetTableName + GetFields + ToValue {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let mut conn = self.acquire()?;
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
                match conn {
                    #[cfg(feature = "akita-mysql")]
                    DatabasePlatform::Mysql(_) => format!("`{}` = ?", &col.alias.to_owned().unwrap_or(col.name.to_string())),
                    #[cfg(feature = "akita-sqlite")]
                    DatabasePlatform::Sqlite(_) => format!("`{}` = ${}",&col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                    _ => format!("`{}` = ${}", &col.alias.to_owned().unwrap_or(col.name.to_string()), x + 1),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
            let sql = match conn {
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
                    None => values.push(Value::Null),
                }
            }
            match id {
                Some(id) => values.push(id.clone()),
                None => {
                    return Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident value...", &table.name)));
                }
            }
            let _ = conn.execute_result(&sql, values.into())?;
            Ok(conn.affected_rows())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }

    }

    #[allow(unused_variables)]
    fn save_batch<T>(&self, entities: &Vec<T>) -> Result<()>
    where
        T: GetTableName + GetFields + ToValue
    {
        let mut conn = self.acquire()?;
        match conn {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(_) => self.save_batch_inner(entities),
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(_) => self.save_batch_inner(entities),
        }
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&self, entity: &T) -> Result<Option<I>>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue,
    {
        let columns = T::fields();
        let mut conn = self.acquire()?;
        let sql = build_insert_clause(&conn, &[entity]);
        let data = entity.to_value();

        let mut table_id_data = Value::Null;
        let mut table_is_auto = false;

        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let mut value = data.get_obj_value(&col.alias.to_owned().unwrap_or(col.name.to_string())).map(Clone::clone).unwrap_or(Value::Null);
            match &col.fill {
                None => {}
                Some(v) => {
                    match v.mode.as_ref() {
                        "insert" | "default" => {
                            value = v.value.clone().unwrap_or_default();
                        }
                        _=> {}
                    }
                }
            }
            let (is_auto ,v) = identifier_generator_value(col,value);
            if col.is_table_id() {
                table_is_auto = is_auto;
                table_id_data = v.clone();
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        conn.execute_result(&sql,values.into())?;

        let last_insert_id = if table_is_auto {
            I::from_value_opt(&Value::from(self.last_insert_id())).ok()
        } else {
            I::from_value_opt(&table_id_data).ok()
        };
        Ok(last_insert_id)
    }

    /// save or update
    fn save_or_update<T, I>(&self, entity: &T) -> Result<Option<I>>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue {
        let data = entity.to_value();
        let id = if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            data.get_obj_value(&field.name.to_string()).unwrap_or(&Value::Null)
        } else { &Value::Null };
        match id {
            Value::Null => {
                self.save(entity)
            },
            _ => {
                self.update_by_id(entity)?;
                Ok(I::from_value(id).into())
            }
        }
    }

    fn exec_iter<S: Into<String>, P: Into<Params>>(&self, sql: S, params: P) -> Result<Rows> {
        let mut conn = self.acquire()?;
        let rows = conn.execute_result(&sql.into(), params.into())?;
        Ok(rows)
    }
}


#[cfg(test)]
#[allow(unused)]
mod test {
    use akita_core::params;
    // use crate as akita;

    use crate::{self as akita, AkitaConfig, AkitaMapper, BaseMapper, Pool, Wrapper, FromValue, ToValue, Entity};

    fn fffff() {

    }

    #[derive(Debug,Entity, Clone)]
    #[table(name="t_system_user")]
    struct SystemUser {
        id: Option<i32>,
        #[id(name="ffff", id_type="none")]
        username: String,
        #[field(name = "ssss", fill( function = "fffff", mode = "default"))]
        age: i32,
    }

    #[test]
    fn get_table_info() {
        let s = params! { "test" => 1, "id" => 3, "id"=> 4};
        let mut sql = "select * from user where id = :id and test = :test and id = :id".to_string();
        let len = sql.len();
        let mut values = s.iter().map(|param| {
            let key = format!(":{}", param.0);
            let index = sql.find(&key).unwrap_or(len);
            sql = sql.replace(&key, "?");
            (index, &param.1)
        }).collect::<Vec<_>>();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        let params = values.iter().map(|v| v.1).collect::<Vec<_>>();
        let params = params.as_slice();
        // let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        // let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        // let mut em = pool.entity_manager().expect("must be ok");
        // let table = em
        //     .get_table(&TableName::from("public.film"))
        //     .expect("must have a table");
        // println!("table: {:#?}", table);
        // let s = serde_json::to_value("[123,3455,556]").unwrap();
        println!("sql:{}, {:?}", sql, params)
    }

    #[test]
    fn remove() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let em = &mut pool.entity_manager().expect("must be ok");
        let mut wrap = Wrapper::new().eq("username", "'ussd'");
        match em.remove::<SystemUser>(wrap) {
            Ok(_res) => {
                println!("success removed data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn count() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrap = Wrapper::new().eq("username", "'ussd'");
        match em.count::<SystemUser>(wrap) {
            Ok(_res) => {
                println!("success count data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn remove_by_id() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        match em.remove_by_id::<SystemUser, String>("'fffsd'".to_string()) {
            Ok(_res) => {
                println!("success removed data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn update() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        let mut wrap = Wrapper::new().eq("username", "'ussd'");
        match em.update(&user, wrap) {
            Ok(_res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn update_by_id() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.update_by_id(&user) {
            Ok(_res) => {
                println!("success update data by id!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn save() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save::<_, i32>(&user) {
            Ok(_res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn save_batch() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save_batch::<_>(&vec![user]) {
            Ok(_res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_insert() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match user.insert::<i32, _>(&mut em) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn select_by_id() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new();
        wrapper.eq("username", "'ussd'");
        match em.select_by_id::<SystemUser, i32>(1) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn select_one() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new().eq("username", "'ussd'");
        match em.select_one::<SystemUser>(wrapper) {
            Ok(_res) => {
                println!("success select data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn list() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new().eq("username", "'ussd'");
        match em.list::<SystemUser>(wrapper) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_list() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new().eq("username", "'ussd'");
        match SystemUser::list(wrapper, &mut em) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn page() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new().eq( "username", "'ussd'");
        match em.page::<SystemUser>(1, 10,wrapper) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_page() {
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let wrapper = Wrapper::new().eq("username", "'ussd'");
        match SystemUser::page(1, 10, wrapper, &mut em) {
            Ok(_res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }
}


