use crate::{self as akita, AkitaError, IPage, UpdateWrapper, Wrapper, database::{Database, DatabasePlatform}, information::{DatabaseName, FieldName, FieldType, GetFields, GetTableName, TableDef, TableName}, mapper::AkitaMapper, pool::AkitaConfig, value::{ToValue, Value}};
use crate::data::{FromAkita, Rows, AkitaData, ToAkita};
/// an interface executing sql statement and getting the results as generic Akita values
/// without any further conversion.
#[allow(unused)]
pub struct AkitaManager(pub DatabasePlatform, pub AkitaConfig);

#[allow(unused)]
pub struct AkitaEntityManager(pub DatabasePlatform, pub AkitaConfig);

pub struct AkitaTransaction<'a> {
    pub(crate) conn: &'a mut AkitaEntityManager,
    committed: bool,
    rolled_back: bool,
}

#[allow(unused)]
impl AkitaTransaction <'_> {
    pub fn commit(mut self) -> Result<(), AkitaError> {
        self.conn.0.commit_transaction()?;
        self.committed = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), AkitaError> {
        self.conn.0.rollback_transaction()?;
        self.rolled_back = true;
        Ok(())
    }

}

impl<'a> Drop for AkitaTransaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            let _ = self.conn.0.rollback_transaction();
        }
    }
}

#[allow(unused)]
impl AkitaMapper for AkitaTransaction <'_> {

    /// Get all the table of records
    fn list<T, W>(&mut self, wrapper: &mut W) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        self.conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T, W>(&mut self, wrapper: &mut W) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        self.conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        I: ToValue
    {
        self.conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T, W>(&mut self, page: usize, size: usize, wrapper: &mut W) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        self.conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T, W>(&mut self, wrapper: &mut W) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper {
        self.conn.count::<T, W>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T, W>(&mut self, wrapper: &mut W) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper {
            self.conn.remove::<T,W>(wrapper)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&mut self, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields {
            self.conn.remove_by_id::<T, I>(id)
        
    }
    

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, wrapper: &mut UpdateWrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToAkita {
            self.conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T, I>(&mut self, entity: &T, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields + ToAkita {
            self.conn.update_by_id(entity, id)
        
    }

    #[allow(unused_variables)]
    fn save_batch<T>(&mut self, entities: &[&T]) -> Result<Vec<usize>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        self.conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T>(&mut self, entity: &T) -> Result<usize, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        self.conn.save(entity)
    }

    /// this is soly for use with sqlite since sqlite doesn't support bulk insert
    fn save_batch_result<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
        R: FromAkita + GetFields,
    {
        self.conn.save_batch_result(entities)
    }

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R>(
        &mut self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromAkita,
    {
        self.conn.execute_result(sql, params)
    }

    fn execute_first<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<R, AkitaError>
    where
        R: FromAkita,
    {
        self.conn.execute_first(sql, params)
    }

    fn execute_result_opt<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita,
    {
        self.conn.execute_result_opt(sql, params)
    }
}


#[allow(unused)]
impl AkitaManager {
    pub fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.start_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.rollback_transaction()
    }

    pub fn execute_result(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Rows, AkitaError> {
        let rows = self.0.execute_result(sql, params)?;
        Ok(rows)
    }

    pub fn execute_iter(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Vec<AkitaData>, AkitaError> {
        let rows = self.0.execute_result(sql, params)?;
        let datas: Vec<AkitaData> = rows.iter().collect();
        Ok(datas)
    }

    pub fn execute_first(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<AkitaData, AkitaError> {
        let record: Result<Option<AkitaData>, AkitaError> =
            self.execute_first_opt(sql, params);
        match record {
            Ok(record) => match record {
                Some(record) => Ok(record),
                None => Err(AkitaError::DataError("Zero record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn execute_first_opt(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Option<AkitaData>, AkitaError> {
        let result: Result<Vec<AkitaData>, AkitaError> = self.execute_iter(sql, params);
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


#[allow(unused)]
impl AkitaEntityManager{
    pub fn start_transaction(&mut self) -> Result<AkitaTransaction, AkitaError> {
        self.0.start_transaction()?;
        Ok(AkitaTransaction {
            conn: self,
            committed: false,
            rolled_back: false,
        })
    }

    pub fn set_session_user(&mut self, username: &str) -> Result<(), AkitaError> {
        let sql = format!("SET SESSION ROLE '{}'", username);
        self.0.execute_result(&sql, &[])?;
        Ok(())
    }

    pub fn database(&mut self) -> &mut dyn Database {
        &mut *self.0
    }
    
    /// get the table from database based on this column name
    pub fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, AkitaError> {
        self.0.get_table(table_name)
    }

    /// set the autoincrement value of the primary column(if present) of this table.
    /// If the primary column of this table is not an autoincrement, returns Ok(None).
    pub fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>, AkitaError> {
        self.0.set_autoincrement_value(table_name, sequence_value)
    }

    pub fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>, AkitaError> {
        self.0.get_autoincrement_last_value(table_name)
    }

    pub fn get_database_name(&mut self) -> Result<Option<DatabaseName>, AkitaError> {
        self.0.get_database_name()
    }

    fn save_batch_inner<T>(&mut self, entities: &[&T]) -> Result<Vec<usize>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let mut result = Vec::new();
        for entity in entities {
            result.push(self.save(*entity)?);
        }
        Ok(result)
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

    /// build an insert clause
    fn build_insert_clause<T>(&self, entities: &[&T]) -> String
    where
        T: GetTableName + GetFields + ToAkita,
    {
        let table = T::table_name();
        let columns = T::fields();
        let columns_len = columns.len();
        let mut sql = String::new();
        sql += &format!("INSERT INTO {} ", table.complete_name());
        sql += &format!(
            "({})\n",
            columns
                .iter()
                .map(|c| c.name.to_owned())
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
                        .iter()
                        .enumerate()
                        .map(|(x, _)| {
                            #[allow(unreachable_patterns)]
                            match self.0 {
                                // #[cfg(feature = "with-mysql")]
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

    /// build an update clause
    fn build_update_clause<T>(&self, entity: &T, wrapper: &mut UpdateWrapper) -> String
    where
        T: GetTableName + GetFields + ToAkita 
    {
        let table = T::table_name();
        let columns = T::fields();
        let set_fields = &wrapper.sql_set;
        let mut sql = String::new();
        sql += &format!("update {} ", table.complete_name());
        sql += &format!(
            "set {}",
            columns
                .iter().filter(|col| col.exist || (!set_fields.is_empty() && set_fields.contains(&col.name)))
                .enumerate()
                .map(|(x, col)| {
                    #[allow(unreachable_patterns)]
                    match self.0 {
                        // #[cfg(feature = "with-mysql")]
                        DatabasePlatform::Mysql(_) => format!("{} = ?", &col.name),
                        _ => format!("${}", x + 1),
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        let where_condition = wrapper.get_sql_segment();
        if !where_condition.is_empty() {
            sql += &format!(" where {} ", where_condition);
        }
        
        sql
    }
}


#[allow(unused)]
impl AkitaMapper for AkitaEntityManager{

    /// Get all the table of records
    fn list<T, W>(&mut self, wrapper: &mut W) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter()
            .map(|c| c.name.to_owned())
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
        let rows = self.0.execute_result(&sql, &[])?;
        let mut entities = vec![];
        for dao in rows.iter() {
            let entity = T::from_data(&dao);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    fn select_one<T, W>(&mut self, wrapper: &mut W) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter()
            .map(|c| c.name.to_owned())
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
        let sql = format!("SELECT {} FROM {} {} limit 1", &enumerated_columns, &table.complete_name(), where_condition);
        let rows = self.0.execute_result(&sql, &[])?;
        Ok(rows.iter().next().map(|data| T::from_data(&data)))
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        I: ToValue
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter()
            .map(|c| c.name.to_owned())
            .collect::<Vec<_>>()
            .join(", ");
        
        if let Some(field) = columns.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = format!("SELECT {} FROM {} WHERE {} = {} limit 1", &enumerated_columns, &table.complete_name(), &field.name, &id.to_value());
            let rows = self.0.execute_result(&sql, &[])?;
            Ok(rows.iter().next().map(|data| T::from_data(&data)))
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }

    /// Get table of records with page
    fn page<T, W>(&mut self, page: usize, size: usize, wrapper: &mut W) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromAkita,
        W: Wrapper
    {
        #[derive(FromAkita)]
        struct Count {
            count: i64,
        }
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let enumerated_columns = columns
            .iter()
            .map(|c| c.name.to_owned())
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
        let count: Count = self.execute_first(&count_sql, &[])?;
        let mut page = IPage::new(page, size ,count.count as usize, vec![]);
        let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &table.complete_name(), where_condition,page.offset(),  page.size);
        let rows = self.0.execute_result(&sql, &[])?;
        let mut entities = vec![];
        for dao in rows.iter() {
            let entity = T::from_data(&dao);
            entities.push(entity)
        }
        page.records = entities;
        Ok(page)
    }

    /// Get the total count of records
    fn count<T, W>(&mut self, wrapper: &mut W) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper {
        #[derive(FromAkita)]
        struct Count {
            count: i64,
        }
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
        let count: Result<Count, AkitaError> = self.execute_first(&sql, &[]);
        count.map(|c| c.count as usize)
    }

    /// Remove the records by wrapper.
    fn remove<T, W>(&mut self, wrapper: &mut W) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields,
        W: Wrapper {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("delete from {} {}", &table.complete_name(), where_condition);
        let _ = self.0.execute_result(&sql, &[])?;
        Ok(())
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&mut self, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = format!("delete from {} where {} = {}", &table.name, &field.name, &id.to_value());
            if let Some(log) = &self.1.log_level {
                let _ = self.0.execute_result_log(&sql, &[], log)?;
            } else {
                let _ = self.0.execute_result(&sql, &[])?;
            }
            
            Ok(())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
        
    }
    

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, wrapper: &mut UpdateWrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToAkita {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let sql = self.build_update_clause(entity, wrapper);
        let update_fields = &wrapper.sql_set;
        let data = entity.to_data();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            if !col.exist | (!update_fields.contains(&col.name) && !update_fields.is_empty()) {
                continue;
            }
            let col_name = &col.name;
            let value = data.get_value(&col.alias.to_owned().unwrap_or(col_name.to_owned()));
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        self.0.execute_result(&sql, &bvalues)?;
        Ok(())
    }

    /// Update the records by id.
    fn update_by_id<T, I>(&mut self, entity: &T, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields + ToAkita {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let data = entity.to_data();
        let columns = T::fields();
        if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let set_fields = columns
            .iter().filter(|col| col.exist)
            .enumerate()
            .map(|(x, col)| {
                #[allow(unreachable_patterns)]
                match self.0 {
                    // #[cfg(feature = "with-mysql")]
                    DatabasePlatform::Mysql(_) => format!("{} = ?", &col.name),
                    _ => format!("${}", x + 1),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
            let sql = format!("update {} set {} where {} = ?", &table.name, &set_fields, &field.name);
            let mut values: Vec<Value> = Vec::with_capacity(columns.len());
            for col in columns.iter() {
                if !col.exist {
                    continue;
                }
                let col_name = &col.name;
                let value = data.get_value(&col.alias.to_owned().unwrap_or(col_name.to_owned()));
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }
            values.push(id.to_value());
            let bvalues: Vec<&Value> = values.iter().collect();
            if let Some(log) = &self.1.log_level {
                let _ = self.0.execute_result_log(&sql, &bvalues, log)?;
            } else {
                let _ = self.0.execute_result(&sql, &bvalues)?;
            }
            
            Ok(())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
        
    }

    #[allow(unused_variables)]
    fn save_batch<T>(&mut self, entities: &[&T]) -> Result<Vec<usize>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        match self.0 {
            DatabasePlatform::Mysql(_) => self.save_batch_inner(entities),
        }
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T>(&mut self, entity: &T) -> Result<usize, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        let columns = T::fields();
        let sql = self.build_insert_clause(&[entity]);
        let data = entity.to_data();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let value = data.get_value(&col.name);
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        self.0.execute_result(&sql, &bvalues)?;
        let rows = self.0.execute_result("SELECT LAST_INSERT_ID();", &[])?;
        let count = rows.iter().next().map(|data| i64::from_data(&data)).unwrap_or_default();
        Ok(count as usize)
    }

    /// this is soly for use with sqlite since sqlite doesn't support bulk insert
    fn save_batch_result<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
        R: FromAkita + GetFields,
    {
        let return_columns = R::fields();
        let return_column_names = return_columns
            .iter()
            .map(|rc| rc.name.to_owned())
            .collect::<Vec<_>>()
            .join(", ");

        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        //TODO: move this specific query to sqlite
        let last_insert_sql = format!(
            "\
             SELECT {} \
             FROM {} \
             WHERE ROWID = (\
             SELECT LAST_INSERT_ROWID() FROM {})",
            return_column_names,
            table.complete_name(),
            table.complete_name()
        );
        let mut retrieved_entities = vec![];
        for entity in entities {
            self.save(*entity)?;
            let retrieved = self.execute_result(&last_insert_sql, &[])?;
            retrieved_entities.extend(retrieved);
        }
        Ok(retrieved_entities)
    }

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R>(
        &mut self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromAkita,
    {
        let values: Vec<Value> = params.iter().map(|p| p.to_value()).collect();
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows = self.0.execute_result(sql, &bvalues)?;
        Ok(rows.iter().map(|data| R::from_data(&data)).collect::<Vec<R>>())
    }

    fn execute_first<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<R, AkitaError>
    where
        R: FromAkita,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.execute_result(&sql, &params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    fn execute_result_opt<'a, R, S: Into<String>>(
        &mut self,
        sql: S,
        params: &[&'a dyn ToValue],
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.execute_result(&sql, &params);
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

#[cfg(test)]
mod test {
    use crate::{akita, AkitaConfig, AkitaMapper, Pool, UpdateWrapper, Wrapper, BaseMapper};

    #[derive(Debug, FromAkita, ToAkita, Table, Clone)]
    #[table(name="t_system_user")]
    struct SystemUser {
        id: Option<i32>,
        #[table_id]
        username: String,
        #[field(name="ages", exist = "false")]
        age: i32,
    }

    #[test]
    fn get_table_info() {
        // let db_url = String::from("mysql://root:password@localhost:3306/akita");
        // let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        // let mut em = pool.entity_manager().expect("must be ok");
        // let table = em
        //     .get_table(&TableName::from("public.film"))
        //     .expect("must have a table");
        // println!("table: {:#?}", table);
        let s = mysql::serde_json::to_value("[123,3455,556]").unwrap();
        println!("{}", s)
    }

    #[test]
    fn remove() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let em = &mut pool.entity_manager().expect("must be ok");
        let mut wrap = UpdateWrapper::new();
        wrap.eq("username", "'ussd'");
        match em.remove::<SystemUser, UpdateWrapper>(&mut wrap) {
            Ok(res) => {
                println!("success removed data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn count() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrap = UpdateWrapper::new();
        wrap.eq("username", "'ussd'");
        match em.count::<SystemUser, UpdateWrapper>(&mut wrap) {
            Ok(res) => {
                println!("success count data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn remove_by_id() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        match em.remove_by_id::<SystemUser, String>("'fffsd'".to_string()) {
            Ok(res) => {
                println!("success removed data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn update() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        let mut wrap = UpdateWrapper::new();
        wrap.eq("username", "'ussd'");
        match em.update(&user, &mut wrap) {
            Ok(res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn update_by_id() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.update_by_id(&user, "id") {
            Ok(res) => {
                println!("success update data by id!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn save() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save(&user) {
            Ok(res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn save_batch() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save_batch::<_>(&vec![&user]) {
            Ok(res) => {
                println!("success update data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_insert() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match user.insert(&mut em) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn select_by_id() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq("username", "'ussd'");
        match em.select_by_id::<SystemUser, i32>(1) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn select_one() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq("username", "'ussd'");
        match em.select_one::<SystemUser, UpdateWrapper>(&mut wrapper) {
            Ok(res) => {
                println!("success select data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn list() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq("username", "'ussd'");
        match em.list::<SystemUser, UpdateWrapper>(&mut wrapper) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_list() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq("username", "'ussd'");
        match SystemUser::list(&mut wrapper, &mut em) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }


    #[test]
    fn page() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq( "username", "'ussd'");
        match em.page::<SystemUser, UpdateWrapper>(1, 10,&mut wrapper) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }

    #[test]
    fn self_page() {
        let db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = UpdateWrapper::new();
        wrapper.eq("username", "'ussd'");
        match SystemUser::page(1, 10, &mut wrapper, &mut em) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }
}


