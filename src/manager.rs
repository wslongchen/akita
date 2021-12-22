use crate::{AkitaError, IPage, Wrapper, database::{Database, DatabasePlatform}, mapper::AkitaMapper, GetFields, GetTableName, FromValue, ToValue, Rows, TableName, DatabaseName, FieldName, Params, Value, FieldType, TableDef, segment::ISegment};
/// an interface executing sql statement and getting the results as generic Akita values
/// without any further conversion.
#[allow(unused)]
pub struct AkitaManager(DatabasePlatform);

#[allow(unused)]
pub struct AkitaEntityManager(DatabasePlatform);

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
    fn list<T>(&mut self, wrapper:Wrapper) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        self.conn.list(wrapper)
    }

    /// Get one the table of records
    fn select_one<T>(&mut self, wrapper:Wrapper) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        self.conn.select_one(wrapper)
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue,
        I: ToValue
    {
        self.conn.select_by_id(id)
    }

    /// Get table of records with page
    fn page<T>(&mut self, page: usize, size: usize, wrapper:Wrapper) -> Result<IPage<T>, AkitaError>
    where
        T: GetTableName + GetFields + FromValue,
        
    {
        self.conn.page(page, size, wrapper)
    }

    /// Get the total count of records
    fn count<T>(&mut self, wrapper:Wrapper) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetFields,
         {
        self.conn.count::<T>(wrapper)
    }

    /// Remove the records by wrapper.
    fn remove<T>(&mut self, wrapper:Wrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields,
         {
            self.conn.remove::<T>(wrapper)
    }

    fn remove_by_ids<T, I>(&mut self, ids: Vec<I>) -> Result<(), AkitaError> where I: ToValue, T: GetTableName + GetFields {
        self.conn.remove_by_ids::<T,I>(ids)
    }

    /// Remove the records by id.
    fn remove_by_id<T, I>(&mut self, id: I) -> Result<(), AkitaError> 
    where
        I: ToValue,
        T: GetTableName + GetFields {
            self.conn.remove_by_id::<T, I>(id)
        
    }

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, wrapper: Wrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToValue {
            self.conn.update(entity, wrapper)
    }

    /// Update the records by id.
    fn update_by_id<T>(&mut self, entity: &T) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToValue {
            self.conn.update_by_id(entity)
        
    }

    #[allow(unused_variables)]
    fn save_batch<T, I>(&mut self, entities: &[&T]) -> Result<Vec<Option<I>>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue,
    {
        self.conn.save_batch(entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue,
    {
        self.conn.save(entity)
    }

    fn save_or_update<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError> where T: GetTableName + GetFields + ToValue, I: FromValue {
        self.conn.save_or_update(entity)
    }

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromValue,
    {
        self.conn.execute_result(sql, params)
    }

    fn execute_drop<'a, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>
    {
        self.conn.execute_drop(sql, params)
    }

    fn execute_first<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<R, AkitaError>
    where
        R: FromValue,
    {
        self.conn.execute_first(sql, params)
    }

    fn execute_result_opt<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromValue,
    {
        self.conn.execute_result_opt(sql, params)
    }
}


#[allow(unused)]
impl AkitaManager {

    pub fn new(db: DatabasePlatform) -> Self {
        AkitaManager(db)
    }

    pub fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.start_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.rollback_transaction()
    }

    pub fn execute_result<S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Rows, AkitaError> {
        let rows = self.0.execute_result(&sql.into(), params.into())?;
        Ok(rows)
    }

    pub fn execute_iter<S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<Value>, AkitaError> {
        let rows = self.0.execute_result(&sql.into(), params.into())?;
        let datas: Vec<Value> = rows.iter().collect();
        Ok(datas)
    }

    pub fn execute_first<S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Value, AkitaError> {
        let record: Result<Option<Value>, AkitaError> =
            self.execute_first_opt(sql, params);
        match record {
            Ok(record) => match record {
                Some(record) => Ok(record),
                None => Err(AkitaError::DataError("Zero record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn execute_first_opt<S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<Value>, AkitaError> {
        let result: Result<Vec<Value>, AkitaError> = self.execute_iter(sql, params);
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

    pub fn new(db: DatabasePlatform) -> Self {
        AkitaEntityManager(db)
    }

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
        self.0.execute_result(&sql, Params::Nil)?;
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

    fn save_batch_inner<T, I>(&mut self, entities: &[&T]) -> Result<Vec<Option<I>>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue
    {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let mut result = Vec::new();
        for entity in entities.into_iter() {
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
                .map(|c| format!("`{}`", c.name))
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
                            match self.0 {
                                #[cfg(feature = "with-sqlite")]
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

    /// build an update clause
    fn build_update_clause<T>(&self, entity: &T, wrapper: &mut Wrapper) -> String
    where
        T: GetTableName + GetFields + ToValue 
    {
        let table = T::table_name();
        let columns = T::fields();
        let columns_len = columns.len();
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
                        match self.0 {
                            #[cfg(feature = "akita-mysql")]
                            DatabasePlatform::Mysql(_) => format!("`{}` = ?", &col.name),
                            #[cfg(feature = "akita-sqlite")]
                            DatabasePlatform::Sqlite(_) => format!("`{}` = ${}", &col.name, x + 1),
                            _ => format!("`{}` = ${}", &col.name, x + 1),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else {
            let fields = set_fields.iter().map(|f| f.0.to_owned()).collect::<Vec<String>>();
            // columns.iter().filter(|col| !set_fields.is_empty() && fields.contains(&col.name) && col.exist).collect::<Vec<_>>()
            sql += &format!(
                "set {}",
                set_fields
                    .iter_mut()
                    .enumerate()
                    .map(|(x, (col, value))| {
                        #[allow(unreachable_patterns)]
                        match self.0 {
                            #[cfg(feature = "akita-mysql")]
                            DatabasePlatform::Mysql(_) => format!("`{}` = {}", col, value.get_sql_segment()),
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
}


#[allow(unused)]
impl AkitaMapper for AkitaEntityManager{

    /// Get all the table of records
    fn list<T>(&mut self, mut wrapper:Wrapper) -> Result<Vec<T>, AkitaError>
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
            .map(|c| format!("`{}`", c.name))
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
        let rows = self.0.execute_result(&sql, Params::Nil)?;
        let mut entities = vec![];
        for data in rows.iter() {
            let entity = T::from_value(&data);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    fn select_one<T>(&mut self, mut wrapper:Wrapper) -> Result<Option<T>, AkitaError>
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
            .map(|c| format!("`{}`", c.name))
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
        let rows = self.0.execute_result(&sql, Params::Nil)?;
        Ok(rows.iter().next().map(|data| T::from_value(&data)))
    }

    /// Get one the table of records by id
    fn select_by_id<T, I>(&mut self, id: I) -> Result<Option<T>, AkitaError>
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
            .map(|c| format!("`{}`", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        
        if let Some(field) = columns.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match self.0 {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("SELECT {} FROM {} WHERE `{}` = ? limit 1", &enumerated_columns, &table.complete_name(), &field.name),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.name, col_len + 1),
                _ => format!("SELECT {} FROM {} WHERE `{}` = ${} limit 1", &enumerated_columns, &table.complete_name(), &field.name, col_len + 1),
            };
            let rows = self.0.execute_result(&sql, (id.to_value(),).into())?;
            Ok(rows.iter().next().map(|data| T::from_value(&data)))
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }

    /// Get table of records with page
    fn page<T>(&mut self, page: usize, size: usize, mut wrapper:Wrapper) -> Result<IPage<T>, AkitaError>
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
            .map(|c| format!("`{}`", c.name))
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
        let count: i64 = self.execute_first(&count_sql, ())?;
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &table.complete_name(), where_condition,page.offset(),  page.size);
            let rows = self.0.execute_result(&sql, Params::Nil)?;
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
    fn count<T>(&mut self, mut wrapper:Wrapper) -> Result<usize, AkitaError> 
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
        self.execute_first(&sql, ())
    }

    /// Remove the records by wrapper.
    fn remove<T>(&mut self, mut wrapper:Wrapper) -> Result<(), AkitaError> 
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
        let _ = self.0.execute_result(&sql, Params::Nil)?;
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
        let cols = T::fields();
        let col_len = cols.len();
        if let Some(field) = cols.iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            let sql = match self.0 {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` = ?", &table.name, &field.name),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` = ${}", &table.name, &field.name, col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.name, col_len + 1),
            };
            let _ = self.0.execute_result(&sql, (id.to_value(),).into())?;
            Ok(())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }


    /// Remove the records by wrapper.
    fn remove_by_ids<T, I>(&mut self, ids: Vec<I>) -> Result<(), AkitaError>
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
            let sql = match self.0 {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("delete from {} where `{}` in (?)", &table.name, &field.name),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("delete from {} where `{}` in (${})", &table.name, &field.name, col_len + 1),
                _ => format!("delete from {} where `{}` = ${}", &table.name, &field.name, col_len + 1),
            };
            let ids = ids.iter().map(|v| v.to_value().to_string()).collect::<Vec<String>>().join(",");
            let _ = self.0.execute_result(&sql, (ids,).into())?;
            Ok(())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
    }
    

    /// Update the records by wrapper.
    fn update<T>(&mut self, entity: &T, mut wrapper: Wrapper) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetFields + ToValue {
        let table = T::table_name();
        if table.complete_name().is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let columns = T::fields();
        let sql = self.build_update_clause(entity, &mut wrapper);
        let update_fields = wrapper.fields_set;
        let mut bvalues: Vec<&Value> = Vec::new();
        if update_fields.is_empty() {
            let data = entity.to_value();
            let mut values: Vec<Value> = Vec::with_capacity(columns.len());
            for col in columns.iter() {
                if !col.exist || col.field_type.ne(&FieldType::TableField) {
                    continue;
                }
                let col_name = &col.name;
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
            self.0.execute_result(&sql, values.into())?;
        } else {
            self.0.execute_result(&sql, Params::Nil)?;
        }
        Ok(())
    }

    /// Update the records by id.
    fn update_by_id<T>(&mut self, entity: &T) -> Result<(), AkitaError> 
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
                match self.0 {
                    #[cfg(feature = "akita-mysql")]
                    DatabasePlatform::Mysql(_) => format!("`{}` = ?", &col.name),
                    #[cfg(feature = "akita-sqlite")]
                    DatabasePlatform::Sqlite(_) => format!("`{}` = ${}",&col.name, x + 1),
                    _ => format!("`{}` = ${}", &col.name, x + 1),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
            let sql = match self.0 {
                #[cfg(feature = "akita-mysql")]
                DatabasePlatform::Mysql(_) => format!("update {} set {} where `{}` = ?", &table.name, &set_fields, &field.name),
                #[cfg(feature = "akita-sqlite")]
                DatabasePlatform::Sqlite(_) => format!("update {} set {} where `{}` = ${}", &table.name, &set_fields, &field.name, col_len + 1),
                _ => format!("update {} set {} where `{}` = ${}", &table.name, &set_fields, &field.name, col_len + 1),
            };
            let mut values: Vec<Value> = Vec::with_capacity(columns.len());
            let id = data.get_obj_value(&field.name);
            for col in columns.iter() {
                if !col.exist || col.field_type.ne(&FieldType::TableField) {
                    continue;
                }
                let col_name = &col.name;
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
            let _ = self.0.execute_result(&sql, values.into())?;
            Ok(())
        } else {
            Err(AkitaError::MissingIdent(format!("Table({}) Missing Ident...", &table.name)))
        }
        
    }

    #[allow(unused_variables)]
    fn save_batch<T, I>(&mut self, entities: &[&T]) -> Result<Vec<Option<I>>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue
    {
        match self.0 {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(_) => self.save_batch_inner(entities),
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(_) => self.save_batch_inner(entities),
        }
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    fn save<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
    where
        T: GetTableName + GetFields + ToValue,
        I: FromValue,
    {
        let columns = T::fields();
        let sql = self.build_insert_clause(&[entity]);
        let data = entity.to_value();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let mut value = data.get_obj_value(&col.name);
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
        let bvalues: Vec<&Value> = values.iter().collect();
        self.0.execute_result(&sql,values.into())?;
        let rows: Rows = match self.0 {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(_) => self.0.execute_result("SELECT LAST_INSERT_ID();", Params::Nil)?,
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(_) => self.0.execute_result("SELECT LAST_INSERT_ROWID();", Params::Nil)?,
        };
        let last_insert_id = rows.iter().next().map(|data| I::from_value(&data));
        Ok(last_insert_id)
    }

    /// save or update
    fn save_or_update<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
        where
            T: GetTableName + GetFields + ToValue,
            I: FromValue {
        let data = entity.to_value();
        let id = if let Some(field) = T::fields().iter().find(| field| match field.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }) {
            data.get_obj_value(&field.name).unwrap_or(&Value::Nil)
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

    #[allow(clippy::redundant_closure)]
    fn execute_result<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromValue,
    {
        let rows = self.0.execute_result(&sql.into(), params.into())?;
        Ok(rows.iter().map(|data| R::from_value(&data)).collect())
    }

    fn execute_first<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<R, AkitaError>
    where
        R: FromValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.execute_result(&sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    fn execute_drop<'a, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>
    {
        let sql: String = sql.into();
        let _result: Result<Vec<()>, AkitaError> = self.execute_result(&sql, params);
        Ok(())
    }

    fn execute_result_opt<'a, R, S: Into<String>, P: Into<Params>>(
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromValue,
    {
        let sql: String = sql.into();
        let result: Result<Vec<R>, AkitaError> = self.execute_result(&sql, params);
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
    use akita_core::params;
    // use crate as akita;

    use crate::{self as akita, AkitaConfig, AkitaMapper, BaseMapper, Pool, Wrapper, FromValue, ToValue, AkitaTable};

    fn fffff() {

    }

    #[derive(Debug,AkitaTable, Clone)]
    #[table(name="t_system_user")]
    struct SystemUser {
        id: Option<i32>,
        #[table_id(name="ffff", id_type="none")]
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrap = Wrapper::new().eq("username", "'ussd'");
        match em.count::<SystemUser>(wrap) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        let mut wrap = Wrapper::new().eq("username", "'ussd'");
        match em.update(&user, wrap) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.update_by_id(&user) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save::<_, i32>(&user) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match em.save_batch::<_, i32>(&vec![&user]) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let user = SystemUser { id: 1.into(), username: "fff".to_string(), age: 1 };
        match user.insert::<i32, _>(&mut em) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new();
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new().eq("username", "'ussd'");
        match em.select_one::<SystemUser>(wrapper) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new().eq("username", "'ussd'");
        match em.list::<SystemUser>(wrapper) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new().eq("username", "'ussd'");
        match SystemUser::list(wrapper, &mut em) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new().eq( "username", "'ussd'");
        match em.page::<SystemUser>(1, 10,wrapper) {
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
        let _db_url = String::from("mysql://root:password@localhost:3306/akita");
        let mut pool = Pool::new(AkitaConfig::default()).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let mut wrapper = Wrapper::new().eq("username", "'ussd'");
        match SystemUser::page(1, 10, wrapper, &mut em) {
            Ok(res) => {
                println!("success insert data!");
            }
            Err(err) => {
                println!("error:{:?}",err);
            }
        }
    }
}


