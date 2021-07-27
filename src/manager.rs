use crate::{AkitaError, ColumnName, DatabaseName, TableDef, TableName, GetColumnNames, GetTableName, Wrapper, database::{Database, DatabasePlatform}, value::{ToValue, Value}};
use crate::data::{FromAkita, Rows, AkitaData, ToAkita};
/// an interface executing sql statement and getting the results as generic Akita values
/// without any further conversion.
pub struct AkitaManager(pub DatabasePlatform);

pub struct AkitaEntityManager(pub DatabasePlatform);

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



impl AkitaEntityManager {
    pub fn start_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.start_transaction()
    }

    pub fn commit_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> Result<(), AkitaError> {
        self.0.rollback_transaction()
    }

    pub fn set_session_user(&mut self, username: &str) -> Result<(), AkitaError> {
        let sql = format!("SET SESSION ROLE '{}'", username);
        self.0.execute_result(&sql, &[])?;
        Ok(())
    }

    pub fn database(&mut self) -> &mut dyn Database {
        &mut *self.0
    }

    /// Get all the table of records
    pub fn list<T, W>(&mut self, wrapper: &mut W) -> Result<Vec<T>, AkitaError>
    where
        T: GetTableName + GetColumnNames + FromAkita,
        W: Wrapper
    {
        let table = T::table_name();
        let columns = T::column_names();
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
        let sql = format!("SELECT {} FROM {} WHERE {}", &enumerated_columns, &table.complete_name(), wrapper.get_sql_segment());
        let rows = self.0.execute_result(&sql, &[])?;
        let mut entities = vec![];
        for dao in rows.iter() {
            let entity = T::from_data(&dao);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get the total count of records
    pub fn count<T, W>(&mut self, wrapper: &mut W) -> Result<usize, AkitaError> 
    where
        T: GetTableName + GetColumnNames,
        W: Wrapper {
        #[derive(FromAkita)]
        struct Count {
            count: i64,
        }
        let table = T::table_name();
        let sql = format!(
            "SELECT COUNT(1) AS count FROM {} WHERE {}",
            table.complete_name(),
            wrapper.get_sql_segment()
        );
        let count: Result<Count, AkitaError> = self.execute_first(&sql, &[]);
        count.map(|c| c.count as usize)
    }

    /// Remove the records by wrapper.
    pub fn remove<T, W>(&mut self, wrapper: &mut W) -> Result<(), AkitaError> 
    where
        T: GetTableName + GetColumnNames,
        W: Wrapper {
        let table = T::table_name();
        let sql = format!("delete from {} where {}", &table.complete_name(), wrapper.get_sql_segment());
        let _ = self.0.execute_result(&sql, &[])?;
        Ok(())
    }

    #[allow(unused_variables)]
    pub fn save_batch<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, AkitaError>
    where
        T: GetTableName + GetColumnNames + ToAkita,
        R: FromAkita + GetColumnNames,
    {
        match self.0 {
            DatabasePlatform::Mysql(_) => self.save_batch_inner(entities),
        }
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save<T>(&mut self, entity: &T) -> Result<(), AkitaError>
    where
        T: GetTableName + GetColumnNames + ToAkita,
    {
        let columns = T::column_names();
        let sql = self.build_insert_clause(&[entity]);
        let dao = entity.to_data();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let value = dao.get_value(&col.name);
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        self.0.execute_result(&sql, &bvalues)?;
        Ok(())
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


   

    /// this is soly for use with sqlite since sqlite doesn't support bulk insert
    fn save_batch_inner<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, AkitaError>
    where
        T: GetTableName + GetColumnNames + ToAkita,
        R: FromAkita + GetColumnNames,
    {
        let return_columns = R::column_names();
        let return_column_names = return_columns
            .iter()
            .map(|rc| rc.name.to_owned())
            .collect::<Vec<_>>()
            .join(", ");

        let table = T::table_name();
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
        println!("sql: {}", last_insert_sql);
        for entity in entities {
            self.save(*entity)?;
            let retrieved = self.execute_result(&last_insert_sql, &[])?;
            retrieved_entities.extend(retrieved);
        }
        Ok(retrieved_entities)
    }

    /// build the returning clause
    fn build_returning_clause(&self, return_columns: Vec<ColumnName>) -> String {
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
        T: GetTableName + GetColumnNames + ToAkita,
    {
        let table = T::table_name();
        let columns = T::column_names();
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
                                #[cfg(feature = "with-mysql")]
                                DBPlatform::Mysql(_) => "?".to_string(),
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

    #[allow(clippy::redundant_closure)]
    pub fn execute_result<'a, R>(
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
        Ok(rows.iter().map(|dao| R::from_data(&dao)).collect::<Vec<R>>())
    }

    pub fn execute_first<'a, R>(
        &mut self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<R, AkitaError>
    where
        R: FromAkita,
    {
        let result: Result<Vec<R>, AkitaError> = self.execute_result(sql, &params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn execute_result_opt<'a, R>(
        &mut self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita,
    {
        let result: Result<Vec<R>, AkitaError> = self.execute_result(sql, &params);
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
    use crate::{TableName, pool::Pool};

    #[test]
    fn film_table_info() {
        let db_url = "mysql://root:shbyd101@localhost:3306/akita";
        let mut pool = Pool::new(db_url).unwrap();
        let mut em = pool.entity_manager().expect("must be ok");
        let table = em
            .get_table(&TableName::from("public.film"))
            .expect("must have a table");
        println!("table: {:#?}", table);
    }
}