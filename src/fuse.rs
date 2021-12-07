//! 
//! Fuse features
//! 

use akita_core::Table;

use crate::{AkitaError, AkitaMapper, IPage, Pool, Wrapper, database::DatabasePlatform};
use crate::{cfg_if, Params, TableName, DatabaseName, SchemaContent, TableDef, Rows, FromValue, Value, ToValue, GetFields};
pub struct Akita {
    db: Option<DatabasePlatform>,
    akita_type: AkitaType,
    wrapper: Wrapper,
    table: String,
}

pub enum AkitaType {
    Query,
    Update,
}

impl Akita {
    
    pub fn new() -> Self {
        Akita { wrapper: Wrapper::new(), table: String::default(), akita_type: AkitaType::Query, db:None }
    }
    
    pub fn wrapper(mut self, wrapper: Wrapper) -> Self {
        self.wrapper = wrapper;
        self
    }

    pub fn conn(mut self, db: DatabasePlatform) -> Self {
        self.db = db.into();
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.table = table.into();
        self
    }

    pub fn list<T>(&mut self) -> Result<Vec<T>, AkitaError>
        where
        T: FromValue {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql, Params::Nil)?;
        let mut entities = vec![];
        for data in rows.iter() {
            let entity = T::from_value(&data);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    pub fn one<T>(&mut self) -> Result<Option<T>, AkitaError>
    where
        T: FromValue
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql, Params::Nil)?;
        Ok(rows.iter().next().map(|data| T::from_value(&data)))
    }

    /// Get table of records with page
    pub fn page<T>(&mut self, page: usize, size: usize) -> Result<IPage<T>, AkitaError>
    where
        T: FromValue
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let count_sql = format!("select count(1) as count from {} {}", &self.table, where_condition);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let result = db.execute_result(&count_sql, Params::Nil)?;
        let count = result.iter().map(|d| i64::from_value(&d)).next().unwrap_or(0);
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &self.table, where_condition,page.offset(),  page.size);
            let rows = db.execute_result(&sql, Params::Nil)?;
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
    pub fn count(&mut self) -> Result<usize, AkitaError> {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!(
            "SELECT COUNT(1) AS count FROM {} {}",
            &self.table,
            where_condition
        );
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let result = db.execute_result(&sql, Params::Nil)?;
        let count = result.iter().map(|d| i64::from_value(&d)).next().map(|c| c as usize).unwrap_or(0);
        Ok(count)
    }

    /// Remove the records by wrapper.
    pub fn remove(&mut self) -> Result<(), AkitaError> {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let where_condition = wrapper.get_sql_segment();
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("delete from {} {}", &self.table, where_condition);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let _ = db.execute_result(&sql, Params::Nil)?;
        Ok(())
    }

    /// Update the records by wrapper.
    pub fn update(&mut self) -> Result<(), AkitaError> {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let sql = self.build_update_clause()?;
        let update_fields = &self.wrapper.fields_set;
        if update_fields.is_empty() {
            return Err(AkitaError::MissingField("Update Error, Missing update fields !".to_string()))
        } else {
            if self.db.is_none() {
                return Err(AkitaError::DataError("Missing database connection".to_string()))
            }
            let db = self.db.as_mut().expect("Missing database connection");
            db.execute_result(&sql, Params::Nil)?;
        }
        Ok(())
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save<T, I>(&mut self, entity: &T) -> Result<Option<I>, AkitaError>
    where
        T: GetFields + ToValue,
        I: FromValue
    {
        let columns = T::fields();
        let sql = self.build_insert_clause(&[entity])?;
        let data = entity.to_value();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let value = data.get_obj_value(&col.name);
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        db.execute_result(&sql,values.into())?;
        let rows: Rows = match *db {
            #[cfg(feature = "akita-mysql")]
            DatabasePlatform::Mysql(_) => db.execute_result("SELECT LAST_INSERT_ID();", Params::Nil)?,
            #[cfg(feature = "akita-sqlite")]
            DatabasePlatform::Sqlite(_) => db.execute_result("SELECT LAST_INSERT_ROWID();", Params::Nil)?,
        };
        let last_insert_id = rows.iter().next().map(|data| I::from_value(&data));
        Ok(last_insert_id)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save_map<T>(&mut self, entity: &T) -> Result<(), AkitaError>
    where
        T: ToValue,
    {
        let columns = entity.to_value();
        let columns = if let Some(columns) = columns.as_object() {
            columns.keys().collect::<Vec<&String>>()
        } else { Vec::new() };
        let sql = self.build_insert_clause_map(entity)?;
        let data = entity.to_value();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let value = data.get_obj_value(col);
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        db.execute_result(&sql,values.into())?;
        Ok(())
    }

    /// Performs text query and maps each row of the first result set.

    #[allow(clippy::redundant_closure)]
    pub fn query_map<T, F, Q, U>(mut self, mut f: F) -> Result<Vec<U>, AkitaError>
    where
        T: FromValue,
        F: FnMut(T) -> U,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        self.query_fold(sql, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    /// Performs text query and maps each row of the first result set.

    #[allow(clippy::redundant_closure)]
    pub fn exec_map<T, F, Q, U>(mut self, query: Q, mut f: F) -> Result<Vec<U>, AkitaError>
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

    /// Performs text query and folds the first result set to a single value.
    pub fn query_fold<T, F, Q, U>(mut self, query: Q, init: U, mut f: F) -> Result<U, AkitaError>
    where
        Q: Into<String>,
        T: FromValue,
        F: FnMut(U, T) -> U,
    {
        self.exec_iter::<_, _>(query, ()).map(|r| r.iter().map(|data| T::from_value(&data))
            .fold(init, |acc, row| f(acc, row)))
    }

    #[allow(clippy::redundant_closure)]
    pub fn query_iter<'a>(
        mut self,
    ) -> Result<Rows, AkitaError>
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql, Params::Nil)?;
        Ok(rows)
    }

    #[allow(clippy::redundant_closure)]
    pub fn exec_iter<'a,S: Into<String>, P: Into<Params>>(
        mut self,
        sql: S,
        params: P,
    ) -> Result<Rows, AkitaError>
    {
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql.into(), params.into())?;
        Ok(rows)
    }

    #[allow(clippy::redundant_closure)]
    pub fn exec_raw<'a, R, S: Into<String>, P: Into<Params>>(
        mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromValue,
    {
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql.into(), params.into())?;
        Ok(rows.iter().map(|data| R::from_value(&data)).collect::<Vec<R>>())
    }

    pub fn query_first<'a, R>(
        mut self
    ) -> Result<R, AkitaError>
    where
        R: FromValue,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        let result: Result<Vec<R>, AkitaError> = self.exec_raw(&sql, ());
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn exec_first<'a, R, S: Into<String>, P: Into<Params>>(
        mut self,
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

    pub fn exec_drop<'a, S: Into<String>, P: Into<Params>>(
        mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>
    {
        let sql: String = sql.into();
        let _result: Result<Vec<()>, AkitaError> = self.exec_raw(&sql, params);
        Ok(())
    }

    pub fn query_first_opt<'a, R>(
        mut self,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromValue,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let wrapper = &mut self.wrapper;
        let select_fields = wrapper.get_select_sql();
        let where_condition = wrapper.get_sql_segment();
        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() {
            "*".to_string()
        } else { 
            select_fields
        };
        
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        let result: Result<Vec<R>, AkitaError> = self.exec_raw(&sql, ());
        match result {
            Ok(mut result) => match result.len() {
                0 => Ok(None),
                1 => Ok(Some(result.remove(0))),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn exec_first_opt<'a, R, S: Into<String>, P: Into<Params>>(
        mut self,
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

    /// build an update clause
    pub fn build_update_clause(&mut self) -> Result<String, AkitaError> {
        let set_fields = &self.wrapper.fields_set;
        let mut sql = String::new();
        sql += &format!("update {} ", &self.table);
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let fields = set_fields.iter().map(|f| f.0.to_owned()).collect::<Vec<String>>();
            // columns.iter().filter(|col| !set_fields.is_empty() && fields.contains(&col.name) && col.exist).collect::<Vec<_>>()
            sql += &format!(
                "set {}",
                set_fields
                    .iter()
                    .enumerate()
                    .map(|(x, (col, value))| {
                        #[allow(unreachable_patterns)]
                        match db {
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
        let where_condition = self.wrapper.get_sql_segment();
        if !where_condition.is_empty() {
            sql += &format!(" where {} ", where_condition);
        }
        Ok(sql)
    }

    /// build an insert clause
    pub fn build_insert_clause_map<T>(&mut self, entity: &T) -> Result<String, AkitaError>
    where
        T: ToValue,
    {
        let table = &self.table;
        let columns = entity.to_value();
        let columns = if let Some(columns) = columns.as_object() {
            columns.keys().collect::<Vec<&String>>()
        } else { Vec::new() };
        let columns_len = columns.len();
        let mut sql = String::new();
        let entities = &[entity];
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        sql += &format!("INSERT INTO {} ", table);
        sql += &format!(
            "({})\n",
            columns
                .iter()
                .map(|c| format!("`{}`", c))
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
                            match db {
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
        Ok(sql)
    }

    /// build an insert clause
    pub fn build_insert_clause<T>(&mut self, entities: &[&T]) -> Result<String, AkitaError>
    where
        T: GetFields + ToValue,
    {
        let table = &self.table;
        let columns = T::fields();
        let columns_len = columns.len();
        let mut sql = String::new();
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        sql += &format!("INSERT INTO {} ", table);
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
                            match db {
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
        Ok(sql)
    }

}