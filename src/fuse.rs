//!
//! Fuse Features
//!

use akita_core::{FromValue, GetFields, Rows, ToValue, Value};
use crate::{Akita, AkitaError, AkitaMapper, IPage, ISegment, Wrapper};
use crate::database::DatabasePlatform;

pub struct Fuse<'a> {
    akita: &'a Akita,
    wrapper: Wrapper,
    table: String,
}

impl<'a> Fuse<'a> {
    pub fn new(akita: &'a Akita) -> Self {
        Self { akita, wrapper: Wrapper::new(), table: String::default() }
    }

    pub fn wrapper(mut self, wrapper: Wrapper) -> Self {
        self.wrapper = wrapper;
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.table = table.into();
        self
    }

    pub fn affected_rows(&self) -> u64 {
        self.akita.affected_rows()
    }

    pub fn last_insert_id(&self) -> u64 {
        self.akita.last_insert_id()
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

        let enumerated_columns = if select_fields.eq("*") || select_fields.is_empty() { "*".to_string() } else { select_fields };
        let where_condition = if where_condition.trim().is_empty() { String::default() } else { format!("WHERE {}",where_condition) };
        let sql = format!("SELECT {} FROM {} {}", &enumerated_columns, &self.table, where_condition);
        let rows = self.akita.exec_iter(&sql, ())?;
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
        let rows = self.akita.exec_iter(&sql, ())?;
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
        let rows = self.akita.exec_iter(&count_sql, ())?;
        let count = rows.iter().map(|d| i64::from_value(&d)).next().unwrap_or(0);
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &self.table, where_condition,page.offset(),  page.size);
            let rows = self.akita.exec_iter(&sql, ())?;
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
        let result = self.akita.exec_iter(&sql, ())?;
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
        let _ = self.akita.exec_iter(&sql, ())?;
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
            self.akita.exec_iter(&sql, ())?;
        }
        Ok(())
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save_map<T>(&mut self, entity: &T) -> Result<(), AkitaError>
        where
            T: ToValue,
    {
        self.save_map_batch(&[entity])
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    pub fn save_map_batch<T>(&mut self, entities: &[&T]) -> Result<(), AkitaError>
        where
            T: ToValue,
    {
        if entities.len() == 0 {
            return Err(AkitaError::DataError("data cannot be empty".to_string()))
        }
        let columns = entities[0].to_value();
        let columns = if let Some(columns) = columns.as_object() {
            columns.keys().collect::<Vec<&String>>()
        } else { Vec::new() };
        let sql = self.build_insert_clause_map(entities)?;
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for entity in entities.iter() {
            for col in columns.iter() {
                let data = entity.to_value();
                let value = data.get_obj_value(col);
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }
        }
        let _bvalues: Vec<&Value> = values.iter().collect();
        self.akita.exec_iter(&sql,values)?;
        Ok(())
    }
    /// build an update clause
    fn build_update_clause(&mut self) -> Result<String, AkitaError> {
        let set_fields = &mut self.wrapper.fields_set;
        let mut sql = String::new();
        sql += &format!("update {} ", &self.table);
        let db = self.akita.acquire()?;
        let fields = set_fields.iter().map(|f| f.0.to_owned()).collect::<Vec<String>>();
        // columns.iter().filter(|col| !set_fields.is_empty() && fields.contains(&col.name) && col.exist).collect::<Vec<_>>()
        sql += &format!(
            "set {}",
            set_fields
                .iter_mut()
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

    pub fn query_first<R>(
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
        let result: Result<Vec<R>, AkitaError> = self.akita.exec_raw(&sql, ());
        match result {
            Ok(mut result) => match result.len() {
                0 => Err(AkitaError::DataError("Zero record returned".to_string())),
                1 => Ok(result.remove(0)),
                _ => Err(AkitaError::DataError("More than one record returned".to_string())),
            },
            Err(e) => Err(e),
        }
    }

    pub fn query_iter(
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
        let rows = self.akita.exec_iter(&sql, ())?;
        Ok(rows)
    }

    /// build an insert clause
    pub fn build_insert_clause_map<T>(&mut self, entities: &[T]) -> Result<String, AkitaError>
        where
            T: ToValue,
    {
        let table = &self.table;
        if entities.len() == 0 {
            return Err(AkitaError::DataError("data cannot be empty".to_string()))
        }
        let columns = entities[0].to_value();
        let columns = if let Some(columns) = columns.as_object() {
            columns.keys().collect::<Vec<&String>>()
        } else { Vec::new() };
        let columns_len = columns.len();
        let mut sql = String::new();
        let db = self.akita.acquire()?;
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
        let db = self.akita.acquire()?;
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