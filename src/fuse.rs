//! 
//! Fuse features
//! 

use crate::{self as akita, AkitaError, AkitaMapper, IPage, Params, Pool, QueryWrapper, Rows, UpdateWrapper, Value, Wrapper, data::{FromAkita, ToAkita}, database::DatabasePlatform, information::{GetFields, GetTableName}};

pub struct Akita {
    db: Option<DatabasePlatform>,
    akita_type: AkitaType,
    update_wrapper: UpdateWrapper,
    query_wrapper: QueryWrapper,
    table: String,
}

pub enum AkitaType {
    Query,
    Update,
}

impl Akita {
    pub fn new(table: String, db: DatabasePlatform) -> Self {
        Akita { update_wrapper: UpdateWrapper::new(), query_wrapper: QueryWrapper::new(), table, akita_type: AkitaType::Query, db:None }
    }
    
    pub fn from_query() -> Self {
        Akita { update_wrapper: UpdateWrapper::new(), query_wrapper: QueryWrapper::new(), table: String::default(), akita_type: AkitaType::Query, db:None }
    }
    
    pub fn from_update() -> Self {
        Akita { update_wrapper: UpdateWrapper::new(), query_wrapper: QueryWrapper::new(), table: String::default(), akita_type: AkitaType::Query, db:None }
    }

    pub fn conn(&mut self, db: DatabasePlatform) -> &mut Self {
        self.db = db.into();
        self
    }

    pub fn table<S: Into<String>>(&mut self, table: S) -> &mut Self {
        self.table = table.into();
        self
    }

    pub fn list<T>(&mut self) -> Result<Vec<T>, AkitaError>
        where
        T: FromAkita {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
            let entity = T::from_data(&data);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// Get one the table of records
    pub fn one<T>(&mut self) -> Result<Option<T>, AkitaError>
    where
        T: FromAkita
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
        Ok(rows.iter().next().map(|data| T::from_data(&data)))
    }

    /// Get table of records with page
    pub fn page<T>(&mut self, page: usize, size: usize) -> Result<IPage<T>, AkitaError>
    where
        T: FromAkita
    {
        #[derive(FromAkita)]
        struct Count {
            count: i64,
        }
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
        let datas: Vec<Count> = result.iter().map(|d| Count::from_data(&d)).collect();
        let count = datas.iter().next().map(|c| c.count).unwrap_or_default();
        let mut page = IPage::new(page, size ,count as usize, vec![]);
        if page.total > 0 {
            let sql = format!("SELECT {} FROM {} {} limit {}, {}", &enumerated_columns, &self.table, where_condition,page.offset(),  page.size);
            let rows = db.execute_result(&sql, Params::Nil)?;
            let mut entities = vec![];
            for dao in rows.iter() {
                let entity = T::from_data(&dao);
                entities.push(entity)
            }
            page.records = entities;
        }
        Ok(page)
    }

    /// Get the total count of records
    pub fn count(&mut self) -> Result<usize, AkitaError> {
        #[derive(FromAkita)]
        struct Count {
            count: i64,
        }
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let where_condition = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                wrapper.get_sql_segment()
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                wrapper.get_sql_segment()
            },
        };
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
        let datas: Vec<Count> = result.iter().map(|d| Count::from_data(&d)).collect();
        let count = datas.iter().next().map(|c| c.count).unwrap_or_default();
        Ok(count as usize)
    }

    /// Remove the records by wrapper.
    pub fn remove<T, W>(&mut self, wrapper: &mut W) -> Result<(), AkitaError> {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let where_condition = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                wrapper.get_sql_segment()
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                wrapper.get_sql_segment()
            },
        };
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
        let update_fields = &self.update_wrapper.fields_set;
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
        T: GetTableName + GetFields + ToAkita,
        I: FromAkita
    {
        let columns = T::fields();
        let sql = self.build_insert_clause(&[entity])?;
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
        let last_insert_id = rows.iter().next().map(|data| I::from_data(&data));
        Ok(last_insert_id)
    }

    /// Performs text query and maps each row of the first result set.

    #[allow(clippy::redundant_closure)]
    pub fn query_map<T, F, Q, U>(&mut self, mut f: F) -> Result<Vec<U>, AkitaError>
    where
        T: FromAkita,
        F: FnMut(T) -> U,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
    pub fn exec_map<T, F, Q, U>(&mut self, query: Q, mut f: F) -> Result<Vec<U>, AkitaError>
    where
        Q: Into<String>,
        T: FromAkita,
        F: FnMut(T) -> U,
    {
        self.query_fold(query, Vec::new(), |mut acc, row| {
            acc.push(f(row));
            acc
        })
    }

    /// Performs text query and folds the first result set to a single value.
    pub fn query_fold<T, F, Q, U>(&mut self, query: Q, init: U, mut f: F) -> Result<U, AkitaError>
    where
        Q: Into<String>,
        T: FromAkita,
        F: FnMut(U, T) -> U,
    {
        self.exec_iter::<_, _>(query, ()).map(|r| r.iter().map(|data| T::from_data(&data))
            .fold(init, |acc, row| f(acc, row)))
    }

    #[allow(clippy::redundant_closure)]
    pub fn query_iter<'a>(
        &mut self,
    ) -> Result<Rows, AkitaError>
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
        &mut self,
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
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Vec<R>, AkitaError>
    where
        R: FromAkita,
    {
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
        let rows = db.execute_result(&sql.into(), params.into())?;
        Ok(rows.iter().map(|data| R::from_data(&data)).collect::<Vec<R>>())
    }

    pub fn query_first<'a, R>(
        &mut self
    ) -> Result<R, AkitaError>
    where
        R: FromAkita,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
        &mut self,
        sql: S,
        params: P,
    ) -> Result<R, AkitaError>
    where
        R: FromAkita,
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
        &mut self,
        sql: S,
        params: P,
    ) -> Result<(), AkitaError>
    {
        let sql: String = sql.into();
        let _result: Result<Vec<()>, AkitaError> = self.exec_raw(&sql, params);
        Ok(())
    }

    pub fn query_first_opt<'a, R>(
        &mut self,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita,
    {
        if self.table.is_empty() {
            return Err(AkitaError::MissingTable("Find Error, Missing Table Name !".to_string()))
        }
        let (select_fields, where_condition) = match self.akita_type {
            AkitaType::Query => {
                let wrapper = &mut self.query_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
            AkitaType::Update => {
                let wrapper = &mut self.update_wrapper;
                (wrapper.get_select_sql(), wrapper.get_sql_segment())
            },
        };
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
        &mut self,
        sql: S,
        params: P,
    ) -> Result<Option<R>, AkitaError>
    where
        R: FromAkita,
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
        let set_fields = &self.update_wrapper.fields_set;
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
        let where_condition = self.update_wrapper.get_sql_segment();
        if !where_condition.is_empty() {
            sql += &format!(" where {} ", where_condition);
        }
        Ok(sql)
    }

    /// build an insert clause
    pub fn build_insert_clause<T>(&mut self, entities: &[&T]) -> Result<String, AkitaError>
    where
        T: GetTableName + GetFields + ToAkita,
    {
        let table = T::table_name();
        let columns = T::fields();
        let columns_len = columns.len();
        let mut sql = String::new();
        if self.db.is_none() {
            return Err(AkitaError::DataError("Missing database connection".to_string()))
        }
        let db = self.db.as_mut().expect("Missing database connection");
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

impl Wrapper for Akita {
    fn eq<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.eq(column, val);},
            AkitaType::Update => {self.update_wrapper.eq(column, val);},
        }
        self
    }

    fn eq_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.eq_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.eq_condition(condition, column, val);},
        }
        self
    }

    fn ne<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.ne(column, val);}
            AkitaType::Update => {self.update_wrapper.ne(column, val);},
        }
        self
    }

    fn ne_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.ne_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.ne_condition(condition, column, val);},
        }
        self
    }

    fn gt<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.gt(column, val);},
            AkitaType::Update => {self.update_wrapper.gt(column, val);},
        }
        self
    }

    fn gt_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.gt_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.gt_condition(condition, column, val);},
        }
        self
    }

    fn ge<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.ge(column, val);},
            AkitaType::Update => {self.update_wrapper.ge(column, val);},
        }
        self
    }

    fn ge_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.ge_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.ge_condition(condition, column, val);},
        }
        self
    }

    fn lt<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.lt(column, val);},
            AkitaType::Update => {self.update_wrapper.lt(column, val);},
        }
        self
    }

    fn lt_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.lt_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.lt_condition(condition, column, val);},
        }
        self
    }

    fn le<S: Into<String>, U: crate::segment::ToSegment>(&mut self, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.le(column, val);},
            AkitaType::Update => {self.update_wrapper.le(column, val);},
        }
        self
    }

    fn le_condition<S: Into<String>, U: crate::segment::ToSegment>(&mut self, condition: bool, column: S, val: U) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.le_condition(condition, column, val);},
            AkitaType::Update => {self.update_wrapper.le_condition(condition, column, val);},
        }
        self
    }

    fn inside<S: Into<String>, U: crate::segment::ToSegment + Clone>(&mut self, column: S, vals: Vec<U>) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.inside(column, vals);},
            AkitaType::Update => {self.update_wrapper.inside(column, vals);},
        }
        self
    }

    fn in_condition<S: Into<String>, U: crate::segment::ToSegment + Clone>(&mut self, condition: bool, column: S, vals: Vec<U>) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.in_condition(condition, column, vals);},
            AkitaType::Update => {self.update_wrapper.in_condition(condition, column, vals);},
        }
        self
    }

    fn first<S: Into<String>>(&mut self, sql: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.first(sql);},
            AkitaType::Update => {self.update_wrapper.first(sql);},
        }
        self
    }

    fn first_condition<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.first_condition(condition, sql);},
            AkitaType::Update => {self.update_wrapper.first_condition(condition, sql);},
        }
        self
    }

    fn last<S: Into<String>>(&mut self, sql: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.last(sql);},
            AkitaType::Update => {self.update_wrapper.last(sql);},
        }
        self
    }

    fn last_condition<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.last_condition(condition, sql);},
            AkitaType::Update => {self.update_wrapper.last_condition(condition, sql);},
        }
        self
    }

    fn comment<S: Into<String>>(&mut self, comment: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.comment(comment);},
            AkitaType::Update => {self.update_wrapper.comment(comment);},
        }
        self
    }

    fn comment_condition<S: Into<String>>(&mut self, condition: bool, comment: S) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.comment_condition(condition, comment);},
            AkitaType::Update => {self.update_wrapper.comment_condition(condition, comment);},
        }
        self
    }

    fn get_select_sql(&mut self) -> String {
        match self.akita_type {
            AkitaType::Query => self.query_wrapper.get_select_sql(),
            AkitaType::Update => self.update_wrapper.get_select_sql(),
        }
    }

    fn select(&mut self, columns: Vec<String>) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.select(columns);},
            AkitaType::Update => {self.update_wrapper.select(columns);},
        }
        self
    }

    fn append_sql_segments(&mut self, sql_segments: Vec<crate::Segment>) {
        match self.akita_type {
            AkitaType::Query => self.query_wrapper.append_sql_segments(sql_segments),
            AkitaType::Update => self.update_wrapper.append_sql_segments(sql_segments),
        }
    }

    fn do_it(&mut self, condition: bool, segments: Vec<crate::Segment>) -> &mut Self {
        match self.akita_type {
            AkitaType::Query => {self.query_wrapper.do_it(condition, segments);},
            AkitaType::Update => {self.update_wrapper.do_it(condition, segments);},
        }
        self
    }

    fn get_sql_segment(&mut self) -> String {
        match self.akita_type {
            AkitaType::Query => self.query_wrapper.get_sql_segment(),
            AkitaType::Update => self.update_wrapper.get_sql_segment(),
        }
    }
} 


pub fn query() -> Akita {
    Akita::from_query()
}

pub fn update() -> Akita {
    Akita::from_update()
}