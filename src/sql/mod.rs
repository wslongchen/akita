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
use akita_core::{cfg_if, AkitaValue, FieldName, FieldType, GetFields, GetTableName, IdentifierType, IntoAkitaValue, QueryData, TableName, Wrapper};
use crate::core::GLOBAL_GENERATOR;
use crate::driver::DriverType;
use crate::errors::{AkitaError, Result};
use crate::key::IdentifierGenerator;
use crate::mapper::PaginationOptions;
use std::fmt;


cfg_if! {
    if #[cfg(any(feature = "mysql-async", feature = "mysql-sync"))] {
        mod mysql;
        use crate::sql::mysql::MySqlBuilder;
    }
}

cfg_if! {
    if #[cfg(any(feature = "postgres-async", feature = "postgres-sync"))] {
        mod postgres;
        use crate::sql::postgres::PostgreSqlBuilder;
    }
}

cfg_if! {
    if #[cfg(any(feature = "oracle-async", feature = "oracle-sync"))] {
        mod oracle;
        use crate::sql::oracle::OracleSqlBuilder;
    }
}

cfg_if! {
    if #[cfg(any(feature = "sqlite-async", feature = "sqlite-sync"))] {
        mod sqlite;
        use crate::sql::sqlite::SqliteBuilder;
    }
}

cfg_if! {
    if #[cfg(any(feature = "mssql-async", feature = "mssql-sync"))] {
        mod mssql;
        use crate::sql::mssql::SqlServerBuilder;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseDialect {
    MySQL,
    Postgres,
    Oracle,
    SQLServer,
    SQLite,
}

pub trait SqlBuilder: Send + Sync {
    // ========== Core methods (must be implemented) ==========
    fn dialect(&self) -> DatabaseDialect;
    fn quote_identifier(&self, identifier: &str) -> String;
    fn quote_table(&self, table: &str) -> String;
    fn process_placeholders(&self, sql: &str) -> String;

    // ========== SQL build method (with default implementation) ==========

    /// Build query SQL - Default implementation (MySQL style)
    fn build_query_sql(&self, wrapper: &Wrapper) -> (String, Vec<AkitaValue>) {
        let data = wrapper.get_query_data();

        if data.from.is_none() {
            return ("".to_string(), vec![]);
        }

        let mut sql_parts = Vec::new();

        // SELECT section
        sql_parts.push(self.build_select_clause(&data));

        // FROM part
        sql_parts.push(format!("FROM {}", self.build_from_clause(data.from.as_ref().unwrap())));

        // JOIN part
        let joins = self.build_join_clauses(&data.joins);
        if !joins.is_empty() {
            sql_parts.push(joins);
        }

        // The WHERE section
        if !data.where_clause.is_empty() {
            sql_parts.push(format!("WHERE {}", self.build_where_clause(&data.where_clause)));
        }

        // GROUP BY part
        if !data.group_by.is_empty() {
            sql_parts.push(format!("GROUP BY {}", self.build_group_by_clause(&data.group_by)));
        }

        // The HAVING part
        if !data.having.is_empty() {
            sql_parts.push(format!("HAVING {}", self.build_having_clause(&data.having)));
        }

        // ORDER BY part
        if !data.order_by.is_empty() {
            sql_parts.push(format!("ORDER BY {}", self.build_order_by_clause(&data.order_by)));
        }

        // Pagination section
        let pagination = self.build_pagination_clause(data.limit, data.offset);
        if !pagination.is_empty() {
            sql_parts.push(pagination);
        }

        // Building a Complete SQL
        let sql = sql_parts.join(" ");
        let final_sql = self.process_placeholders(&sql);
        let params = wrapper.get_parameters();

        (final_sql, params)
    }

    /// Build COUNT SQL-default implementation
    fn build_count_sql(&self, wrapper: &Wrapper) -> String {
        let data = wrapper.get_query_data();

        if data.from.is_none() {
            return "".to_string();
        }

        let mut sql = format!("SELECT COUNT(*) FROM {}", self.build_from_clause(data.from.as_ref().unwrap()));

        if !data.where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", self.build_where_clause(&data.where_clause)));
        }

        self.process_placeholders(&sql)
    }

    /// Building INSERT SQL
    fn build_insert_sql(&self, table: &TableName, columns: Vec<FieldName>, datas: Vec<AkitaValue>) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if datas.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // Filter the fields to be inserted
        if columns.is_empty() {
            return Err(AkitaError::InvalidSQL("No columns to insert".to_string()));
        }

        // Building column names
        let column_names: Vec<(String, FieldName)> = columns.into_iter()
            .filter(|c| c.exist)
            .map(|c| {
                let col_name = c.alias.as_ref().unwrap_or(&c.name);
                (self.quote_identifier(col_name), c)
            })
            .collect();

        // Build the VALUES and parameters
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for data in datas.into_iter() {
            let mut entity_placeholders = Vec::new();
            for (_col_name, field) in column_names.iter() {
                let col_name = field.alias.as_ref().unwrap_or(&field.name);
                let mut value = data.get_obj_value(col_name)
                    .cloned()
                    .unwrap_or(AkitaValue::Null);
                // Handling field padding
                if let Some(fill) = &field.fill {
                    match fill.mode.as_str() {
                        "insert" | "default" => {
                            value = fill.value.clone().unwrap_or_default();
                        }
                        _ => {}
                    }
                }

                // Handle the ID generator
                value = self.identifier_generator_value(field, value);

                entity_placeholders.push("?".to_string());
                params.push(value);
            }
            placeholders.push(format!("({})", entity_placeholders.join(", ")));
        }

        // Building a Complete SQL
        let column_names = column_names.iter().map(|(c, _)| c.to_string()).collect::<Vec<_>>();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&table.complete_name()),
            column_names.join(", "),
            placeholders.join(", ")
        );

        Ok((sql, params))
    }

    fn build_delete_sql(&self, table: &TableName, wrapper: &Wrapper) -> String {
        let mut sql = format!("DELETE FROM {}", self.quote_table(&table.complete_name()));
        let where_clause = wrapper.build_where_clause();

        if !where_clause.trim().is_empty() {
            // Handle identifiers in WHERE clauses
            let processed_where = self.build_where_clause(&where_clause);
            sql.push_str(&format!(" WHERE {}", processed_where));
        }

        if let Some(limit_val) = wrapper.get_limit() {
            sql.push_str(&format!(" LIMIT {}", limit_val));
        }

        sql
    }

    fn build_update_sql(&self, table: &TableName, wrapper: &Wrapper) -> Option<String> {
        let set_clause = wrapper.build_set_clause();
        let where_clause = wrapper.build_where_clause();
        let mut sql = format!("UPDATE {} SET {}", &table.complete_name(), set_clause);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if let Some(limit_val) = wrapper.get_limit() {
            sql.push_str(&format!(" LIMIT {}", limit_val));
        }
        Some(sql)
    }
    
    // ========== SQL fragment construction method (with default implementation) ==========

    /// Build the SELECT clause
    fn build_select_clause(&self, data: &QueryData) -> String {
        let select_keyword = if data.distinct { "SELECT DISTINCT" } else { "SELECT" };

        if data.select == "*" {
            format!("{} *", select_keyword)
        } else {
            let columns = self.build_column_list(&data.select);
            format!("{} {}", select_keyword, columns)
        }
    }

    /// Construct the FROM clause
    fn build_from_clause(&self, from: &str) -> String {
        if from.contains(" AS ") {
            let parts: Vec<&str> = from.split(" AS ").collect();
            if parts.len() == 2 {
                return format!("{} AS {}",
                               self.quote_identifier(parts[0].trim()),
                               self.quote_identifier(parts[1].trim())
                );
            }
        } else if from.contains(' ') {
            let parts: Vec<&str> = from.split_whitespace().collect();
            if parts.len() == 2 {
                // Implicit aliases: table alias
                return format!("{} {}",
                               self.quote_identifier(parts[0]),
                               self.quote_identifier(parts[1])
                );
            }
        }

        self.quote_table(from)
    }

    /// Construct the JOIN clause
    fn build_join_clauses(&self, joins: &[String]) -> String {
        joins.iter()
            .map(|join| self.build_single_join_clause(join))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Build a single JOIN clause
    fn build_single_join_clause(&self, join: &str) -> String {
        // Format: JOIN_TYPE table [AS alias] ON condition
        let parts: Vec<&str> = join.split_whitespace().collect();
        if parts.len() < 4 {
            return join.to_string();
        }

        let join_type = parts[0];
        let join_keyword = parts[1];
        let mut i = 2;
        let mut table_part = String::new();

        // Collect table names and aliases
        while i < parts.len() && parts[i].to_uppercase() != "ON" {
            table_part.push_str(parts[i]);
            table_part.push(' ');
            i += 1;
        }

        let processed_table = self.build_from_clause(table_part.trim());
        let mut result = format!("{} {} {}", join_type, join_keyword, processed_table);

        // Add an ON condition
        if i < parts.len() && parts[i].to_uppercase() == "ON" {
            result.push_str(" ON ");
            i += 1;

            let condition_parts = &parts[i..];
            let condition = condition_parts.join(" ");
            result.push_str(&self.build_join_condition(&condition));
        }

        result
    }

    /// Construct JOIN conditions
    fn build_join_condition(&self, condition: &str) -> String {
        // Simple: Assume the format is table.column = table.column
        condition.to_string()
    }

    /// Build a WHERE clause
    fn build_where_clause(&self, where_clause: &str) -> String {
        where_clause.to_string() // 默认不处理，由Wrapper生成
    }

    /// Construct a GROUP BY clause
    fn build_group_by_clause(&self, group_by: &str) -> String {
        if group_by.trim().is_empty() {
            return String::new();
        }

        group_by.split(',')
            .map(|col| col.trim())
            .filter(|col| !col.is_empty())
            .map(|col| self.quote_identifier(col))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Construct a HAVING clause
    fn build_having_clause(&self, having: &str) -> String {
        having.to_string() // 默认与WHERE相同
    }

    /// Build the ORDER BY clause
    fn build_order_by_clause(&self, order_by: &str) -> String {
        if order_by.trim().is_empty() {
            return String::new();
        }

        order_by.split(',')
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .map(|item| {
                let parts: Vec<&str> = item.split_whitespace().collect();
                match parts.len() {
                    1 => format!("{} ASC", self.quote_identifier(parts[0])),
                    2 => {
                        let direction = if parts[1].to_uppercase() == "DESC" {
                            "DESC"
                        } else {
                            "ASC"
                        };
                        format!("{} {}", self.quote_identifier(parts[0]), direction)
                    }
                    _ => item.to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build paging clauses - default MySQL style
    fn build_pagination_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (Some(limit), Some(offset)) => format!("LIMIT {} OFFSET {}", limit, offset),
            (Some(limit), None) => format!("LIMIT {}", limit),
            (None, Some(offset)) => format!("LIMIT 18446744073709551615 OFFSET {}", offset),
            (None, None) => String::new(),
        }
    }

    /// Building a list of fields
    fn build_column_list(&self, columns: &str) -> String {
        if columns == "*" {
            return "*".to_string();
        }

        columns.split(',')
            .map(|col| col.trim())
            .filter(|col| !col.is_empty())
            .map(|col| {
                // Handling aliases
                if col.contains(" AS ") {
                    let parts: Vec<&str> = col.split(" AS ").collect();
                    if parts.len() == 2 {
                        return format!("{} AS {}",
                                       self.quote_identifier(parts[0].trim()),
                                       self.quote_identifier(parts[1].trim())
                        );
                    }
                }

                // Handle table names. Field names
                if col.contains('.') {
                    let parts: Vec<&str> = col.split('.').collect();
                    if parts.len() == 2 {
                        return format!("{}.{}",
                                       self.quote_identifier(parts[0]),
                                       self.quote_identifier(parts[1])
                        );
                    }
                }

                self.quote_identifier(col)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    // ========== Helper methods ==========

    /// Handle the ID generator
    fn identifier_generator_value(&self, field_name: &FieldName, mut value: AkitaValue) -> AkitaValue {
        if !field_name.is_table_id() {
            return value;
        }
        if let Some(id_type) = field_name.get_table_id_type() {
            match id_type {
                IdentifierType::Auto => {
                    if matches!(value, AkitaValue::Null) {
                        return value;
                    }
                }
                IdentifierType::AssignId => {
                    let id = GLOBAL_GENERATOR.next_id();
                    value = match value {
                        AkitaValue::Text(_) => AkitaValue::Text(id.to_string()),
                        AkitaValue::Bigint(_) => AkitaValue::Bigint(id as i64),
                        AkitaValue::Int(_) => AkitaValue::Int(id as i32),
                        _ => AkitaValue::Text(id.to_string()),
                    };
                }
                IdentifierType::AssignUuid => {
                    let uuid = GLOBAL_GENERATOR.next_uuid();
                    value = AkitaValue::Text(uuid);
                }
                IdentifierType::None => {
                    value = AkitaValue::Null;
                }
                IdentifierType::Input => {
                    
                }
            }
        }
        value
    }

    /// Finding the ID field
    fn find_id_field(&self, fields: Vec<FieldName>) -> Option<FieldName> {
        fields.into_iter()
            .find(|field| matches!(field.field_type, FieldType::TableId(_)))
    }

    /// Check if it is a reserved keyword
    fn is_reserved_keyword(&self, _identifier: &str) -> bool {
        false // Default not checked
    }

    /// Build bulk INSERT SQL
    fn build_batch_insert_sql(
        &self,
        data: &BatchInsertData
    ) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if data.columns.is_empty() || data.rows.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        let column_names: Vec<String> = data.columns.iter()
            .map(|col_name| {
                let col_name = col_name.alias.as_ref().unwrap_or(&col_name.name).as_str();
                self.quote_identifier(col_name) 
            })
            .collect();

        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for row in data.rows.iter() {
            let row_placeholders: Vec<String> = row.iter()
                .map(|_| "?".to_string())
                .collect();

            placeholders.push(format!("({})", row_placeholders.join(", ")));
            params.extend(row.clone());
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&data.table.complete_name()),
            column_names.join(", "),
            placeholders.join(", ")
        );
        Ok((sql, params))
    }
}


pub struct SqlBuilderFactory;

impl SqlBuilderFactory {
    pub fn create(dialect: DatabaseDialect) -> Box<dyn SqlBuilder> {
        match dialect {
            #[cfg(any(feature = "mysql-sync", feature = "mysql-async"))]
            DatabaseDialect::MySQL => Box::new(MySqlBuilder::default()),
            #[cfg(any(feature = "postgres-sync", feature = "postgres-async"))]
            DatabaseDialect::Postgres => Box::new(PostgreSqlBuilder::default()),
            #[cfg(any(feature = "oracle-sync", feature = "oracle-async"))]
            DatabaseDialect::Oracle => Box::new(OracleSqlBuilder::default()),
            #[cfg(any(feature = "mssql-sync", feature = "mssql-async"))]
            DatabaseDialect::SQLServer => Box::new(SqlServerBuilder::default()),
            #[cfg(any(feature = "sqlite-sync", feature = "sqlite-async"))]
            DatabaseDialect::SQLite => Box::new(SqliteBuilder::default()),
            _ => {
                panic!("Unsupport Database")
            }
        }
    }

    pub fn create_with_version(dialect: DatabaseDialect, version: &str) -> Box<dyn SqlBuilder> {
        match dialect {
            #[cfg(any(feature = "mysql-sync", feature = "mysql-async"))]
            DatabaseDialect::MySQL => Box::new(MySqlBuilder {
                version: Some(version.to_string()),
            }),
            #[cfg(any(feature = "postgres-sync", feature = "postgres-async"))]
            DatabaseDialect::Postgres => Box::new(PostgreSqlBuilder {
                version: Some(version.to_string()),
                use_std_conforming_strings: true,
            }),
            #[cfg(any(feature = "oracle-sync", feature = "oracle-async"))]
            DatabaseDialect::Oracle => Box::new(OracleSqlBuilder {
                version: Some(version.to_string()),
                use_ansi_quotes: false,
            }),
            #[cfg(any(feature = "mssql-sync", feature = "mssql-async"))]
            DatabaseDialect::SQLServer => Box::new(SqlServerBuilder {
                version: version.to_string(),
                quoted_identifier: true,
                use_named_params: true,
            }),
            #[cfg(any(feature = "sqlite-sync", feature = "sqlite-async"))]
            DatabaseDialect::SQLite => Box::new(SqliteBuilder {
                version: Some(version.to_string()),
            }),
            _=> {
                panic!("Unsupport Database")
            }
        }
    }
}

pub struct BatchInsertData {
    pub table: TableName,
    pub columns: Vec<FieldName>,
    pub rows: Vec<Vec<AkitaValue>>,
    pub id_field: Option<FieldName>,
}
