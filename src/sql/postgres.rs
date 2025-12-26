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
use std::collections::HashSet;
use regex::Regex;
use akita_core::{AkitaValue, Condition, FieldName, FieldType, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, SqlOperator, TableName, Wrapper};
use crate::core::GLOBAL_GENERATOR;
use crate::driver::DriverType;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder};
use crate::errors::AkitaError;
use crate::key::IdentifierGenerator;
use crate::mapper::PaginationOptions;

pub struct PostgreSqlBuilder {
    pub version: Option<String>,
    pub use_std_conforming_strings: bool,
}

impl Default for PostgreSqlBuilder {
    fn default() -> Self {
        Self {
            version: None,
            use_std_conforming_strings: true,
        }
    }
}

impl SqlBuilder for PostgreSqlBuilder {
    fn dialect(&self) -> DatabaseDialect {
        DatabaseDialect::Postgres
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        let identifier = identifier.trim();
        
        // When quotation marks are required
        let needs_quotes =
            // 1. Contains uppercase letters
            identifier.chars().any(|c| c.is_uppercase()) ||
                // 2. Contains special characters (-, Spaces, etc.)
                identifier.contains('-') ||
                identifier.contains(' ') ||
                identifier.contains('$') || // $ Allowed in PostgreSQL, but sometimes quotes are required
                // 3. Start with a number
                identifier.chars().next().map_or(false, |c| c.is_numeric()) ||
                // 4. It's reserved keywords
                self.is_reserved_keyword(identifier) ||
                // 5. Contains other special characters
                identifier.chars().any(|c|
                    !c.is_alphanumeric() && c != '_' && c != '$'
                );

        if needs_quotes {
            // Escape the double quotes
            let escaped = identifier.replace('"', "\"\"");
            format!("\"{}\"", escaped)
        } else {
            identifier.to_string()
        }
    }

    fn quote_table(&self, table: &str) -> String {
        let table = table.trim();

        // Special case: System table
        if self.is_system_table(table) {
            return table.to_string();
        }

        // Split the dot, but consider the dot inside the quotation marks
        let parts = self.split_table_parts(table);

        parts.iter()
            .map(|part| self.quote_identifier_part(part))
            .collect::<Vec<String>>()
            .join(".")
    }

    fn process_placeholders(&self, sql: &str) -> String {
        // PostgreSQL uses the $1, $2, $3 placeholders
        let mut result = String::new();
        let mut counter = 1;

        for ch in sql.chars() {
            if ch == '?' {
                result.push_str(&format!("${}", counter));
                counter += 1;
            } else {
                result.push(ch);
            }
        }

        result
    }

    // PostgreSQL standard paging syntax
    fn build_pagination_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (Some(limit), Some(offset)) => format!("LIMIT {} OFFSET {}", limit, offset),
            (Some(limit), None) => format!("LIMIT {}", limit),
            (None, Some(offset)) => format!("OFFSET {}", offset),
            (None, None) => String::new(),
        }
    }

    fn build_insert_sql(&self, table: &TableName,columns: Vec<FieldName>, datas: Vec<AkitaValue>) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if columns.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // Building column names
        let column_names: Vec<(String, FieldName)> = columns.into_iter()
            .filter(|c| c.exist)
            .filter(|c| {
                !(c.is_auto_increment() && datas.iter().all(|data| {
                    let col_name = c.alias.as_ref().unwrap_or(&c.name);
                    data.get_obj_value(col_name)
                        .map_or(true, |v| v.is_null() || v.is_zero())
                }))
            })
            .map(|c| {
                
                let col_name = c.alias.as_ref().unwrap_or(&c.name);
                (self.quote_identifier(col_name), c)
            })
            .collect();
        // PostgreSQL uses the $1, $2, $3 placeholders
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for data in datas.into_iter() {
            for (i, (_col_name, field)) in column_names.iter().enumerate() {
                placeholders.push(format!("${}", i + 1));

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
                params.push(value);
            }
        }
        
        // Building INSERT SQL
        let column_names = column_names.iter().map(|(c, _)| c.to_string()).collect::<Vec<_>>();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote_table(&table.complete_name()),
            column_names.join(", "),
            placeholders.join(", ")
        );

        Ok((sql, params))
    }

    /// PostgreSQL Bulk Insert - Supports multi-line VALUES syntax
    fn build_batch_insert_sql(
        &self,
        data: &BatchInsertData
    ) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if data.columns.is_empty() || data.rows.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        let id_field_name = data.id_field.as_ref()
            .map(|f| f.alias.as_ref().unwrap_or(&f.name).to_string());
        
        // Building column names
        let (column_names, column_indices): (Vec<String>, Vec<usize>) = data.columns.iter()
            .enumerate()
            .filter(|(_, col)| {
                let col_name = col.alias.as_ref().unwrap_or(&col.name);
                // Excludes autoincrement fields and specified id fields
                !col.is_auto_increment() &&
                    id_field_name.as_ref().map_or(true, |id| col_name != id)
            })
            .map(|(idx, col)| {
                let col_name = col.alias.as_ref().unwrap_or(&col.name);
                (self.quote_identifier(col_name), idx)
            })
            .unzip();

        if column_names.is_empty() {
            return Err(AkitaError::EmptyData);
        }
        
        // Build multiple rows of VALUES
        let mut all_placeholders = Vec::new();
        let mut all_params = Vec::new();
        let cols_count = column_names.len();
        for row in data.rows.iter() {
            let mut row_placeholders = Vec::with_capacity(cols_count);
            let mut row_params = Vec::with_capacity(cols_count);

            for &col_idx in &column_indices {
                if col_idx < row.len() {
                    row_params.push(row[col_idx].clone());
                } else {
                    row_params.push(AkitaValue::Null);
                }
            }

            // Generate placeholders (each line is numbered individually)
            let start_idx = all_params.len() + 1;
            for i in 0..cols_count {
                row_placeholders.push(format!("${}", start_idx + i));
            }

            all_placeholders.push(format!("({})", row_placeholders.join(", ")));
            all_params.extend(row_params);
        }

        // Building SQL-PostgreSQL supports multi-line VALUES syntax
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&data.table.complete_name()),
            column_names.join(", "),
            all_placeholders.join(", ")
        );
        Ok((sql, all_params))
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
        Some(self.process_placeholders(&sql))
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

        self.process_placeholders(&sql)
    }
    
    // Unique to PostgreSQL, ILIKE is case insensitive
    fn build_where_clause(&self, where_clause: &str) -> String {
        // PostgreSQL supports TRUE/FALSE literals
        // Also replace LIKE with ILIKE (case insensitive)
        where_clause.replace(" LIKE ", " ILIKE ")
            .replace(" NOT LIKE ", " NOT ILIKE ")
    }
    
    fn build_column_list(&self, columns: &str) -> String {
        if columns == "*" {
            return "*".to_string();
        }

        columns.split(',')
            .map(|col| col.trim())
            .filter(|col| !col.is_empty())
            .map(|col| {
                if col.contains(" AS ") {
                    let parts: Vec<&str> = col.split(" AS ").collect();
                    if parts.len() == 2 {
                        return format!("{} AS {}",
                                       self.quote_identifier(parts[0].trim()),
                                       self.quote_identifier(parts[1].trim())
                        );
                    }
                }

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

    fn is_reserved_keyword(&self, identifier: &str) -> bool {
        let keywords = [
            "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
            "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST",
            "CHECK", "COLLATE", "COLUMN", "CONCURRENTLY", "CONSTRAINT",
            "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE",
            "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
            "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE",
            "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE",
            "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT",
            "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER",
            "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LEADING", "LEFT",
            "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT",
            "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER",
            "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES",
            "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR",
            "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE",
            "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE",
            "WHEN", "WHERE", "WINDOW", "WITH"
        ];

        keywords.contains(&identifier.to_uppercase().as_str())
    }
}

impl PostgreSqlBuilder {
    fn build_json_contains(&self, column: &str, json_path: &str, value: &str) -> String {
        // PostgreSQL JSON operators
        format!("{} #>> '{}' = '{}'",
                self.quote_identifier(column),
                json_path,
                value
        )
    }

    /// Unique to PostgreSQL, arrays contain queries
    fn build_array_contains(&self, column: &str, value: &str) -> String {
        format!("{} @> ARRAY['{}']",
                self.quote_identifier(column),
                value
        )
    }

    /// Unique to PostgreSQL: Generate sequential values
    pub fn build_sequence_nextval(&self, sequence_name: &str) -> String {
        format!("nextval('{}')", self.quote_identifier(sequence_name))
    }

    /// PostgreSQL specific: Generate a UUID
    pub fn build_uuid_generate(&self) -> String {
        "gen_random_uuid()".to_string()
    }

    /// Unique to PostgreSQL: time zone conversion
    pub fn build_timezone_conversion(&self, column: &str, from_tz: &str, to_tz: &str) -> String {
        format!(
            "{} AT TIME ZONE '{}' AT TIME ZONE '{}'",
            self.quote_identifier(column),
            from_tz,
            to_tz
        )
    }

    /// Unique to PostgreSQL: full-text search
    pub fn build_fulltext_search(&self, column: &str, query: &str) -> String {
        format!(
            "to_tsvector('english', {}) @@ to_tsquery('english', '{}')",
            self.quote_identifier(column),
            query.replace("'", "''")
        )
    }

    /// Unique to PostgreSQL: the window function
    pub fn build_window_function(
        &self,
        function: &str,
        column: &str,
        partition_by: Option<&[&str]>,
        order_by: Option<&[&str]>
    ) -> String {
        let mut window_spec = String::new();

        if let Some(partitions) = partition_by {
            let partition_clause = partitions.iter()
                .map(|col| self.quote_identifier(col))
                .collect::<Vec<_>>()
                .join(", ");
            window_spec.push_str(&format!("PARTITION BY {}", partition_clause));
        }

        if let Some(orders) = order_by {
            if !window_spec.is_empty() {
                window_spec.push(' ');
            }
            let order_clause = orders.iter()
                .map(|col| self.quote_identifier(col))
                .collect::<Vec<_>>()
                .join(", ");
            window_spec.push_str(&format!("ORDER BY {}", order_clause));
        }

        format!("{}({}) OVER ({})",
                function,
                self.quote_identifier(column),
                window_spec
        )
    }

    // PostgreSQL has support for RETURNING
    fn build_insert_returning(&self, _table: &str, id_column: &str) -> Option<String> {
        Some(format!(" RETURNING {}", self.quote_identifier(id_column)))
    }

    fn split_table_parts(&self, table: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut escape_next = false;

        for ch in table.chars() {
            if escape_next {
                current.push(ch);
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => {
                    escape_next = true;
                    current.push(ch);
                },
                '"' => {
                    in_quotes = !in_quotes;
                    current.push(ch);
                },
                '.' if !in_quotes => {
                    parts.push(current);
                    current = String::new();
                },
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        parts
    }

    fn quote_identifier_part(&self, part: &str) -> String {
        let part = part.trim();

        // If you already have full double quotes, leave them as they are
        if part.starts_with('"') && part.ends_with('"') {
            // Check that the quotes are paired correctly
            let inner = &part[1..part.len()-1];
            if !inner.contains('"') || inner.matches('"').count() % 2 == 0 {
                return part.to_string();
            }
        }

        self.quote_identifier(part)
    }

    fn is_system_table(&self, table: &str) -> bool {
        let lower_table = table.to_lowercase();
        lower_table.starts_with("pg_catalog.") ||
            lower_table.starts_with("information_schema.") ||
            lower_table.starts_with("pg_toast.") ||
            lower_table.starts_with("pg_temp.")
    }

}


#[test]
#[cfg(feature = "postgres-sync")]
fn test_postgres_sqlbuilder() {
    // Create the postgre builder
    let builder = PostgreSqlBuilder::default();

    // Example 1: Single-line insertion
    let field_id = FieldName {
        name: "user_id".to_string(),
        table: "user".to_string().into(),
        alias: None,
        exist: true,
        select: false,
        fill: None,
        field_type: FieldType::TableId(IdentifierType::Auto),
    };
    let columns = vec![
        field_id.clone(),
        FieldName::from("user_name"),
        FieldName::from("email_address"),
    ];
    let mut imap = indexmap::IndexMap::new();
    imap.insert("id".to_string(), AkitaValue::Int(1));
    imap.insert("user_name".to_string(), AkitaValue::Text("John".to_string()));
    imap.insert("email_address".to_string(), AkitaValue::Text("john@example.com".to_string()));
    let data = AkitaValue::Object(imap);

    let (sql, params) = builder.build_insert_sql(&TableName::from("users"), columns, vec![data]).unwrap();
    println!("build_insert_sql postgres :{} \nparams:{}", sql, Params::Positional(params));

    // Example 2: Query
    let wrapper = Wrapper::new()
        .table("users")
        .eq("user_id", 1)
        .like("user_name", "%john%");

    let (query_sql, query_params) = builder.build_query_sql(&wrapper);
    println!("build_query_sql postgres :{} \n params:{}", query_sql, Params::Positional(query_params));

    // Example 3: Bulk insertion
    let columns = vec![field_id, FieldName::from("user_name")];
    let rows = vec![
        vec![AkitaValue::Int(1), AkitaValue::Text("John".to_string())],
        vec![AkitaValue::Int(2), AkitaValue::Text("Jane".to_string())],
    ];
    let batch_data = BatchInsertData {
        table: TableName::from("users"),
        columns,
        rows,
        id_field: None,
    };
    let (batch_sql, batch_params) = builder.build_batch_insert_sql(&batch_data).unwrap();
    println!("batch_sql postgres :{} \n params:{}", batch_sql, Params::Positional(batch_params));
}