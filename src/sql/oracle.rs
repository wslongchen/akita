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
use akita_core::{AkitaValue, Condition, FieldName, FieldType, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, QueryData, TableName, Wrapper};
use crate::driver::DriverType;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder};
use crate::errors::AkitaError;
use crate::mapper::PaginationOptions;

pub struct OracleSqlBuilder {
    pub version: Option<String>, // "11g", "12c", "19c"
    pub use_ansi_quotes: bool,
}

impl Default for OracleSqlBuilder {
    fn default() -> Self {
        Self {
            version: Some("12c".to_string()),
            use_ansi_quotes: false,
        }
    }
}

impl SqlBuilder for OracleSqlBuilder {
    fn dialect(&self) -> DatabaseDialect {
        DatabaseDialect::Oracle
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        let uppercase_id = identifier.to_uppercase();

        if self.is_reserved_keyword(&uppercase_id) || identifier.chars().any(|c| c.is_lowercase()) {
            if self.use_ansi_quotes {
                format!("\"{}\"", uppercase_id)
            } else {
                format!("\"{}\"", uppercase_id)
            }
        } else {
            uppercase_id
        }
    }

    fn quote_table(&self, table: &str) -> String {
        // Split the dot and process each part separately
        table
            .split('.')
            .map(|part| self.quote_identifier(part))
            .collect::<Vec<String>>()
            .join(".")
    }

    fn process_placeholders(&self, sql: &str) -> String {
        // Oracle uses :1, :2, :3 placeholders
        let mut result = String::new();
        let mut counter = 1;

        for ch in sql.chars() {
            if ch == '?' {
                result.push_str(&format!(":{}", counter));
                counter += 1;
            } else {
                result.push(ch);
            }
        }

        result
    }

    // Oracle requires complete rewrite of query construction (paging special)
    fn build_query_sql(&self, wrapper: &Wrapper) -> (String, Vec<AkitaValue>) {
        let wrapper = self.quote_wrapper_identifier(wrapper.clone());
        let data = wrapper.get_query_data();

        if data.from.is_none() {
            return ("".to_string(), vec![]);
        }

        // Building the base query (without paging)
        let base_sql = self.build_inner_query_sql(&data);

        // Oracle pagination
        let final_sql = if data.limit.is_some() || data.offset.is_some() {
            self.build_oracle_pagination(&base_sql, &data)
        } else {
            base_sql
        };

        let processed_sql = self.process_placeholders(&final_sql);
        let params = wrapper.get_parameters();

        (processed_sql, params)
    }

    fn build_delete_sql(&self, table: &TableName, wrapper: &Wrapper) -> String {
        let wrapper = self.quote_wrapper_identifier(wrapper.clone());
        let where_clause = wrapper.build_where_clause();
        let sql = if let Some(limit_val) = wrapper.get_limit() {
            if where_clause.trim().is_empty() {
                // If you don't have a WHERE condition, you need to add a ROWNUM
                format!("DELETE FROM {} WHERE ROWNUM <= {}",
                        self.quote_table(&table.complete_name()),
                        limit_val
                )
            } else {
                // When there is a WHERE condition, use a subquery
                format!(
                    "DELETE FROM {} WHERE ROWID IN (
                        SELECT ROWID FROM {} WHERE {} AND ROWNUM <= {}
                    )",
                    self.quote_table(&table.complete_name()),
                    self.quote_table(&table.complete_name()),
                    self.build_where_clause(&where_clause),
                    limit_val
                )
            }
        } else {
            let mut sql = format!("DELETE FROM {}", self.quote_table(&table.complete_name()));
            if !where_clause.trim().is_empty() {
                // Handle identifiers in WHERE clauses
                let processed_where = self.build_where_clause(&where_clause);
                sql.push_str(&format!(" WHERE {}", processed_where));
            }
            sql
        };
        self.process_placeholders(&sql)
    }

    fn build_update_sql(&self, table: &TableName, wrapper: &Wrapper) -> Option<String> {
        let wrapper = self.quote_wrapper_identifier(wrapper.clone());
        let set_clause = wrapper.build_set_clause();
        let where_clause = wrapper.build_where_clause();
        let mut sql = format!("UPDATE {} SET {}", self.quote_table(&table.complete_name()), set_clause);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if let Some(limit_val) = wrapper.get_limit() {
            sql.push_str(&format!(" LIMIT {}", limit_val));
        }
        Some(self.process_placeholders(&sql))
    }
    
    // Oracle does not have a Boolean type
    fn build_where_clause(&self, where_clause: &str) -> String {
        where_clause.replace("TRUE", "1").replace("FALSE", "0")
    }

    // Oracle needs to handle double table inserts
    fn build_insert_sql(&self, table: &TableName, columns: Vec<FieldName>, datas: Vec<AkitaValue>) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if columns.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // Build column names (uppercase for Oracle)
        let column_names: Vec<(String, FieldName)> = columns.into_iter()
            .filter(|c| c.exist)
            .filter(|c| !c.is_auto_increment())
            .map(|c| {
                let col_name = c.alias.as_ref().unwrap_or(&c.name);
                (self.quote_identifier(col_name), c)
            })
            .collect();
        // Construct placeholders for single-line insertions :1, :2, :3
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for data in datas.into_iter() {
            for (i, (_col_name, field)) in column_names.iter().enumerate() {
                placeholders.push(format!(":{}", i + 1));

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

        // Build a single-row INSERT SQL
        let column_names = column_names.iter().map(|(c, _)| c.to_string()).collect::<Vec<_>>();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote_table(&table.complete_name()),
            column_names.join(", "),
            placeholders.join(", ")
        );
        Ok((sql, params))
    }


    /// Oracle Bulk INSERT - uses the INSERT ALL syntax
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
        
        let mut sql_parts = Vec::new();
        let mut all_params = Vec::new();
        // Oracle uses the INSERT ALL syntax for bulk insertion
        sql_parts.push("INSERT ALL".to_string());
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
                row_placeholders.push(format!(":{}", start_idx + i));
            }
            all_params.extend(row_params);
            sql_parts.push(format!(
                "INTO {} ({}) VALUES ({})",
                self.quote_table(&data.table.complete_name()),
                column_names.join(", "),
                row_placeholders.join(", ")
            ));
        }

        sql_parts.push("SELECT 1 FROM DUAL".to_string());

        let sql = sql_parts.join("\n");
        Ok((sql, all_params))
    }

    fn is_reserved_keyword(&self, identifier: &str) -> bool {
        let keywords = [
            "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN",
            "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT",
            "CREATE", "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT",
            "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT",
            "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX",
            "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE",
            "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MLSLABEL", "MODE", "MODIFY", "NOAUDIT",
            "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE",
            "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW",
            "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS", "SELECT",
            "SESSION", "SET", "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM",
            "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE",
            "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE",
            "WITH"
        ];

        keywords.contains(&identifier.to_uppercase().as_str())
    }
}

impl OracleSqlBuilder {

    /// Optional: Support Oracle named parameters
    fn process_named_placeholders(&self, sql: &str, param_names: &[&str]) -> String {
        let mut result = String::new();
        let mut param_index = 0;

        for ch in sql.chars() {
            if ch == '?' {
                if param_index < param_names.len() {
                    // Use meaningful parameter names such as :user_id, :user_name
                    let param_name = self.normalize_oracle_param_name(param_names[param_index]);
                    result.push_str(&format!(":{}", param_name));
                } else {
                    // No name; use positional arguments
                    result.push_str(&format!(":p{}", param_index + 1));
                }
                param_index += 1;
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Normalize Oracle parameter names
    fn normalize_oracle_param_name(&self, name: &str) -> String {
        let mut result = String::new();

        for ch in name.chars() {
            if ch.is_alphanumeric() || ch == '_' {
                result.push(ch.to_lowercase().next().unwrap_or(ch));
            } else if ch == '-' || ch == ' ' {
                result.push('_');
            }
            // Other special characters are ignored
        }

        // Make sure you don't start with a number
        if result.chars().next().map_or(false, |c| c.is_numeric()) {
            format!("p_{}", result)
        } else {
            result
        }
    }
    
    fn quote_wrapper_identifier(&self, mut wrapper: Wrapper) -> Wrapper {
        // 1. Handle column names in the WHERE condition
        let quoted_where_conditions = wrapper
            .get_where_conditions()
            .iter()
            .map(|cond| {
                let mut condition = cond.clone();
                condition.column = self.quote_identifier(&condition.column);
                condition
            })
            .collect();
        wrapper.where_conditions(quoted_where_conditions);

        // 2. Handling column names in a SET operation (UPDATE statement)
        let quoted_set_operations = wrapper
            .get_set_operations()
            .iter()
            .map(|opt| {
                let mut operation = opt.clone();
                operation.column = self.quote_identifier(&operation.column);
                operation
            })
            .collect();
        wrapper.set_operations(quoted_set_operations);

        // 3. Handle SELECT column names
        let select_columns = wrapper.get_select_columns();
        let quoted_select_columns: Vec<String> = select_columns
            .iter()
            .map(|col| {
                // Handle columns that may contain aliases, such as" column as alias" or "table.column"
                self.quote_identifier(col)
            })
            .collect();
        wrapper = wrapper.select(quoted_select_columns);

        // 4. Process the GROUP BY column name
        let group_by_columns = wrapper.get_group_by();
        let quoted_group_by_columns: Vec<String> = group_by_columns
            .iter()
            .map(|col| self.quote_identifier(col))
            .collect();
        wrapper = wrapper.group_by(quoted_group_by_columns);

        // 5. Process column names in the ORDER BY clause
        let order_by_clauses = wrapper.get_order_by_clauses();
        let quoted_order_by_clauses = order_by_clauses
            .iter()
            .map(|order_by| {
                let mut new_order_by = order_by.clone();
                new_order_by.column = self.quote_identifier(&order_by.column);
                new_order_by
            })
            .collect();
        wrapper.order_by_clauses(quoted_order_by_clauses);

        // 6. Handle column names in the HAVING condition
        let having_conditions = wrapper.get_having_conditions();
        let quoted_having_conditions = having_conditions
            .iter()
            .map(|cond| {
                let mut condition = cond.clone();
                condition.column = self.quote_identifier(&condition.column);
                condition
            })
            .collect();
        wrapper.having_conditions(quoted_having_conditions);

        // 7. Process column names in the JOIN clause
        let join_clauses = wrapper.get_join_clauses();
        let quoted_join_clauses = join_clauses
            .iter()
            .map(|join| {
                let mut new_join = join.clone();

                //Handle JOIN table names
                new_join.table = self.quote_table(&join.table);

                // Handle column names in the JOIN condition
                let mut new_condition = new_join.condition;
                new_condition.column = self.quote_identifier(&new_condition.column);
                new_join.condition = new_condition;
                new_join
            })
            .collect();
        wrapper.join_clauses(quoted_join_clauses);

        // 8. Handle table names and aliases
        if let Some(table) = wrapper.get_table().cloned() {
            wrapper = wrapper.table(self.quote_table(&table));
        }

        // 9. Handle column names in the APPLY condition (if present)
        let quoted_apply_conditions: Vec<String> = wrapper.get_apply_conditions()
            .iter()
            .map(|cond| {
                self.quote_identifier(cond)
            })
            .collect();
        wrapper.apply_conditions(quoted_apply_conditions);
        wrapper
    }
    
    fn build_inner_query_sql(&self, data: &QueryData) -> String {
        let mut sql_parts = Vec::new();

        sql_parts.push(self.build_select_clause(data));
        sql_parts.push(format!("FROM {}", self.build_from_clause(data.from.as_ref().unwrap())));

        let joins = self.build_join_clauses(&data.joins);
        if !joins.is_empty() {
            sql_parts.push(joins);
        }

        if !data.where_clause.is_empty() {
            sql_parts.push(format!("WHERE {}", self.build_where_clause(&data.where_clause)));
        }

        if !data.group_by.is_empty() {
            sql_parts.push(format!("GROUP BY {}", self.build_group_by_clause(&data.group_by)));
        }

        if !data.having.is_empty() {
            sql_parts.push(format!("HAVING {}", self.build_having_clause(&data.having)));
        }

        sql_parts.join(" ")
    }

    fn build_oracle_pagination(&self, base_sql: &str, data: &QueryData) -> String {
        let limit = data.limit.unwrap_or(u64::MAX);
        let offset = data.offset.unwrap_or(0);

        // For the special case where limit=0
        if limit == 0 {
            return format!("SELECT * FROM ({}) WHERE 1=0", base_sql);
        }

        if let Some(version) = &self.version {
            if version.starts_with("12") || version.starts_with("19") || version.starts_with("21") {
                // Oracle 12c+ uses standard paging
                let mut sql = base_sql.to_string();

                // Add ORDER BY (if needed)
                if !data.order_by.is_empty() && !base_sql.to_uppercase().contains("ORDER BY") {
                    sql.push_str(&format!(" ORDER BY {}", self.build_order_by_clause(&data.order_by)));
                }

                if offset > 0 {
                    sql.push_str(&format!(" OFFSET {} ROWS", offset));
                }

                if limit < u64::MAX {
                    if offset > 0 {
                        sql.push_str(&format!(" FETCH NEXT {} ROWS ONLY", limit));
                    } else {
                        sql.push_str(&format!(" FETCH FIRST {} ROWS ONLY", limit));
                    }
                }

                return sql;
            }
        }

        // Oracle 11g and below uses ROWNUM
        // Note: ORDER BY is required to have a definite paging order
        if !data.order_by.is_empty() && !base_sql.to_uppercase().contains("ORDER BY") {
            let ordered_sql = format!("{} ORDER BY {}", base_sql, self.build_order_by_clause(&data.order_by));
            format!(
                "SELECT * FROM (
                    SELECT t.*, ROWNUM r FROM ({}) t 
                    WHERE ROWNUM <= {}
                ) WHERE r > {}",
                ordered_sql, offset + limit, offset
            )
        } else {
            format!(
                "SELECT * FROM (
                    SELECT t.*, ROWNUM r FROM ({}) t 
                    WHERE ROWNUM <= {}
                ) WHERE r > {}",
                base_sql, offset + limit, offset
            )
        }
    }

    // Oracle supports RETURNING INTO
    fn build_insert_returning(&self, _table: &str, id_column: &str) -> Option<String> {
        Some(format!(" RETURNING {} INTO :{}",
                     self.quote_identifier(id_column),
                     id_column.to_lowercase()
        ))
    }

    fn quote_dblink_table(&self, table: &str, at_pos: usize) -> String {
        let object = &table[..at_pos];
        let dblink = &table[at_pos + 1..];

        let quoted_object = if object.contains('.') {
            self.quote_table(object) // Recursive processing
        } else {
            self.quote_identifier_with_context(object)
        };

        // Database links are usually not quoted and are uppercase
        let quoted_dblink = if dblink.chars().any(|c| c.is_lowercase()) && self.use_ansi_quotes {
            format!("\"{}\"", dblink.to_uppercase())
        } else {
            dblink.to_uppercase()
        };

        format!("{}@{}", quoted_object, quoted_dblink)
    }

    fn quote_identifier_with_context(&self, identifier: &str) -> String {
        let trimmed = identifier.trim();

        // If there are already quotes, leave them as they are
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            return trimmed.to_string();
        }

        let uppercase = trimmed.to_uppercase();

        // Decide if quotation marks are needed
        let needs_quotes =
            // 1. It's reserved keywords
            self.is_reserved_keyword(&uppercase) ||
                // 2. Contains lowercase letters (case sensitive)
                trimmed.chars().any(|c| c.is_lowercase()) ||
                // 3. Contains special characters (except $, #, _)
                trimmed.chars().any(|c|
                    !c.is_alphanumeric() && c != '$' && c != '#' && c != '_'
                ) ||
                // 4. ANSI mode enforces quotation marks
                self.use_ansi_quotes;

        if needs_quotes {
            if self.use_ansi_quotes {
                format!("\"{}\"", uppercase)
            } else {
                format!("\"{}\"", trimmed) // Keep the case as is
            }
        } else {
            uppercase
        }
    }
}


#[test]
#[cfg(feature = "oracle-sync")]
fn test_oracle_sqlbuilder() {
    // Create the oracle builder
    let builder = OracleSqlBuilder::default();

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
    println!("build_insert_sql oracle :{} \nparams:{}", sql, Params::Positional(params));

    // Example 2: Query
    let wrapper = Wrapper::new()
        .table("users")
        .eq("user_id", 1)
        .like("user_name", "%john%");

    let (query_sql, query_params) = builder.build_query_sql(&wrapper);
    println!("build_query_sql oracle :{} \n params:{}", query_sql, Params::Positional(query_params));

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
    println!("batch_sql oracle :{} \n params:{}", batch_sql, Params::Positional(batch_params));
}