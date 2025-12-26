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
use akita_core::{AkitaValue, FieldName, FieldType, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, QueryData, TableName, Wrapper};
use crate::driver::DriverType;
use crate::errors::AkitaError;
use crate::mapper::PaginationOptions;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder};

pub(crate) struct SqlServerBuilder {
    pub version: String, // "2008", "2012", "2016", "2019"
    pub quoted_identifier: bool,
    pub use_named_params: bool, // Whether to use named parameters @p1, @p2, etc
}

impl Default for SqlServerBuilder {
    fn default() -> Self {
        Self {
            version: "2016".to_string(),
            quoted_identifier: true,
            use_named_params: true,
        }
    }
}

impl SqlBuilder for SqlServerBuilder {
    fn dialect(&self) -> DatabaseDialect {
        DatabaseDialect::SQLServer
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        if self.quoted_identifier {
            format!("[{}]", identifier.replace(']', "]]"))
        } else {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
    }

    fn quote_table(&self, table: &str) -> String {
        // Quick check: If it already has quotes, just return
        if table.starts_with('[') && table.ends_with(']') {
            return table.to_string();
        }
        if table.starts_with('"') && table.ends_with('"') {
            return table.to_string();
        }

        // Segment and reference the parts
        table
            .split('.')
            .map(|part| {
                // If the part is already quoted, leave it as is
                if (part.starts_with('[') && part.ends_with(']'))
                    || (part.starts_with('"') && part.ends_with('"')) {
                    part.to_string()
                } else {
                    self.quote_identifier(part)
                }
            })
            .collect::<Vec<String>>()
            .join(".")
    }

    fn process_placeholders(&self, sql: &str) -> String {
        // tiberius uses @p1, @p2, @p3... Format
        let mut result = String::new();
        let mut param_index = 1;

        for ch in sql.chars() {
            if ch == '?' {
                // tiberius style parameter names: @p1, @p2...
                let param_name = format!("@p{}", param_index);
                result.push_str(&param_name);
                param_index += 1;
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// SQL Server specific INSERT SQL build - using column names as parameter names
    fn build_insert_sql(&self, table: &TableName, columns: Vec<FieldName>, datas: Vec<AkitaValue>) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if datas.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // Filter out autoincrement fields
        let column_names: Vec<(String, FieldName)> = columns.into_iter()
            .filter(|c| c.exist)
            .filter(|c| !c.is_auto_increment())  // 排除自增字段
            .map(|c| {
                let col_name = c.alias.as_ref().unwrap_or(&c.name);
                (self.quote_identifier(col_name), c)
            })
            .collect();

        if column_names.is_empty() {
            return Err(AkitaError::DatabaseError("No columns to insert after filtering auto-increment fields".into()));
        }

        // Use Tiberius-style parameter names: @p1, @p2, @p3...
        let mut placeholders = Vec::new();
        let mut params = Vec::new();

        for data in datas.into_iter() {
            for (i, (_col_name, field)) in column_names.iter().enumerate() {
                let col_name = field.alias.as_ref().unwrap_or(&field.name);

                // Tiberius Style parameter names
                let param_name = format!("@p{}", i + 1);
                placeholders.push(param_name);

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

        // Build the base INSERT SQL
        let column_names_str = column_names.iter()
            .map(|(c, _)| c.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote_table(&table.complete_name()),
            column_names_str,
            placeholders.join(", ")
        );

        Ok((sql, params))
    }

    /// Build bulk INSERT SQL - Generate unique parameter names for each row
    fn build_batch_insert_sql(
        &self,
        data: &BatchInsertData
    ) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if data.columns.is_empty() || data.rows.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // 1. 预计算：哪些列需要插入
        let mut valid_column_indices = Vec::new();
        let mut quoted_names = Vec::new();

        for (idx, col) in data.columns.iter().enumerate() {
            if col.exist && !col.is_auto_increment() {
                valid_column_indices.push(idx);
                let col_name = col.alias.as_ref().unwrap_or(&col.name);
                quoted_names.push(self.quote_identifier(col_name));
            }
        }

        if valid_column_indices.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // 2. 为每一行构建数据和占位符
        let mut all_params = Vec::with_capacity(valid_column_indices.len() * data.rows.len());
        let mut all_placeholders = Vec::with_capacity(data.rows.len());
        let mut param_num = 1;

        for row in &data.rows {
            let mut row_placeholders = Vec::with_capacity(valid_column_indices.len());
            let mut row_params = Vec::with_capacity(valid_column_indices.len());

            for &col_idx in &valid_column_indices {
                // 获取该列的值
                let value = row.get(col_idx)
                    .cloned()
                    .unwrap_or(AkitaValue::Null);

                row_placeholders.push(format!("@p{}", param_num));
                row_params.push(value);
                param_num += 1;
            }

            all_placeholders.push(format!("({})", row_placeholders.join(", ")));
            all_params.extend(row_params);
        }

        // 3. 构建完整 SQL
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&data.table.complete_name()),
            quoted_names.join(", "),
            all_placeholders.join(", ")
        );

        Ok((sql, all_params))
    }

    
    // SQL Server requires rewriting query construction (paging special)
    fn build_query_sql(&self, wrapper: &Wrapper) -> (String, Vec<AkitaValue>) {
        let data = wrapper.get_query_data();

        if data.from.is_none() {
            return ("".to_string(), vec![]);
        }

        let mut sql_parts = Vec::new();

        // SELECT part - SQL Server may use TOP
        let select_clause = if self.version < "2012".to_string() && data.limit.is_some() && data.offset.is_none() {
            //SQL Server 2008 uses TOP
            let limit = data.limit.unwrap();
            let columns = if data.select == "*" {
                "*".to_string()
            } else {
                self.build_column_list(&data.select)
            };

            if data.distinct {
                format!("SELECT DISTINCT TOP {} {}", limit, columns)
            } else {
                format!("SELECT TOP {} {}", limit, columns)
            }
        } else {
            self.build_select_clause(&data)
        };
        sql_parts.push(select_clause);

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

        // SQL Server 2012+ Pagination
        self.build_sqlserver_pagination(&mut sql_parts, &data);

        let sql = sql_parts.join(" ");
        let final_sql = self.process_placeholders(&sql);
        let params = wrapper.get_parameters();

        (final_sql, params)
    }

    fn build_delete_sql(&self, table: &TableName, wrapper: &Wrapper) -> String {
        let where_clause = wrapper.build_where_clause();
        let mut sql = if let Some(limit_val) = wrapper.get_limit() {
            let sql = format!("DELETE TOP({}) FROM {}", limit_val, self.quote_table(&table.complete_name()));
            sql
        } else {
            let sql = format!("DELETE FROM {}", self.quote_table(&table.complete_name()));
            sql
        };
        if !where_clause.trim().is_empty() {
            sql.push_str(&format!(" WHERE {}", self.build_where_clause(&where_clause)));
        }
        self.process_placeholders(&sql)
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
    
    // SQL Server special identifier handling
    fn build_column_list(&self, columns: &str) -> String {
        if columns == "*" {
            return "*".to_string();
        }

        columns.split(',')
            .map(|col| col.trim())
            .filter(|col| !col.is_empty())
            .map(|col| {
                // SQL Server to the AS processing
                if col.contains(" AS ") {
                    let parts: Vec<&str> = col.split(" AS ").collect();
                    if parts.len() == 2 {
                        return format!("{} AS {}",
                                       self.quote_identifier(parts[0].trim()),
                                       self.quote_identifier(parts[1].trim())
                        );
                    }
                }

                // Process the table.column
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
            "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP",
            "BEGIN", "BETWEEN", "BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK",
            "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT",
            "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT",
            "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT",
            "DELETE", "DENY", "DESC", "DISK", "DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP",
            "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS",
            "EXIT", "EXTERNAL", "FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT",
            "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING",
            "HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX",
            "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT",
            "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED",
            "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE",
            "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER",
            "PERCENT", "PIVOT", "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE",
            "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES",
            "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT",
            "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT",
            "SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE",
            "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME",
            "STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO",
            "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT", "TSEQUAL",
            "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES",
            "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE", "WITH", "WITHIN GROUP",
            "WRITETEXT"
        ];

        keywords.contains(&identifier.to_uppercase().as_str())
    }
}

impl SqlServerBuilder {

    /// Generate valid SQL Server parameter names
    fn make_param_name(&self, _column_name: &str, index: usize) -> String {
        // Basic rule: Prefixing column names with @ to handle special characters
        // tiberius 风格：@p1, @p2, @p3...
        format!("@p{}", index)
    }

    /// Normalize parameter names to remove special characters and ensure they are valid
    fn normalize_param_name(&self, name: &str) -> String {
        // tiberius is not strict with parameter names, but rather with well-formed parameters
        // Remove special characters and keep alphanumeric characters and underscores
        let mut result = String::new();

        for ch in name.chars() {
            if ch.is_alphanumeric() || ch == '_' {
                result.push(ch);
            } else {
                result.push('_');
            }
        }

        // Make sure not to start with a number (the parameter name @1 can be problematic in some cases)
        if result.chars().next().map_or(false, |c| c.is_numeric()) {
            format!("p{}", result)
        } else {
            result
        }
    }
    
    /// Generate parameters based on column names for query construction
    fn process_query_placeholders(&self, sql: &str, _column_names: &[&str]) -> String {
        self.process_placeholders(sql)
    }
    
    fn build_sqlserver_pagination(&self, sql_parts: &mut Vec<String>, data: &QueryData) {
        if self.version >= "2012".to_string() && (data.limit.is_some() || data.offset.is_some()) {
            // You must have an ORDER BY to use OFFSET
            if data.order_by.is_empty() {
                sql_parts.push("ORDER BY (SELECT NULL)".to_string());
            } else {
                sql_parts.push(format!("ORDER BY {}", self.build_order_by_clause(&data.order_by)));
            }

            // Handling OFFSET
            if let Some(offset) = data.offset {
                sql_parts.push(format!("OFFSET {} ROWS", offset));
            } else if data.limit.is_some() {
                // Only LIMIT with no OFFSET is OFFSET 0 ROWS
                sql_parts.push("OFFSET 0 ROWS".to_string());
            }

            // Handling FETCH
            if let Some(limit) = data.limit {
                sql_parts.push(format!("FETCH NEXT {} ROWS ONLY", limit));
            }
        } else if !data.order_by.is_empty() {
            // If it's not 2012+ or you don't need paging, add ORDER BY as normal
            sql_parts.push(format!("ORDER BY {}", self.build_order_by_clause(&data.order_by)));
        }
    }

    // SQL Server supports OUTPUT
    fn build_insert_returning(&self, _table: &str, id_column: &str) -> Option<String> {
        Some(format!(" OUTPUT INSERTED.{}", self.quote_identifier(id_column)))
    }
}

#[test]
fn test_mssql_sqlbuilder() {
    // Create the SQL Server builder
    let builder = SqlServerBuilder::default();

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
    println!("build_insert_sql mssql :{} \nparams:{}", sql, Params::Positional(params));
    

    // Example 2: Query
    let wrapper = Wrapper::new()
        .table("users")
        .eq("user_id", 1)
        .like("user_name", "%john%");

    let (query_sql, query_params) = builder.build_query_sql(&wrapper);
    println!("build_query_sql mssql :{} \n params:{}", query_sql, Params::Positional(query_params));

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
    println!("batch_sql mssql :{} \n params:{}", batch_sql, Params::Positional(batch_params));
}