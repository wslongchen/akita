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
use akita_core::{AkitaValue, FieldName, FieldType, IdentifierType, Params, TableName, Wrapper};
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder};
use crate::errors::AkitaError;

pub struct SqliteBuilder {
    pub version: Option<String>,
}

impl Default for SqliteBuilder {
    fn default() -> Self {
        Self {
            version: None,
        }
    }
}

impl SqlBuilder for SqliteBuilder {
    fn dialect(&self) -> DatabaseDialect {
        DatabaseDialect::SQLite
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn quote_table(&self, table: &str) -> String {
        // SQLite's special database name
        let special_dbs = ["main", "temp"];

        let parts: Vec<&str> = table.split('.').collect();

        match parts.len() {
            1 => self.quote_identifier(parts[0]),
            2 => {
                let db = parts[0];
                let tbl = parts[1];

                // Special database names (main, temp) usually don't need quotes
                let quoted_db = if special_dbs.contains(&db) {
                    db.to_string()
                } else {
                    self.quote_identifier(db)
                };

                let quoted_tbl = self.quote_identifier(tbl);
                format!("{}.{}", quoted_db, quoted_tbl)
            },
            _ => {
                // FTS5 Virtual table: table.column
                parts.iter()
                    .map(|part| self.quote_identifier(part))
                    .collect::<Vec<String>>()
                    .join(".")
            }
        }
    }

    fn process_placeholders(&self, sql: &str) -> String {
        sql.to_string() // Using SQLite? Placeholders
    }

    // SQLite uses mysqL-style paging
    fn build_pagination_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (Some(limit), Some(offset)) => format!("LIMIT {} OFFSET {}", limit, offset),
            (Some(limit), None) => format!("LIMIT {}", limit),
            (None, Some(offset)) => format!("LIMIT -1 OFFSET {}", offset),
            (None, None) => String::new(),
        }
    }

    fn build_insert_sql(&self, table: &TableName, columns: Vec<FieldName>, datas: Vec<AkitaValue>) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if columns.is_empty() {
            return Err(AkitaError::EmptyData);
        }
        
        // Building column names
        let column_names: Vec<(String, FieldName)> = columns.into_iter()
            .filter(|c| c.exist)
            .map(|c| {
                let col_name = c.alias.as_ref().unwrap_or(&c.name);
                (self.quote_identifier(col_name), c)
            })
            .collect();

        // Use column names as parameter names, such as @id, @name
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

        // Building INSERT SQL
        let column_names = column_names.iter().map(|(c, _)| c.to_string()).collect::<Vec<_>>();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&table.complete_name()),
            column_names.join(", "),
            placeholders.join(", ")
        );
        Ok((sql, params))
    }

    /// SQLite bulk insert
    fn build_batch_insert_sql(
        &self,
        data: &BatchInsertData
    ) -> crate::errors::Result<(String, Vec<AkitaValue>)> {
        if data.columns.is_empty() || data.rows.is_empty() {
            return Err(AkitaError::EmptyData);
        }

        // Building column names
        let column_names: Vec<String> = data.columns.iter()
            .map(|col_name| {
                let col_name = col_name.alias.as_ref().unwrap_or(&col_name.name).as_str();
                self.quote_identifier(col_name)
            })
            .collect();
        
        // Build multiple rows of VALUES
        let mut all_placeholders = Vec::new();
        let mut all_params = Vec::new();

        for row in data.rows.iter() {
            let row_placeholders: Vec<String> = row.iter()
                .map(|_| "?".to_string())
                .collect();

            all_placeholders.push(format!("({})", row_placeholders.join(", ")));
            all_params.extend(row.clone());
        }

        // Building SQL-SQLite supports multi-line VALUES syntax
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_table(&data.table.complete_name()),
            column_names.join(", "),
            all_placeholders.join(", ")
        );
        Ok((sql, all_params))
    }

    fn build_delete_sql(&self, table: &TableName, wrapper: &Wrapper) -> String {
        let where_clause = wrapper.build_where_clause();
        if let Some(limit_val) = wrapper.get_limit() {
            let mut sql = format!("DELETE FROM {}", self.quote_table(&table.complete_name()));
            if !where_clause.trim().is_empty() {
                sql.push_str(&format!(" WHERE {}", self.build_where_clause(&where_clause)));
            }
            // SQLite's DELETE... LIMIT needs to be ordered BY
            // Here we default to ROWID sort
            sql.push_str(&format!(" ORDER BY ROWID LIMIT {}", limit_val));
            sql
        } else {
            let mut sql = format!("DELETE FROM {}", self.quote_table(&table.complete_name()));
            if !where_clause.trim().is_empty() {
                // Handle identifiers in WHERE clauses
                let processed_where = self.build_where_clause(&where_clause);
                sql.push_str(&format!(" WHERE {}", processed_where));
            }
            sql
        }
    }
    
    /// SQLite specific: COLLATE NOCASE support
    fn build_where_clause(&self, where_clause: &str) -> String {
        // SQLite is case sensitive by default, and the COLLATE NOCASE option can be added
        // An option is provided for the user to choose whether to add it or not
        if self.should_use_nocase() {
            // Add the COLLATE NOCASE after the LIKE operation
            let mut result = String::new();
            let mut in_like = false;
            let mut buffer = String::new();

            for ch in where_clause.chars() {
                buffer.push(ch);

                if buffer.ends_with(" LIKE ") {
                    in_like = true;
                } else if in_like && (ch == '?' || ch == '\'') {
                    // Add a COLLATE NOCASE before the value after LIKE
                    result.push_str(" COLLATE NOCASE");
                    in_like = false;
                }

                result.push(ch);
            }

            result
        } else {
            where_clause.to_string()
        }
    }
    

    fn is_reserved_keyword(&self, identifier: &str) -> bool {
        let keywords = [
            "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
            "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE",
            "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE",
            "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT",
            "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH",
            "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FOR",
            "FOREIGN", "FROM", "FULL", "GLOB", "GROUP", "HAVING", "IF", "IGNORE", "IMMEDIATE",
            "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT",
            "INTO", "IS", "ISNULL", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL",
            "NO", "NOT", "NOTNULL", "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER",
            "PLAN", "PRAGMA", "PRIMARY", "QUERY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP",
            "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW",
            "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", "TRANSACTION",
            "TRIGGER", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL",
            "WHEN", "WHERE", "WITH", "WITHOUT"
        ];

        keywords.contains(&identifier.to_uppercase().as_str())
    }
}

impl SqliteBuilder {
    /// SQLite specific: JSON support (requires JSON1 extension enabled)
    fn build_json_extract(&self, column: &str, json_path: &str) -> String {
        format!("json_extract({}, '{}')",
                self.quote_identifier(column),
                json_path
        )
    }

    /// SQLite specific: Full-text search (requires FTS extension)
    fn build_fulltext_search(&self, fts_table: &str, _column: &str, query: &str) -> Option<String> {
        // FTS syntax for SQLite
        Some(format!(
            "{} MATCH '{}'",
            self.quote_identifier(fts_table),
            query.replace("'", "''")
        ))
    }

    /// Whether COLLATE NOCASE should be used
    fn should_use_nocase(&self) -> bool {
        // This can be based on configuration or version
        // SQLite's default behavior is case-sensitive
        false
    }

    /// SQLite specific: Get the last inserted rowid
    pub fn build_last_insert_rowid(&self) -> String {
        "last_insert_rowid()".to_string()
    }

    /// SQLite specific: Generate random UUids (requires extension enabled)
    pub fn build_uuid_generate(&self) -> Option<String> {
        // SQLite does not have a built-in UUID function
        // You can return NULL or use an extension
        Some("NULL".to_string())
    }

    /// SQLite specific: date-time functions
    pub fn build_datetime_function(&self, modifier: &str) -> String {
        match modifier {
            "now" => "datetime('now')".to_string(),
            "date" => "date('now')".to_string(),
            "time" => "time('now')".to_string(),
            "unixepoch" => "strftime('%s', 'now')".to_string(),
            _ => format!("datetime('now', '{}')", modifier),
        }
    }

    /// SQLite specific: Build virtual table queries
    pub fn build_virtual_table_query(&self, module: &str, args: &[&str]) -> String {
        let args_str = args.iter()
            .map(|arg| format!("'{}'", arg.replace("'", "''")))
            .collect::<Vec<_>>()
            .join(", ");

        format!("CREATE VIRTUAL TABLE temp.vt USING {} ({})", module, args_str)
    }

    // SQLite 3.35+ supports RETURNING
    fn build_insert_returning(&self, _table: &str, id_column: &str) -> Option<String> {
        if let Some(version) = &self.version {
            if version >= &"3.35.0".to_string() {
                return Some(format!(" RETURNING {}", self.quote_identifier(id_column)));
            }
        }
        None
    }

}


#[test]
#[cfg(feature = "sqlite-sync")]
fn test_sqlite_sqlbuilder() {
    // Create the sqlite builder
    let builder = SqliteBuilder::default();

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
    println!("build_insert_sql sqlite :{} \nparams:{}", sql, Params::Positional(params));

    // Example 2: Query
    let wrapper = Wrapper::new()
        .table("users")
        .eq("user_id", 1)
        .like("user_name", "%john%");

    let (query_sql, query_params) = builder.build_query_sql(&wrapper);
    println!("build_query_sql sqlite :{} \n params:{}", query_sql, Params::Positional(query_params));

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
    println!("batch_sql sqlite :{} \n params:{}", batch_sql, Params::Positional(batch_params));
}