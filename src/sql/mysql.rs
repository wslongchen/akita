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
use akita_core::{AkitaValue, FieldName, FieldType, GetFields, GetTableName, IdentifierType, IntoAkitaValue, Params, TableName, Wrapper};
use crate::driver::DriverType;
use crate::sql::{BatchInsertData, DatabaseDialect, SqlBuilder};
use crate::errors::AkitaError;
use crate::mapper::PaginationOptions;

pub struct MySqlBuilder {
    pub version: Option<String>,
}

impl Default for MySqlBuilder {
    fn default() -> Self {
        Self {
            version: None,
        }
    }
}

impl MySqlBuilder {
    // MySQL 8.0+ supports RETURNING
    fn build_insert_returning(&self, _table: &str, id_column: &str) -> Option<String> {
        if let Some(version) = &self.version {
            if version.starts_with("8.") {
                return Some(format!(" RETURNING {}", self.quote_identifier(id_column)));
            }
        }
        None
    }
}


impl SqlBuilder for MySqlBuilder {
    fn dialect(&self) -> DatabaseDialect {
        DatabaseDialect::MySQL
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn quote_table(&self, table: &str) -> String {
        // Remove whitespace at both ends
        let table = table.trim();

        // If the reference is already in full form, it is returned
        if table.starts_with('`') && table.ends_with('`') {
            // Check whether there is a bit sign inside (indicating a split)
            let inner = &table[1..table.len()-1];
            if !inner.contains('.') {
                return table.to_string();
            }
            // If it has a sign, it needs to be split
        }

        // Separate the parts and refer to them separately
        table
            .split('.')
            .map(|part| {
                let part = part.trim();
                // If the part is already quoted, leave it as is
                if part.starts_with('`') && part.ends_with('`') {
                    part.to_string()
                } else if part.is_empty() {
                    // Handle the case of consecutive points
                    "``".to_string()
                } else {
                    self.quote_identifier(part)
                }
            })
            .collect::<Vec<String>>()
            .join(".")
    }

    fn process_placeholders(&self, sql: &str) -> String {
        sql.to_string() // MySQL使用?占位符
    }

    // Rewrite the paging method for MySQL compatibility
    fn build_pagination_clause(&self, limit: Option<u64>, offset: Option<u64>) -> String {
        match (limit, offset) {
            (Some(limit), Some(offset)) => format!("LIMIT {} OFFSET {}", limit, offset),
            (Some(limit), None) => format!("LIMIT {}", limit),
            (None, Some(offset)) => format!("LIMIT 18446744073709551615 OFFSET {}", offset),
            (None, None) => String::new(),
        }
    }

    // Mysql-specific Boolean value handling
    fn build_where_clause(&self, where_clause: &str) -> String {
        where_clause.replace("TRUE", "1").replace("FALSE", "0")
    }
}


#[test]
#[cfg(feature = "mysql-sync")]
fn test_mysql_sqlbuilder() {
    // Create a Mysql builder
    let builder = MySqlBuilder::default();

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
    println!("build_insert_sql mysql :{} \nparams:{}", sql, Params::Positional(params));

    // Example 2: Query
    let wrapper = Wrapper::new()
        .table("users")
        .eq("user_id", 1)
        .like("user_name", "%john%");

    let (query_sql, query_params) = builder.build_query_sql(&wrapper);
    println!("build_query_sql mysql :{} \n params:{}", query_sql, Params::Positional(query_params));

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
    println!("batch_sql mysql :{} \n params:{}", batch_sql, Params::Positional(batch_params));
}