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
use std::hash::{Hasher, Hash};
use regex::Regex;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::{types::SqlType, AkitaValue};

/// Table

pub trait GetTableName {
    /// extract the table name from a struct
    fn table_name() -> TableName;
}

pub trait GetFields {
    /// extract the columns from struct
    fn fields() -> Vec<FieldName>;
}

pub trait Table {
    /// extract the table name from a struct
    fn table_name() -> TableName;

     /// extract the columns from struct
     fn fields() -> Vec<FieldName>;
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableName {
    /// table name
    pub name: String,
    /// table of schema
    pub schema: Option<String>,
    /// table alias
    pub alias: Option<String>,
    pub ignore_interceptors: HashSet<String>,
}

impl Default for TableName {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            schema: None,
            alias: None,
            ignore_interceptors: Default::default(),
        }
    }
}

impl Hash for TableName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
    }
}

impl TableName {
    /// create table with name
    pub fn from(name: &str) -> Self {
        let name = name.trim();

        // Separate aliases (if any)
        let (table_part, alias) = Self::split_table_and_alias(name);

        // Separate schema and table names
        let (schema, name) = Self::split_schema_and_table(&table_part);

        TableName {
            name,
            schema,
            alias,
            ignore_interceptors: HashSet::new(),
        }
    }

    pub fn parse_table_name(sql: &str) -> TableName {
        let tables = TableName::parse_from_sql(sql);
        // Store the first table
        if let Some(first_table) = tables.first() {
            first_table.clone()
        } else {
            TableName::default()
        }

    }
    
    pub fn name(&self) -> String { self.name.to_string() }

    pub fn parse_from_sql(sql: &str) -> Vec<TableName> {
        let normalized_sql = Self::normalize_sql(sql);

        // Try parsing different types of SQL statements
        if let Some(tables) = Self::parse_insert_update_delete(&normalized_sql) {
            return tables;
        }

        if let Some(tables) = Self::parse_select(&normalized_sql) {
            return tables;
        }

        if let Some(tables) = Self::parse_ddl(&normalized_sql) {
            return tables;
        }

        vec![]
    }

    /// return the long name of the table using schema.table_name
    pub fn complete_name(&self) -> String {
        match self.schema {
            Some(ref schema) => format!("{}.{}", schema, self.name),
            None => self.name.to_owned(),
        }
    }

    pub fn sql_reference(&self) -> String {
        let full_name = self.complete_name();

        if let Some(alias) = &self.alias {
            format!("{} AS {}", full_name, alias)
        } else {
            full_name
        }
    }

    pub fn equals_ignore_alias(&self, other: &TableName) -> bool {
        self.name == other.name && self.schema == other.schema
    }

    fn normalize_sql(sql: &str) -> String {
        let sql = sql.trim();

        // Remove a one-line comment
        let re_comment = Regex::new(r"--.*$|/\*.*?\*/").unwrap();
        let sql = re_comment.replace_all(sql, "");

        // Replace multiple whitespace characters with a single space
        let re_whitespace = Regex::new(r"\s+").unwrap();
        re_whitespace.replace_all(&sql, " ").to_string()
    }

    fn parse_insert_update_delete(sql: &str) -> Option<Vec<TableName>> {
        let patterns = [
            // INSERT INTO/INSERT
            (r"(?i)^\s*INSERT\s+(?:INTO\s+)?(\S+)", "INSERT"),
            // UPDATE
            (r"(?i)^\s*UPDATE\s+(\S+)", "UPDATE"),
            // DELETE FROM/DELETE
            (r"(?i)^\s*DELETE\s+(?:FROM\s+)?(\S+)", "DELETE"),
        ];

        for (pattern, _) in patterns {
            if let Some(caps) = Regex::new(pattern).unwrap().captures(sql) {
                let table_expr = &caps[1];
                // Remove possible semicolons
                let table_expr = table_expr.split(';').next().unwrap_or(table_expr);
                return Some(vec![TableName::from(table_expr)]);
            }
        }

        None
    }

    fn parse_select(sql: &str) -> Option<Vec<TableName>> {
        let re_from = Regex::new(r"(?i)FROM\s+([^;]+?)(?:\s+(?:WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET))?(?:;|$)").unwrap();

        if let Some(caps) = re_from.captures(sql) {
            let from_clause = &caps[1];

            let tables = Self::split_table_list(from_clause);

            if !tables.is_empty() {
                return Some(tables.into_iter()
                    .map(|v| TableName::from(v.as_str()))
                    .collect());
            }
        }

        None
    }

    /// Parse DDL statements（CREATE/DROP/ALTER TABLE）
    fn parse_ddl(sql: &str) -> Option<Vec<TableName>> {
        let pattern = r"(?i)^\s*(?:CREATE|DROP|ALTER|TRUNCATE|RENAME)\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(\S+)";

        if let Some(caps) = Regex::new(pattern).unwrap().captures(sql) {
            let table_expr = &caps[1];
            let table_expr = table_expr.split(';').next().unwrap_or(table_expr);
            return Some(vec![TableName::from(table_expr)]);
        }

        None
    }

    /// Split table list (handles commas and joins)
    fn split_table_list(from_clause: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let mut current = String::new();
        let mut paren_depth = 0;

        for ch in from_clause.chars() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                ',' if paren_depth == 0 => {
                    if !current.trim().is_empty() {
                        tables.push(current.trim().to_string());
                    }
                    current.clear();
                    continue;
                }
                _ => {}
            }
            current.push(ch);
        }

        if !current.trim().is_empty() {
            tables.push(current.trim().to_string());
        }

        tables
    }

    /// Separation table and aliases
    fn split_table_and_alias(s: &str) -> (String, Option<String>) {
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.len() >= 3 && parts[1].to_uppercase() == "AS" {
            // Format: table AS alias
            (parts[0].to_string(), Some(parts[2].to_string()))
        } else if parts.len() >= 2 {
            // Format: table alias(Implicit aliases)
            (parts[0].to_string(), Some(parts[1].to_string()))
        } else {
            // There are no aliases
            (s.to_string(), None)
        }
    }

    /// Separate schema and table names
    fn split_schema_and_table(s: &str) -> (Option<String>, String) {
        let parts: Vec<&str> = s.split('.').collect();

        match parts.len() {
            1 => (None, parts[0].to_string()),  // table
            2 => (Some(parts[0].to_string()), parts[1].to_string()),  // schema.table
            _ => {
                // For situations like db.schema.table, take the last two parts
                let len = parts.len();
                (Some(parts[len-2].to_string()), parts[len-1].to_string())
            }
        }
    }

}

/// Field

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct FieldName {
    pub name: String,
    pub table: Option<String>,
    pub alias: Option<String>,
    /// exist in actual table
    pub exist: bool,
    pub select: bool,
    pub fill: Option<Fill>,
    pub field_type: FieldType,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub mode: String,
    pub value: Option<AkitaValue>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum IdentifierType {
    Auto,
    Input,
    AssignId,
    AssignUuid
}

impl IdentifierType {
    pub fn from_str(ident: &str) -> Self {
        let ident = ident.to_lowercase();
        match ident.as_str() {
            "auto" => Self::Auto,
            "input" => Self::Input,
            "assign_id" => Self::AssignId,
            "assign_uuid" => Self::AssignUuid,
            _=> Self::Auto,
        }

    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum FieldType {
    TableId(IdentifierType),
    TableField
}

impl FieldName {
    /// create table with name
    pub fn from(arg: &str) -> Self {
        if arg.contains('.') {
            let splinters = arg.split('.').collect::<Vec<&str>>();
            assert!(
                splinters.len() == 2,
                "There should only be 2 parts, trying to split `.` {}",
                arg
            );
            let table = splinters[0].to_owned();
            let name = splinters[1].to_owned();
            FieldName {
                name,
                table: Some(table),
                alias: None,
                exist: true,
                select: true,
                fill: None,
                field_type: FieldType::TableField,
            }
        } else {
            FieldName {
                name: arg.to_owned(),
                table: None,
                alias: None,
                exist: true,
                select: true,
                fill: None,
                field_type: FieldType::TableField,
            }
        }
    }

    /// return the long name of the table using schema.table_name
    pub fn complete_name(&self) -> String {
        match self.table {
            Some(ref table) => format!("{}.{}", table, self.name),
            None => self.name.to_owned(),
        }
    }

    pub fn name(&self) -> String {
        self.name.to_owned()
    }

    /// 判断是否主键
    pub fn is_table_id(&self) -> bool {
        match self.field_type {
            FieldType::TableId(_) => true,
            FieldType::TableField => false,
        }
    }
    
    pub fn is_auto_increment(&self) -> bool {
        match &self.field_type {
            FieldType::TableId(id_type) => { 
                match id_type {
                    IdentifierType::Auto => {
                        true
                    }
                    _ => false,
                }
            },
            FieldType::TableField => false,
        }
    }

    /// 获取主键类型
    pub fn get_table_id_type(&self) -> Option<&IdentifierType> {
        match &self.field_type {
            FieldType::TableId(id_type) => Some(id_type),
            FieldType::TableField => None,
        }
    }
}





#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: TableName,

    /// comment of this table
    pub comment: Option<String>,

    /// columns of this table
    pub columns: Vec<ColumnInfo>,

    /// views can also be generated
    pub is_view: bool,

    pub table_key: Vec<TableKey>,
}

impl TableInfo {
    pub fn name(&self) -> String {
        self.name.name()
    }
    pub fn comment(&self) -> Option<String> {
        self.comment.to_owned()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub table: TableName,
    pub name: FieldName,
    pub comment: Option<String>,
    pub specification: ColumnSpecification,
    pub stat: Option<ColumnStat>,
}

impl ColumnInfo {
    pub fn name(&self) -> String {
        self.name.name()
    }
    pub fn comment(&self) -> Option<String> {
        self.comment.to_owned()
    }
    pub fn data_type(&self) -> String {
        self.specification.sql_type.as_string()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnSpecification {
    pub sql_type: SqlType,
    pub capacity: Option<Capacity>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Capacity {
    Limit(i32),
    Range(i32, i32),
}

impl Capacity {
    fn get_limit(&self) -> Option<i32> {
        match *self {
            Capacity::Limit(limit) => Some(limit),
            Capacity::Range(_whole, _decimal) => None,
        }
    }

    pub fn sql_format(&self) -> String {
        match *self {
            Capacity::Limit(limit) => format!("({})", limit),
            Capacity::Range(_whole, _decimal) => format!("({}, {})", _whole, _decimal),
        }
    }

}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ColumnConstraint {
    NotNull,
    DefaultValue(Literal),
    /// the string contains the sequence name of this serial column
    AutoIncrement(Option<String>),
}

impl ColumnConstraint {
    pub fn sql_format(&self) -> String {
        match self {
            ColumnConstraint::NotNull => "not null".into(),
            ColumnConstraint::DefaultValue(v) => v.sql_format(), 
            ColumnConstraint::AutoIncrement(_) => "auto_increment".into(),
        }
    }
}


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Literal {
    Bool(bool),
    Null,
    Integer(i64),
    Double(f64),
    UuidGenerateV4, // pg: uuid_generate_v4();
    Uuid(Uuid),
    String(String),
    Blob(Vec<u8>),
    CurrentTime,      // pg: now()
    CurrentDate,      //pg: today()
    CurrentTimestamp, // pg: now()
    ArrayInt(Vec<i64>),
    ArrayFloat(Vec<f64>),
    ArrayString(Vec<String>),
}

impl Literal {
    pub fn sql_format(&self) -> String {
        match self {
            Literal::Bool(v) => v.to_string(),
            Literal::Integer(v) => v.to_string(),
            Literal::Double(v) => v.to_string(),
            Literal::Uuid(v) => v.to_string(),
            Literal::String(v) => v.to_owned(),
            Literal::Blob(v) => String::from_utf8(v.to_owned()).unwrap_or_default(),
            Literal::CurrentTime => "now()".to_string(),
            Literal::CurrentDate => "now()".to_string(),
            _ => String::default(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnStat {
    pub avg_width: i32, /* average width of the column, (the number of characters) */
    //most_common_AkitaValues: AkitaValue,//top 5 most common AkitaValues
    pub n_distinct: f32, // the number of distinct AkitaValues of these column
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<String> for Literal {
    fn from(s: String) -> Self {
        Literal::String(s)
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(s: &'a str) -> Self {
        Literal::String(String::from(s))
    }
}


impl ColumnSpecification {
    pub fn get_limit(&self) -> Option<i32> {
        match self.capacity {
            Some(ref capacity) => capacity.get_limit(),
            None => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Key {
    pub name: Option<String>,
    pub columns: Vec<FieldName>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: Option<String>,
    // the local columns of this table local column = foreign_column
    pub columns: Vec<FieldName>,
    // referred foreign table
    pub foreign_table: TableName,
    // referred column of the foreign table
    // this is most likely the primary key of the table in context
    pub referred_columns: Vec<FieldName>,
}


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey(Key),
    UniqueKey(Key),
    Key(Key),
    ForeignKey(ForeignKey),
}

impl TableKey {
    pub fn is_pri(&self) -> bool {
        match self {
            TableKey::PrimaryKey(_) => true,
            _ => false
        }
    }
}

#[derive(Debug)]
pub struct SchemaContent {
    pub schema: String,
    pub tablenames: Vec<TableName>,
    pub views: Vec<TableName>,
}

#[allow(unused)]
pub struct DatabaseName {
    pub name: String,
    pub description: Option<String>,
}