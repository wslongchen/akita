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

use std::hash::{Hasher, Hash};

use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::{types::SqlType, comm::keywords_safe, Value};

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
        if name.contains('.') {
            let splinters = name.split('.').collect::<Vec<&str>>();
            assert!(splinters.len() == 2, "There should only be 2 parts");
            let schema = splinters[0].to_owned();
            let table = splinters[1].to_owned();
            TableName {
                schema: Some(schema),
                name: table,
                alias: None,
            }
        } else {
            TableName {
                schema: None,
                name: name.to_owned(),
                alias: None,
            }
        }
    }

    pub fn name(&self) -> String { self.name.to_string() }

    pub fn safe_name(&self) -> String { keywords_safe(&self.name) }

    /// return the long name of the table using schema.table_name
    pub fn complete_name(&self) -> String {
        match self.schema {
            Some(ref schema) => format!("{}.{}", schema, self.name),
            None => self.name.to_owned(),
        }
    }

    pub fn safe_complete_name(&self) -> String {
        match self.schema {
            Some(ref schema) => format!("{}.{}", schema, self.safe_name()),
            None => self.name.to_owned(),
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
    pub value: Option<Value>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum IdentifierType {
    Auto,
    Input,
    None,
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
            _=> Self::None,
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

    pub fn safe_complete_name(&self) -> String {
        match self.table {
            Some(ref table) => format!("{}.{}", keywords_safe(table), self.name),
            None => self.name.to_owned(),
        }
    }

    /// 判断是否主键
    pub fn is_table_id(&self) -> bool {
        match self.field_type {
            FieldType::TableId(_) => true,
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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub table: TableName,
    pub name: FieldName,
    pub comment: Option<String>,
    pub specification: ColumnSpecification,
    pub stat: Option<ColumnStat>,
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
    //most_common_values: Value,//top 5 most common values
    pub n_distinct: f32, // the number of distinct values of these column
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