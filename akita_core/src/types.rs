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
use regex::Regex;
use serde::{Deserialize, Serialize};
use crate::value::{Array, AkitaValue};


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Real,
    Float,
    Double,
    Numeric,
    Tinyblob,
    Mediumblob,
    Blob,
    Longblob,
    Varbinary,
    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,
    Json,
    TsVector,
    Uuid,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Interval,
    IpAddress,
    Point,
    // enum list with the choices value
    Enum(String, Vec<String>),
    Array(Box<SqlType>),
}

impl SqlType {
    pub fn is_array_type(&self) -> bool {
        match *self {
            SqlType::Array(_) => true,
            _ => false,
        }
    }

    pub fn is_integer_type(&self) -> bool {
        match *self {
            SqlType::Int => true,
            SqlType::Tinyint => true,
            SqlType::Smallint => true,
            SqlType::Bigint => true,
            _ => false,
        }
    }

    pub fn is_decimal_type(&self) -> bool {
        match *self {
            SqlType::Real => true,
            SqlType::Float => true,
            SqlType::Double => true,
            SqlType::Numeric => true,
            _ => false,
        }
    }

    pub fn cast_as(&self) -> Option<SqlType> {
        match *self {
            SqlType::TsVector => Some(SqlType::Text),
            _ => None,
        }
    }

    pub fn name(&self) -> String {
        match *self {
            SqlType::Text => "text".into(),
            SqlType::TsVector => "tsvector".into(),
            SqlType::Array(ref ty) => match ty.as_ref() {
                SqlType::Text => "text[]".into(),
                _ => panic!("not yet dealt {:?}", self),
            },
            _ => panic!("not yet dealt {:?}", self),
        }
    }

    pub fn as_string(&self) -> String {
        match *self {
            SqlType::Text => "text".into(),
            SqlType::Bool => "bool".into(),
            SqlType::Tinyint => "tinyint".into(),
            SqlType::Smallint => "smallint".into(),
            SqlType::Int => "int".into(),
            SqlType::Bigint => "bigint".into(),
            SqlType::Real => "real".into(),
            SqlType::Float => "float".into(),
            SqlType::Double => "double".into(),
            SqlType::Numeric => "numeric".into(),
            SqlType::Tinyblob => "tinyblob".into(),
            SqlType::Mediumblob => "mediumblob".into(),
            SqlType::Blob => "blob".into(),
            SqlType::Longblob => "longblob".into(),
            SqlType::Varbinary => "varbinary".into(),
            SqlType::Char => "char".into(),
            SqlType::Varchar => "varchar".into(),
            SqlType::Tinytext => "tinytext".into(),
            SqlType::Mediumtext => "mediumtext".into(),
            SqlType::Json => "json".into(),
            SqlType::Date => "date".into(),
            SqlType::Timestamp => "timestamp".into(),
            SqlType::TimestampTz => "timestamp".into(),
            SqlType::Time => "time".into(),
            SqlType::TimeTz => "time".into(),
            SqlType::Point => "point".into(),
            SqlType::Enum(_, _) => "enum".into(),
            _ => String::default(),
        }
    }

    pub fn from_str(sql_type: &str) -> Self {
        match sql_type {
            "text" => SqlType::Text,
            "bool"=> SqlType::Bool ,
            "tinyint" => SqlType::Tinyint,
            "smallint" => SqlType::Smallint,
            "int" => SqlType::Int,
            "bigint" => SqlType::Bigint,
            "real" => SqlType::Real,
            "float" => SqlType::Float,
            "double" =>SqlType::Double ,
            "numeric" => SqlType::Numeric,
            "tinyblob" => SqlType::Tinyblob,
            "mediumblob" => SqlType::Mediumblob,
            "blob" => SqlType::Blob,
            "longblob" => SqlType::Longblob,
            "varbinary" => SqlType::Varbinary,
            "char" => SqlType::Char,
            "varchar" => SqlType::Varchar,
            "tinytext"=> SqlType::Tinytext,
            "mediumtext" => SqlType::Mediumtext,
            "json" => SqlType::Json,
            "date" => SqlType::Date,
            "timestamp" => SqlType::Timestamp,
            "time" => SqlType::Time,
            "point" => SqlType::Point,
            _ => SqlType::Text,
        }
    }
}

#[allow(unused)]
#[derive(Debug, PartialEq, Clone)]
pub enum ArrayType {
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,

    Real,
    Float,
    Double,
    Numeric,

    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    Enum(String, Vec<String>),
}

trait HasType {
    fn get_type(&self) -> Option<SqlType>;
}

impl HasType for AkitaValue {
    fn get_type(&self) -> Option<SqlType> {
        match self {
            AkitaValue::Null => None,
            AkitaValue::Bool(_) => Some(SqlType::Bool),
            AkitaValue::Tinyint(_) => Some(SqlType::Tinyint),
            AkitaValue::Smallint(_) => Some(SqlType::Smallint),
            AkitaValue::Int(_) => Some(SqlType::Int),
            AkitaValue::Bigint(_) => Some(SqlType::Bigint),
            AkitaValue::Float(_) => Some(SqlType::Float),
            AkitaValue::Double(_) => Some(SqlType::Double),
            AkitaValue::BigDecimal(_) => Some(SqlType::Numeric),
            AkitaValue::Blob(_) => Some(SqlType::Blob),
            AkitaValue::Char(_) => Some(SqlType::Char),
            AkitaValue::Text(_) => Some(SqlType::Text),
            AkitaValue::Json(_) => Some(SqlType::Json),
            AkitaValue::Uuid(_) => Some(SqlType::Uuid),
            AkitaValue::Date(_) => Some(SqlType::Date),
            AkitaValue::Time(_) => Some(SqlType::Time),
            AkitaValue::DateTime(_) => Some(SqlType::Timestamp),
            AkitaValue::Timestamp(_) => Some(SqlType::Timestamp),
            AkitaValue::Interval(_) => Some(SqlType::Interval),
            // AkitaValue::Point(_) => Some(SqlType::Point),
            AkitaValue::Array(Array::Int(_)) => Some(SqlType::Array(Box::new(SqlType::Int))),
            AkitaValue::Array(Array::Float(_)) => Some(SqlType::Array(Box::new(SqlType::Float))),
            AkitaValue::Array(Array::Text(_)) => Some(SqlType::Array(Box::new(SqlType::Text))),
            AkitaValue::Array(Array::Json(_)) => Some(SqlType::Array(Box::new(SqlType::Json))),
            AkitaValue::Array(Array::Bool(_)) => Some(SqlType::Array(Box::new(SqlType::Bool))),
            AkitaValue::Array(Array::Tinyint(_)) => Some(SqlType::Array(Box::new(SqlType::Tinyint))),
            AkitaValue::Array(Array::Smallint(_)) => Some(SqlType::Array(Box::new(SqlType::Smallint))),
            AkitaValue::Array(Array::Bigint(_)) => Some(SqlType::Array(Box::new(SqlType::Bigint))),
            AkitaValue::Array(Array::BigDecimal(_)) => Some(SqlType::Array(Box::new(SqlType::Float))),
            AkitaValue::Array(Array::Date(_)) => Some(SqlType::Array(Box::new(SqlType::Date))),
            AkitaValue::Array(Array::Timestamp(_)) => Some(SqlType::Array(Box::new(SqlType::Timestamp))),
            AkitaValue::Array(Array::Uuid(_)) => Some(SqlType::Array(Box::new(SqlType::Uuid))),
            AkitaValue::Array(Array::Double(_)) => Some(SqlType::Array(Box::new(SqlType::Double))),
            AkitaValue::Array(Array::Char(_)) => Some(SqlType::Array(Box::new(SqlType::Char))),
            // AkitaValue::SerdeJson(_) => Some(SqlType::Json),
            AkitaValue::Object(_) => Some(SqlType::Json),
            _ => None
        }
    }
}

impl SqlType {
    pub fn same_type(&self, value: &AkitaValue) -> bool {
        if let Some(simple_type) = value.get_type() {
            if simple_type == *self {
                return true;
            }
            match (self, value) {
                (SqlType::Varchar, AkitaValue::Text(_)) => true,
                (SqlType::TimestampTz, AkitaValue::Timestamp(_)) => true,
                (_, _) => false,
            }
        } else {
            false
        }
    }
}



/// Action Type - Covers all actions in Akita
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum OperationType {
    Select,
    Insert(InsertType),
    Update,
    Delete,
    CreateTable,
    AlterTable,
    DropTable,
    TruncateTable,
    TransactionStart,
    TransactionCommit,
    TransactionRollback,
    Call,
    Other(String),
}

impl OperationType {
    /// Use regular expressions to detect action types
    pub fn detect_from_sql(sql: &str) -> Self {
        let normalized_sql = Self::normalize_sql(sql);

        lazy_static::lazy_static! {
            // Use regular expressions to match various action types
            static ref OPERATION_REGEXES: Vec<(Regex, OperationType)> =  vec![
                // INSERT（Support INSERT INTO and INSERT）
                (Regex::new(r"(?i)^\s*insert(?:\s+into)?\b.*values\b.*\),\s*\(").unwrap(),OperationType::Insert(InsertType::BatchInsert)),
                // SELECT (including SELECT at the beginning of the WITH clause)
                (Regex::new(r"^\s*(?:with\s+\w+\s+as\s*\(.*?\)\s*)?select\b").unwrap(), OperationType::Select),
                // UPDATE
                (Regex::new(r"^\s*update\b").unwrap(), OperationType::Update),
                // DELETE（Support DELETE FROM and DELETE）
                (Regex::new(r"^\s*delete(?:\s+from)?\b").unwrap(), OperationType::Delete),
                // CREATE TABLE（Includes temporary tables and IF NOT EXISTS）
                (Regex::new(r"^\s*create(?:\s+temporary)?\s+table(?:\s+if\s+not\s+exists)?\b").unwrap(), OperationType::CreateTable),
                // ALTER TABLE
                (Regex::new(r"^\s*alter\s+table\b").unwrap(), OperationType::AlterTable),
                // DROP TABLE（Includes IF EXISTS）
                (Regex::new(r"^\s*drop\s+table(?:\s+if\s+exists)?\b").unwrap(), OperationType::DropTable),
                // TRUNCATE TABLE
                (Regex::new(r"^\s*truncate(?:\s+table)?\b").unwrap(), OperationType::TruncateTable),
                // Transaction operations
                (Regex::new(r"^\s*(?:start|begin)(?:\s+transaction)?\b").unwrap(), OperationType::TransactionStart),
                (Regex::new(r"^\s*commit(?:\s+transaction)?(?:\s+work)?\b").unwrap(), OperationType::TransactionCommit),
                (Regex::new(r"^\s*rollback(?:\s+transaction)?(?:\s+work)?\b").unwrap(), OperationType::TransactionRollback),
                // CALL（Stored procedure calls）
                (Regex::new(r"^\s*call\b").unwrap(), OperationType::Call),
                // Bottom INSERT match (without VALUES clause)
                (Regex::new(r"(?i)^\s*insert(?:\s+into)?\b").unwrap(),OperationType::Insert(InsertType::SingleInsert)),
            ];
        }

        for (regex, op_type) in OPERATION_REGEXES.iter() {
            if regex.is_match(&normalized_sql) {
                return op_type.clone();
            }
        }

        // If no match is made to a known type, return Other
        OperationType::Other(normalized_sql)
    }

    /// Detect the type of action
    pub fn detect_operation_type(sql: &str) -> OperationType {
        let sql_lower = sql.trim_start().to_lowercase();
        let operation_type = OperationType::detect_from_sql(&sql_lower);
        operation_type
    }
    
    pub fn is_batch_insert(&self) -> bool {
        match self {
            OperationType::Insert(InsertType::BatchInsert) => true,
            _ => false,
        }
    }

    fn normalize_sql(sql: &str) -> String {
        let sql = sql.trim();

        if sql.is_empty() {
            return String::new();
        }

        let re_comment = Regex::new(r"--[^\n\r]*|/\*[\s\S]*?\*/").unwrap();
        let sql = re_comment.replace_all(sql, "");

        let re_whitespace = Regex::new(r"\s+").unwrap();
        let sql = re_whitespace.replace_all(&sql, " ");

        sql.trim().to_lowercase()
    }

    /// Check if it is a query operation
    pub fn is_query(&self) -> bool {
        matches!(self, OperationType::Select)
    }

    /// Check if it's a DML operation
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            OperationType::Select |
            OperationType::Insert(_) |
            OperationType::Update |
            OperationType::Delete
        )
    }

    /// Check if it's a DDL operation
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            OperationType::CreateTable |
            OperationType::AlterTable |
            OperationType::DropTable |
            OperationType::TruncateTable
        )
    }

    /// Check if it's a transactional action
    pub fn is_transaction(&self) -> bool {
        matches!(
            self,
            OperationType::TransactionStart |
            OperationType::TransactionCommit |
            OperationType::TransactionRollback
        )
    }

    /// Get a string representation of the action type
    pub fn as_str(&self) -> &str {
        match self {
            OperationType::Select => "SELECT",
            OperationType::Insert(_) => "INSERT",
            OperationType::Update => "UPDATE",
            OperationType::Delete => "DELETE",
            OperationType::CreateTable => "CREATE TABLE",
            OperationType::AlterTable => "ALTER TABLE",
            OperationType::DropTable => "DROP TABLE",
            OperationType::TruncateTable => "TRUNCATE TABLE",
            OperationType::TransactionStart => "START TRANSACTION",
            OperationType::TransactionCommit => "COMMIT",
            OperationType::TransactionRollback => "ROLLBACK",
            OperationType::Call => "CALL",
            OperationType::Other(sql) => sql,
        }
    }

    /// Get a short identification of the type of action (for logs, etc.)
    pub fn short_code(&self) -> &str {
        match self {
            OperationType::Select => "SEL",
            OperationType::Insert(_) => "INS",
            OperationType::Update => "UPD",
            OperationType::Delete => "DEL",
            OperationType::CreateTable => "CRT",
            OperationType::AlterTable => "ALT",
            OperationType::DropTable => "DRP",
            OperationType::TruncateTable => "TRN",
            OperationType::TransactionStart => "TXN_S",
            OperationType::TransactionCommit => "TXN_C",
            OperationType::TransactionRollback => "TXN_R",
            OperationType::Call => "CALL",
            OperationType::Other(_) => "OTH",
        }
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}


#[allow(unused)]
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum InsertType {
    NotInsert,
    SingleInsert,
    BatchInsert
}

#[allow(unused)]
impl InsertType {
    /// Determine whether SQL is inserted for multiple rows
    fn is_batch_insert_sql(sql: &str) -> bool {
        let sql_lower = sql.to_lowercase();

        // Check if it is an INSERT statement
        if !sql_lower.trim_start().starts_with("insert") {
            return false;
        }

        // Statistics in the VALUES clause? Quantity
        if let Some(values_start) = sql_lower.find("values") {
            let values_part = &sql[values_start..];
            let placeholder_count = values_part.chars().filter(|&c| c == '?').count();

            // If there are more than 11 placeholders (assuming a maximum of 11 fields in a row), it is likely to be multiple rows
            placeholder_count > 11
        } else {
            false
        }
    }

    
    /// More accurate multi-line insertion determination
    fn detect_insert_type(sql: &str) -> InsertType {
        let sql_lower = sql.to_lowercase();

        if !sql_lower.trim_start().starts_with("insert") {
            return InsertType::NotInsert;
        }

        // Look for the VALUES keyword
        if let Some(values_start) = sql_lower.find("values") {
            let values_part = &sql[values_start..];

            // 统计 VALUES 后面有多少组括号
            let mut char_iter = values_part.chars();
            let mut paren_depth = 0;
            let mut value_groups = 0;
            let mut in_string = false;
            let mut escape_next = false;

            while let Some(ch) = char_iter.next() {
                if escape_next {
                    escape_next = false;
                    continue;
                }

                match ch {
                    '\\' if !in_string => escape_next = true,
                    '\'' | '"' => in_string = !in_string,
                    '(' if !in_string => {
                        paren_depth += 1;
                        if paren_depth == 1 {
                            value_groups += 1;
                        }
                    }
                    ')' if !in_string => {
                        paren_depth -= 1;
                    }
                    _ => {}
                }
            }

            // If there are multiple sets of parentheses, it's batch insertion
            if value_groups > 1 {
                InsertType::BatchInsert
            } else {
                InsertType::SingleInsert
            }
        } else {
            InsertType::SingleInsert
        }
    }
}
