/*
 *
 *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *
 *   Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *   Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *   Author: SnackCloud
 *
 */

use std::collections::HashMap;
use std::fmt::Debug;
use getset::{Getters, Setters};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::DeserializeOwned;
use crate::config::{GlobalConfig, NamingStrategy, StrategyConfig};
use crate::util::{contains_upper_case, remove_is_prefix_if_boolean};

/// 表数据查询接口
pub trait IDbQuery : Clone {
    /// 数据库类型
    fn db_type(&self) -> DbType {
        DbType::Mysql
    }

    /// 表信息查询 SQL
    fn tables_sql(&self) -> String;

    /// 表字段信息查询 SQL
    fn table_fields_sql(&self) -> String;

    /// 表名称
    fn table_name(&self) -> String;

    /// 表注释
    fn table_comment(&self) -> String;

    /// 字段名称
    fn field_name(&self) -> String;

    /// 字段类型
    fn field_type(&self) -> String;

    /// 字段注释
    fn field_comment(&self) -> String;

    /// 主键字段
    fn field_key(&self) -> String;

    /// 判断主键是否为identity，目前仅对mysql进行检查
    fn is_key_identity(extra: akita::Value) -> bool;

    /// 自定义字段名称
    fn field_custom(&self) -> Vec<String>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MySqlQuery;

impl IDbQuery for MySqlQuery {
    fn tables_sql(&self) -> String {
        "show table status WHERE 1=1 ".to_string()
    }

    fn table_fields_sql(&self) -> String {
        "show full fields from `%s`".to_string()
    }

    fn table_name(&self) -> String {
        "Name".to_string()
    }

    fn table_comment(&self) -> String {
        "Comment".to_string()
    }

    fn field_name(&self) -> String {
        "Field".to_string()
    }

    fn field_type(&self) -> String {
        "Type".to_string()
    }

    fn field_comment(&self) -> String {
        "Comment".to_string()
    }

    fn field_key(&self) -> String {
        "Key".to_string()
    }

    fn is_key_identity(extra: akita::Value) -> bool {
        let extra = extra.get_obj::<String>("Extra").unwrap_or_default();
        extra.eq("auto_increment")
    }

    fn field_custom(&self) -> Vec<String> {
        vec![]
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DbType {
    /**
     * MYSQL
     */
    Mysql,
    /**
     * MARIADB
     */
    Mariadb,
    /**
     * ORACLE
     */
    Oracle,
    /**
     * oracle12c new pagination
     */
    Oracle12c,

    /**
     * DB2
     */
    Db2,
    /**
     * H2
     */
    H2,
    /**
     * HSQL
     */
    Hsql,
    /**
     * SQLITE
     */
    Sqlite,
    /**
     * POSTGRE
     */
    PostgreSql,
    /**
     * SQLSERVER2005
     */
    SqlServer2005,
    /**
     * SQLSERVER
     */
    SqlServer,
    /**
     * DM
     */
    Dm,
    /**
     * xugu
     */
    XuGu,
    /**
     * Kingbase
     */
    KingbaseEs,

    /**
     * Phoenix
     */
    Phoenix,

    /**
     * Gauss
     */
    Gauss,

    /**
     * UNKONWN DB
     */
    Other,
}


#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    convert: bool,
    name: String,
    comment: String,
    entity_name: String,
    mapper_name: String,
    request_name: String,
    response_name: String,
    xml_name: String,
    service_name: String,
    service_impl_name: String,
    controller_name: String,
    entity_path: String,
    fields: Vec<TableField>,
    /**
     * 公共字段
     */
    common_fields: Vec<TableField>,
    field_names: String,
}

impl TableInfo {
    pub fn set_entity_path_info(&mut self) {
        if self.entity_name.is_empty() {
            return;
        }

        let first_char = self.entity_name.chars().next().unwrap_or_default().to_lowercase();
        let rest = &self.entity_name[1..];
        self.entity_path = format!("{}{}", first_char, rest);
    }
}

impl Default for TableInfo {
    fn default() -> Self {
        Self {
            convert: false,
            name: "".to_string(),
            comment: "".to_string(),
            entity_name: "".to_string(),
            mapper_name: "".to_string(),
            request_name: "".to_string(),
            response_name: "".to_string(),
            xml_name: "".to_string(),
            service_name: "".to_string(),
            entity_path: "".to_string(),
            service_impl_name: "".to_string(),
            controller_name: "".to_string(),
            fields: vec![],
            common_fields: vec![],
            field_names: "".to_string(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
#[serde(rename_all = "camelCase")]
pub struct TableField {
    convert: bool,
    key_flag: bool,
    /**
     * 主键是否为自增类型
     */
    key_identity_flag: bool,
    name: String,
    r#type: String,
    capital_name: String,
    property_name: String,
    column_type: DbColumnType,
    comment: String,
    fill: String,
    /**
     * 是否关键字
     *
     * @since 3.3.2
     */
    key_words: bool,
    /**
     * 数据库字段（关键字含转义符号）
     *
     * @since 3.3.2
     */
    column_name: String,
    /**
     * 自定义查询字段列表
     */
    custom_map: HashMap<String, serde_json::Value>,
}

impl TableField {
    pub fn set_property_name_with_strategy(&mut self, strategy_config: &StrategyConfig, property_name: String) {
        self.property_name = property_name;
        self.set_converter(strategy_config);
    }

    pub fn set_converter(&mut self, strategy_config: &StrategyConfig) {
        if *strategy_config.entity_table_field_annotation_enable() || self.key_words {
            self.convert = true;
        }
        if strategy_config.is_capital_mode_naming(&self.name) {
            self.convert = false;
        } else {
            // 转换字段
            if (NamingStrategy::UnderlineToCamel == strategy_config.get_column_naming_strategy()) {
                // 包含大写处理
                if contains_upper_case(&self.name) {
                    self.convert = true;
                }
            } else if !self.name.eq(&self.property_name) {
                self.convert = true;
            }
        }
    }

    pub fn get_inner_capital_name(&self) -> String {
        if self.property_name.len() <= 1 {
            return self.property_name.to_uppercase();
        }

        let mut set_get_name = self.property_name.to_string();

        // 如果列类型是布尔类型，处理去掉 "is" 前缀
        if self.column_type.get_type().eq_ignore_ascii_case("bool") {
            set_get_name = remove_is_prefix_if_boolean(&set_get_name);
        }

        // 处理第一个字母小写，第二个字母大写的情况
        let first_char = set_get_name.chars().next().unwrap_or_default();
        if first_char.is_lowercase()
            && set_get_name.chars().nth(1).unwrap_or_default().is_uppercase()
        {
            return first_char.to_lowercase().to_string() + &set_get_name[1..];
        }

        first_char.to_uppercase().to_string() + &set_get_name[1..]
    }

}

impl Default for TableField {
    fn default() -> Self {
        Self {
            convert: false,
            key_flag: false,
            key_identity_flag: false,
            name: "".to_string(),
            r#type: "".to_string(),
            property_name: "".to_string(),
            capital_name: "".to_string(),
            column_type: DbColumnType::RustColumnType(RustDbColumnType::String),
            comment: "".to_string(),
            fill: "".to_string(),
            key_words: false,
            column_name: "".to_string(),
            custom_map: Default::default(),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum DbColumnType {
    RustColumnType(RustDbColumnType),
    JavaColumnType(JavaDbColumnType),
}

impl DbColumnType {
    pub fn get_type(&self) -> String {
        match self {
            DbColumnType::RustColumnType(v) => v.to_string(),
            DbColumnType::JavaColumnType(v) => v.to_string(),
        }
    }
}

impl ToString for DbColumnType {
    fn to_string(&self) -> String {
        match self {
            DbColumnType::RustColumnType(v) => v.to_string(),
            DbColumnType::JavaColumnType(v) => v.to_string(),
        }
    }
}

impl Serialize for DbColumnType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DbColumnType::RustColumnType(r) => serializer.serialize_str(&format!("{}", r.to_string())),
            DbColumnType::JavaColumnType(j) => serializer.serialize_str(&format!("{}", j.to_string())),
        }
    }
}

impl<'de> Deserialize<'de> for DbColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let r = RustDbColumnType::from_str(&s);
        if RustDbColumnType::Unknown != r {
            return Ok(DbColumnType::RustColumnType(r));
        }
        let j = JavaDbColumnType::from_str(&s);
        if JavaDbColumnType::Unknown != j {
            return Ok(DbColumnType::JavaColumnType(j));
        }
        // 默认兜底
        Ok(DbColumnType::RustColumnType(RustDbColumnType::String))
    }
}

//
// ---------------------- Rust Column Types ----------------------
//
#[derive(PartialEq, Clone, Debug)]
pub enum RustDbColumnType {
    // 基本数值类型
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    Char,
    String,

    // 时间与日期
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    DateTimeUtc,
    Instant,

    // 数组 / 二进制
    ByteArray,
    Blob,
    Clob,

    // 其他
    BigInt,
    Decimal,
    Json,
    Uuid,
    Object,
    Unknown
}

impl RustDbColumnType {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            // 基本数值
            "i8" => Self::I8,
            "i16" => Self::I16,
            "i32" => Self::I32,
            "i64" => Self::I64,
            "f32" => Self::F32,
            "f64" => Self::F64,
            "bool" => Self::Bool,
            "char" => Self::Char,
            "string" => Self::String,

            // 时间
            "naivedate" => Self::NaiveDate,
            "naivetime" => Self::NaiveTime,
            "naivedatetime" => Self::NaiveDateTime,
            "datetimeutc" => Self::DateTimeUtc,
            "instant" => Self::Instant,

            // 二进制
            "bytearray" | "vec<u8>" | "bytes" => Self::ByteArray,
            "blob" => Self::Blob,
            "clob" => Self::Clob,

            // 其他
            "bigint" => Self::BigInt,
            "decimal" | "bigdecimal" => Self::Decimal,
            "json" => Self::Json,
            "uuid" => Self::Uuid,
            "object" => Self::Object,

            _ => Self::Unknown,
        }
    }

    pub fn process_type_convert(field_type: &str) -> DbColumnType {
        let t = field_type.to_lowercase();
        let ctype = if t.contains("char") {
            RustDbColumnType::String
        } else if t.contains("bigint") {
            RustDbColumnType::I64
        } else if t.contains("tinyint(1)") {
            RustDbColumnType::Bool
        } else if t.contains("int") {
            RustDbColumnType::I32
        } else if t.contains("text") {
            RustDbColumnType::String
        } else if t.contains("bit") {
            RustDbColumnType::Bool
        } else if t.contains("decimal") || t.contains("numeric") {
            RustDbColumnType::Decimal
        } else if t.contains("clob") {
            RustDbColumnType::ByteArray
        } else if t.contains("blob") {
            RustDbColumnType::ByteArray
        } else if t.contains("binary") {
            RustDbColumnType::ByteArray
        } else if t.contains("float") {
            RustDbColumnType::F32
        } else if t.contains("double") {
            RustDbColumnType::F64
        } else if t.contains("json") || t.contains("enum") {
            RustDbColumnType::String
        } else if t.contains("datetime") {
            RustDbColumnType::NaiveDateTime
        } else if t.contains("date") {
            RustDbColumnType::NaiveDate
        } else if t.contains("time") {
            RustDbColumnType::NaiveTime
        } else if t.contains("year") {
            RustDbColumnType::String
        } else {
            RustDbColumnType::String
        };
        DbColumnType::RustColumnType(ctype)
    }
}

impl ToString for RustDbColumnType {
    fn to_string(&self) -> String {
        match self {
            Self::I8 => "i8".to_string(),
            Self::I16 => "i16".to_string(),
            Self::I32 => "i32".to_string(),
            Self::I64 => "i64".to_string(),
            Self::F32 => "f32".to_string(),
            Self::Decimal | Self::F64 => "f64".to_string(),
            Self::Bool => "bool".to_string(),
            Self::Json | Self::Char | Self::String => "String".to_string(),
            Self::NaiveDate => "NaiveDate".to_string(),
            Self::NaiveTime => "NaiveTime".to_string(),
            Self::NaiveDateTime => "NaiveDateTime".to_string(),
            Self::DateTimeUtc => "DateTimeUtc".to_string(),
            Self::Instant => "Instant".to_string(),
            Self::Clob | Self::Blob | Self::ByteArray => "Vec<u8>".to_string(),
            Self::BigInt => "i64".to_string(),
            Self::Uuid => "Uuid".to_string(),
            Self::Object => "Value".to_string(),
            _ => "Unknown".to_string()
        }
    }
}

//
// ---------------------- Java Column Types ----------------------
//
#[derive(PartialEq, Clone, Debug)]
pub enum JavaDbColumnType {
    // 基本类型
    BaseByte,
    BaseShort,
    BaseChar,
    BaseInt,
    BaseLong,
    BaseFloat,
    BaseDouble,
    BaseBoolean,

    // 包装类型
    Byte,
    Short,
    Character,
    Integer,
    Long,
    Float,
    Double,
    Boolean,
    String,

    // sql 包下数据类型
    DateSql,
    Time,
    Timestamp,
    Blob,
    Clob,

    // java8 新时间类型
    LocalDate,
    LocalTime,
    Year,
    YearMonth,
    LocalDateTime,
    Instant,

    // 其他
    ByteArray,
    Object,
    Date,
    BigInteger,
    BigDecimal,

    Unknown
}

impl JavaDbColumnType {
    pub fn from_str(s: &str) -> Self {
        use JavaDbColumnType::*;
        match s {
            // 基本类型
            "byte" => BaseByte,
            "short" => BaseShort,
            "char" => BaseChar,
            "int" => BaseInt,
            "long" => BaseLong,
            "float" => BaseFloat,
            "double" => BaseDouble,
            "boolean" => BaseBoolean,

            // 包装类型
            "Byte" => Byte,
            "Short" => Short,
            "Character" => Character,
            "Integer" => Integer,
            "Long" => Long,
            "Float" => Float,
            "Double" => Double,
            "Boolean" => Boolean,
            "String" => String,

            // SQL 类型
            "Date" => DateSql,
            "Time" => Time,
            "Timestamp" => Timestamp,
            "Blob" => Blob,
            "Clob" => Clob,

            // Java8 时间
            "LocalDate" => LocalDate,
            "LocalTime" => LocalTime,
            "Year" => Year,
            "YearMonth" => YearMonth,
            "LocalDateTime" => LocalDateTime,
            "Instant" => Instant,

            // 其他
            "byte[]" => ByteArray,
            "Object" => Object,
            "java.util.Date" => Date,
            "BigInteger" => BigInteger,
            "BigDecimal" => BigDecimal,
            _ => Unknown,
        }
    }

    pub fn process_type_convert(field_type: &str) -> DbColumnType {
        let t = field_type.to_lowercase();
        let ctype = if t.contains("char") {
            JavaDbColumnType::String
        } else if t.contains("bigint") {
            JavaDbColumnType::Long
        } else if t.contains("tinyint(1)") {
            JavaDbColumnType::Boolean
        } else if t.contains("int") {
            JavaDbColumnType::Integer
        } else if t.contains("text") {
            JavaDbColumnType::String
        } else if t.contains("bit") {
            JavaDbColumnType::Boolean
        } else if t.contains("decimal") {
            JavaDbColumnType::BigDecimal
        } else if t.contains("clob") {
            JavaDbColumnType::Clob
        } else if t.contains("blob") {
            JavaDbColumnType::Blob
        } else if t.contains("binary") {
            JavaDbColumnType::ByteArray
        } else if t.contains("float") {
            JavaDbColumnType::Float
        } else if t.contains("double") {
            JavaDbColumnType::Double
        } else if t.contains("json") || t.contains("enum") {
            JavaDbColumnType::String
        } else if t.contains("date") {
            JavaDbColumnType::Date
        } else if t.contains("time") || t.contains("datetime") {
            JavaDbColumnType::LocalTime
        } else if t.contains("year") {
            JavaDbColumnType::Year
        } else {
            JavaDbColumnType::String
        };
        DbColumnType::JavaColumnType(ctype)
    }
}

impl ToString for JavaDbColumnType {
    fn to_string(&self) -> String {
        use JavaDbColumnType::*;
        match self {
            BaseByte => "byte",
            BaseShort => "short",
            BaseChar => "char",
            BaseInt => "int",
            BaseLong => "long",
            BaseFloat => "float",
            BaseDouble => "double",
            BaseBoolean => "boolean",
            Byte => "Byte",
            Short => "Short",
            Character => "Character",
            Integer => "Integer",
            Long => "Long",
            Float => "Float",
            Double => "Double",
            Boolean => "Boolean",
            String => "String",
            DateSql => "Date",
            Time => "Time",
            Timestamp => "Timestamp",
            Blob => "Blob",
            Clob => "Clob",
            LocalDate => "LocalDate",
            LocalTime => "LocalTime",
            Year => "Year",
            YearMonth => "YearMonth",
            LocalDateTime => "LocalDateTime",
            Instant => "Instant",
            ByteArray => "byte[]",
            Object => "Object",
            Date => "java.util.Date",
            BigInteger => "BigInteger",
            BigDecimal => "BigDecimal",
            _ => "Unknown"
        }
            .to_string()
    }
}

pub struct MySqlKeyWordsHandler;

impl MySqlKeyWordsHandler {
    pub const KEY_WORDS: &'static [&'static str] = &[
        "ACCESSIBLE",
        "ACCOUNT",
        "ACTION",
        "ADD",
        "AFTER",
        "AGAINST",
        "AGGREGATE",
        "ALGORITHM",
        "ALL",
        "ALTER",
        "ALWAYS",
        "ANALYSE",
        "ANALYZE",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "ASCII",
        "ASENSITIVE",
        "AT",
        "AUTOEXTEND_SIZE",
        "AUTO_INCREMENT",
        "AVG",
        "AVG_ROW_LENGTH",
        "BACKUP",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BIGINT",
        "BINARY",
        "BINLOG",
        "BIT",
        "BLOB",
        "BLOCK",
        "BOOL",
        "BOOLEAN",
        "BOTH",
        "BTREE",
        "BY",
        "BYTE",
        "CACHE",
        "CALL",
        "CASCADE",
        "CASCADED",
        "CASE",
        "CATALOG_NAME",
        "CHAIN",
        "CHANGE",
        "CHANGED",
        "CHANNEL",
        "CHAR",
        "CHARACTER",
        "CHARSET",
        "CHECK",
        "CHECKSUM",
        "CIPHER",
        "CLASS_ORIGIN",
        "CLIENT",
        "CLOSE",
        "COALESCE",
        "CODE",
        "COLLATE",
        "COLLATION",
        "COLUMN",
        "COLUMNS",
        "COLUMN_FORMAT",
        "COLUMN_NAME",
        "COMMENT",
        "COMMIT",
        "COMMITTED",
        "COMPACT",
        "COMPLETION",
        "COMPRESSED",
        "COMPRESSION",
        "CONCURRENT",
        "CONDITION",
        "CONNECTION",
        "CONSISTENT",
        "CONSTRAINT",
        "CONSTRAINT_CATALOG",
        "CONSTRAINT_NAME",
        "CONSTRAINT_SCHEMA",
        "CONTAINS",
        "CONTEXT",
        "CONTINUE",
        "CONVERT",
        "CPU",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "CURSOR",
        "CURSOR_NAME",
        "DATA",
        "DATABASE",
        "DATABASES",
        "DATAFILE",
        "DATE",
        "DATETIME",
        "DAY",
        "DAY_HOUR",
        "DAY_MICROSECOND",
        "DAY_MINUTE",
        "DAY_SECOND",
        "DEALLOCATE",
        "DEC",
        "DECIMAL",
        "DECLARE",
        "DEFAULT",
        "DEFAULT_AUTH",
        "DEFINER",
        "DELAYED",
        "DELAY_KEY_WRITE",
        "DELETE",
        "DESC",
        "DESCRIBE",
        "DES_KEY_FILE",
        "DETERMINISTIC",
        "DIAGNOSTICS",
        "DIRECTORY",
        "DISABLE",
        "DISCARD",
        "DISK",
        "DISTINCT",
        "DISTINCTROW",
        "DIV",
        "DO",
        "DOUBLE",
        "DROP",
        "DUAL",
        "DUMPFILE",
        "DUPLICATE",
        "DYNAMIC",
        "EACH",
        "ELSE",
        "ELSEIF",
        "ENABLE",
        "ENCLOSED",
        "ENCRYPTION",
        "END",
        "ENDS",
        "ENGINE",
        "ENGINES",
        "ENUM",
        "ERROR",
        "ERRORS",
        "ESCAPE",
        "ESCAPED",
        "EVENT",
        "EVENTS",
        "EVERY",
        "EXCHANGE",
        "EXECUTE",
        "EXISTS",
        "EXIT",
        "EXPANSION",
        "EXPIRE",
        "EXPLAIN",
        "EXPORT",
        "EXTENDED",
        "EXTENT_SIZE",
        "FALSE",
        "FAST",
        "FAULTS",
        "FETCH",
        "FIELDS",
        "FILE",
        "FILE_BLOCK_SIZE",
        "FILTER",
        "FIRST",
        "FIXED",
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "FLUSH",
        "FOLLOWS",
        "FOR",
        "FORCE",
        "FOREIGN",
        "FORMAT",
        "FOUND",
        "FROM",
        "FULL",
        "FULLTEXT",
        "FUNCTION",
        "GENERAL",
        "GENERATED",
        "GEOMETRY",
        "GEOMETRYCOLLECTION",
        "GET",
        "GET_FORMAT",
        "GLOBAL",
        "GRANT",
        "GRANTS",
        "GROUP",
        "GROUP_REPLICATION",
        "HANDLER",
        "HASH",
        "HAVING",
        "HELP",
        "HIGH_PRIORITY",
        "HOST",
        "HOSTS",
        "HOUR",
        "HOUR_MICROSECOND",
        "HOUR_MINUTE",
        "HOUR_SECOND",
        "IDENTIFIED",
        "IF",
        "IGNORE",
        "IGNORE_SERVER_IDS",
        "IMPORT",
        "IN",
        "INDEX",
        "INDEXES",
        "INFILE",
        "INITIAL_SIZE",
        "INNER",
        "INOUT",
        "INSENSITIVE",
        "INSERT",
        "INSERT_METHOD",
        "INSTALL",
        "INSTANCE",
        "INT",
        "INT1",
        "INT2",
        "INT3",
        "INT4",
        "INT8",
        "INTEGER",
        "INTERVAL",
        "INTO",
        "INVOKER",
        "IO",
        "IO_AFTER_GTIDS",
        "IO_BEFORE_GTIDS",
        "IO_THREAD",
        "IPC",
        "IS",
        "ISOLATION",
        "ISSUER",
        "ITERATE",
        "JOIN",
        "JSON",
        "KEY",
        "KEYS",
        "KEY_BLOCK_SIZE",
        "KILL",
        "LANGUAGE",
        "LAST",
        "LEADING",
        "LEAVE",
        "LEAVES",
        "LEFT",
        "LESS",
        "LEVEL",
        "LIKE",
        "LIMIT",
        "LINEAR",
        "LINES",
        "LINESTRING",
        "LIST",
        "LOAD",
        "LOCAL",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "LOCK",
        "LOCKS",
        "LOGFILE",
        "LOGS",
        "LONG",
        "LONGBLOB",
        "LONGTEXT",
        "LOOP",
        "LOW_PRIORITY",
        "MASTER",
        "MASTER_AUTO_POSITION",
        "MASTER_BIND",
        "MASTER_CONNECT_RETRY",
        "MASTER_DELAY",
        "MASTER_HEARTBEAT_PERIOD",
        "MASTER_HOST",
        "MASTER_LOG_FILE",
        "MASTER_LOG_POS",
        "MASTER_PASSWORD",
        "MASTER_PORT",
        "MASTER_RETRY_COUNT",
        "MASTER_SERVER_ID",
        "MASTER_SSL",
        "MASTER_SSL_CA",
        "MASTER_SSL_CAPATH",
        "MASTER_SSL_CERT",
        "MASTER_SSL_CIPHER",
        "MASTER_SSL_CRL",
        "MASTER_SSL_CRLPATH",
        "MASTER_SSL_KEY",
        "MASTER_SSL_VERIFY_SERVER_CERT",
        "MASTER_TLS_VERSION",
        "MASTER_USER",
        "MATCH",
        "MAXVALUE",
        "MAX_CONNECTIONS_PER_HOUR",
        "MAX_QUERIES_PER_HOUR",
        "MAX_ROWS",
        "MAX_SIZE",
        "MAX_STATEMENT_TIME",
        "MAX_UPDATES_PER_HOUR",
        "MAX_USER_CONNECTIONS",
        "MEDIUM",
        "MEDIUMBLOB",
        "MEDIUMINT",
        "MEDIUMTEXT",
        "MEMORY",
        "MERGE",
        "MESSAGE_TEXT",
        "MICROSECOND",
        "MIDDLEINT",
        "MIGRATE",
        "MINUTE",
        "MINUTE_MICROSECOND",
        "MINUTE_SECOND",
        "MIN_ROWS",
        "MOD",
        "MODE",
        "MODIFIES",
        "MODIFY",
        "MONTH",
        "MULTILINESTRING",
        "MULTIPOINT",
        "MULTIPOLYGON",
        "MUTEX",
        "MYSQL_ERRNO",
        "NAME",
        "NAMES",
        "NATIONAL",
        "NATURAL",
        "NCHAR",
        "NDB",
        "NDBCLUSTER",
        "NEVER",
        "NEW",
        "NEXT",
        "NO",
        "NODEGROUP",
        "NONBLOCKING",
        "NONE",
        "NOT",
        "NO_WAIT",
        "NO_WRITE_TO_BINLOG",
        "NULL",
        "NUMBER",
        "NUMERIC",
        "NVARCHAR",
        "OFFSET",
        "OLD_PASSWORD",
        "ON",
        "ONE",
        "ONLY",
        "OPEN",
        "OPTIMIZE",
        "OPTIMIZER_COSTS",
        "OPTION",
        "OPTIONALLY",
        "OPTIONS",
        "OR",
        "ORDER",
        "OUT",
        "OUTER",
        "OUTFILE",
        "OWNER",
        "PACK_KEYS",
        "PAGE",
        "PARSER",
        "PARSE_GCOL_EXPR",
        "PARTIAL",
        "PARTITION",
        "PARTITIONING",
        "PARTITIONS",
        "PASSWORD",
        "PHASE",
        "PLUGIN",
        "PLUGINS",
        "PLUGIN_DIR",
        "POINT",
        "POLYGON",
        "PORT",
        "PRECEDES",
        "PRECISION",
        "PREPARE",
        "PRESERVE",
        "PREV",
        "PRIMARY",
        "PRIVILEGES",
        "PROCEDURE",
        "PROCESSLIST",
        "PROFILE",
        "PROFILES",
        "PROXY",
        "PURGE",
        "QUARTER",
        "QUERY",
        "QUICK",
        "RANGE",
        "READ",
        "READS",
        "READ_ONLY",
        "READ_WRITE",
        "REAL",
        "REBUILD",
        "RECOVER",
        "REDOFILE",
        "REDO_BUFFER_SIZE",
        "REDUNDANT",
        "REFERENCES",
        "REGEXP",
        "RELAY",
        "RELAYLOG",
        "RELAY_LOG_FILE",
        "RELAY_LOG_POS",
        "RELAY_THREAD",
        "RELEASE",
        "RELOAD",
        "REMOVE",
        "RENAME",
        "REORGANIZE",
        "REPAIR",
        "REPEAT",
        "REPEATABLE",
        "REPLACE",
        "REPLICATE_DO_DB",
        "REPLICATE_DO_TABLE",
        "REPLICATE_IGNORE_DB",
        "REPLICATE_IGNORE_TABLE",
        "REPLICATE_REWRITE_DB",
        "REPLICATE_WILD_DO_TABLE",
        "REPLICATE_WILD_IGNORE_TABLE",
        "REPLICATION",
        "REQUIRE",
        "RESET",
        "RESIGNAL",
        "RESTORE",
        "RESTRICT",
        "RESUME",
        "RETURN",
        "RETURNED_SQLSTATE",
        "RETURNS",
        "REVERSE",
        "REVOKE",
        "RIGHT",
        "RLIKE",
        "ROLLBACK",
        "ROLLUP",
        "ROTATE",
        "ROUTINE",
        "ROW",
        "ROWS",
        "ROW_COUNT",
        "ROW_FORMAT",
        "RTREE",
        "SAVEPOINT",
        "SCHEDULE",
        "SCHEMA",
        "SCHEMAS",
        "SCHEMA_NAME",
        "SECOND",
        "SECOND_MICROSECOND",
        "SECURITY",
        "SELECT",
        "SENSITIVE",
        "SEPARATOR",
        "SERIAL",
        "SERIALIZABLE",
        "SERVER",
        "SESSION",
        "SET",
        "SHARE",
        "SHOW",
        "SHUTDOWN",
        "SIGNAL",
        "SIGNED",
        "SIMPLE",
        "SLAVE",
        "SLOW",
        "SMALLINT",
        "SNAPSHOT",
        "SOCKET",
        "SOME",
        "SONAME",
        "SOUNDS",
        "SOURCE",
        "SPATIAL",
        "SPECIFIC",
        "SQL",
        "SQLEXCEPTION",
        "SQLSTATE",
        "SQLWARNING",
        "SQL_AFTER_GTIDS",
        "SQL_AFTER_MTS_GAPS",
        "SQL_BEFORE_GTIDS",
        "SQL_BIG_RESULT",
        "SQL_BUFFER_RESULT",
        "SQL_CACHE",
        "SQL_CALC_FOUND_ROWS",
        "SQL_NO_CACHE",
        "SQL_SMALL_RESULT",
        "SQL_THREAD",
        "SQL_TSI_DAY",
        "SQL_TSI_HOUR",
        "SQL_TSI_MINUTE",
        "SQL_TSI_MONTH",
        "SQL_TSI_QUARTER",
        "SQL_TSI_SECOND",
        "SQL_TSI_WEEK",
        "SQL_TSI_YEAR",
        "SSL",
        "STACKED",
        "START",
        "STARTING",
        "STARTS",
        "STATS_AUTO_RECALC",
        "STATS_PERSISTENT",
        "STATS_SAMPLE_PAGES",
        "STATUS",
        "STOP",
        "STORAGE",
        "STORED",
        "STRAIGHT_JOIN",
        "STRING",
        "SUBCLASS_ORIGIN",
        "SUBJECT",
        "SUBPARTITION",
        "SUBPARTITIONS",
        "SUPER",
        "SUSPEND",
        "SWAPS",
        "SWITCHES",
        "TABLE",
        "TABLES",
        "TABLESPACE",
        "TABLE_CHECKSUM",
        "TABLE_NAME",
        "TEMPORARY",
        "TEMPTABLE",
        "TERMINATED",
        "TEXT",
        "THAN",
        "THEN",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMPADD",
        "TIMESTAMPDIFF",
        "TINYBLOB",
        "TINYINT",
        "TINYTEXT",
        "TO",
        "TRAILING",
        "TRANSACTION",
        "TRIGGER",
        "TRIGGERS",
        "TRUE",
        "TRUNCATE",
        "TYPE",
        "TYPES",
        "UNCOMMITTED",
        "UNDEFINED",
        "UNDO",
        "UNDOFILE",
        "UNDO_BUFFER_SIZE",
        "UNICODE",
        "UNINSTALL",
        "UNION",
        "UNIQUE",
        "UNKNOWN",
        "UNLOCK",
        "UNSIGNED",
        "UNTIL",
        "UPDATE",
        "UPGRADE",
        "USAGE",
        "USE",
        "USER",
        "USER_RESOURCES",
        "USE_FRM",
        "USING",
        "UTC_DATE",
        "UTC_TIME",
        "UTC_TIMESTAMP",
        "VALIDATION",
        "VALUE",
        "VALUES",
        "VARBINARY",
        "VARCHAR",
        "VARCHARACTER",
        "VARIABLES",
        "VARYING",
        "VIEW",
        "VIRTUAL",
        "WAIT",
        "WARNINGS",
        "WEEK",
        "WEIGHT_STRING",
        "WHEN",
        "WHERE",
        "WHILE",
        "WITH",
        "WITHOUT",
        "WORK",
        "WRAPPER",
        "WRITE",
        "X509",
        "XA",
        "XID",
        "XML",
        "XOR",
        "YEAR",
        "YEAR_MONTH",
        "ZEROFILL"
    ];


    pub fn is_key_words(columnname: &str) -> bool {
        Self::KEY_WORDS.contains(&columnname.to_uppercase().as_str())
    }
    pub fn format_column(columnname: &str) -> String {
        format!("`{}`", columnname)
    }
}

/// MYSQL 数据库字段类型转换
pub struct MySqlTypeConvert;

pub enum TargetLang {
    Java,
    Rust,
}

impl MySqlTypeConvert {

    pub fn process_type_convert(field_type: String, target: TargetLang) -> DbColumnType {
        match target {
            TargetLang::Java => JavaDbColumnType::process_type_convert(&field_type),
            TargetLang::Rust => RustDbColumnType::process_type_convert(&field_type),
        }
    }
}

/// MYSQL 数据库名称转换
pub struct MySqlNameConvert;

#[derive(Debug, Clone, serde::Deserialize, Serialize)]
pub enum NamingConvert {
    Mysql
}

impl INameConvert for NamingConvert {

}


/// 名称转换接口类
pub trait INameConvert {
    /// 执行实体名称转换
    fn entity_name_convert(&self, table_info: &TableInfo) -> String {
        // 默认实现
        table_info.entity_name.to_string()
    }
    /// 执行属性名称转换
    fn property_name_convert(&self, field: &TableField) -> String {
        field.property_name.to_string()
    }
}
