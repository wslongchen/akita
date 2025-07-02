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
use serde::{Deserialize, Serialize};
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
        if self.column_type.get_type().eq_ignore_ascii_case("boolean") {
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
            column_type: DbColumnType::String,
            comment: "".to_string(),
            fill: "".to_string(),
            key_words: false,
            column_name: "".to_string(),
            custom_map: Default::default(),
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub enum DbColumnType {
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

    // 其他杂类
    ByteArray,
    Object,
    Date,
    BigInteger,
    BigDecimal,

}

impl DbColumnType {
    pub fn get_type(&self) -> String {
        match self {
            DbColumnType::BaseByte => "byte".to_string(),
            DbColumnType::BaseShort => "short".to_string(),
            DbColumnType::BaseChar => "char".to_string(),
            DbColumnType::BaseInt => "int".to_string(),
            DbColumnType::BaseLong => "long".to_string(),
            DbColumnType::BaseFloat => "float".to_string(),
            DbColumnType::BaseDouble => "double".to_string(),
            DbColumnType::BaseBoolean => "boolean".to_string(),

            // 包装类型
            DbColumnType::Byte => "Byte".to_string(),
            DbColumnType::Short => "Short".to_string(),
            DbColumnType::Character => "Character".to_string(),
            DbColumnType::Integer => "Integer".to_string(),
            DbColumnType::Long => "Long".to_string(),
            DbColumnType::Float => "Float".to_string(),
            DbColumnType::Double => "Double".to_string(),
            DbColumnType::Boolean => "Boolean".to_string(),
            DbColumnType::String => "String".to_string(),

            // sql 包下数据类型
            DbColumnType::DateSql => "Date".to_string(),
            DbColumnType::Time => "Time".to_string(),
            DbColumnType::Timestamp => "Timestamp".to_string(),
            DbColumnType::Blob => "Blob".to_string(),
            DbColumnType::Clob => "Clob".to_string(),

            // java8 新时间类型
            DbColumnType::LocalDate => "LocalDate".to_string(),
            DbColumnType::LocalTime => "LocalTime".to_string(),
            DbColumnType::Year => "Year".to_string(),
            DbColumnType::YearMonth => "YearMonth".to_string(),
            DbColumnType::LocalDateTime => "LocalDateTime".to_string(),
            DbColumnType::Instant => "Instant".to_string(),

            // 其他杂类
            DbColumnType::ByteArray => "byte[]".to_string(),
            DbColumnType::Object => "Object".to_string(),
            DbColumnType::Date => "Date".to_string(),
            DbColumnType::BigInteger => "BigInteger".to_string(),
            DbColumnType::BigDecimal => "BigDecimal".to_string(),
        }
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

impl MySqlTypeConvert {
    pub fn process_type_convert(field_type: String) -> DbColumnType {
        let t = field_type.to_lowercase();
        if t.contains("char") {
            return DbColumnType::String;
        } else if t.contains("bigint") {
            return DbColumnType::Long;
        } else if t.contains("tinyint(1)") {
            return DbColumnType::Boolean;
        } else if t.contains("int") {
            return DbColumnType::Integer;
        } else if t.contains("text") {
            return DbColumnType::String;
        } else if t.contains("bit") {
            return DbColumnType::Boolean;
        } else if t.contains("decimal") {
            return DbColumnType::BigDecimal;
        } else if t.contains("clob") {
            return DbColumnType::Clob;
        } else if t.contains("blob") {
            return DbColumnType::Blob;
        } else if t.contains("binary") {
            return DbColumnType::ByteArray;
        } else if t.contains("float") {
            return DbColumnType::Float;
        } else if t.contains("double") {
            return DbColumnType::Double;
        } else if t.contains("json") || t.contains("enum") {
            return DbColumnType::String;
        } else if t.contains("date") {
            return DbColumnType::Date;
        } else if t.contains("time") {
            return DbColumnType::LocalTime;
        } else if t.contains("year") {
            return DbColumnType::Year;
        }
        DbColumnType::String
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
