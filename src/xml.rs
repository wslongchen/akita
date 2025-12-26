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
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use dashmap::DashMap;
use crate::config::XmlSqlLoaderConfig;
use crate::errors::{AkitaError, SqlLoaderError};

// SQL Statements are supported with parameters
#[derive(Debug, Clone)]
pub struct SqlStatement {
    pub id: String,
    pub raw_sql: String,
    pub description: Option<String>,
    pub parameters: Vec<String>, // Extracted named parameters
}

// SQL Parameter types
#[derive(Debug, Clone)]
pub enum SqlParameter {
    Text(String),
    Number(String),
    Boolean(bool),
    Null,
}

// Cache structure
#[derive(Debug, Clone)]
pub struct SqlCache {
    pub sql_map: HashMap<String, String>,
    pub statements: HashMap<String, SqlStatement>,
    pub last_modified: u64,
    pub file_path: PathBuf,
}

// The main XML SQL loader
#[derive(Debug, Clone)]
pub struct XmlSqlLoader {
    cache: DashMap<PathBuf, SqlCache>,
    auto_reload: bool,
    parameter_detection: bool,
    sql_formatting: bool,
}

impl Default for XmlSqlLoader {
    fn default() -> Self {
        Self::new(XmlSqlLoaderConfig::default())
    }
}

impl XmlSqlLoader {
    /// Create a new SQL loader
    pub fn new(cfg: XmlSqlLoaderConfig) -> Self {
        Self {
            auto_reload: cfg.auto_reload,
            parameter_detection: cfg.parameter_detection,
            sql_formatting: cfg.sql_formatting,
            cache: DashMap::new(),
        }
    }

    /// Loading SQL statements (with smart caching)
    pub fn load_sql(&self, xml_file: &str, sql_id: &str) -> Result<String, AkitaError> {
        let path = PathBuf::from(xml_file);

        // Check if a reload is required
        if self.should_reload(&path) {
            self.reload_file(&path)?;
        }

        // Get from cache
        if let Some(cache) = self.cache.get(&path) {
            if let Some(sql) = cache.sql_map.get(sql_id) {
                return Ok(self.format_sql(sql));
            }
        }

        // Cache miss, reload file
        let cache = self.load_and_cache(&path)?;
        cache.sql_map.get(sql_id)
            .cloned()
            .map(|sql| self.format_sql(&sql))
            .ok_or_else(|| AkitaError::SqlLoaderError(SqlLoaderError::SqlNotFound(sql_id.to_string())))
    }

    /// Loading SQL statement structs (containing parameter information)
    pub fn load_sql_statement(&mut self, xml_file: &str, sql_id: &str) -> Result<SqlStatement, AkitaError> {
        let path = PathBuf::from(xml_file);

        if self.should_reload(&path) {
            self.reload_file(&path)?;
        }

        if let Some(cache) = self.cache.get(&path) {
            if let Some(statement) = cache.statements.get(sql_id) {
                return Ok(statement.clone());
            }
        }

        let cache = self.load_and_cache(&path)?;
        cache.statements.get(sql_id)
            .cloned()
            .ok_or_else(|| AkitaError::SqlLoaderError(SqlLoaderError::SqlNotFound(sql_id.to_string())))
    }

    /// Load namespace SQL
    pub fn load_namespaced_sql(&mut self, xml_file: &str, namespace: &str, sql_id: &str) -> Result<String, AkitaError> {
        let full_id = format!("{}:{}", namespace, sql_id);
        self.load_sql(xml_file, &full_id)
    }

    /// Load all SQL statements in bulk
    pub fn load_all_sql(&mut self, xml_file: &str) -> Result<HashMap<String, String>, AkitaError> {
        let path = PathBuf::from(xml_file);
        let cache = self.load_and_cache(&path)?;
        Ok(cache.sql_map.clone())
    }

    /// Get a list of SQL parameters
    pub fn get_sql_parameters(&mut self, xml_file: &str, sql_id: &str) -> Result<Vec<String>, AkitaError> {
        let statement = self.load_sql_statement(xml_file, sql_id)?;
        Ok(statement.parameters)
    }

    /// Replacing SQL parameters
    pub fn replace_sql_parameters(&self, sql: &str, params: &HashMap<&str, SqlParameter>) -> Result<String, AkitaError> {
        let mut result = sql.to_string();

        for (key, value) in params {
            let placeholder = format!(":{}", key);
            let replacement = match value {
                SqlParameter::Text(s) => format!("'{}'", s.replace('\'', "''")),
                SqlParameter::Number(n) => n.clone(),
                SqlParameter::Boolean(b) => b.to_string(),
                SqlParameter::Null => "NULL".to_string(),
            };

            result = result.replace(&placeholder, &replacement);
        }

        // Check if there are any unreplaced arguments left
        if self.parameter_detection && result.contains(':') {
            // Extract the remaining named arguments
            let remaining_params: Vec<String> = result
                .split_whitespace()
                .filter(|word| word.starts_with(':') && word.len() > 1)
                .map(|s| s[1..].to_string())
                .collect();

            if !remaining_params.is_empty() {
                return Err(AkitaError::SqlLoaderError(SqlLoaderError::ParameterError(
                    format!("Parameters not provided: {:?}", remaining_params)
                )));
            }
        }

        Ok(result)
    }

    /// Clearing the cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Clear the cache of a specific file
    pub fn clear_file_cache(&mut self, xml_file: &str) {
        let path = PathBuf::from(xml_file);
        self.cache.remove(&path);
    }

    /// Gets when the file was last modified
    pub fn get_file_last_modified(&self, xml_file: &str) -> Result<u64, AkitaError> {
        let path = PathBuf::from(xml_file);
        let metadata = fs::metadata(&path)
            .map_err(|e| SqlLoaderError::FileReadError(e.to_string()))?;

        let modified = metadata.modified()
            .map_err(|e| SqlLoaderError::FileReadError(e.to_string()))?;

        Ok(modified.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs())
    }

    /// Validate SQL files
    pub fn validate_xml_file(&self, xml_file: &str) -> Result<(), AkitaError> {
        let content = fs::read_to_string(xml_file)
            .map_err(|e| SqlLoaderError::FileReadError(e.to_string()))?;

        // Basic XML structure validation
        if !content.contains("<sqls>") && !content.contains("<sql ") {
            return Err(AkitaError::SqlLoaderError(SqlLoaderError::XmlParseError("缺少根元素或 SQL 元素".to_string())));
        }

        // Use quick-xml validation
        let mut reader = Reader::from_str(&content);
        let mut buf = Vec::new();
        let mut sql_count = 0;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    if e.name().as_ref() == b"sql" {
                        sql_count += 1;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(AkitaError::SqlLoaderError(SqlLoaderError::XmlParseError(e.to_string()))),
                _ => {}
            }
            buf.clear();
        }

        if sql_count == 0 {
            return Err(AkitaError::SqlLoaderError(SqlLoaderError::XmlParseError("未找到 SQL 语句".to_string())));
        }

        Ok(())
    }

    /// Exporting SQL to a file
    pub fn export_sql_to_file(&self, xml_file: &str, output_file: &str, format: bool) -> Result<(), AkitaError> {
        let sql_map = self.parse_xml_file(xml_file)?;
        let mut output = String::from("-- SQL statement export\n-- Source: ");
        output.push_str(xml_file);
        output.push_str("\n-- Generation time: ");
        output.push_str(&chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
        output.push_str("\n\n");

        for (id, sql) in sql_map {
            output.push_str(&format!("-- SQL ID: {}\n", id));
            if format {
                output.push_str(&self.format_sql(&sql));
            } else {
                output.push_str(&sql);
            }
            output.push_str("\n\n");
        }

        fs::write(output_file, output)
            .map_err(|e| AkitaError::SqlLoaderError(SqlLoaderError::FileReadError(e.to_string())))
    }

    // --- Private methods ---

    fn should_reload(&self, path: &Path) -> bool {
        if !self.auto_reload {
            return false;
        }

        if let Some(cache) = self.cache.get(path) {
            match self.get_file_last_modified(path.to_str().unwrap()) {
                Ok(current) => current > cache.last_modified,
                Err(_) => false,
            }
        } else {
            true
        }
    }

    /// Loading SQL statements from multiple XML files (supports SQL fragment inheritance or override)
    pub fn load_sql_from_multiple_files(&self, files: &[&str], sql_id: &str) -> Result<String, String> {
        for file in files {
            if let Ok(sql) = self.load_sql(file, sql_id) {
                return Ok(sql);
            }
        }
        Err(format!("SQL with id '{}' not found in any of the provided files", sql_id))
    }
    
    fn reload_file(&self, path: &Path) -> Result<(), AkitaError> {
        self.load_and_cache(path)?;
        Ok(())
    }

    fn load_and_cache(&self, path: &Path) -> Result<SqlCache, AkitaError> {
        let last_modified = self.get_file_last_modified(path.to_str().unwrap())?;
        let (sql_map, statements) = self.parse_xml_file_with_statements(path.to_str().unwrap())?;

        let cache = SqlCache {
            sql_map,
            statements,
            last_modified,
            file_path: path.to_path_buf(),
        };

        self.cache.insert(path.to_path_buf(), cache);
        if let Some(v) = self.cache.get(path) {
            let cache = v.value();
            Ok(cache.clone())
        } else {
            Err(AkitaError::SqlLoaderError(SqlLoaderError::FileReadError("load cache error".to_string())))
        }
        
    }

    fn parse_xml_file(&self, xml_file: &str) -> Result<HashMap<String, String>, AkitaError> {
        let content = fs::read_to_string(xml_file)
            .map_err(|e| SqlLoaderError::FileReadError(e.to_string()))?;

        Self::parse_xml_content(&content)
    }

    fn parse_xml_file_with_statements(&self, xml_file: &str) -> Result<(HashMap<String, String>, HashMap<String, SqlStatement>), AkitaError> {
        let content = fs::read_to_string(xml_file)
            .map_err(|e| SqlLoaderError::FileReadError(e.to_string()))?;

        let statements = Self::parse_sql_statements(&content)?;
        let mut sql_map = HashMap::new();
        let mut stmt_map = HashMap::new();

        for stmt in statements {
            sql_map.insert(stmt.id.clone(), stmt.raw_sql.clone());
            stmt_map.insert(stmt.id.clone(), stmt);
        }

        Ok((sql_map, stmt_map))
    }

    pub fn format_sql(&self, sql: &str) -> String {
        if !self.sql_formatting {
            return sql.to_string();
        }

        // Simplified SQL formatting
        let mut formatted = String::with_capacity(sql.len() * 2);
        let mut indent: usize = 0;

        let lines: Vec<&str> = sql.lines().collect();
        let mut i = 0;
        let line_count = lines.len();

        while i < line_count {
            let line = lines[i].trim();

            // Skip blank lines
            if line.is_empty() {
                i += 1;
                continue;
            }

            // Check if you need to reduce the indentation
            let upper_line = line.to_uppercase();
            if upper_line.starts_with("END") ||
                upper_line.starts_with("ELSE") ||
                line.ends_with(')') ||
                line.contains("};") {
                if indent > 0 {
                    indent -= 1;
                }
            }

            // Adding indentation
            formatted.push_str(&"    ".repeat(indent));
            formatted.push_str(line);
            formatted.push('\n');

            // Check if you need to increase the indentation
            if line.ends_with('(') ||
                upper_line.starts_with("SELECT") ||
                upper_line.starts_with("CASE") ||
                upper_line.starts_with("WHEN") ||
                upper_line.starts_with("BEGIN") ||
                upper_line.contains(" THEN") {
                indent += 1;
            }

            i += 1;
        }

        formatted.trim().to_string()
    }

    fn parse_xml_content(content: &str) -> Result<HashMap<String, String>, AkitaError> {
        let statements = Self::parse_sql_statements(content)?;
        let mut map = HashMap::new();

        for stmt in statements {
            map.insert(stmt.id, stmt.raw_sql);
        }

        Ok(map)
    }

    fn parse_sql_statements(content: &str) -> Result<Vec<SqlStatement>, SqlLoaderError> {
        let mut statements = Vec::new();
        let mut reader = Reader::from_str(content);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut current_id = None;
        let mut current_desc = None;
        let mut current_sql = String::new();
        let mut in_sql = false;
        let mut depth = 0;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    if e.name().as_ref() == b"sql" {
                        // Parsing attributes
                        for attr in e.attributes() {
                            match attr {
                                Ok(a) => match a.key.as_ref() {
                                    b"id" => {
                                        current_id = Some(
                                            std::str::from_utf8(&a.value)
                                                .map_err(|e| SqlLoaderError::XmlParseError(e.to_string()))?
                                                .to_string()
                                        );
                                    }
                                    b"desc" | b"description" => {
                                        current_desc = Some(
                                            std::str::from_utf8(&a.value)
                                                .map_err(|e| SqlLoaderError::XmlParseError(e.to_string()))?
                                                .to_string()
                                        );
                                    }
                                    _ => {}
                                },
                                Err(_) => continue,
                            }
                        }

                        current_sql.clear();
                        in_sql = true;
                        depth = 1;
                    } else if in_sql {
                        depth += 1;
                        current_sql.push_str(&String::from_utf8_lossy(&e.to_vec()));
                    }
                }

                Ok(Event::End(e)) => {
                    if in_sql {
                        if e.name().as_ref() == b"sql" && depth == 1 {
                            if let Some(id) = current_id.take() {
                                let raw_sql = current_sql.trim().to_string();
                                let parameters = Self::extract_parameters(&raw_sql);

                                statements.push(SqlStatement {
                                    id: id.clone(),
                                    raw_sql,
                                    description: current_desc.take(),
                                    parameters,
                                });
                            }
                            in_sql = false;
                            current_sql.clear();
                        } else if depth > 1 {
                            depth -= 1;
                            current_sql.push_str(&format!("</{}>",
                                                          String::from_utf8_lossy(e.name().as_ref())));
                        }
                    }
                }

                Ok(Event::Text(e)) => {
                    if in_sql {
                        // Convert text directly using from_utf8
                        let text = std::str::from_utf8(&e)
                            .map_err(|e| SqlLoaderError::XmlParseError(e.to_string()))?;
                        current_sql.push_str(text);
                    }
                }

                Ok(Event::CData(e)) => {
                    if in_sql {
                        // Convert CDATA directly using from_utf8
                        let cdata = std::str::from_utf8(&e)
                            .map_err(|e| SqlLoaderError::XmlParseError(e.to_string()))?;
                        current_sql.push_str(cdata);
                    }
                }

                Ok(Event::Eof) => break,

                Err(e) => return Err(SqlLoaderError::XmlParseError(e.to_string())),

                _ => {}
            }

            buf.clear();
        }

        Ok(statements)
    }

    fn extract_parameters(sql: &str) -> Vec<String> {
        let mut params = Vec::new();
        let words: Vec<&str> = sql.split_whitespace().collect();

        for word in words {
            if word.starts_with(':') && word.len() > 1 {
                let param = word[1..].trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if !param.is_empty() && !params.contains(&param.to_string()) {
                    params.push(param.to_string());
                }
            }
        }

        params
    }
}


// Test module
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_xml() -> NamedTempFile {
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<sqls>
    <!-- 用户相关 SQL -->
    <sql id="user:getById" description="根据ID查询用户">
        SELECT * FROM users WHERE id = :id AND status = :status
    </sql>
    
    <sql id="user:getAll" description="获取所有活跃用户">
        SELECT id, name, email 
        FROM users 
        WHERE status = 'active'
        ORDER BY created_at DESC
    </sql>
    
    <sql id="user:update" description="更新用户信息">
        UPDATE users 
        SET name = :name, email = :email, updated_at = NOW()
        WHERE id = :id
    </sql>
    
    <sql id="order:getByUser" description="获取用户订单">
        <![CDATA[
        SELECT o.*, u.name as user_name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE u.id = :userId 
        AND o.status IN (:status1, :status2)
        ORDER BY o.created_at DESC
        ]]>
    </sql>
</sqls>"#;

        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", xml_content).unwrap();
        file
    }

    #[test]
    fn test_basic_loading() {
        let xml_file = create_test_xml();
        let path = xml_file.path().to_str().unwrap();

        let mut loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());

        // 测试基本加载
        let sql = loader.load_sql(path, "user:getById").unwrap();
        assert!(sql.contains("SELECT * FROM users"));
        assert!(sql.contains("WHERE id = :id"));

        // 测试参数提取
        let stmt = loader.load_sql_statement(path, "user:getById").unwrap();
        assert_eq!(stmt.parameters, vec!["id", "status"]);

        // 测试命名空间查询
        let user_sql = loader.load_namespaced_sql(path, "user", "getAll").unwrap();
        assert!(user_sql.contains("FROM users"));
    }

    #[test]
    fn test_parameter_replacement() {
        let loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());
        let sql = "SELECT * FROM users WHERE id = :id AND name = :name";

        let mut params = HashMap::new();
        params.insert("id", SqlParameter::Number("123".to_string()));
        params.insert("name", SqlParameter::Text("John".to_string()));

        let replaced = loader.replace_sql_parameters(sql, &params).unwrap();
        assert_eq!(replaced, "SELECT * FROM users WHERE id = 123 AND name = 'John'");

        // 测试缺失参数
        let mut params2 = HashMap::new();
        params2.insert("id", SqlParameter::Number("123".to_string()));

        let result = loader.replace_sql_parameters(sql, &params2);
        assert!(result.is_err());
    }

    #[test]
    fn test_cache_operations() {
        let xml_file = create_test_xml();
        let path = xml_file.path().to_str().unwrap();

        let mut loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());

        // 首次加载
        loader.load_sql(path, "user:getById").unwrap();
        assert!(loader.cache.contains_key(&PathBuf::from(path)));

        // 清除缓存
        loader.clear_file_cache(path);
        assert!(!loader.cache.contains_key(&PathBuf::from(path)));

        // 再次加载
        loader.load_sql(path, "user:getById").unwrap();
        assert!(loader.cache.contains_key(&PathBuf::from(path)));

        // 清除所有缓存
        loader.clear_cache();
        assert!(loader.cache.is_empty());
    }

    #[test]
    fn test_validation() {
        let xml_file = create_test_xml();
        let path = xml_file.path().to_str().unwrap();

        let loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());

        // 验证有效文件
        let result = loader.validate_xml_file(path);
        assert!(result.is_ok());

        // 创建无效 XML 文件
        let invalid_file = NamedTempFile::new().unwrap();
        let invalid_path = invalid_file.path().to_str().unwrap();
        fs::write(invalid_path, "invalid content").unwrap();

        let result = loader.validate_xml_file(invalid_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_formatting() {
        let loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());

        let unformatted = "SELECT * FROM users WHERE id = :id ORDER BY name";
        let formatted = loader.format_sql(unformatted);

        assert!(formatted.starts_with("SELECT * FROM users"));
        // 可以根据需要添加更具体的格式断言
    }

    #[test]
    fn test_load_all_sql() {
        let xml_file = create_test_xml();
        let path = xml_file.path().to_str().unwrap();

        let mut loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());
        let all_sql = loader.load_all_sql(path).unwrap();

        assert_eq!(all_sql.len(), 4);
        assert!(all_sql.contains_key("user:getById"));
        assert!(all_sql.contains_key("user:getAll"));
        assert!(all_sql.contains_key("user:update"));
        assert!(all_sql.contains_key("order:getByUser"));
    }
}

// 使用示例
pub fn example_usage() {
    // 创建加载器
    let mut loader = XmlSqlLoader::new(XmlSqlLoaderConfig::default());

    // 加载 SQL
    match loader.load_sql("sql/queries.xml", "user:getById") {
        Ok(sql) => {
            println!("加载的 SQL: {}", sql);

            // 准备参数
            let mut params = HashMap::new();
            params.insert("id", SqlParameter::Number("123".to_string()));
            params.insert("status", SqlParameter::Text("active".to_string()));

            // 替换参数
            match loader.replace_sql_parameters(&sql, &params) {
                Ok(final_sql) => println!("最终的 SQL: {}", final_sql),
                Err(e) => eprintln!("参数替换错误: {}", e),
            }
        }
        Err(e) => eprintln!("SQL 加载错误: {}", e),
    }

    // 批量操作
    match loader.load_all_sql("sql/queries.xml") {
        Ok(all_sql) => {
            println!("共加载 {} 条 SQL 语句", all_sql.len());
        }
        Err(e) => eprintln!("批量加载错误: {}", e),
    }
}