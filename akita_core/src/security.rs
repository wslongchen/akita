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
use regex::Regex;
use lazy_static::lazy_static;
use tracing::trace;
use crate::{AkitaDataError, OperationType, Params};

/// SQL Inject the inspection module
#[derive(Debug, Clone)]
pub struct SqlInjectionDetector {
    // SQL Inject feature patterns
    injection_patterns: Vec<Regex>,
    // Blacklist of dangerous keywords
    high_risk_keywords: HashSet<String>,
    suspicious_keywords: HashSet<String>,
    // Safe function whitelisting
    safe_functions: HashSet<String>,
    // Dangerous function blacklist
    dangerous_functions: HashSet<String>,
    // Secure configuration
    config: SqlSecurityConfig,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectionSeverity {
    Low,   
    Medium,   
    High,    
    Critical,
}

#[derive(Debug, Clone)]
pub struct DetectionResult {
    pub is_dangerous: bool,
    pub severity: DetectionSeverity,
    pub reason: String,
    pub patterns: Vec<String>,
    pub suggestions: Vec<String>,
}

impl DetectionResult {
    pub fn safe() -> Self {
        Self {
            is_dangerous: false,
            severity: DetectionSeverity::Low,
            reason: String::new(),
            patterns: Vec::new(),
            suggestions: Vec::new(),
        }
    }

    pub fn dangerous(severity: DetectionSeverity, reason: String, patterns: Vec<String>, suggestions: Vec<String>) -> Self {
        Self {
            is_dangerous: true,
            severity,
            reason,
            patterns,
            suggestions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlSecurityConfig {
    pub max_placeholders: usize,
    pub max_query_length: usize,
    pub max_values_clauses_insert: usize,
    pub max_values_clauses_insert_select: usize,
    pub max_params_clauses_insert_multi: usize,
    pub max_params_clauses_insert: usize,
    pub max_params_clauses_insert_select: usize,
    pub max_update_rows: u32,
    pub max_delete_rows: u32,
    pub allow_nested_queries: bool,
    pub allow_union_all: bool,
    pub allow_window_functions: bool,
    pub allow_recursive_queries: bool,
    pub require_where_clause: bool,
    pub require_limit_on_delete: bool,
    pub max_subquery_depth: usize,
    pub max_window_function_count: usize,
}

#[derive(Debug, Default)]
struct SqlStructure {
    string_starts: Vec<usize>,
    string_ends: Vec<usize>,
    comment_positions: Vec<usize>,
}


impl Default for SqlSecurityConfig {
    fn default() -> Self {
        Self {
            max_placeholders: 100,
            max_query_length: 10000,
            max_values_clauses_insert: 1000,
            max_values_clauses_insert_select: 1,
            max_params_clauses_insert_multi: 100,
            max_params_clauses_insert: 1000,
            max_params_clauses_insert_select: 500,
            max_update_rows: 10000,
            max_delete_rows: 5000,
            allow_nested_queries: true,
            allow_union_all: false,
            allow_window_functions: true,
            allow_recursive_queries: false,
            require_where_clause: true,
            require_limit_on_delete: false,
            max_subquery_depth: 5,
            max_window_function_count: 10,
        }
    }
}
impl PartialEq for SqlInjectionDetector {
    fn eq(&self, other: &Self) -> bool {
        self.high_risk_keywords == other.high_risk_keywords &&
        self.suspicious_keywords == other.suspicious_keywords &&
            self.safe_functions == other.safe_functions &&
            self.dangerous_functions == other.dangerous_functions
    }
}


impl Eq for SqlInjectionDetector {}
impl SqlInjectionDetector {
    pub fn new() -> Self {
        Self::with_config(SqlSecurityConfig::default())
    }

    pub fn with_config(config: SqlSecurityConfig) -> Self {
        lazy_static! {
            static ref SQL_INJECTION_PATTERNS: Vec<Regex> = {
                let mut patterns = Vec::new();

                // ========== Basic attack patterns ==========

                // 1. Annotation attacks
                patterns.push(Regex::new(r"(?i)--\s*$").unwrap());
                patterns.push(Regex::new(r"(?i)/\*[^*]*\*/").unwrap());
                patterns.push(Regex::new(r"(?i)#[^\n]*$").unwrap());

                // 2. Federated query attacks
                patterns.push(Regex::new(r"(?i)\bunion\s+all\s+select\b").unwrap());
                patterns.push(Regex::new(r"(?i)\bunion\s+select\b").unwrap());

                // 3. Multi-sentence attack
                patterns.push(Regex::new(r"(?i);\s*(select|insert|update|delete|drop|create|alter)\b").unwrap());

                // 4. Stacked query attacks
                patterns.push(Regex::new(r"(?i);\s*(drop\s+table|truncate\s+table|delete\s+from)\b").unwrap());

                // ========== SQL injection into the classic pattern ==========

                // 5. Conditional Eternal Attack
                patterns.push(Regex::new(r#"['"]\s*or\s*['"]\s*['"]\s*=\s*['"]"#).unwrap());
                patterns.push(Regex::new(r#"['"]\s*and\s*['"]\s*['"]\s*=\s*['"]"#).unwrap());
                patterns.push(Regex::new(r"1\s*=\s*1\s*$").unwrap());
                patterns.push(Regex::new(r#"'1'='1'"#).unwrap());

                // 6. Time-lapse attack
                patterns.push(Regex::new(r"(?i)sleep\s*\(\s*\d+\s*\)").unwrap());
                patterns.push(Regex::new(r#"(?i)waitfor\s+delay\s+['"][^'"]+['"]"#).unwrap());
                patterns.push(Regex::new(r"(?i)pg_sleep\s*\(").unwrap());
                patterns.push(Regex::new(r"(?i)benchmark\s*\(").unwrap());

                // 7. File manipulation attacks
                patterns.push(Regex::new(r"(?i)\binto\s+(outfile|dumpfile)\b").unwrap());
                patterns.push(Regex::new(r"(?i)\bload_file\s*\(").unwrap());

                // 8. Coding bypass
                patterns.push(Regex::new(r"%27").unwrap());
                patterns.push(Regex::new(r"0x27").unwrap());
                patterns.push(Regex::new(r"\\x27").unwrap());
                patterns.push(Regex::new(r"(?i)chr\s*\(\s*39\s*\)").unwrap());

                // 9. Stored procedure attacks
                patterns.push(Regex::new(r"(?i)\bxp_cmdshell\b").unwrap());
                patterns.push(Regex::new(r"(?i)\bsp_oacreate\b").unwrap());

                // 10. Boolean blinds
                patterns.push(Regex::new(r"(?i)if\s*\([^)]+\)\s*(begin|then)").unwrap());
                patterns.push(Regex::new(r"(?i)case\s+when\s+[^)]+\s+then").unwrap());

                // 11. System table query
                patterns.push(Regex::new(r"(?i)information_schema\.").unwrap());
                patterns.push(Regex::new(r"(?i)sys\.").unwrap());

                // 12. Type conversion attacks
                patterns.push(Regex::new(r"(?i)convert\s*\([^,]+,\s*int").unwrap());
                patterns.push(Regex::new(r"(?i)cast\s*\([^)]+\s+as\s+int\s*\)").unwrap());

                // 13. Closed attacks
                patterns.push(Regex::new(r"\)\s*--").unwrap());
                patterns.push(Regex::new(r"\)\s*#").unwrap());

                // 14. Error injection
                patterns.push(Regex::new(r"(?i)extractvalue\s*\(").unwrap());
                patterns.push(Regex::new(r"(?i)updatexml\s*\(").unwrap());

                patterns
            };
        }

        let high_risk_keywords: HashSet<String> = vec![
            // Data destruction operations
            "DROP DATABASE", "DROP SCHEMA", "TRUNCATE TABLE", "DROP TABLE",
            "DROP INDEX", "DROP VIEW", "DROP FUNCTION", "DROP PROCEDURE",
            "DROP TRIGGER", "DROP CONSTRAINT", "DROP USER",

            // Permission actions
            "GRANT ALL", "REVOKE ALL", "CREATE USER", "ALTER USER",

            // System command execution
            "XP_CMDSHELL", "EXEC ", "EXECUTE ", "SP_EXECUTESQL",

            // File operations
            "INTO OUTFILE", "INTO DUMPFILE", "LOAD_FILE",

            // System table access
            "INFORMATION_SCHEMA.", "SYS.DATABASES", "SYS.TABLES",
            "MYSQL.", "PG_",

            // Hazard configuration modifications
            "SHUTDOWN", "KILL",
        ].into_iter().map(|s| s.to_string()).collect();

        let suspicious_keywords: HashSet<String> = vec![
            // Annotations (may be used for comment bypass)
            "--", "/*", "#",

            // Semicolon (may be used for multi-statement attacks)
            ";",

            // UNION operation (need to check if there is a LIMIT)
            "UNION ALL", "UNION SELECT",

            // Time-Delay Function (for Blinding)
            "SLEEP(", "BENCHMARK(", "WAITFOR DELAY", "PG_SLEEP(",

            // Coding bypass
            "0x27", "0x22", "%27", "%22",
            "CHR(39)", "CHR(34)", "\\x27", "\\x22",

            // Stacked query indicator
            ";SELECT", ";INSERT", ";UPDATE", ";DELETE", ";DROP",

            // Eternal condition
            "1=1", "'1'='1'", "OR 1=1", "AND 1=1",
        ].into_iter().map(|s| s.to_string()).collect();

        let dangerous_functions: HashSet<String> = vec![
            // System command execution
            "XP_CMDSHELL", "EXEC", "EXECUTE", "SP_", "SYS_EXEC", "SYS_EVAL",

            // File operations
            "LOAD_FILE", "LOAD_DATA",

            // Time-lapse attack
            "SLEEP", "BENCHMARK", "PG_SLEEP", "WAITFOR",

            // System information (possible information leakage)
            "@@VERSION", "@@HOSTNAME", "@@BASEDIR",

            // Encryption function (can be abused)
            "AES_ENCRYPT", "AES_DECRYPT",

            // Lock operation (possible DoS)
            "GET_LOCK", "RELEASE_LOCK",
        ].into_iter().map(|s| s.to_string()).collect();

        let safe_functions: HashSet<String> = vec![
            // Mathematical functions
            "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING",
            "COS", "COT", "DEGREES", "EXP", "FLOOR", "LN", "LOG",
            "LOG10", "LOG2", "MOD", "PI", "POW", "POWER", "RADIANS",
            "RAND", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE",

            // String functions
            "ASCII", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT",
            "CONCAT_WS", "FIELD", "FIND_IN_SET", "FORMAT", "INSERT",
            "INSTR", "LCASE", "LEFT", "LENGTH", "LOCATE", "LOWER",
            "LPAD", "LTRIM", "MID", "POSITION", "REPEAT", "REPLACE",
            "REVERSE", "RIGHT", "RPAD", "RTRIM", "SPACE", "STRCMP",
            "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX", "TRIM", "UCASE",
            "UPPER",

            // Date function
            "ADDDATE", "ADDTIME", "CURDATE", "CURRENT_DATE",
            "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURTIME", "DATE",
            "DATEDIFF", "DATE_ADD", "DATE_FORMAT", "DATE_SUB", "DAY",
            "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "EXTRACT",
            "FROM_DAYS", "FROM_UNIXTIME", "GET_FORMAT", "HOUR", "LAST_DAY",
            "MAKEDATE", "MAKETIME", "MICROSECOND", "MINUTE", "MONTH",
            "MONTHNAME", "NOW", "PERIOD_ADD", "PERIOD_DIFF", "QUARTER",
            "SECOND", "SEC_TO_TIME", "STR_TO_DATE", "SUBDATE", "SUBTIME",
            "SYSDATE", "TIME", "TIME_FORMAT", "TIME_TO_SEC", "TIMEDIFF",
            "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO_DAYS",
            "TO_SECONDS", "UNIX_TIMESTAMP", "UTC_DATE", "UTC_TIME",
            "UTC_TIMESTAMP", "WEEK", "WEEKDAY", "WEEKOFYEAR", "YEAR",
            "YEARWEEK",

            // Logical functions
            "IF", "IFNULL", "NULLIF", "COALESCE", "CASE",

            // Encryption function (contains only hash, not encryption and decryption)
            "MD5", "SHA1", "SHA2", "CRC32",

            // Conversion function
            "HEX", "UNHEX", "BIN", "OCT", "CONV", "CAST", "CONVERT",
        ].into_iter().map(|s| s.to_string()).collect();

        SqlInjectionDetector {
            injection_patterns: SQL_INJECTION_PATTERNS.clone(),
            high_risk_keywords,
            suspicious_keywords,
            safe_functions,
            dangerous_functions,
            config,
        }
    }

    // ========== Main detection method ==========

    pub fn contains_dangerous_operations(&self, sql: &str, params: &Params) -> Result<DetectionResult, AkitaDataError> {
        // Use the detector
        let security_result = self.detect_sql_security(sql,
                                                                              Some(&params.iter().map(|(k, v)| (k.unwrap_or_default().to_string(), v.as_str().unwrap_or_default().to_string())).collect()));
        if security_result.is_dangerous {
            tracing::warn!("{} [Akita] Security Event Detection - SQL: {}, Severity: {:?}, Reason: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), sql,security_result.severity,security_result.reason);
            // Decide whether to block execution based on severity
            match security_result.severity {
                DetectionSeverity::Critical | DetectionSeverity::High => {
                    return Err(AkitaDataError::sql_injection_error(sql, security_result));
                }
                DetectionSeverity::Medium => {
                    tracing::warn!("{} [Akita] Medium risk SQL allows execution, but logs are logged: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), sql);
                }
                DetectionSeverity::Low => {
                    trace!("{} [Akita] Low-risk SQL warnings: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), sql);
                }
            }
        }
        Ok(security_result)
    }
    
    pub fn detect_sql_security(&self, sql: &str, params: Option<&Vec<(String, String)>>) -> DetectionResult {
        let mut results = Vec::new();
        let mut patterns = Vec::new();
        let mut suggestions = Vec::new();

        // 1. SQL Injection detection
        if let Some(result) = self.detect_sql_injection(sql) {
            results.push((result.severity, result.reason.clone()));
            patterns.extend(result.patterns);
            suggestions.extend(result.suggestions);
        }

        // 2. SQL Structural safety inspection
        if let Some(result) = self.detect_structure_issues(sql) {
            results.push((result.severity, result.reason.clone()));
            patterns.extend(result.patterns);
            suggestions.extend(result.suggestions);
        }

        // 3. Parameter safety check
        if let Some(param_list) = params {
            for (key, value) in param_list {
                if let Some(result) = self.detect_parameter_issues(key, value) {
                    results.push((result.severity, result.reason.clone()));
                    patterns.extend(result.patterns);
                    suggestions.extend(result.suggestions);
                }
            }
        }

        // 4. Statement type-specific checks
        if let Some(result) = self.detect_statement_specific_issues(sql) {
            results.push((result.severity, result.reason.clone()));
            patterns.extend(result.patterns);
            suggestions.extend(result.suggestions);
        }

        // Summarize the results
        if results.is_empty() {
            DetectionResult::safe()
        } else {
            // Get the highest severity
            let max_severity = results.iter()
                .map(|(severity, _)| severity)
                .max_by_key(|s| match s {
                    DetectionSeverity::Critical => 4,
                    DetectionSeverity::High => 3,
                    DetectionSeverity::Medium => 2,
                    DetectionSeverity::Low => 1,
                })
                .unwrap_or(&DetectionSeverity::Low);

            let reason = results.iter()
                .map(|(_, r)| r.clone())
                .collect::<Vec<_>>()
                .join("; ");

            DetectionResult::dangerous(
                max_severity.clone(),
                reason,
                patterns,
                suggestions,
            )
        }
    }

    // ========== SQL Injection detection ==========

    pub fn detect_sql_injection(&self, input: &str) -> Option<DetectionResult> {
        if input.trim().is_empty() {
            return None;
        }

        let input_upper = input.to_uppercase();
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        // 1. Check for dangerous keywords
        for keyword in &self.high_risk_keywords {
            // High-risk keywords need to be matched exactly
            if self.contains_exact_keyword(&input_upper, keyword) {
                detected_patterns.push(format!("High-risk operations: {}", keyword));
                suggestions.push(format!("High-risk operation detected '{}'", keyword));
            }
        }

        // Check for suspicious keywords (loose match, context analysis required)
        for keyword in &self.suspicious_keywords {
            if input_upper.contains(&keyword.to_uppercase()) {
                // Check if it is really suspicious
                if self.is_truly_suspicious(input, keyword) {
                    detected_patterns.push(format!("Suspicious mode: {}", keyword));
                    suggestions.push("Please check the legitimacy of the SQL statement".to_string());
                }
            }
        }

        // Check the danger function
        for func in &self.dangerous_functions {
            let func_pattern = format!("{}\\(", func);
            if input_upper.contains(&func_pattern) || input_upper.contains(func) {
                detected_patterns.push(format!("Hazard function: {}", func));
                suggestions.push(format!("Avoid using dangerous functions '{}'", func));
            }
        }

        // 2. Check the regex pattern
        for pattern in &self.injection_patterns {
            if pattern.is_match(input) {
                detected_patterns.push(format!("Injection mode: {}", pattern.as_str()));
                suggestions.push("SQL injection feature pattern detected".to_string());
            }
        }

        // 3. Check for encoding bypass
        if self.detect_encoding_bypass(input) {
            detected_patterns.push("Coding bypass technology".to_string());
            suggestions.push("Coding bypass technique detected".to_string());
        }

        // 4. Check for abnormal character sequences
        if self.detect_anomalous_sequences(input) {
            detected_patterns.push("Exceptional character sequences".to_string());
            suggestions.push("An abnormal character sequence is detected".to_string());
        }

        // 5. Check for SQL structure anomalies
        if self.detect_sql_structure_anomalies(input) {
            detected_patterns.push("SQL Structural abnormalities".to_string());
            suggestions.push("SQL structure anomalies detected".to_string());
        }

        if !detected_patterns.is_empty() {
            let severity = if detected_patterns.len() > 3 {
                DetectionSeverity::Critical
            } else if detected_patterns.len() > 1 {
                DetectionSeverity::High
            } else {
                DetectionSeverity::Medium
            };

            Some(DetectionResult::dangerous(
                severity,
                format!("{} SQL injection features detected", detected_patterns.len()),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    fn is_truly_suspicious(&self, input: &str, keyword: &str) -> bool {
        match keyword {
            // UNION check
            "UNION ALL" | "UNION SELECT" => {
                // Check if there is a LIMIT or ORDER BY
                !input.to_uppercase().contains("LIMIT") &&
                    !input.to_uppercase().contains("ORDER BY")
            }

            // Comment Checking - Use the new string inspection method
            "--" | "/*" | "#" => {
                // Check if it's in the string, if it's in the string it's not really a comment
                !self.is_pattern_in_string(input, keyword)
            }

            // Semicolon check
            ";" => {
                // Check if there are multiple statements and the semicolon is not in the string
                let semicolon_positions: Vec<usize> = input.match_indices(';').map(|(i, _)| i).collect();

                // If there is only one semicolon, it is usually the normal statement to end
                if semicolon_positions.len() <= 1 {
                    return false;
                }

                // Check for semicolons that are not in the string (possibly a multi-statement attack)
                let structure = self.analyze_sql_structure(input);
                for pos in semicolon_positions {
                    if !self.is_position_in_string(pos, 1, &structure) {
                        return true;
                    }
                }

                false
            }

            // Other suspicious keywords
            _ => true,
        }
    }

    fn is_pattern_in_string(&self, sql: &str, pattern: &str) -> bool {
        let structure = self.analyze_sql_structure(sql);

        // Find all the occurrences of the pattern
        let pattern_len = pattern.len();
        let mut pattern_positions = Vec::new();

        for (i, _) in sql.match_indices(pattern) {
            pattern_positions.push(i);
        }

        // Check if each mode position is in a string
        for pos in pattern_positions {
            if self.is_position_in_string(pos, pattern_len, &structure) {
                return true;
            }
        }

        false
    }

    fn is_position_in_string(&self, position: usize, length: usize, structure: &SqlStructure) -> bool {
        // Check if the position is within any string range
        for i in 0..structure.string_starts.len() {
            if let Some(&start) = structure.string_starts.get(i) {
                if let Some(&end) = structure.string_ends.get(i) {
                    if position >= start && position + length <= end {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn analyze_sql_structure(&self, sql: &str) -> SqlStructure {
        let mut structure = SqlStructure::default();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut escaped = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let ch = chars[i];

            if escaped {
                escaped = false;
                i += 1;
                continue;
            }

            // Handle comments
            if !in_single_quote && !in_double_quote && !in_backtick {
                // Line notes
                if ch == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
                    in_line_comment = true;
                    structure.comment_positions.push(i);
                    i += 2;
                    continue;
                }

                // BLOCK ANNOTATIONS BEGIN
                if ch == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
                    in_block_comment = true;
                    structure.comment_positions.push(i);
                    i += 2;
                    continue;
                }

                // Block comments end
                if ch == '*' && i + 1 < chars.len() && chars[i + 1] == '/' && in_block_comment {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
            }

            // Process line comment end (line break)
            if ch == '\n' && in_line_comment {
                in_line_comment = false;
                i += 1;
                continue;
            }

            // If in the comment, skip the character
            if in_line_comment || in_block_comment {
                i += 1;
                continue;
            }

            // Handling escaping
            if ch == '\\' {
                escaped = true;
                i += 1;
                continue;
            }

            // Handle quotes
            match ch {
                '\'' => {
                    if !in_double_quote && !in_backtick {
                        in_single_quote = !in_single_quote;
                        if in_single_quote {
                            structure.string_starts.push(i);
                        } else {
                            structure.string_ends.push(i);
                        }
                    }
                }
                '"' => {
                    if !in_single_quote && !in_backtick {
                        in_double_quote = !in_double_quote;
                        if in_double_quote {
                            structure.string_starts.push(i);
                        } else {
                            structure.string_ends.push(i);
                        }
                    }
                }
                '`' => {
                    if !in_single_quote && !in_double_quote {
                        in_backtick = !in_backtick;
                    }
                }
                _ => {}
            }

            i += 1;
        }

        structure
    }

    fn contains_exact_keyword(&self, input: &str, keyword: &str) -> bool {
        // Check if precise keywords are included
        let keyword_upper = keyword.to_uppercase();

        // Include the check briefly
        if !input.contains(&keyword_upper) {
            return false;
        }

        // If it's a short keyword, a more precise check is required
        if keyword.len() <= 3 {
            // Use regular expressions to ensure that the word is complete
            let pattern = format!(r"\b{}\b", keyword_upper);
            if let Ok(re) = regex::Regex::new(&pattern) {
                return re.is_match(input);
            }
        }

        true
    }

    // ========== SQL Structural safety inspection ==========

    pub fn detect_structure_issues(&self, sql: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        // 1. Check for unclosed strings or comments
        if self.has_unclosed_strings_or_comments(sql) {
            detected_patterns.push("Unclosed strings or comments".to_string());
            suggestions.push("Make sure that the strings and comments in the SQL statement are properly closed".to_string());
        }

        // 2. Check for nested queries (if the configuration doesn't allow it)
        if !self.config.allow_nested_queries && self.has_nested_queries(sql) {
            detected_patterns.push("Nested queries".to_string());
            suggestions.push("The current configuration does not allow nested queries".to_string());
        }

        // 3. Check for adjacent placeholders
        if self.has_adjacent_placeholders(sql) {
            detected_patterns.push("Adjacent placeholders".to_string());
            suggestions.push("Avoid using adjacent placeholders (?? or ?, ?)".to_string());
        }

        // 4. Check the number of placeholders
        let placeholder_count = sql.matches('?').count();
        if placeholder_count > self.config.max_placeholders {
            detected_patterns.push(format!("Excessive number of placeholders: {}", placeholder_count));
            suggestions.push(format!("Reduce the number of placeholders, maximum allowed {}", self.config.max_placeholders));
        }

        // 5. Check the query length
        if sql.len() > self.config.max_query_length {
            detected_patterns.push(format!("Query is too long: {} character", sql.len()));
            suggestions.push(format!("Simplify queries with a maximum allowed {} character", self.config.max_query_length));
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::Medium,
                "SQL Structural safety issues".to_string(),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    // ========== Parameter safety check ==========

    pub fn detect_parameter_issues(&self, key: &str, value: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        // 1. Check for SQL injection in parameter values
        if let Some(result) = self.detect_sql_injection(value) {
            detected_patterns.extend(result.patterns);
            suggestions.extend(result.suggestions);
        }

        // 2. Check the parameter length
        if value.len() > 65535 {
            detected_patterns.push(format!("The length of the parameter '{}' exceeds the limit", key));
            suggestions.push("Reduce the length of the parameter value".to_string());
        }

        // 3. Check the binary data
        if value.contains('\0') {
            detected_patterns.push(format!("The parameter '{}' contains empty characters", key));
            suggestions.push("Avoid using null characters in parameters".to_string());
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::High,
                format!("There is a security issue with the parameter '{}'", key),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    // ========== Statement type-specific checks ==========

    pub fn detect_statement_specific_issues(&self, sql: &str) -> Option<DetectionResult> {
        let statement_type = OperationType::detect_operation_type(sql);

        match statement_type {
            OperationType::Select => self.validate_select_statement(sql),
            OperationType::Update => self.validate_update_statement(sql),
            OperationType::Delete => self.validate_delete_statement(sql),
            OperationType::Insert(..) => self.validate_insert_statement(sql),
            _ => None,
        }
    }

    fn validate_select_statement(&self, sql: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        let lower_sql = sql.to_lowercase();

        // 1. Check the UNION query
        if lower_sql.contains("union") {
            // Check UNION ALL
            if lower_sql.contains("union all") && !self.config.allow_union_all {
                detected_patterns.push("UNION ALL Query".to_string());
                suggestions.push("The current configuration does not allow it UNION ALL".to_string());
            }

            // Check if there is a LIMIT or ORDER BY
            let has_limit = lower_sql.contains("limit") || lower_sql.contains("top");
            let has_order_by = lower_sql.contains("order by");

            if !has_limit && !has_order_by {
                detected_patterns.push("UNION The query is missing LIMIT or ORDER BY".to_string());
                suggestions.push("Add a LIMIT or ORDER BY clause to the UNION query".to_string());
            }
        }

        // 2. Check the danger function
        for func in &self.dangerous_functions {
            if lower_sql.contains(&func.to_lowercase()) {
                detected_patterns.push(format!("Hazard function: {}", func));
                suggestions.push(format!("Avoid using dangerous functions '{}'", func));
            }
        }

        // 3. Check the subquery depth
        let subquery_depth = lower_sql.matches("(select").count();
        if subquery_depth > self.config.max_subquery_depth {
            detected_patterns.push(format!("The subquery depth is too large: {}", subquery_depth));
            suggestions.push(format!("Reduce subquery nesting, maximum allowed {}", self.config.max_subquery_depth));
        }

        // 4. Check Cartesian product attacks
        if lower_sql.matches(" cross join ").count() > 3 {
            detected_patterns.push("Overmuch CROSS JOIN".to_string());
            suggestions.push("Reduces the number of CROSS JOINS to avoid Cartesian product attacks".to_string());
        }

        // 5. Check the recursive query
        if lower_sql.contains("with recursive") && !self.config.allow_recursive_queries {
            detected_patterns.push("Recursive query".to_string());
            suggestions.push("The current configuration does not allow recursive queries".to_string());
        }

        // 6. Check the window function
        let window_function_count = lower_sql.matches("over (").count();
        if window_function_count > self.config.max_window_function_count {
            detected_patterns.push(format!("Too many window functions: {}", window_function_count));
            suggestions.push(format!("Reduce the number of window functions, maximum allowed {}", self.config.max_window_function_count));
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::Medium,
                "SELECT statement security issues".to_string(),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    fn validate_update_statement(&self, sql: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        let lower_sql = sql.to_lowercase();

        // 1. Check the WHERE clause (if configured to require)
        if self.config.require_where_clause && !lower_sql.contains("where") {
            detected_patterns.push("UPDATE Statements are missing WHERE clause".to_string());
            suggestions.push("Add a WHERE clause to the UPDATE statement".to_string());
        }

        // 2. Check if the WHERE condition is too simple
        if let Some(where_clause) = self.extract_where_clause(&lower_sql) {
            if self.is_dangerous_where_condition(&where_clause) {
                detected_patterns.push("Dangerous WHERE conditions".to_string());
                suggestions.push("Avoid using true or overly simplistic WHERE conditions".to_string());
            }
        }

        // 3. Check the SET clause security
        if let Some(set_clause) = self.extract_set_clause(&lower_sql) {
            if !self.is_update_set_clause_safe(&set_clause) {
                detected_patterns.push("Unsafe SET clause".to_string());
                suggestions.push("Check if the expression in the SET clause is safe".to_string());
            }
        }

        // 4. Check the update line limit
        if let Some(limit) = self.extract_limit_value(&lower_sql) {
            if limit > self.config.max_update_rows {
                detected_patterns.push(format!("Too many lines are updated: {}", limit));
                suggestions.push(format!("Reduce the number of update lines, maximum allowed {}", self.config.max_update_rows));
            }
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::High,
                "UPDATE Statement security issues".to_string(),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    fn validate_delete_statement(&self, sql: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        let lower_sql = sql.to_lowercase();

        // 1. Check the WHERE clause (if configured to require)
        if self.config.require_where_clause && !lower_sql.contains("where") {
            detected_patterns.push("DELETE Statements are missing WHERE clause".to_string());
            suggestions.push("For DELETE 语句Statement added WHERE clause".to_string());
        }

        // 2. Check the LIMIT clause (if configured as required)
        if self.config.require_limit_on_delete && !lower_sql.contains("limit") {
            detected_patterns.push("DELETE Statements are missing LIMIT clause".to_string());
            suggestions.push("For DELETE Statement added LIMIT clause".to_string());
        }

        // 3. Check the limit on the number of rows to be deleted
        if let Some(limit) = self.extract_limit_value(&lower_sql) {
            if limit > self.config.max_delete_rows {
                detected_patterns.push(format!("Delete too many lines: {}", limit));
                suggestions.push(format!("Reduce the number of rows deleted, maximum allowed {}", self.config.max_delete_rows));
            }
        }

        // 4. Check for multi-table deletions
        if lower_sql.contains("delete ") && lower_sql.matches("from ").count() > 1 {
            detected_patterns.push("Multi-table deletion".to_string());
            suggestions.push("Avoid using multi-table DELETE statements".to_string());
        }

        // 5. Check for JOIN deletion
        if lower_sql.contains("join") && lower_sql.contains("delete") {
            detected_patterns.push("JOIN Delete".to_string());
            suggestions.push("Use JOIN deletion sparingly, ensuring there are clear ON conditions".to_string());
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::High,
                "DELETE statement security issue".to_string(),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    fn validate_insert_statement(&self, sql: &str) -> Option<DetectionResult> {
        let mut detected_patterns = Vec::new();
        let mut suggestions = Vec::new();

        let lower_sql = sql.to_lowercase();

        // 1. Check the number of VALUES clauses
        let values_count = self.count_values_clauses_regex(&lower_sql);
        let max_values_clauses = if sql.contains("insert") && sql.contains("select") {
            self.config.max_values_clauses_insert_select  // INSERT ... SELECT ... Usually there is only one VALUES
        } else {
            self.config.max_values_clauses_insert  // Normal INSERT It can be multi-line
        };

        if values_count > max_values_clauses {
            detected_patterns.push(format!("Too many VALUES clauses: {}", values_count));
            suggestions.push(format!("Currently there are {} VALUES clauses, exceeding the limit {}, suggestion: reduce the number of VALUES clauses", values_count, max_values_clauses));
        }

        // 2. Check the number of parameters
        let placeholder_count = self.count_insert_placeholders(&lower_sql);
        let max_params = self.calculate_max_insert_params(sql);

        if placeholder_count > max_params {
            detected_patterns.push(format!("Too many parameters: {}", placeholder_count));
            suggestions.push(format!("Currently there are {} parameters, exceeding the limit {}, it is recommended: perform INSERTs in batches or reduce the number of columns per row", placeholder_count, max_params));
            // If it's a multi-line INSERT, provide a more specific suggestion
            if sql.to_lowercase().contains("),(") {
                suggestions.push("For multi-line INSERTs, consider using the Bulk INSERT API".to_string());
            }
            
        }

        // 3. Check the SELECT subquery
        if lower_sql.contains("select") && lower_sql.contains("values") {
            if self.has_dangerous_subquery(&lower_sql) {
                detected_patterns.push("Dangerous subqueries".to_string());
                suggestions.push("Check INSERT ... Whether the subqueries in the SELECT statement are safe".to_string());
            }
        }

        if !detected_patterns.is_empty() {
            Some(DetectionResult::dangerous(
                DetectionSeverity::Medium,
                "INSERT Statement security issues".to_string(),
                detected_patterns,
                suggestions,
            ))
        } else {
            None
        }
    }

    // ========== Auxiliary methods ==========

    fn calculate_max_insert_params(&self, sql: &str) -> usize {
        let sql_lower = sql.to_lowercase();

        // Set different limits depending on the type of INSERT
        if sql_lower.contains("insert") && sql_lower.contains("select") {
            // INSERT ... SELECT ... Statement
            self.config.max_params_clauses_insert_select
        } else if self.is_multi_row_insert(&sql_lower) {
            // MULTI INSERT: VALUES (...),(...)
            let estimated_rows = self.estimate_insert_rows(&sql_lower);
            self.config.max_params_clauses_insert_multi * estimated_rows.min(100) // Up to 100 columns per row, up to 100 rows
        } else {
            // SINGLE INSERT
            self.config.max_params_clauses_insert
        }
    }

    fn is_multi_row_insert(&self, sql_lower: &str) -> bool {
        // Look for a "" pattern in the content after VALUES
        if let Some(values_pos) = sql_lower.find("values") {
            let after_values = &sql_lower[values_pos + 6..];
            // Simply check if there is a ""," pattern (not in the string)
            let mut in_string = false;
            let mut string_char = '\0';
            let chars = after_values.chars().collect::<Vec<_>>();

            for i in 0..chars.len().saturating_sub(2) {
                if chars[i] == '\'' || chars[i] == '"' {
                    if !in_string {
                        in_string = true;
                        string_char = chars[i];
                    } else if chars[i] == string_char {
                        in_string = false;
                    }
                }

                if !in_string && chars[i] == ')' && chars[i + 1] == ',' && chars[i + 2] == '(' {
                    return true;
                }
            }
        }

        false
    }

    // Estimate the number of INSERT lines
    fn estimate_insert_rows(&self, sql_lower: &str) -> usize {
        if let Some(values_pos) = sql_lower.find("values") {
            let after_values = &sql_lower[values_pos + 6..];
            // Statistics "",(" + 1
            let mut count = 1; // At least one line
            let mut in_string = false;
            let mut string_char = '\0';
            let chars = after_values.chars().collect::<Vec<_>>();

            for i in 0..chars.len().saturating_sub(2) {
                if chars[i] == '\'' || chars[i] == '"' {
                    if !in_string {
                        in_string = true;
                        string_char = chars[i];
                    } else if chars[i] == string_char {
                        in_string = false;
                    }
                }

                if !in_string && chars[i] == ')' && chars[i + 1] == ',' && chars[i + 2] == '(' {
                    count += 1;
                }
            }

            count
        } else {
            1
        }
    }

    pub fn count_insert_placeholders(&self, sql: &str) -> usize {
        match self.count_values_placeholders(sql) {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("Count placeholder failure: {}, use simple statistics", e);
                // Revert to simple statistics
                sql.matches('?').count()
            }
        }
    }

    fn count_values_placeholders(&self, sql: &str) -> Result<usize, String> {
        // 1. Start by targeting the VALUES keyword
        let sql_lower = sql.to_lowercase();

        // Look for the VALUES keyword
        let values_pattern = r"(?i)\bvalues\b";
        let values_re = match Regex::new(values_pattern) {
            Ok(re) => re,
            Err(e) => return Err(format!("Regular expression compilation failed: {}", e)),
        };

        if let Some(values_match) = values_re.find(&sql_lower) {
            let values_start = values_match.start();
            let after_values = &sql[values_start..];

            // 2. Find the first left bracket after VALUES
            let mut paren_start = None;
            let mut in_string = false;
            let mut string_char = '\0';
            let mut escaped = false;

            for (i, ch) in after_values.chars().enumerate() {
                if escaped {
                    escaped = false;
                    continue;
                }

                if ch == '\\' {
                    escaped = true;
                    continue;
                }

                // Handle strings
                if ch == '\'' || ch == '"' {
                    if !in_string {
                        in_string = true;
                        string_char = ch;
                    } else if ch == string_char {
                        in_string = false;
                    }
                    continue;
                }

                // Find the first left bracket that is not in the string
                if ch == '(' && !in_string {
                    paren_start = Some(i);
                    break;
                }
            }

            let paren_start_idx = match paren_start {
                Some(idx) => idx,
                None => return Ok(0), // No left brackets found
            };

            let paren_content_start = values_start + paren_start_idx;
            let after_paren = &sql[paren_content_start..];

            // 3. Extract everything in parentheses (Handling nested parentheses)
            let mut depth = 0;
            let mut content_end = 0;
            let mut in_string = false;
            let mut string_char = '\0';
            let mut escaped = false;

            for (i, ch) in after_paren.chars().enumerate() {
                if escaped {
                    escaped = false;
                    continue;
                }

                if ch == '\\' {
                    escaped = true;
                    continue;
                }

                // Handle strings
                if ch == '\'' || ch == '"' {
                    if !in_string {
                        in_string = true;
                        string_char = ch;
                    } else if ch == string_char {
                        in_string = false;
                    }
                }

                // Handle parentheses
                match ch {
                    '(' if !in_string => {
                        depth += 1;
                        if depth == 1 {
                            continue; // Skip the first left bracket
                        }
                    }
                    ')' if !in_string => {
                        depth -= 1;
                        if depth == 0 {
                            content_end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if content_end == 0 {
                return Ok(0); // No matching brackets found
            }

            let values_content = &after_paren[0..content_end];

            // 4. Count the number of placeholders (question marks not in the string)
            let mut placeholder_count = 0;
            let mut in_string = false;
            let mut string_char = '\0';
            let mut escaped = false;

            for ch in values_content.chars() {
                if escaped {
                    escaped = false;
                    continue;
                }

                if ch == '\\' {
                    escaped = true;
                    continue;
                }

                // Handle strings
                if ch == '\'' || ch == '"' {
                    if !in_string {
                        in_string = true;
                        string_char = ch;
                    } else if ch == string_char {
                        in_string = false;
                    }
                    continue;
                }

                // Count question marks that aren't in the string
                if ch == '?' && !in_string {
                    placeholder_count += 1;
                }
            }

            Ok(placeholder_count)
        } else {
            Ok(0) // There is no VALUES keyword
        }
    }

    fn count_values_clauses_regex(&self, sql: &str) -> usize {
        use regex::Regex;

        // Match the "values" keyword followed by parentheses
        // (?i) indicates case-insensitive, \b indicates word boundaries
        let pattern = r"(?i)\bvalues\s*\(";

        if let Ok(re) = Regex::new(pattern) {
            re.find_iter(sql).count()
        } else {
            // If the regex fails, fall back to the simple count
            sql.to_lowercase().matches("values(").count()
        }
    }

    fn has_unclosed_strings_or_comments(&self, sql: &str) -> bool {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_comment = false;

        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            match chars[i] {
                '\'' => {
                    if i > 0 && chars[i-1] != '\\' {
                        in_single_quote = !in_single_quote;
                    }
                }
                '"' => {
                    if i > 0 && chars[i-1] != '\\' {
                        in_double_quote = !in_double_quote;
                    }
                }
                '-' if i + 1 < chars.len() && chars[i+1] == '-' => {
                    if !in_single_quote && !in_double_quote {
                        in_comment = true;
                    }
                }
                '\n' if in_comment => {
                    in_comment = false;
                }
                '/' if i + 1 < chars.len() && chars[i+1] == '*' => {
                    if !in_single_quote && !in_double_quote {
                        in_comment = true;
                    }
                }
                '*' if i + 1 < chars.len() && chars[i+1] == '/' => {
                    if in_comment {
                        in_comment = false;
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        in_single_quote || in_double_quote || in_comment
    }

    fn has_nested_queries(&self, sql: &str) -> bool {
        let lower_sql = sql.to_lowercase();
        let select_count = lower_sql.matches("select").count();
        let from_count = lower_sql.matches("from").count();

        select_count > from_count && from_count > 0
    }

    fn has_adjacent_placeholders(&self, sql: &str) -> bool {
        let mut chars = sql.chars().peekable();
        let mut in_string = false;
        let mut string_char = '\0';

        while let Some(ch) = chars.next() {
            // Track string status
            if ch == '\'' || ch == '"' {
                if !in_string {
                    in_string = true;
                    string_char = ch;
                } else if ch == string_char {
                    in_string = false;
                }
            }

            // Check for placeholders when not in the string
            if !in_string && ch == '?' {
                // Check if the next character is another ?
                if let Some(&next) = chars.peek() {
                    if next == '?' {
                        return true;
                    }
                }

                // Skip the comma and spaces and check if the next one is ?
                let mut temp_chars = chars.clone();
                while let Some(&next) = temp_chars.peek() {
                    match next {
                        ',' | ' ' | '\t' | '\n' => {
                            temp_chars.next();
                        }
                        '?' => return true,
                        _ => break,
                    }
                }
            }
        }

        false
    }

    fn extract_where_clause(&self, sql: &str) -> Option<String> {
        if let Some(where_pos) = sql.find(" where ") {
            let after_where = &sql[where_pos + 7..];
            let where_end = self.find_where_clause_end(after_where);
            Some(after_where[..where_end].trim().to_string())
        } else {
            None
        }
    }

    fn extract_set_clause(&self, sql: &str) -> Option<String> {
        if let Some(set_pos) = sql.find(" set ") {
            if let Some(where_pos) = sql.find(" where ") {
                Some(sql[set_pos + 5..where_pos].trim().to_string())
            } else {
                Some(sql[set_pos + 5..].trim().to_string())
            }
        } else {
            None
        }
    }

    fn extract_limit_value(&self, sql: &str) -> Option<u32> {
        if let Some(limit_pos) = sql.find("limit") {
            let after_limit = &sql[limit_pos + 5..];
            if let Some(num_str) = after_limit.split_whitespace().next() {
                num_str.parse::<u32>().ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn is_dangerous_where_condition(&self, condition: &str) -> bool {
        let patterns = [
            (r"^\s*1\s*=\s*1\s*$", "1=1"),
            (r"^\s*2\s*=\s*2\s*$", "2=2"),
            (r"^\s*0\s*=\s*0\s*$", "0=0"),
            (r"^\s*'a'\s*=\s*'a'\s*$", "'a'='a'"),
            (r#"^\s*"a"\s*=\s*"a"\s*$"#, "\"a\"=\"a\""),
            (r"^\s*true\s*$", "true"),
            (r"^\s*1\s*$", "1"),
            (r"^\s*'1'\s*=\s*'1'\s*$", "'1'='1'"),
        ];

        for (pattern, _) in &patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                if re.is_match(condition) {
                    return true;
                }
            }
        }

        false
    }

    fn is_update_set_clause_safe(&self, set_clause: &str) -> bool {
        let lower_clause = set_clause.to_lowercase();

        // Check for really dangerous SET expressions
        let dangerous_patterns = [
            "(select",
            "@@version",
            "@@identity",
            "@@rowcount",
            "xp_cmdshell",
            "exec(",
            "execute(",
            "load_file(",
            "into outfile",
            "sleep(",
            "benchmark(",
            "aes_encrypt(",
            "aes_decrypt(",
        ];

        for pattern in &dangerous_patterns {
            if lower_clause.contains(pattern) {
                return false;
            }
        }

        true
    }

    fn has_dangerous_subquery(&self, sql: &str) -> bool {
        let dangerous_patterns = [
            "union",
            "information_schema",
            "@@version",
            "sleep(",
            "benchmark(",
        ];

        for pattern in &dangerous_patterns {
            if sql.contains(pattern) {
                return true;
            }
        }

        false
    }

    fn find_where_clause_end(&self, text: &str) -> usize {
        let mut depth = 0;
        let mut in_string = false;
        let mut string_char = '\0';
        let mut escape_next = false;

        let keywords = [" order by ", " group by ", " having ", " limit ", " offset ", ";"];

        for (i, ch) in text.char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => escape_next = true,
                '\'' | '"' | '`' => {
                    if !in_string {
                        in_string = true;
                        string_char = ch;
                    } else if ch == string_char {
                        in_string = false;
                    }
                }
                '(' if !in_string => depth += 1,
                ')' if !in_string => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                _ => {}
            }

            // When not in the string and the parentheses depth is 0, check if the SQL keyword is encountered
            if !in_string && depth == 0 && i > 0 {
                let remaining = &text[i-1..];
                for keyword in &keywords {
                    if remaining.starts_with(keyword) {
                        return i - 1;
                    }
                }
            }
        }

        text.len()
    }

    fn detect_encoding_bypass(&self, input: &str) -> bool {
        if input.contains("%27") || input.contains("%22") || input.contains("%3B") {
            return true;
        }

        if input.contains("\\u0027") || input.contains("\\u0022") {
            return true;
        }

        if let Some(pos) = input.find("0x") {
            let hex_start = pos + 2;
            if hex_start < input.len() {
                let remaining = &input[hex_start..];
                let hex_length = remaining.chars()
                    .take_while(|c| c.is_ascii_hexdigit())
                    .count();
                if hex_length >= 8 {
                    return true;
                }
            }
        }

        if input.to_uppercase().contains("CHR(") {
            if input.to_uppercase().contains("CHR(39)") || input.to_uppercase().contains("CHR(34)") {
                return true;
            }
        }

        false
    }

    fn detect_anomalous_sequences(&self, input: &str) -> bool {
        let single_quote_count = input.matches('\'').count();
        let double_quote_count = input.matches('"').count();

        if single_quote_count > 3 || double_quote_count > 3 {
            return true;
        }

        let open_paren = input.matches('(').count();
        let close_paren = input.matches(')').count();

        if open_paren != close_paren && open_paren > 0 {
            if open_paren.abs_diff(close_paren) > 1 {
                return true;
            }
        }

        let semicolon_count = input.matches(';').count();
        if semicolon_count > 1 {
            let mut in_quote = false;
            let mut quote_char = '\0';
            for ch in input.chars() {
                if ch == '\'' || ch == '"' {
                    if !in_quote {
                        in_quote = true;
                        quote_char = ch;
                    } else if ch == quote_char {
                        in_quote = false;
                    }
                } else if ch == ';' && !in_quote {
                    return true;
                }
            }
        }

        false
    }

    fn detect_sql_structure_anomalies(&self, input: &str) -> bool {
        let input_upper = input.to_uppercase();
        let sql_keywords = vec!["SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "DROP"];
        let mut count = 0;

        for keyword in &sql_keywords {
            if input_upper.contains(keyword) {
                count += 1;
            }
        }

        if count > 3 {
            return true;
        }

        if (input_upper.contains("SELECT") && !input_upper.contains("FROM")) ||
            (input_upper.contains("WHERE") && !(input_upper.contains("SELECT") || input_upper.contains("UPDATE") || input_upper.contains("DELETE"))) ||
            (input_upper.contains("SET") && !input_upper.contains("UPDATE")) {
            return true;
        }

        false
    }

    // ========== Tool method ==========

    pub fn build_safe_sql_fragment(&self, sql: &str) -> Result<String, AkitaDataError> {
        let result = self.detect_sql_security(sql, None);

        match result.is_dangerous {
            false => Ok(sql.to_string()),
            true => Err(AkitaDataError::sql_injection_error(sql, result)),
        }
    }

    pub fn batch_detect(&self, inputs: &[&str]) -> Vec<(String, DetectionResult)> {
        inputs.iter()
            .map(|&input| (input.to_string(), self.detect_sql_security(input, None)))
            .collect()
    }

    pub fn update_config(&mut self, config: SqlSecurityConfig) {
        self.config = config;
    }

    pub fn get_config(&self) -> &SqlSecurityConfig {
        &self.config
    }
}