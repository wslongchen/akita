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

//!
//! Generate Wrapper.
//! ```ignore
//! 
//! let mut wrapper = Wrapper::new();
//! wrapper.like(true, "column1", "ffff");
//! wrapper.eq(true, "column2", 12);
//! wrapper.eq(true, "column3", "3333");
//! wrapper.in_(true, "column4", vec![1,44,3]);
//! wrapper.not_between(true, "column5", 2, 8);
//! wrapper.set(true, "column1", 4);
//! match wrapper.get_target_sql("t_user") {
//!     Ok(sql) => {println!("ok:{}", sql);}
//!     Err(err) => {println!("err:{}", err);}
//! }
//! ```
//!
//!

use crate::{AkitaValue, DetectionResult, DetectionSeverity, SqlInjectionDetector};
use std::fmt;
use std::fmt::{Display, Formatter};
use tracing::trace;

#[derive(Debug, Clone, PartialEq)]
pub struct Wrapper {
    // Base configuration
    table: Option<String>,
    alias: Option<String>,

    // SQL SUBASSEMBLY
    select_columns: Vec<String>,
    join_clauses: Vec<JoinClause>,
    where_conditions: Vec<Condition>,
    group_by_columns: Vec<String>,
    having_conditions: Vec<Condition>,
    order_by_clauses: Vec<OrderByClause>,
    set_operations: Vec<SetOperation>,

    // SQL modified
    distinct: bool,
    limit_value: Option<u64>,
    offset_value: Option<u64>,
    comment: Option<String>,

    // Parameter management
    param_name_seq: i32,
    parameters: Vec<AkitaValue>,

    // ========== Conditional Control field ==========
    /// The next mark of whether the condition takes effect
    next_condition_active: bool,

    /// Whether you are currently in skip mode
    skip_mode: bool,

    /// The state used for Option mode
    option_mode: OptionState,

    last_sql: Option<String>,
    apply_conditions: Vec<String>,

    sql_injection_detector: SqlInjectionDetector,
}

/// Option The state of the mode
#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
enum OptionState {
    Normal,
    ExpectingValue, 
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    join_type: JoinType,
    pub table: String,
    alias: Option<String>,
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
        }
    }
}

impl ToString for Wrapper {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub column: String,
    pub operator: SqlOperator,
    pub value: AkitaValue,
    and_or: AndOr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub column: String,
    direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub column: String,
    pub value: AkitaValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlOperator {
    Eq, Ne, Gt, Ge, Lt, Le,
    Like, NotLike,
    IsNull, IsNotNull,
    In, NotIn,
    Between, NotBetween,
}

impl SqlOperator {
    pub fn is_null_check(&self) -> bool {
        matches!(self, SqlOperator::IsNull | SqlOperator::IsNotNull)
    }
}

impl Display for SqlOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            SqlOperator::Eq => write!(f, "="),
            SqlOperator::Ne => write!(f, "!="),
            SqlOperator::Gt => write!(f, ">"),
            SqlOperator::Ge => write!(f, ">="),
            SqlOperator::Lt => write!(f, "<"),
            SqlOperator::Le => write!(f, "<="),
            SqlOperator::Like => write!(f, "LIKE"),
            SqlOperator::NotLike => write!(f, "NOT LIKE"),
            SqlOperator::IsNull => write!(f, "IS NULL"),
            SqlOperator::IsNotNull => write!(f, "IS NOT NULL"),
            SqlOperator::In => write!(f, "IN"),
            SqlOperator::NotIn => write!(f, "NOT IN"),
            SqlOperator::Between => write!(f, "BETWEEN"),
            SqlOperator::NotBetween => write!(f, "NOT BETWEEN"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AndOr {
    And,
    Or,
}

impl Display for AndOr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            AndOr::And => write!(f, "AND"),
            AndOr::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

impl Wrapper{

    // Foundation construction method
    pub fn new() -> Self {
        Self {
            table: None,
            alias: None,
            select_columns: Vec::new(),
            join_clauses: Vec::new(),
            where_conditions: Vec::new(),
            group_by_columns: Vec::new(),
            having_conditions: Vec::new(),
            order_by_clauses: Vec::new(),
            set_operations: Vec::new(),
            distinct: false,
            sql_injection_detector: SqlInjectionDetector::new(),
            limit_value: None,
            offset_value: None,
            comment: None,
            param_name_seq: 0,
            parameters: Vec::new(),
            apply_conditions: Vec::new(),
            last_sql: None,
            next_condition_active: true,
            skip_mode: false,
            option_mode: OptionState::Normal,
        }
    }

    pub fn get_where_conditions(&self) -> &Vec<Condition> {
        self.where_conditions.as_ref()
    }

    pub fn get_join_clauses(&self) -> &Vec<JoinClause> {
        self.join_clauses.as_ref()
    }

    pub fn get_having_conditions(&self) -> &Vec<Condition> {
        self.having_conditions.as_ref()
    }

    pub fn get_select_columns(&self) -> Vec<String> {
        self.select_columns.clone()
    }

    pub fn get_set_operations(&self) -> &Vec<SetOperation> {
        self.set_operations.as_ref()
    }

    pub fn where_conditions(&mut self, where_conditions: Vec<Condition>) {
        self.where_conditions = where_conditions;
    }
    pub fn join_clauses(&mut self, join_clauses: Vec<JoinClause>) {
        self.join_clauses = join_clauses;
    }
    pub fn apply_conditions(&mut self, apply_conditions: Vec<String>) {
        self.apply_conditions = apply_conditions;
    }
    
    pub fn having_conditions(&mut self, having_conditions: Vec<Condition>) {
        self.having_conditions = having_conditions;
    }
    pub fn order_by_clauses(&mut self, order_by_clauses: Vec<OrderByClause>) {
        self.order_by_clauses = order_by_clauses;
    }

    pub fn set_operations(&mut self, set_operations: Vec<SetOperation>) {
        self.set_operations = set_operations;
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn get_table(&self) -> Option<&String> {
        self.table.as_ref()
    }

    pub fn alias<S: Into<String>>(mut self, alias: S) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn last<S: Into<String>>(mut self, sql: S) -> Self {
        self.last_sql = Some(sql.into());
        self
    }

    // ========== Apply ==========

    /// Full apply method - supports static SQL and parameterized queries
    ///
    /// # Parameters
    /// - `sql`:SQL templates, which can contain ? Placeholders
    /// - `params`: Optional parameter list, with ? Placeholders correspond to each other
    ///
    /// # 示例
    /// ```
    /// // Static SQL (no parameters)
    /// use akita_core::{AkitaValue, Wrapper};
    /// let mut wrapper = Wrapper::new();
    /// wrapper = wrapper.apply("status = 1", None);
    ///
    /// // Parametric queries
    /// wrapper = wrapper.apply("username = ?", vec![AkitaValue::Text("john".to_string())].into());
    ///
    /// // Multi-parameter
    /// wrapper = wrapper.apply("date BETWEEN ? AND ?", vec![
    ///     AkitaValue::Text("2023-01-01".to_string()),
    ///     AkitaValue::Text("2023-12-31".to_string()),
    /// ].into());
    /// ```
    pub fn apply<S, V, I>(mut self, sql: S, params: Option<I>) -> Self
    where
        S: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        if !self.should_add_condition() {
            return self;
        }

        let sql_template = sql.into();

        // 1. SQL Inject safety checks
        let params_vec: Option<Vec<AkitaValue>> = params.map(|iter| {
            iter.into_iter().map(|v| v.into()).collect()
        });
        if let Some(ref params_vec) = params_vec {
            let param_list: Vec<(String, String)> = params_vec.iter()
                .enumerate()
                .filter_map(|(i, param)| {
                    if let Some(str_val) = param.as_str() {
                        Some((format!("param_{}", i), str_val.to_string()))
                    } else {
                        Some((format!("param_{}", i), param.to_string()))
                    }
                })
                .collect();

            let security_result = self.sql_injection_detector.detect_sql_security(
                &sql_template,
                Some(&param_list)
            );

            self.handle_security_result(&sql_template, &security_result);

            // If the severity is Critical, it is returned directly without adding any conditions
            if security_result.is_dangerous && matches!(security_result.severity, DetectionSeverity::Critical) {
                tracing::error!("Serious security threats, skip condition additions: {}", sql_template);
                return self;
            }
        } else {
            // There are no parameters
            let security_result = self.sql_injection_detector.detect_sql_security(&sql_template, None);
            self.handle_security_result(&sql_template, &security_result);

            if security_result.is_dangerous && matches!(security_result.severity, DetectionSeverity::Critical) {
                tracing::error!("Serious security threats, skip condition addition: {}", sql_template);
                return self;
            }
        }

        // 2. Processing parameters
        let placeholder_count = sql_template.matches('?').count();

        if let Some(params_vec) = params_vec {
            if params_vec.len() != placeholder_count {
                tracing::warn!("Number of parameters does not match - SQL has {} placeholders, but {} parameters are provided",placeholder_count,params_vec.len());
            }

            for param in params_vec {
                if !self.should_skip_condition(&param) {
                    self.param_name_seq += 1;
                    self.parameters.push(param);
                }
            }
        } else if placeholder_count > 0 {
            tracing::warn!("SQL template contains {} placeholders but no parameters provided",placeholder_count);
        }

        // 3.Add a SQL template to the apply condition
        self.apply_conditions.push(sql_template);

        self
    }

    fn handle_security_result(&self, sql: &str, result: &DetectionResult) {
        if !result.is_dangerous {
            return;
        }

        match result.severity {
            DetectionSeverity::Critical => {
                tracing::error!(
                "Critical Security Threat - SQL: {}, Cause: {}, Pattern: {:?}",
                sql,
                result.reason,
                result.patterns
            );
            }
            DetectionSeverity::High => {
                tracing::warn!(
                "High Risk SQL Pattern - SQL: {}, Reason: {}, Recommendation: {:?}",
                sql,
                result.reason,
                result.suggestions
            );
            }
            DetectionSeverity::Medium => {
                tracing::info!(
                "Medium Risk SQL Pattern - SQL: {}, Reason: {}",
                sql,
                result.reason
            );
            }
            DetectionSeverity::Low => {
                trace!(
                "Low Risk SQL Warning - SQL: {}, Reason: {}",
                sql,
                result.reason
            );
            }
        }
    }

    /// Simplified apply method - only accepts static SQL
    pub fn apply_raw<S: Into<String>>(self, sql: S) -> Self {
        self.apply(sql, None::<Vec<AkitaValue>>)
    }

    // ========== SELECT ==========

    pub fn select<T: Into<String>>(mut self, columns: Vec<T>) -> Self {
        self.select_columns = columns.into_iter().map(|c| c.into()).collect();
        self
    }

    pub fn select_distinct<T: Into<String>>(mut self, columns: Vec<T>) -> Self {
        self.select_columns = columns.into_iter().map(|c| c.into()).collect();
        self.distinct = true;
        self
    }

    // ========== WHERE ==========

    fn add_condition<T, V>(mut self, column: T, operator: SqlOperator, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        let value = value.into();
        if !self.should_add_condition() {
            return self;
        }

        if operator.is_null_check() {
            self.where_conditions.push(Condition {
                column: column.into(),
                operator,
                value,
                and_or: AndOr::And,
            });
            return self;
        }

        if !self.should_skip_condition(&value) {
            self.where_conditions.push(Condition {
                column: column.into(),
                operator,
                value,
                and_or: AndOr::And,
            });
        }
        
        self
    }

    pub fn eq<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Eq, value)
    }

    pub fn ne<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Ne, value)
    }


    pub fn gt<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Gt, value)
    }

    pub fn ge<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Ge, value)
    }


    pub fn lt<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Lt, value)
    }

    pub fn le<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Le, value)
    }

    pub fn like<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Like, value)
    }

    pub fn not_like<T, V>(self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::NotLike, value)
    }

    pub fn is_null<T: Into<String>>(self, column: T) -> Self {
        self.add_condition(column, SqlOperator::IsNull, AkitaValue::Null)
    }

    pub fn is_not_null<T: Into<String>>(self, column: T) -> Self {
        self.add_condition(column, SqlOperator::IsNotNull, AkitaValue::Null)
    }

    pub fn r#in<T, V, I>(self, column: T, values: I) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        let values: Vec<AkitaValue> = values.into_iter().map(|v| v.into()).collect();
        self.add_condition(column, SqlOperator::In, AkitaValue::List(values))
    }

    pub fn not_in<T, V, I>(self, column: T, values: I) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = V>,
    {
        let values: Vec<AkitaValue> = values.into_iter().map(|v| v.into()).collect();
        self.add_condition(column, SqlOperator::NotIn, AkitaValue::List(values))
    }

    pub fn between<T, V>(self, column: T, start: V, end: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::Between, AkitaValue::List(vec![start.into(), end.into()]))
    }

    pub fn not_between<T, V>(self, column: T, start: V, end: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        self.add_condition(column, SqlOperator::NotBetween, AkitaValue::List(vec![start.into(), end.into()]))
    }

    // ========== Logical operations ==========

    pub fn and<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        let nested = func(Wrapper::new());
        self.where_conditions.extend(nested.where_conditions);
        self
    }

    pub fn or<F>(mut self, func: F) -> Self
    where
        F: FnOnce(Wrapper) -> Wrapper,
    {
        let mut nested = func(Wrapper::new());
        // Mark the nested condition as  OR
        for condition in &mut nested.where_conditions {
            condition.and_or = AndOr::Or;
        }
        self.where_conditions.extend(nested.where_conditions);
        self
    }

    pub fn or_direct(mut self) -> Self {
        if let Some(last_condition) = self.where_conditions.last_mut() {
            last_condition.and_or = AndOr::Or;
        }
        self
    }

    // ========== JOIN ==========

    fn add_join(mut self, join_type: JoinType, table: String, condition: String) -> Self {
        if self.should_add_condition() {
            self.join_clauses.push(JoinClause {
                join_type,
                table,
                alias: None,
                condition: Condition {
                    column: condition,
                    operator: SqlOperator::Eq,
                    value: AkitaValue::Null, // JOIN conditions are handled specially when SQL is generated
                    and_or: AndOr::And,
                },
            });
        }
        self
    }

    pub fn inner_join<T, C>(self, table: T, condition: C) -> Self
    where
        T: Into<String>,
        C: Into<String>,
    {
        self.add_join(JoinType::Inner, table.into(), condition.into())
    }

    pub fn left_join<T, C>(self, table: T, condition: C) -> Self
    where
        T: Into<String>,
        C: Into<String>,
    {
        self.add_join(JoinType::Left, table.into(), condition.into())
    }

    pub fn right_join<T, C>(self, table: T, condition: C) -> Self
    where
        T: Into<String>,
        C: Into<String>,
    {
        self.add_join(JoinType::Right, table.into(), condition.into())
    }

    pub fn full_join<T, C>(self, table: T, condition: C) -> Self
    where
        T: Into<String>,
        C: Into<String>,
    {
        self.add_join(JoinType::Full, table.into(), condition.into())
    }

    // ========== GROUP BY / HAVING ==========

    pub fn group_by<T: Into<String>>(mut self, columns: Vec<T>) -> Self {
        if self.should_add_condition() {
            self.group_by_columns = columns.into_iter().map(|c| c.into()).collect();
        }
        self
    }

    pub fn having<T, V>(mut self, column: T, operator: SqlOperator, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        let value = value.into();
        if self.should_add_condition() && !self.should_skip_condition(&value) {
            self.having_conditions.push(Condition {
                column: column.into(),
                operator,
                value,
                and_or: AndOr::And,
            });
        }
        
        self
    }

    // ========== ORDER BY ==========

    fn add_order_by<T: Into<String>>(mut self, columns: Vec<T>, direction: OrderDirection) -> Self {
        if self.should_add_condition() {
            for column in columns {
                self.order_by_clauses.push(OrderByClause {
                    column: column.into(),
                    direction: direction.clone(),
                });
            }
        }
        
        self
    }

    pub fn order_by_asc<T: Into<String>>(self, columns: Vec<T>) -> Self {
        self.add_order_by(columns, OrderDirection::Asc)
    }

    pub fn order_by_desc<T: Into<String>>(self, columns: Vec<T>) -> Self {
        self.add_order_by(columns, OrderDirection::Desc)
    }

    // ========== SET operation (UPDATE) ==========

    pub fn set<T, V>(mut self, column: T, value: V) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
    {
        let value = value.into();
        if self.should_add_condition() && !self.should_skip_condition(&value) {
            self.set_operations.push(SetOperation {
                column: column.into(),
                value,
            });
        }
        
        self
    }

    pub fn set_multiple<T, V, I>(mut self, operations: I) -> Self
    where
        T: Into<String>,
        V: Into<AkitaValue>,
        I: IntoIterator<Item = (T, V)>,
    {
        if self.should_add_condition() {
            for (column, value) in operations {
                self.set_operations.push(SetOperation {
                    column: column.into(),
                    value: value.into(),
                });
            }
        }
        
        self
    }

    // ========== pagination ==========

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit_value = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset_value = Some(offset);
        self
    }

    pub fn page(mut self, page: u64, page_size: u64) -> Self {
        let offset = (page - 1) * page_size;
        self.limit_value = Some(page_size);
        self.offset_value = Some(offset);
        self
    }

    // ========== Conditional tagging method ==========

    /// When the condition is true, subsequent chained calls are executed
    pub fn when(mut self, condition: bool) -> Self {
        self.next_condition_active = condition;
        self
    }

    /// When the condition is false, subsequent chained calls are executed
    pub fn unless(mut self, condition: bool) -> Self {
        self.next_condition_active = !condition;
        self
    }

    /// Skip the next condition (whatever it is)
    pub fn skip_next(mut self) -> Self {
        self.skip_mode = true;
        self
    }

    /// Internal method: Check if a condition should be added
    fn should_add_condition(&mut self) -> bool {
        
        if self.skip_mode {
            self.skip_mode = false;
            return false;
        }

        let should_add = self.next_condition_active;
        self.next_condition_active = true; // 重置为默认 true
        should_add
    }

    fn should_skip_condition(&self, value: &AkitaValue) -> bool {
        match value {
            // If the AkitaValue::Null is passed, it may mean that it should be skipped
            AkitaValue::Null => true,

            // If it is an empty List, it is also skipped
            AkitaValue::List(list) if list.is_empty() => true,

            // If it is an empty Text, it may be skipped (depending on demand)
            AkitaValue::Text(text) if text.is_empty() => true,

            // Other cases are not skipped
            _ => false,
        }
    }

    // ========== SQL generated ==========

    /// Generate a SELECT field list fragment (without the SELECT keyword)
    pub fn build_select_clause(&self) -> String {
        if self.distinct {
            format!("DISTINCT {}", self.build_column_list())
        } else {
            self.build_column_list()
        }
    }

    /// Generate a list of fields
    pub fn build_column_list(&self) -> String {
        if self.select_columns.is_empty() {
            "*".to_string()
        } else {
            // 注意：这里不进行标识符引用，由SqlBuilder处理
            self.select_columns.join(", ")
        }
    }

    /// Generate the FROM clause fragment
    pub fn build_from_clause(&self) -> Option<String> {
        self.table.as_ref().map(|table| {
            if let Some(alias) = &self.alias {
                format!("{} AS {}", table, alias)
            } else {
                table.clone()
            }
        })
    }

    /// Generate the JOIN clause fragment
    pub fn build_join_clauses(&self) -> Vec<String> {
        self.join_clauses.iter().map(|join| {
            let mut clause = format!("{} {}", join.join_type, join.table);
            if let Some(alias) = &join.alias {
                clause.push_str(&format!(" AS {}", alias));
            }
            clause.push_str(&format!(" ON {}", join.condition.column));
            clause
        }).collect()
    }

    /// Generate a WHERE condition fragment (without the WHERE keyword)
    pub fn build_where_clause(&self) -> String {
        self.build_conditions_clause(&self.where_conditions, &self.apply_conditions)
    }

    /// Generates a fragment of the HAVING condition (without the HAVING keyword)
    pub fn build_having_clause(&self) -> String {
        self.build_conditions_clause(&self.having_conditions, &[])
    }

    /// A generic conditional build method
    fn build_conditions_clause(&self, conditions: &[Condition], apply_conditions: &[String]) -> String {
        let mut parts = Vec::new();

        // Building the original condition
        if !conditions.is_empty() {
            let condition_parts: Vec<String> = conditions.iter()
                .map(|cond| self.format_condition_fragment(cond))
                .collect();
            parts.push(self.join_condition_fragments(condition_parts));
        }

        // Add the apply condition
        parts.extend_from_slice(apply_conditions);

        if parts.is_empty() {
            return String::new();
        }

        parts.join(" AND ")
    }

    /// Format a single condition fragment
    fn format_condition_fragment(&self, condition: &Condition) -> String {
        match &condition.operator {
            SqlOperator::IsNull | SqlOperator::IsNotNull => {
                format!("{} {}", condition.column, condition.operator)
            }
            SqlOperator::In | SqlOperator::NotIn => {
                match &condition.value {
                    AkitaValue::List(values) => {
                        let placeholders: Vec<String> = values.iter()
                            .map(|_| "?".to_string())
                            .collect();
                        format!("{} {} ({})", condition.column, condition.operator, placeholders.join(", "))
                    }
                    AkitaValue::RawSql(sql) => {
                        format!("{} {} ({})", condition.column, condition.operator, sql)
                    }
                    _ => {
                        format!("{} {} (?)", condition.column, condition.operator)
                    }
                }
            }
            SqlOperator::Between | SqlOperator::NotBetween => {
                match &condition.value {
                    AkitaValue::List(values) if values.len() == 2 => {
                        format!("{} {} ? AND ?", condition.column, condition.operator)
                    }
                    _ => {
                        format!("{} {} ? AND ?", condition.column, condition.operator)
                    }
                }
            }
            _ => {
                match &condition.value {
                    AkitaValue::RawSql(sql) => {
                        format!("{} {} {}", condition.column, condition.operator, sql)
                    }
                    AkitaValue::Column(col) => {
                        format!("{} {} {}", condition.column, condition.operator, col)
                    }
                    _ => {
                        format!("{} {} ?", condition.column, condition.operator)
                    }
                }
            }
        }
    }

    /// Join condition fragment
    fn join_condition_fragments(&self, conditions: Vec<String>) -> String {
        if conditions.is_empty() {
            return String::new();
        }

        let mut result = conditions[0].clone();
        for i in 1..conditions.len() {
            // Here we use the conditional and_or to decide the linker
            if i - 1 < self.where_conditions.len() {
                match self.where_conditions[i - 1].and_or {
                    AndOr::And => result.push_str(" AND "),
                    AndOr::Or => result.push_str(" OR "),
                }
            } else {
                result.push_str(" AND ");
            }
            result.push_str(&conditions[i]);
        }

        result
    }

    /// Generate the GROUP BY clause fragment
    pub fn build_group_by_clause(&self) -> String {
        if self.group_by_columns.is_empty() {
            String::new()
        } else {
            self.group_by_columns.join(", ")
        }
    }

    /// Generate the ORDER BY clause fragment
    pub fn build_order_by_clause(&self) -> String {
        if self.order_by_clauses.is_empty() {
            String::new()
        } else {
            let orders: Vec<String> = self.order_by_clauses.iter()
                .map(|order| format!("{} {}", order.column, order.direction))
                .collect();
            orders.join(", ")
        }
    }

    /// Generate SET clause fragment (for UPDATE)
    pub fn build_set_clause(&self) -> String {
        self.set_operations.iter()
            .map(|op| match &op.value {
                AkitaValue::RawSql(sql_expr) => format!("{} = {}", op.column, sql_expr),
                AkitaValue::Column(col_name) => format!("{} = {}", op.column, col_name),
                _ => format!("{} = ?", op.column),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Getting the LIMIT value
    pub fn get_limit(&self) -> Option<u64> {
        self.limit_value
    }

    /// Getting OFFSET values
    pub fn get_offset(&self) -> Option<u64> {
        self.offset_value
    }

    /// Gets the paging parameters
    pub fn get_pagination(&self) -> (Option<u64>, Option<u64>) {
        (self.limit_value, self.offset_value)
    }

    /// Get the final SQL fragment
    pub fn get_last_sql(&self) -> Option<&String> {
        self.last_sql.as_ref()
    }
    
    // ========== Methods that are deprecated or marked private ==========

    #[deprecated(since = "0.6.0", note = "Use SqlBuilder.build_query_sql instead")]
    pub fn build_select_sql(&self) -> String {
        // Backward compatibility is maintained, but only base SQL fragments are generated
        let select = self.build_select_clause();
        let from = self.build_from_clause().unwrap_or_default();
        let joins = self.build_join_clauses().join(" ");
        let where_clause = self.build_where_clause();
        let group_by = self.build_group_by_clause();
        let having = self.build_having_clause();
        let order_by = self.build_order_by_clause();

        let mut sql = format!("SELECT {}", select);
        if !from.is_empty() {
            sql.push_str(&format!(" FROM {}", from));
        }
        if !joins.is_empty() {
            sql.push_str(&format!(" {}", joins));
        }
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if !group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by));
        }
        if !having.is_empty() {
            sql.push_str(&format!(" HAVING {}", having));
        }
        if !order_by.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", order_by));
        }

        sql
    }

    #[deprecated(since = "0.6.0", note = "Use SqlBuilder.build_count_sql instead")]
    pub fn build_count_sql(&self) -> String {
        let from = self.build_from_clause().unwrap_or_default();
        let where_clause = self.build_where_clause();

        let mut sql = format!("SELECT COUNT(*) FROM {}", from);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        sql
    }

    #[deprecated(since = "0.6.0", note = "Use SqlBuilder.build_update_sql instead")]
    pub fn build_update_sql(&self) -> Option<String> {
        let table = self.table.as_ref()?;
        let set_clause = self.build_set_clause();
        let where_clause = self.build_where_clause();

        let mut sql = format!("UPDATE {} SET {}", table, set_clause);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        Some(sql)
    }

    #[deprecated(since = "0.6.0", note = "Use SqlBuilder.build_delete_sql instead")]
    pub fn build_delete_sql(&self) -> Option<String> {
        let table = self.table.as_ref()?;
        let where_clause = self.build_where_clause();

        let mut sql = format!("DELETE FROM {}", table);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        Some(sql)
    }


    // ========== New: A method for providing data to SqlBuilder ==========

    /// Get all condition data for use by SqlBuilder
    pub fn get_query_data(&self) -> QueryData {
        QueryData {
            select: self.build_select_clause(),
            from: self.build_from_clause(),
            joins: self.build_join_clauses(),
            where_clause: self.build_where_clause(),
            group_by: self.build_group_by_clause(),
            having: self.build_having_clause(),
            order_by: self.build_order_by_clause(),
            limit: self.limit_value,
            offset: self.offset_value,
            last_sql: self.last_sql.clone(),
            distinct: self.distinct,
        }
    }

    /// Getting the UPDATE data
    pub fn get_update_data(&self) -> Option<UpdateData> {
        Some(UpdateData {
            table: self.table.clone()?,
            set_clause: self.build_set_clause(),
            where_clause: self.build_where_clause(),
        })
    }

    /// Getting DELETE data
    pub fn get_delete_data(&self) -> Option<DeleteData> {
        Some(DeleteData {
            table: self.table.clone()?,
            where_clause: self.build_where_clause(),
        })
    }

    /// Auxiliary method: construct conditional clauses
    #[deprecated(since = "0.6.0", note = "Use Wrapper.build_conditions_clause instead")]
    fn build_conditions(&self, prefix: &str, conditions: &[Condition]) -> String {
        if conditions.is_empty() && self.apply_conditions.is_empty() {
            return String::new();
        }

        let mut all_parts = Vec::new();

        // The original conditional part
        if !conditions.is_empty() {
            let condition_parts: Vec<String> = conditions.iter()
                .map(|cond| self.format_condition(cond))
                .collect();
            let joined = self.join_conditions(condition_parts);
            all_parts.push(joined);
        }

        // Add a custom SQL snippet to apply
        if !self.apply_conditions.is_empty() {
            all_parts.extend(self.apply_conditions.iter().cloned());
        }

        if all_parts.is_empty() {
            return String::new();
        }

        format!("{} {}", prefix, all_parts.join(" AND "))
    }

    // Secondary method: Format a single condition
    fn format_condition(&self, condition: &Condition) -> String {
        match &condition.operator {
            SqlOperator::IsNull | SqlOperator::IsNotNull => {
                format!("{} {}", condition.column, condition.operator)
            }
            SqlOperator::In | SqlOperator::NotIn => {
                match &condition.value {
                    AkitaValue::List(values) => {
                        // Processes each value in the list
                        let placeholders: Vec<String> = values.iter()
                            .map(|v| match v {
                                AkitaValue::RawSql(sql) => sql.clone(),
                                AkitaValue::Column(col) => col.clone(),
                                _ => "?".to_string(),
                            })
                            .collect();

                        format!("{} {} ({})", condition.column, condition.operator, placeholders.join(", "))
                    }
                    AkitaValue::RawSql(sql) => {
                        format!("{} {} ({})", condition.column, condition.operator, sql)
                    }
                    _ => {
                        format!("{} {} (?)", condition.column, condition.operator)
                    }
                }
            }
            SqlOperator::Between | SqlOperator::NotBetween => {
                match &condition.value {
                    AkitaValue::List(values) if values.len() == 2 => {
                        let start = match &values[0] {
                            AkitaValue::RawSql(sql) => sql.clone(),
                            AkitaValue::Column(col) => col.clone(),
                            _ => "?".to_string(),
                        };

                        let end = match &values[1] {
                            AkitaValue::RawSql(sql) => sql.clone(),
                            AkitaValue::Column(col) => col.clone(),
                            _ => "?".to_string(),
                        };

                        format!("{} {} {} AND {}", condition.column, condition.operator, start, end)
                    }
                    _ => {
                        format!("{} {} ? AND ?", condition.column, condition.operator)
                    }
                }
            }
            _ => {
                match &condition.value {
                    AkitaValue::RawSql(sql) => {
                        format!("{} {} {}", condition.column, condition.operator, sql)
                    }
                    AkitaValue::Column(col) => {
                        format!("{} {} {}", condition.column, condition.operator, col)
                    }
                    _ => {
                        format!("{} {} ?", condition.column, condition.operator)
                    }
                }
            }
        }
    }

    // Auxiliary Method: Join Condition, Handle AND/OR Logic
    fn join_conditions(&self, conditions: Vec<String>) -> String {
        if conditions.is_empty() {
            return String::new();
        }

        let mut result = conditions[0].clone();
        for (_i, condition) in conditions.iter().enumerate().skip(1) {
            result.push_str(" AND ");
            result.push_str(condition);
        }
        result
    }

    // Get the parameters
    pub fn get_parameters(&self) -> Vec<AkitaValue> {
        let mut params = Vec::new();
        // SET Operating parameters
        for operation in &self.set_operations {
            match &operation.value {
                // RawSql and Column should not be passed as parameters
                AkitaValue::RawSql(_) | AkitaValue::Column(_) => {
                    // Skip, not as a parameter
                }
                _ => {
                    params.push(operation.value.clone());
                }
            }
        }
        // WHERE CONDITIONAL PARAMETERS
        for condition in &self.where_conditions {
            match &condition.operator {
                SqlOperator::IsNull | SqlOperator::IsNotNull => {
                    // These operators do not require value arguments
                }
                SqlOperator::In | SqlOperator::NotIn => {
                    if let AkitaValue::List(values) = &condition.value {
                        // Only non-RawSql/Column parameters are collected
                        for value in values {
                            if !matches!(value, AkitaValue::RawSql(_) | AkitaValue::Column(_)) {
                                params.push(value.clone());
                            }
                        }
                    } else if !matches!(&condition.value, AkitaValue::RawSql(_)) {
                        // If it's not a list or RawSql, it's treated as a normal parameter
                        params.push(condition.value.clone());
                    }
                }
                SqlOperator::Between | SqlOperator::NotBetween => {
                    if let AkitaValue::List(values) = &condition.value {
                        // Only non-RawSql/Column parameters are collected
                        for value in values {
                            if !matches!(value, AkitaValue::RawSql(_) | AkitaValue::Column(_)) {
                                params.push(value.clone());
                            }
                        }
                    }
                }
                _ => {
                    match &condition.value {
                        AkitaValue::RawSql(_) | AkitaValue::Column(_) => {
                            // Skip, not as a parameter
                        }
                        _ => {
                            params.push(condition.value.clone());
                        }
                    }
                }
            }
        }

        // Add the parameter added by the apply method
        params.extend(self.parameters.clone());
        params
    }

    /// Methodology provided for DatabasePlatform
    #[deprecated(since = "0.6.0", note = "Use Wrapper.build_where_clause instead")]
    pub fn get_sql_segment(&self) -> String {
        if self.where_conditions.is_empty() {
            return String::new();
        }
        self.build_conditions("", &self.where_conditions).trim().to_string()
    }

    pub fn get_order_by(&self) -> Vec<String> {
        let orders: Vec<String> = self.order_by_clauses.iter()
            .map(|order| format!("{} {}", order.column, order.direction))
            .collect();
        orders
    }
    
    pub fn get_group_by(&self) -> &Vec<String> {
        &self.group_by_columns
    }
    
    pub fn get_order_by_clauses(&self) -> &Vec<OrderByClause> {
        &self.order_by_clauses
    }
    
    pub fn get_apply_conditions(&self) -> &Vec<String> {
        &self.apply_conditions
    }
    
    pub fn get_select_sql(&self) -> String {
        if self.select_columns.is_empty() {
            "*".to_string()
        } else {
            self.select_columns.join(", ")
        }
    }
}


// ========== 数据结构，供SqlBuilder使用 ==========

#[derive(Debug, Clone)]
pub struct QueryData {
    pub select: String,
    pub from: Option<String>,
    pub joins: Vec<String>,
    pub where_clause: String,
    pub group_by: String,
    pub having: String,
    pub order_by: String,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub last_sql: Option<String>,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateData {
    pub table: String,
    pub set_clause: String,
    pub where_clause: String,
}

#[derive(Debug, Clone)]
pub struct DeleteData {
    pub table: String,
    pub where_clause: String,
}

#[test]
#[allow(unused)]
fn basic_test() {
    let s : Option<String> = Some("ffffa".to_string());
    let d: Option<i32> = None;
    let mut wrapper = Wrapper::new().eq("a", "bn");// .last("limit 1");
        //.not_in("vecs", vec!["a","f","g"]);
    println!("{}", wrapper.build_select_sql());
}