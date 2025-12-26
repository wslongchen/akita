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
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use akita_core::{AkitaValue, DetectionResult, InterceptorType, IntoAkitaValue, OperationType, Params, Rows, TableName, Wrapper};

/// Query result type
pub enum ExecuteResult {
    Rows(Rows),
    AffectedRows(u64),
    None
}

impl ExecuteResult {
    pub fn len(&self) -> u64 {
        match self {
            ExecuteResult::Rows(rows) => rows.len() as u64,
            ExecuteResult::AffectedRows(_) => 0,
            _ => 0,
        }
    }
    
    pub fn affected_rows(&self) -> u64 {
        match self {
            ExecuteResult::Rows(rows) => rows.len() as u64,
            ExecuteResult::AffectedRows(af) => *af,
            _ => 0,
        }
    }
    
    
    pub fn rows(self) -> Rows {
        match self {
            ExecuteResult::Rows(rows) => rows,
            _ => Rows::new(),
        }
    }
    
}

/// Execution context
#[allow(unused)]
pub struct ExecuteContext {
    // ===== BASIC INFORMATION SET AT THE START OF THE QUERY =====

    /// Raw SQL - Set by the caller at the start of the query
    original_sql: String,

    /// Final SQL - Updated after interceptor processing
    final_sql: String,

    /// Original parameter - Set by the caller at the beginning of the query
    original_params: Params,

    /// Final parameter - Updated after interceptor processing
    final_params: Params,

    /// Table information - Inferred from SQL parsing or entity type
    table_info: TableName,

    /// Entity Type - For ORM operations, record the corresponding Rust type
    entity_type: Option<TypeId>,

    /// Operation type - Inferred from SQL or method calls
    operation_type: OperationType,

    /// Wrapper - For queries that use the Wrapper
    wrapper: Wrapper,

    // ===== Execution information (set during execution) =====

    /// Start time - Set at the start of the query
    start_time: Instant,

    // ===== Control information (set during interceptor execution) =====

    /// Metadata - A container that passes data between interceptors
    metadata: HashMap<String, AkitaValue>,

    /// Executed Interceptors - Records the order in which the interceptors were executed
    executed_interceptors: Vec<InterceptorType>,

    /// Stop propagation - Set by the interceptor to terminate subsequent interceptors
    pub stop_propagation: bool,

    /// Skip next interceptor - dynamically controls interceptor execution
    skip_next_interceptors: HashSet<InterceptorType>,

    connection_id: Option<u32>,

    // ===== Performance metrics (collected during execution) =====

    /// Performance metrics - Document the time spent at each stage of query execution
    metrics: QueryMetrics,

    detection_result: Option<DetectionResult>,
}

impl ExecuteContext {
    /// Create a new execution context
    pub fn new(
        sql: String,
        params: Params,
        table_info: TableName,
        operation_type: OperationType,
    ) -> Self {
        let now = Instant::now();

        Self {
            original_sql: sql.clone(),
            final_sql: sql,
            original_params: params.clone(),
            final_params: params,
            table_info,
            entity_type: None,
            operation_type,
            wrapper: Wrapper::new(),

            start_time: now,
            metadata: HashMap::new(),
            executed_interceptors: Vec::new(),
            stop_propagation: false,
            skip_next_interceptors: HashSet::new(),

            connection_id: None,
            metrics: QueryMetrics::new(),
            detection_result: None,
        }
    }

    /// Create context for entity actions
    pub fn for_entity<T: 'static>(
        sql: String,
        params: Params,
        table_info: TableName,
        operation_type: OperationType,
        wrapper: Wrapper,
    ) -> Self {
        let mut ctx = Self::new(sql, params, table_info, operation_type);
        ctx.entity_type = Some(TypeId::of::<T>());
        ctx.wrapper = wrapper;
        ctx
    }

    /// Record interceptor execution
    pub fn record_interceptor(&mut self, interceptor_type: InterceptorType) {
        self.executed_interceptors.push(interceptor_type);
    }

    /// Set to stop propagation
    pub fn stop_propagation(&mut self) {
        self.stop_propagation = true;
    }

    /// Skip the designated interceptor
    pub fn skip_interceptor(&mut self, interceptor_type: InterceptorType) {
        self.skip_next_interceptors.insert(interceptor_type);
    }

    /// Set the metadata
    pub fn set_metadata<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: IntoAkitaValue,
    {
        self.metadata.insert(key.into(), value.into_value());
    }

    /// Get metadata
    pub fn get_metadata<K>(&self, key: K) -> Option<&AkitaValue>
    where
        K: AsRef<str>,
    {
        self.metadata.get(key.as_ref())
    }

    /// Update the final SQL and parameters
    pub fn update_sql_and_params(&mut self, sql: String, params: Params) {
        self.final_sql = sql;
        self.final_params = params;
    }

    /// Record the resolution completion time
    pub fn record_parse_complete(&mut self) {
        self.metrics.parse_time = self.start_time.elapsed();
    }

    /// Record the execution completion time
    pub fn record_execute_complete(&mut self, rows_affected: u64) {
        let now = Instant::now();
        self.metrics.execute_time = now - self.start_time - self.metrics.parse_time;
        self.metrics.total_time = now - self.start_time;
        self.metrics.rows_affected = rows_affected;

        // Estimated memory usage (more precise measurements may be required for actual implementation)
        self.metrics.memory_usage = self.final_sql.capacity()
            + self.original_sql.capacity()
            + std::mem::size_of_val(&self.metadata);
    }

    /// Check if the specified interceptor should be skipped
    pub fn should_skip_interceptor(&self, interceptor_type: &InterceptorType) -> bool {
        self.skip_next_interceptors.contains(interceptor_type)
    }

    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    pub fn final_sql(&self) -> &String {
        &self.final_sql
    }

    pub fn original_sql(&self) -> &String {
        &self.original_sql
    }

    pub fn operation_type(&self) -> &OperationType {
        &self.operation_type
    }

    pub fn final_params(&self) -> &Params {
        &self.final_params
    }

    pub fn start_time(&self) -> &Instant {
        &self.start_time
    }

    pub fn set_detection_result(&mut self, result: DetectionResult) {
        self.detection_result = Some(result);
    }


    pub fn set_final_sql(&mut self, final_sql: String) {
        self.final_sql = final_sql;
    }

    pub fn detection_result(&self) -> Option<&DetectionResult> {
        self.detection_result.as_ref()
    }

    pub fn set_connection_id(&mut self, connection_id: u32) {
        self.connection_id = Some(connection_id);
    }

    pub fn connection_id(&self) -> Option<&u32> {
        self.connection_id.as_ref()
    }

    pub fn executed_interceptors(&self) -> &Vec<InterceptorType> {
        &self.executed_interceptors
    }

    pub fn executed_interceptors_mut(&mut self) -> &mut Vec<InterceptorType> {
        &mut self.executed_interceptors
    }

    pub fn metadata_mut(&mut self) -> &mut HashMap<String, AkitaValue> {
        &mut self.metadata
    }

    pub fn metadata(&mut self) -> &HashMap<String, AkitaValue> {
        &self.metadata
    }

    pub fn table_info(&self) -> &TableName {
        &self.table_info
    }

    pub fn skip_next_interceptors(&self) -> &HashSet<InterceptorType> {
        &self.skip_next_interceptors
    }

    pub fn record_query_metrics(&self) {
        tracing::debug!(
            "Query executed: {}ms (parse: {}ms, execute: {}ms), rows: {}, memory: {}bytes",
            self.metrics().total_time.as_millis(),
            self.metrics().parse_time.as_millis(),
            self.metrics().execute_time.as_millis(),
            self.metrics().rows_affected,
            self.metrics().memory_usage
        );

        // Log slow queries
        if self.metrics().total_time > Duration::from_millis(1000) {
            tracing::warn!(
                "{} [Akita] Slow query detected: {}ms - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                self.metrics().total_time.as_millis(),
                self.final_sql()
            );
        }
    }
}

/// Query metrics
#[derive(Debug, Clone)]
pub struct QueryMetrics {
    pub parse_time: Duration,
    pub execute_time: Duration,
    pub total_time: Duration,
    pub rows_affected: u64,
    pub memory_usage: usize,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            parse_time: Duration::default(),
            execute_time: Duration::default(),
            total_time: Duration::default(),
            rows_affected: 0,
            memory_usage: 0,
        }
    }
}