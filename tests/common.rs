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
use std::sync::{Arc, Mutex};
use std::time::Duration;
use chrono::{NaiveDate, NaiveDateTime};
use uuid::Uuid;
use akita_core::{AkitaValue, InterceptorType, OperationType};
use akita_derive::{sql, sql_xml, AkitaEnum, Entity};

use akita::prelude::*;

pub struct EncrptConverter;

#[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))]
impl Converter<String> for EncrptConverter {
    fn convert(data: &String) -> String {
        "1".to_string()
    }

    fn revert(data: &String) -> String {
        "2".to_string()
    }
}

/// Connection configuration
#[derive(Debug)]
pub struct Database {
    pub ip: String,
    pub username: String,
    pub password: String,
    pub db_name: String,
    pub port: u16,
}

#[derive(Entity, Clone, Default, Debug)]
#[table(name = "t_system_user")]
// #[schema(name = "TEST")]
pub struct User {
    #[id(name = "id")]
    pub id: i64,
    pub pk: String,
    pub user_type: Option<String>,
    pub name: Option<String>,
    pub headline: Option<NaiveDateTime>,
    pub tenant_id: i64,
    /// 状态
    pub status: u8,
    /// 用户等级 0.普通会员 1.VIP会员
    #[field(name="level")]
    pub level: Priority,
    pub age: Option<u8>,
    /// 生日
    pub birthday: Option<NaiveDate>,
    /// 性别
    pub gender: u8,
    #[field(exist = false)]
    pub is_org: bool,
    #[field(name = "token")]
    pub url_token: String,
    // pub hashmap: HashMap<String, serde_json::Value>,
}

#[derive(AkitaEnum, Debug, Clone)]
#[akita_enum(storage = "string")]
pub enum Status {
    Active,
    Inactive,
    Pending,
}


#[derive(AkitaEnum, Debug, Clone, PartialEq)]
#[akita_enum(storage = "int")]
pub enum Priority {
    Low = 1,
    Medium = 2,
    High = 3,
}

impl Default for Priority {
    fn default() -> Self {
        Self::High
    }
}

#[derive(AkitaEnum, Debug, Clone)]
#[akita_enum(storage = "json")]
enum Message {
    Text(String),
    Image { width: i32, height: i32, url: String },
    Error(String, i32),
}

// 可追踪的拦截器用于测试
#[derive(Debug)]
pub struct TrackableTenantInterceptor {
    tenant_column: String,
    pub before_call_count: Arc<Mutex<usize>>,
    pub after_call_count: Arc<Mutex<usize>>,
}

impl TrackableTenantInterceptor {
    pub fn new(tenant_column: String) -> Self {
        Self {
            tenant_column,
            before_call_count: Arc::new(Mutex::new(0)),
            after_call_count: Arc::new(Mutex::new(0)),
        }
    }
}

#[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))]
impl AkitaInterceptor for TrackableTenantInterceptor {
    fn name(&self) -> &'static str {
        "trackable_tenant"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Tenant
    }

    fn order(&self) -> i32 {
        -100
    }

    fn supports_operation(&self, operation: &OperationType) -> bool {
        matches!(operation,
                OperationType::Select | OperationType::Insert(..) |
                OperationType::Update | OperationType::Delete
            )
    }

    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }

    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<(), AkitaError> {
        *self.before_call_count.lock().unwrap() += 1;

        let tenant_id = "1".to_string();

        // TODO: ctx.wrapper = ctx.wrapper.clone().eq(&self.tenant_column, tenant_id);

        // 记录到元数据中用于测试验证
        ctx.set_metadata("tenant_applied", true);
        ctx.set_metadata("tenant_id", tenant_id.to_string());

        Ok(())
    }

    fn after_execute(&self, ctx: &mut ExecuteContext, result: &mut Result<ExecuteResult, AkitaError>) -> Result<(), AkitaError> {
        *self.after_call_count.lock().unwrap() += 1;

        // 记录执行结果到元数据
        if let Ok(rows) = result {
            ctx.metadata_mut().insert("rows_affected".to_string(), AkitaValue::Int(rows.len() as i32));
        }

        Ok(())
    }
}

// 可追踪的性能拦截器
#[derive(Debug)]
pub struct TrackablePerformanceInterceptor {
    pub before_call_count: Arc<Mutex<usize>>,
    pub after_call_count: Arc<Mutex<usize>>,
    pub query_times: Arc<Mutex<Vec<Duration>>>,
}

impl TrackablePerformanceInterceptor {
    pub fn new() -> Self {
        Self {
            before_call_count: Arc::new(Mutex::new(0)),
            after_call_count: Arc::new(Mutex::new(0)),
            query_times: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))]
impl AkitaInterceptor for TrackablePerformanceInterceptor {
    fn name(&self) -> &'static str {
        "trackable_performance"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Performance
    }

    fn order(&self) -> i32 {
        100
    }

    fn supports_operation(&self, _operation: &OperationType) -> bool {
        true
    }

    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }

    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<(), AkitaError> {
        *self.before_call_count.lock().unwrap() += 1;
        ctx.set_metadata("performance_started", true);
        Ok(())
    }

    fn after_execute(&self, ctx: &mut ExecuteContext, _result: &mut Result<ExecuteResult, AkitaError>) -> Result<(), AkitaError> {
        *self.after_call_count.lock().unwrap() += 1;

        let duration = ctx.start_time().elapsed();
        self.query_times.lock().unwrap().push(duration);

        ctx.set_metadata("performance_measured", true);
        ctx.set_metadata("query_duration_ms", duration.as_millis() as i64);

        Ok(())
    }
}

// 可追踪的日志拦截器
#[derive(Debug)]
pub struct TrackableLoggingInterceptor {
    pub log_entries: Arc<Mutex<Vec<String>>>,
    pub before_call_count: Arc<Mutex<usize>>,
    pub after_call_count: Arc<Mutex<usize>>,
}

impl TrackableLoggingInterceptor {
    pub fn new() -> Self {
        Self {
            log_entries: Arc::new(Mutex::new(Vec::new())),
            before_call_count: Arc::new(Mutex::new(0)),
            after_call_count: Arc::new(Mutex::new(0)),
        }
    }
}

#[cfg(all(
    any(
        feature = "mysql-sync",
        feature = "postgres-sync",
        feature = "sqlite-sync",
        feature = "oracle-sync",
        feature = "mssql-sync"
    ),
    not(any(
        feature = "mysql-async",
        feature = "postgres-async",
        feature = "sqlite-async",
        feature = "mssql-async",
        feature = "oracle-async"
    ))
))]
impl AkitaInterceptor for TrackableLoggingInterceptor {
    fn name(&self) -> &'static str {
        "trackable_logging"
    }

    fn interceptor_type(&self) -> InterceptorType {
        InterceptorType::Logging
    }

    fn order(&self) -> i32 {
        90
    }

    fn supports_operation(&self, _operation: &OperationType) -> bool {
        true
    }

    fn will_ignore_table(&self, _table_name: &str) -> bool {
        false
    }

    fn before_execute(&self, ctx: &mut ExecuteContext) -> Result<(), AkitaError> {
        *self.before_call_count.lock().unwrap() += 1;

        let log_entry = format!("BEFORE: {:?} - {}", ctx.operation_type(), ctx.original_sql());
        self.log_entries.lock().unwrap().push(log_entry);

        ctx.set_metadata("logged_before", true);

        Ok(())
    }

    fn after_execute(&self, ctx: &mut ExecuteContext, result: &mut Result<ExecuteResult, AkitaError>) -> Result<(), AkitaError> {
        *self.after_call_count.lock().unwrap() += 1;

        let status = if result.is_ok() { "SUCCESS" } else { "FAILED" };
        let log_entry = format!("AFTER: {:?} - {} - {}", ctx.operation_type(), ctx.final_sql(), status);
        self.log_entries.lock().unwrap().push(log_entry);

        ctx.set_metadata("logged_after", true);

        Ok(())
    }
}

pub fn create_test_akita_cfg() -> AkitaConfig {
    let database = Database {
        ip: "127.0.0.1".to_string(),
        username: "test".to_string(),
        password: "password".to_string(),
        db_name: "test".to_string(),
        port: 3306,
    };
    
    let cfg = AkitaConfig::default()
        .max_size(5)
        .connection_timeout(Duration::from_secs(5))
        .password(&database.password)
        .username(&database.username)
        .port(database.port)
        .database(&database.db_name)
        .hostname(&database.ip);
    cfg
}

pub fn create_test_user() -> User {
    User {
        id: 0,
        pk: uuid::Uuid::new_v4().simple().to_string(),
        user_type: Some("super".to_string()),
        name: Some(format!("NAME{}",uuid::Uuid::new_v4().simple().to_string())),
        headline: Some(NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
        tenant_id: 0,
        status: 1,
        level: Priority::Medium,
        age: 18.into(),
        birthday: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        gender: 1,
        is_org: false,
        url_token: format!("{}{}", Local::now().naive_local().timestamp_subsec_nanos(), Uuid::new_v4().simple().to_string()),
        // hashmap: Default::default(),
    }
}