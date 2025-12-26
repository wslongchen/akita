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
use crate::config::AkitaConfig;
use crate::prelude::AkitaError;

pub type PostgresPool = r2d2::Pool<PostgresConnectionManager>;
pub type PostgresConnection = r2d2::PooledConnection<PostgresConnectionManager>;



#[allow(unused)]
#[derive(Clone, Debug)]
pub struct PostgresConnectionManager {
    config: postgres::Config,
    connection_timeout: std::time::Duration,
    application_name: Option<String>,
    cfg: AkitaConfig,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without.
    Prefer,
    /// Require the use of TLS.
    Require,
}

impl From<SslMode> for postgres::config::SslMode {
    fn from(mode: SslMode) -> Self {
        match mode {
            SslMode::Disable => postgres::config::SslMode::Disable,
            SslMode::Prefer => postgres::config::SslMode::Prefer,
            SslMode::Require => postgres::config::SslMode::Require
        }
    }
}

impl PostgresConnectionManager {

    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        let config = cfg.into();
        Ok(Self {
            config,
            connection_timeout: cfg.get_connection_timeout(),
            application_name: None,
            cfg: cfg.clone(),
        })
    }

    /// Created from configuration parameters
    pub fn from_params(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> Self {
        let mut config = postgres::Config::new();
        config.host(host);
        config.port(port);
        config.dbname(database);
        config.user(username);
        config.password(password);

        Self {
            config,
            connection_timeout: std::time::Duration::from_secs(30),
            application_name: None,
            cfg: Default::default(),
        }
    }

    /// Set a connection timeout
    pub fn with_connection_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set the application name
    pub fn with_application_name(mut self, name: &str) -> Self {
        self.application_name = Some(name.to_string());
        self
    }

    /// Setting SSL mode
    pub fn with_ssl_mode(mut self, mode: SslMode) -> Self {
        self.config.ssl_mode(mode.into());
        self
    }

    /// Setting connection options
    pub fn with_option(mut self, key: &str, value: &str) -> Self {
        self.config.options(&format!("{}={}", key, value));
        self
    }
}

impl r2d2::ManageConnection for PostgresConnectionManager {
    type Connection = postgres::Client;
    type Error = postgres::Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let mut config = self.config.clone();

        // Set the application name
        if let Some(app_name) = &self.application_name {
            config.application_name(app_name);
        }

        // Set a connection timeout
        config.connect_timeout(self.connection_timeout);

        config.connect(postgres::NoTls)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.simple_query("SELECT 1").map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }
}

///
/// Create a connection pool
///
/// cfg Configuration information
///
pub fn init_postgres_pool(cfg: AkitaConfig) -> Result<PostgresPool, AkitaError> {
    let manager = PostgresConnectionManager::new(&cfg)?;

    let pool = r2d2::Pool::builder()
        .connection_timeout(cfg.get_connection_timeout())
        .min_idle(cfg.get_min_idle().into())
        .max_size(cfg.get_max_size())
        .idle_timeout(Some(cfg.get_idle_timeout()))
        .max_lifetime(Some(cfg.get_max_lifetime()))
        .test_on_check_out(cfg.get_test_on_check_out())
        .build(manager)
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to create PostgreSQL connection pool: {}", e))
        })?;

    let mut conn = pool.get().map_err(|e| {
        AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e))
    })?;

    conn.simple_query("SELECT 1").map_err(|e| {
        AkitaError::DatabaseError(format!("PostgreSQL connection test failed: {}", e))
    })?;

    Ok(pool)
}

impl From<AkitaConfig> for postgres::Config {
    fn from(v: AkitaConfig) -> Self {
        postgres::Config::from(&v)
    }
}
impl From<&AkitaConfig> for postgres::Config {
    fn from(config: &AkitaConfig) -> Self {
        let mut cfg = postgres::Config::new();
        if let Ok(Some(host)) = config.get_hostname() {
            cfg.host(&host);
        }
        if let Ok(Some(port)) = config.get_port() {
            cfg.port(port);
        }
        if let Ok(Some(db)) = config.get_database() {
            cfg.dbname(&db);
        }
        if let Ok(Some(username)) = config.get_username() {
            cfg.user(&username);
        }
        if let Ok(Some(password)) = config.get_password() {
            cfg.password(&password);
        }
        cfg.connect_timeout(config.get_connection_timeout());
        cfg
    }
}