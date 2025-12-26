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
use mysql::{Conn, Error, Opts};
use mysql::prelude::Queryable;
use crate::config::AkitaConfig;
use crate::errors::AkitaError;

pub type MysqlPool = r2d2::Pool<MysqlConnectionManager>;
pub type MysqlConnection = r2d2::PooledConnection<MysqlConnectionManager>;



#[allow(unused)]
#[derive(Clone, Debug)]
pub struct MysqlConnectionManager {
    params: Opts,
    cfg: AkitaConfig,
}

impl MysqlConnectionManager {
    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        // Gets the parsed connection string
        let connection_string = cfg.get_connection_string()?;
        let opts = mysql::Opts::from_url(&connection_string)
            .map_err(|e| AkitaError::DatabaseError(format!("Invalid MySQL URL: {}", e)))?;
        Ok(Self {
            params: Opts::from(opts),
            cfg: cfg.clone(),
        })
    }
}

impl r2d2::ManageConnection for MysqlConnectionManager {
    type Connection = Conn;
    type Error = Error;

    fn connect(&self) -> std::result::Result<Conn, Error> {
        Conn::new(self.params.clone())
    }

    fn is_valid(&self, conn: &mut Conn) -> std::result::Result<(), Error> {
        match conn.ping() {
            Ok(_) => Ok(()),
            Err(_) => {
                // If the ping fails, try a simple query as a secondary validation
                conn.query_drop("SELECT 1").map_err(|e| {
                    tracing::warn!("Connection validation failed: {}", e);
                    e
                })
            }
        }
    }

    fn has_broken(&self, conn: &mut Conn) -> bool {
        // Check if the connection is broken（ MySQL server has gogo away）
        !conn.ping().is_ok() || self.is_valid(conn).is_err()
    }
}

///
/// Create a connection pool
///
/// cfg Configuration information
///
pub fn init_mysql_pool(cfg: AkitaConfig) -> Result<MysqlPool, AkitaError> {
    let manager = MysqlConnectionManager::new(&cfg)?;

    let pool = r2d2::Pool::builder()
        .connection_timeout(cfg.get_connection_timeout())
        .min_idle(cfg.get_min_idle().into())
        .max_size(cfg.get_max_size())
        .idle_timeout(Some(cfg.get_idle_timeout()))
        .max_lifetime(Some(cfg.get_max_lifetime()))
        .test_on_check_out(cfg.get_test_on_check_out())
        .build(manager)
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to create MySQL connection pool: {}", e))
        })?;

    // Testing connections
    let mut conn = pool.get().map_err(|e| {
        AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e))
    })?;

    conn.query_drop("SELECT 1").map_err(|e| {
        AkitaError::DatabaseError(format!("MySQL connection test failed: {}", e))
    })?;

    Ok(pool)
}



#[cfg(feature = "mysql-sync")]
impl From<&AkitaConfig> for mysql::OptsBuilder {
    fn from(config: &AkitaConfig) -> Self {
        let mut opts = mysql::OptsBuilder::new();
        if let Ok(host) = config.get_hostname() {
            opts = opts.ip_or_hostname(host);
        }

        if let Ok(port) = config.get_port() {
            opts = opts.tcp_port(port.unwrap_or(3306));
        }

        if let Ok(db) = config.get_database() {
            opts = opts.db_name(db);
        }

        if let Ok(username) = config.get_username() {
            opts = opts.user(username);
        }

        if let Ok(password) = config.get_password() {
            opts = opts.pass(password);
        }

        // Handling extra parameters
        if let Ok(params) = config.get_params() {
            for (key, value) in params {
                match key.as_str() {
                    "useSSL" if value == "false" => {
                        opts = opts.ssl_opts(None);
                    }
                    _ => {}
                }
            }
        }
        opts
    }
}


#[cfg(feature = "mysql-sync")]
impl From<AkitaConfig> for mysql::OptsBuilder {
    fn from(config: AkitaConfig) -> Self {
        mysql::OptsBuilder::from(&config)
    }
}