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
use oracle::Connector;
use crate::config::AkitaConfig;
use crate::driver::DriverType;
use crate::errors::AkitaError;

pub type OracleConnection = r2d2::PooledConnection<OracleConnectionManager>;
pub type OraclePool = r2d2::Pool<OracleConnectionManager>;

/// Oracle Connection Manager
pub struct OracleConnectionManager {
    connector: Connector,
}

impl OracleConnectionManager {
    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        let connector = cfg.try_into()?;
        Ok(Self {
            connector
        })
    }

    /// Create a connection using the TNS name
    pub fn with_tns(username: &str, password: &str, tns_name: &str) -> Self {
        let connector = Connector::new(username, password, tns_name);
        Self {
            connector,
        }
    }

    /// Create a connection using the Easy Connect string
    pub fn with_easy_connect(username: &str, password: &str, easy_connect: &str) -> Self {
        let connector = Connector::new(username, password, easy_connect);
        Self {
            connector,
        }
    }
}

impl r2d2::ManageConnection for OracleConnectionManager {
    type Connection = oracle::Connection;
    type Error = oracle::Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        self.connector.connect()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query("SELECT 1 FROM DUAL", &[]).map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.ping().is_err()
    }
}

/// Initialize the Oracle connection pool
pub fn init_oracle_pool(cfg: AkitaConfig) -> Result<OraclePool, AkitaError> {
    let manager = OracleConnectionManager::new(&cfg)?;

    let pool = r2d2::Pool::builder()
        .connection_timeout(cfg.get_connection_timeout())
        .min_idle(cfg.get_min_idle().into())
        .max_size(cfg.get_max_size())
        .idle_timeout(Some(cfg.get_idle_timeout()))
        .max_lifetime(Some(cfg.get_max_lifetime()))
        .test_on_check_out(cfg.get_test_on_check_out())
        .build(manager)
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to create Oracle connection pool: {}", e))
        })?;

    let conn = pool.get().map_err(|e| {
        AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e))
    })?;

    conn.query_row("SELECT 1 FROM DUAL", &[])
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Oracle connection test failed: {}", e))
        })?;

    Ok(pool)
}


impl TryFrom<AkitaConfig> for Connector {
    type Error = AkitaError;

    fn try_from(v: AkitaConfig) -> Result<Self, Self::Error> {
        Connector::try_from(&v)
    }
}

impl TryFrom<&AkitaConfig> for Connector {
    type Error = AkitaError;

    fn try_from(cfg: &AkitaConfig) -> Result<Self, Self::Error> {
        if cfg.get_platform()? != DriverType::Oracle {
            return Err(AkitaError::DatabaseError(
                "Database type mismatch: expected Oracle".to_string()
            ));
        }

        // Use smart acquisition methods
        let username = cfg.get_username()?.ok_or_else(|| {
            AkitaError::DatabaseError("Oracle username is required".to_string())
        })?;

        let password = cfg.get_password()?.ok_or_else(|| {
            AkitaError::DatabaseError("Oracle password is required".to_string())
        })?;

        // Building connection strings
        let mut connect_string = String::new();

        if let Some(host) = cfg.get_hostname()? {
            connect_string.push_str(&host);
            if let Some(port) = cfg.get_port()? {
                connect_string.push_str(&format!(":{}", port));
            }
            if let Some(service) = cfg.get_database()? {
                connect_string.push_str(&format!("/{}", service));
            }
        } else {
            // If the host is empty, it may be the full Easy Connect format
            if let Some(url) = cfg.get_url() {
                if let Some(at_index) = url.find('@') {
                    connect_string = url[at_index + 1..].to_string();
                } else {
                    return Err(AkitaError::DatabaseError(
                        "Oracle connection string is required".to_string()
                    ));
                }
            } else {
                return Err(AkitaError::DatabaseError(
                    "Oracle connection string is required".to_string()
                ));
            }
        }
        
        
        Ok(Connector::new(username, password, connect_string))
    }
}

