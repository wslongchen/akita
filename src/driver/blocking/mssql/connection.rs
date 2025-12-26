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
use crate::driver::blocking::mssql::SyncMssqlClient;
use crate::driver::DriverType;
use crate::errors::AkitaError;

pub type MssqlConnection = r2d2::PooledConnection<MssqlConnectionManager>;
pub type MssqlPool = r2d2::Pool<MssqlConnectionManager>;



/// Mssql Connection Manager
pub struct MssqlConnectionManager {
    config: tiberius::Config,
}

impl MssqlConnectionManager {
    pub fn new(cfg: &AkitaConfig) -> Result<Self, AkitaError> {
        if cfg.get_platform()? != DriverType::Mssql {
            return Err(AkitaError::DatabaseError(
                "Database type mismatch: expected SQL Server".to_string()
            ));
        }

        let connection_string = cfg.get_connection_string()?;

        let config = tiberius::Config::from_ado_string(&connection_string)
            .map_err(|e| AkitaError::DatabaseError(format!("Invalid SQL Server connection string: {}", e)))?;

        Ok(Self { config })
    }
}

impl r2d2::ManageConnection for MssqlConnectionManager {
    type Connection = SyncMssqlClient;
    type Error = AkitaError;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        SyncMssqlClient::connect(self.config.clone())
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        // Execute a simple query to test the connection
        let _result = conn.execute("SELECT 1", &[])?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}



/// Initialize the SQL Server connection pool
pub fn init_mssql_pool(cfg: AkitaConfig) -> Result<MssqlPool, AkitaError> {
    let manager = MssqlConnectionManager::new(&cfg)?;

    let pool = r2d2::Pool::builder()
        .connection_timeout(cfg.get_connection_timeout())
        .min_idle(cfg.get_min_idle().into())
        .max_size(cfg.get_max_size())
        .idle_timeout(Some(cfg.get_idle_timeout()))
        .max_lifetime(Some(cfg.get_max_lifetime()))
        .test_on_check_out(cfg.get_test_on_check_out())
        .build(manager)
        .map_err(|e| {
            AkitaError::DatabaseError(format!("Failed to create SQL Server connection pool: {}", e))
        })?;

    let conn = pool.get().map_err(|e| {
        AkitaError::DatabaseError(format!("Failed to get connection from pool: {}", e))
    })?;

    conn.query("SELECT 1", &[]).map_err(|e| {
        AkitaError::DatabaseError(format!("SQL Server connection test failed: {}", e))
    })?;

    Ok(pool)
}
