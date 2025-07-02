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

use std::time::Duration;
use url::Url;
use crate::Platform;

#[derive(Clone, Debug)]
pub struct AkitaConfig {
    connection_timeout: Duration,
    min_idle: Option<u32>,
    max_size: u32,
    platform: Platform,
    url: Option<String>,
    password: Option<String>,
    db_name: Option<String>,
    port: Option<u16>,
    ip_or_hostname: Option<String>,
    username: Option<String>,
}


#[cfg(feature = "akita-mysql")]
impl From<&AkitaConfig> for mysql::OptsBuilder {
    fn from(v: &AkitaConfig) -> Self {
        let mut opts = mysql::OptsBuilder::new();
        if let Some(url) = &v.url {
            let url_opts = mysql::Opts::from_url(url).expect("MySQL URL  parse error");
            opts = mysql::OptsBuilder::from_opts(url_opts);
        }
        if let Some(db_name) = v.db_name.to_owned() {
            opts = opts.db_name(Some(db_name));
        }
        if let Some(username) = v.username.to_owned() {
            opts = opts.user(Some(username));
        }
        if let Some(password) = v.password.to_owned() {
            opts = opts.pass(Some(password));
        }
        if let Some(ip_or_hostname) = v.ip_or_hostname.to_owned() {
            opts = opts.ip_or_hostname(Some(ip_or_hostname));
        }
        opts
    }
}

impl Default for AkitaConfig {
    fn default() -> Self {
        AkitaConfig {
            max_size: 16,
            platform: Platform::Unsupported(String::default()),
            url: None,
            password: None,
            username: None,
            ip_or_hostname: None,
            db_name: None,
            connection_timeout: Duration::from_secs(6),
            min_idle: None,
            port: Some(3306)
        }
    }
}

impl AkitaConfig {

    #[cfg(feature = "akita-mysql")]
    pub fn mysql_builder(&self) -> mysql::OptsBuilder {
        self.into()
    }

    pub fn new(url: &str) -> Self {
        let mut cfg = AkitaConfig::default();
        cfg.url = url.to_string().into();
        cfg.parse_url()
    }

    /// parse the url with akita config
    fn parse_url(mut self) -> Self {
        let url = Url::parse(&self.url.to_owned().unwrap_or_default());
        match url {
            Ok(url) => {
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "akita-mysql")]
                    "mysql" => {
                        self.platform = Platform::Mysql;
                        let host = url.host_str().unwrap_or_default();
                        let port = url.port().unwrap_or_default();
                        self.ip_or_hostname = host.to_string().into();
                        self.port = port.into();
                        if let Some(mut db) = url.path_segments() {
                            self.db_name = db.next().map(ToString::to_string);
                        }
                    },
                    #[cfg(feature = "akita-sqlite")]
                    "sqlite" => {
                        let host = url.host_str().unwrap_or_default();
                        let path = url.path();
                        let path = if path == "/" { "" } else { path };
                        let db_file = format!("{}{}", host, path);
                        self.platform = Platform::Sqlite(db_file);
                    },
                    _ => {
                        self.platform = Platform::Unsupported(scheme.to_string());
                    },
                }
            }
            Err(_e) => {

            },
        }
        self
    }

    pub fn set_url(mut self, url: String) -> Self {
        self.url = url.into();
        self = self.parse_url();
        self
    }

    pub fn url(&self) -> Option<&String> {
        self.url.as_ref()
    }

    pub fn set_username(mut self, username: String) -> Self {
        self.username = username.into();
        self
    }

    pub fn username(&self) -> Option<&String> {
        self.username.as_ref()
    }

    pub fn set_password(mut self, password: String) -> Self {
        self.password = password.into();
        self
    }

    pub fn password(&self) -> Option<&String> {
        self.password.as_ref()
    }

    pub fn set_db_name(mut self, db_name: String) -> Self {
        self.db_name = db_name.into();
        self
    }

    pub fn db_name(&self) -> Option<&String> {
        self.db_name.as_ref()
    }

    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port.into();
        self
    }

    pub fn port(&self) -> u16 {
        self.port.to_owned().unwrap_or_default()
    }

    #[allow(unused_mut)]
    pub fn set_platform(mut self, platform: &str) -> Self {
        match platform {
            #[cfg(feature = "akita-mysql")]
            "mysql" => {
                self.platform = Platform::Mysql;
            },
            #[cfg(feature = "akita-sqlite")]
            "sqlite" => {
                self.platform = Platform::Sqlite("".to_string());
            },
            _ => {},
        }
        self
    }

    pub fn platform(&self) -> &Platform {
        &self.platform
    }

    pub fn set_ip_or_hostname(mut self, ip_or_hostname: String) -> Self {
        self.ip_or_hostname = ip_or_hostname.into();
        self
    }

    pub fn ip_or_hostname(&self) -> Option<&String> {
        self.ip_or_hostname.as_ref()
    }

    pub fn set_max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    pub fn set_connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }

    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    pub fn set_min_idle(mut self, min_idle: Option<u32>) -> Self {
        self.min_idle = min_idle;
        self
    }

    pub fn min_idle(&self) -> Option<u32> {
        self.min_idle
    }

}