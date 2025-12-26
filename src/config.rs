use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::fmt::Write;
use std::hash::{DefaultHasher, Hash, Hasher};
use url::Url;
use akita_core::SqlSecurityConfig;
use crate::driver::DriverType;
use crate::errors::AkitaError;

#[derive(Debug, Clone)]
pub struct XmlSqlLoaderConfig {
    pub auto_reload: bool,
    pub parameter_detection: bool,
    pub sql_formatting: bool,
}

impl Default for XmlSqlLoaderConfig {
    fn default() -> Self {
        Self {
            auto_reload: false,
            parameter_detection: true,
            sql_formatting: true,
        }
    }
}

/// Parsed connection information cached
#[derive(Clone, Debug, Default)]
struct ParsedConnectionInfo {
    pub platform: DriverType,
    pub hostname: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_params: HashMap<String, String>,
}

/// The main configuration structure
#[derive(Clone, Debug)]
pub struct AkitaConfig {
    // Connection pool configuration
    connection_timeout: Duration,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    min_idle: Option<u32>,
    max_size: u32,
    test_on_check_out: bool,

    // SQL Related Configuration
    pub platform: DriverType,
    pub sql_security: Option<SqlSecurityConfig>,
    pub xml_sql_loader: Option<XmlSqlLoaderConfig>,

    // Connection information (externally configurable)
    url: Option<String>,
    hostname: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    extra_params: HashMap<String, String>,

    // Internal state
    parsed_cache: Arc<RwLock<Option<(ParsedConnectionInfo, u64)>>>,
}

impl Default for AkitaConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
            min_idle: Some(1),
            max_size: 10,
            test_on_check_out: true,
            platform: DriverType::MySQL,
            sql_security: None,
            xml_sql_loader: None,
            url: None,
            hostname: None,
            port: None,
            database: None,
            username: None,
            password: None,
            extra_params: HashMap::new(),
            parsed_cache: Arc::new(RwLock::new(None)),
        }
    }
}

impl AkitaConfig {
    pub fn new() -> Self {
        Self::default()
    }

    // Setting methods
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self.invalidate_cache();
        self
    }

    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self.invalidate_cache();
        self
    }

    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn min_idle(mut self, min_idle: u32) -> Self {
        self.min_idle = Some(min_idle);
        self
    }

    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self.invalidate_cache();
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self.invalidate_cache();
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.invalidate_cache();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self.invalidate_cache();
        self
    }

    pub fn platform(mut self, platform: DriverType) -> Self {
        self.platform = platform;
        self
    }

    pub fn param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_params.insert(key.into(), value.into());
        self.invalidate_cache();
        self
    }

    // Get method (resolve if needed)
    pub fn get_connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    pub fn get_idle_timeout(&self) -> Duration {
        self.idle_timeout.unwrap_or_else(|| Duration::from_secs(600))
    }

    pub fn get_max_lifetime(&self) -> Duration {
        self.max_lifetime.unwrap_or_else(|| Duration::from_secs(1800))
    }

    pub fn get_min_idle(&self) -> u32 {
        self.min_idle.unwrap_or(1)
    }

    pub fn get_max_size(&self) -> u32 {
        self.max_size
    }

    pub fn get_test_on_check_out(&self) -> bool {
        self.test_on_check_out
    }

    pub fn get_url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    pub fn sql_security(&self) -> Option<&SqlSecurityConfig> {
        self.sql_security.as_ref()
    }

    pub fn xml_sql_loader(&self) -> Option<&XmlSqlLoaderConfig> {
        self.xml_sql_loader.as_ref()
    }

    pub fn has_url(&self) -> bool {
        self.url.is_some()
    }
    
    pub fn get_platform(&self) -> Result<DriverType, AkitaError> {
        self.ensure_parsed().map(|info| info.platform.clone())
    }

    pub fn get_hostname(&self) -> Result<Option<String>, AkitaError> {
        let info = self.ensure_parsed()?;
        Ok(info.hostname)
    }

    pub fn get_port(&self) -> Result<Option<u16>, AkitaError> {
        let info = self.ensure_parsed()?;
        Ok(info.port)
    }

    pub fn get_database(&self) -> Result<Option<String>, AkitaError> {
        let info = self.ensure_parsed()?;
        Ok(info.database)
    }

    pub fn get_username(&self) -> Result<Option<String>, AkitaError> {
        let info = self.ensure_parsed()?;
        Ok(info.username)
    }

    pub fn get_password(&self) -> Result<Option<String>, AkitaError> {
        let info = self.ensure_parsed()?;
        Ok(info.password)
    }

    pub fn get_params(&self) -> Result<HashMap<String, String>, AkitaError> {
        self.ensure_parsed().map(|info| info.extra_params.clone())
    }

    pub fn get_connection_string(&self) -> Result<String, AkitaError> {
        self.build_connection_string()
    }

    // Internal methods
    pub fn invalidate_cache(&mut self) {
        let mut cache = self.parsed_cache.write().unwrap();
        *cache = None;
    }

    fn ensure_parsed(&self) -> Result<ParsedConnectionInfo, AkitaError> {
        // Calculate the hash of the current configuration
        let current_hash = self.config_hash();

        {
            let cache = self.parsed_cache.read().unwrap();
            if let Some((info, cached_hash)) = cache.as_ref() {
                if current_hash == *cached_hash {
                    return Ok(info.clone());
                }
            }
        }

        // Need to parse
        let info = self.parse()?;
        let mut cache = self.parsed_cache.write().unwrap();
        *cache = Some((info.clone(), current_hash));
        Ok(info)
    }

    fn config_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash all relevant fields
        self.url.hash(&mut hasher);
        self.hostname.hash(&mut hasher);
        self.port.hash(&mut hasher);
        self.database.hash(&mut hasher);
        self.username.hash(&mut hasher);
        self.password.hash(&mut hasher);

        // EXTRA_PARAMS ARE SORTED TO ENSURE CONSISTENCY
        let mut params: Vec<_> = self.extra_params.iter().collect();
        params.sort_by_key(|(k, _)| *k);
        for (k, v) in params {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }

        // If both URL and individual parameters are set, we need to combine them
        self.connection_timeout.as_secs().hash(&mut hasher);

        hasher.finish()
    }

    fn parse(&self) -> Result<ParsedConnectionInfo, AkitaError> {
        let mut info = ParsedConnectionInfo::default();

        // Urls are treated first, but separate parameters are allowed to override values in urls
        if let Some(url) = &self.url {
            self.parse_url(url, &mut info)?;
        }

        // Individually set parameters override the resolved URL values
        self.apply_individual_parameters(&mut info);

        // If the URL does not specify a platform, try to infer it from the parameters
        if info.platform == DriverType::Unsupported {
            info.platform = self.detect_platform_from_params();
        }

        Ok(info)
    }

    fn apply_individual_parameters(&self, info: &mut ParsedConnectionInfo) {
        // Separately set parameters override the URL resolved values
        if let Some(hostname) = &self.hostname {
            info.hostname = Some(hostname.clone());
        }

        if self.port.is_some() {
            info.port = self.port;
        }

        if let Some(database) = &self.database {
            info.database = Some(database.clone());
        }

        if let Some(username) = &self.username {
            info.username = Some(username.clone());
        }

        if let Some(password) = &self.password {
            info.password = Some(password.clone());
        }

        // Combining additional parameters (individually set higher priority)
        for (key, value) in &self.extra_params {
            info.extra_params.insert(key.clone(), value.clone());
        }
    }

    fn parse_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        let url = url.trim();

        if url.is_empty() {
            return Ok(());
        }

        // Detecting protocol type
        if let Some(url_without_jdbc) = url.strip_prefix("jdbc:") {
            self.parse_jdbc_url(url_without_jdbc, info)
        } else if url.starts_with("mysql://") {
            info.platform = DriverType::MySQL;
            self.parse_standard_url(url, info)
        } else if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            info.platform = DriverType::Postgres;
            self.parse_standard_url(url, info)
        } else if url.starts_with("oracle://") {
            info.platform = DriverType::Oracle;
            self.parse_standard_url(url, info)
        } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
            info.platform = DriverType::Mssql;
            self.parse_standard_url(url, info)
        } else if url.contains(';') && (url.contains("Server=") || url.contains("Database=")) {
            // ADO.NET format
            info.platform = DriverType::Mssql;
            self.parse_adonet_url(url, info)
        } else if url.ends_with(".db") || url.ends_with(".sqlite") || url.contains(".sqlite") || url == ":memory:" {
            // SQLite file
            info.platform = DriverType::Sqlite;
            info.database = Some(url.to_string());
            Ok(())
        } else if url.contains('@') && !url.contains("://") {
            // Oracle Easy Connect
            info.platform = DriverType::Oracle;
            self.parse_oracle_easy_connect(url, info)
        } else {
            // Attempt to resolve to a standard URL or file path
            self.try_parse_as_generic(url, info)
        }
    }

    fn try_parse_as_generic(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        // Try to resolve as a standard URL
        if let Ok(parsed) = Url::parse(url) {
            // Guess the platform based on the protocol
            info.platform = match parsed.scheme() {
                "mysql" => DriverType::MySQL,
                "postgresql" | "postgres" => DriverType::Postgres,
                "oracle" => DriverType::Oracle,
                "sqlserver" | "mssql" => DriverType::Mssql,
                _ => DriverType::Unsupported,
            };

            self.parse_standard_url(url, info)
        } else {
            // If not a valid URL, try as a file path
            if url.contains('/') || url.contains('\\') || url.contains('.') {
                info.platform = DriverType::Sqlite;
                info.database = Some(url.to_string());
                Ok(())
            } else {
                Err(AkitaError::DatabaseError(format!("Unresolvable connection string: {}", url)))
            }
        }
    }

    fn parse_jdbc_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        if let Some(url_without_mysql) = url.strip_prefix("mysql:") {
            info.platform = DriverType::MySQL;
            self.parse_standard_url(url_without_mysql, info)
        } else if let Some(url_without_pg) = url.strip_prefix("postgresql:") {
            info.platform = DriverType::Postgres;
            self.parse_standard_url(url_without_pg, info)
        } else if let Some(url_without_oracle) = url.strip_prefix("oracle:") {
            info.platform = DriverType::Oracle;
            self.parse_oracle_jdbc_url(url_without_oracle, info)
        } else if let Some(url_without_sqlserver) = url.strip_prefix("sqlserver:")
            .or_else(|| url.strip_prefix("microsoft:sqlserver:")) {
            info.platform = DriverType::Mssql;
            self.parse_sqlserver_jdbc_url(url_without_sqlserver, info)
        } else if let Some(url_without_sqlite) = url.strip_prefix("sqlite:") {
            info.platform = DriverType::Sqlite;
            info.database = Some(url_without_sqlite.to_string());
            Ok(())
        } else {
            Err(AkitaError::DatabaseError(format!("Unsupported JDBC drivers: jdbc:{}", url)))
        }
    }

    fn parse_standard_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        let parsed = Url::parse(url)
            .map_err(|e| AkitaError::DatabaseError(format!("Failed URL resolution: {}", e)))?;

        // Only set values that are not specified in separate parameters
        if info.hostname.is_none() {
            if let Some(host) = parsed.host_str() {
                info.hostname = Some(host.to_string());
            }
        }

        if info.port.is_none() {
            info.port = parsed.port();
        }

        // Resolving the database name
        if info.database.is_none() {
            let path = parsed.path();
            if !path.is_empty() && path != "/" {
                let db_name = path.trim_start_matches('/');
                if !db_name.is_empty() {
                    info.database = Some(db_name.to_string());
                }
            }
        }

        // Username and password (only use the URL if a separate parameter is not set)
        if info.username.is_none() {
            let username = parsed.username();
            if !username.is_empty() {
                info.username = Some(username.to_string());
            }
        }

        if info.password.is_none() {
            if let Some(password) = parsed.password() {
                info.password = Some(password.to_string());
            }
        }

        // Query parameters (merged, URL parameters have lower priority)
        for (key, value) in parsed.query_pairs() {
            if !info.extra_params.contains_key(&key.to_string()) {
                info.extra_params.insert(key.into_owned(), value.into_owned());
            }
        }

        Ok(())
    }

    fn parse_oracle_jdbc_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        let url = if let Some(url_without_thin) = url.strip_prefix("thin:@") {
            url_without_thin
        } else if let Some(url_without_oci) = url.strip_prefix("oci:@") {
            url_without_oci
        } else if let Some(url_without_at) = url.strip_prefix('@') {
            url_without_at
        } else {
            url
        };

        // parsing host:port/service or host:port:service
        if url.contains('/') {
            let (host_port, service) = url.split_once('/')
                .ok_or_else(|| AkitaError::DatabaseError("Invalid Oracle URL format".to_string()))?;

            info.database = Some(service.to_string());
            self.parse_host_port(host_port, info)
        } else if url.contains(':') {
            let parts: Vec<&str> = url.split(':').collect();
            match parts.len() {
                2 => {
                    // host:port
                    info.hostname = Some(parts[0].to_string());
                    info.port = parts[1].parse().ok();
                    Ok(())
                }
                3 => {
                    // host:port:service
                    info.hostname = Some(parts[0].to_string());
                    info.port = parts[1].parse().ok();
                    info.database = Some(parts[2].to_string());
                    Ok(())
                }
                _ => Err(AkitaError::DatabaseError("Invalid Oracle connection string".to_string())),
            }
        } else {
            // Hostname only
            info.hostname = Some(url.to_string());
            Ok(())
        }
    }

    fn parse_sqlserver_jdbc_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        if let Some(rest) = url.strip_prefix("//") {
            let (host_port, params) = rest.split_once(';').unwrap_or((rest, ""));

            // Resolve the host and port
            self.parse_host_port(host_port, info)?;

            // Parsing parameters
            for param in params.split(';') {
                if let Some((key, value)) = param.split_once('=') {
                    let key_lower = key.to_lowercase();
                    match key_lower.as_str() {
                        "databasename" | "databasename" | "database" => {
                            info.database = Some(value.to_string());
                        }
                        "user" | "username" => {
                            info.username = Some(value.to_string());
                        }
                        "password" => {
                            info.password = Some(value.to_string());
                        }
                        _ => {
                            info.extra_params.insert(key_lower, value.to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_host_port(&self, host_port: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        if let Some((host, port)) = host_port.split_once(':') {
            info.hostname = Some(host.to_string());
            let port = port.parse::<u16>()
                .map_err(|_| AkitaError::DatabaseError(format!("Invalid port number: {}", port)))?;
            info.port = Some(port);
        } else {
            info.hostname = Some(host_port.to_string());
        }
        Ok(())
    }

    fn parse_native_url(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        if url.starts_with("mysql://") {
            info.platform = DriverType::MySQL;
        } else if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            info.platform = DriverType::Postgres;
        } else if url.starts_with("oracle://") {
            info.platform = DriverType::Oracle;
        } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
            info.platform = DriverType::Mssql;
        } else {
            return Err(AkitaError::DatabaseError(format!("Unsupported protocol: {}", url)));
        }

        self.parse_standard_url(url, info)
    }

    fn parse_adonet_url(&self, conn_str: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        info.platform = DriverType::Mssql;

        for part in conn_str.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let kv: Vec<&str> = part.splitn(2, '=').collect();
            if kv.len() == 2 {
                let key = kv[0].trim().to_lowercase();
                let value = kv[1].trim();

                match key.as_str() {
                    "server" | "data source" => {
                        let parts: Vec<&str> = value.split(',').collect();
                        info.hostname = Some(parts[0].to_string());
                        if parts.len() > 1 {
                            info.port = parts[1].parse().ok();
                        }
                    }
                    "database" | "initial catalog" => {
                        info.database = Some(value.to_string());
                    }
                    "user id" | "uid" => {
                        info.username = Some(value.to_string());
                    }
                    "password" | "pwd" => {
                        info.password = Some(value.to_string());
                    }
                    "port" => {
                        info.port = value.parse().ok();
                    }
                    _ => {
                        info.extra_params.insert(key, value.to_string());
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_oracle_easy_connect(&self, url: &str, info: &mut ParsedConnectionInfo) -> Result<(), AkitaError> {
        info.platform = DriverType::Oracle;

        let parts: Vec<&str> = url.split('@').collect();
        if parts.len() != 2 {
            return Err(AkitaError::DatabaseError("Invalid Oracle connection string".to_string()));
        }

        // Resolve the username and password
        let cred_part = parts[0];
        if let Some(slash_pos) = cred_part.find('/') {
            info.username = Some(cred_part[..slash_pos].to_string());
            info.password = Some(cred_part[slash_pos + 1..].to_string());
        } else {
            info.username = Some(cred_part.to_string());
        }

        // Parsing connection information
        let connect_part = parts[1];
        if connect_part.contains('/') {
            let connect_parts: Vec<&str> = connect_part.splitn(2, '/').collect();
            let host_port = connect_parts[0];
            info.database = Some(connect_parts[1].to_string());

            let host_port_parts: Vec<&str> = host_port.split(':').collect();
            if !host_port_parts.is_empty() {
                info.hostname = Some(host_port_parts[0].to_string());
                if host_port_parts.len() > 1 {
                    info.port = host_port_parts[1].parse().ok();
                }
            }
        } else {
            let host_port_parts: Vec<&str> = connect_part.split(':').collect();
            if !host_port_parts.is_empty() {
                info.hostname = Some(host_port_parts[0].to_string());
                if host_port_parts.len() > 1 {
                    info.port = host_port_parts[1].parse().ok();
                }
            }
        }

        Ok(())
    }

    fn detect_platform_from_params(&self) -> DriverType {
        // Guess the platform based on the database name or other parameters
        if let Some(db) = self.database.as_ref() {
            if db.ends_with(".db") || db.ends_with(".sqlite") || db == ":memory:" {
                return DriverType::Sqlite;
            }
        }
        self.platform.clone()
    }

    fn build_connection_string(&self) -> Result<String, AkitaError> {
        let info = self.ensure_parsed()?;

        match info.platform {
            DriverType::MySQL => self.build_mysql_url(&info),
            DriverType::Postgres => self.build_postgres_url(&info),
            DriverType::Oracle => self.build_oracle_url(&info),
            DriverType::Mssql => self.build_sqlserver_url(&info),
            DriverType::Sqlite => Ok(info.database.unwrap_or_else(|| ":memory:".to_string())),
            DriverType::Unsupported => Err(AkitaError::DatabaseError("Unknown database type".to_string())),
        }
    }

    fn build_mysql_url(&self, info: &ParsedConnectionInfo) -> Result<String, AkitaError> {
        let mut url = String::from("mysql://");

        match (&info.username, &info.password) {
            (Some(u), Some(p)) => write!(&mut url, "{}:{}@", u, p).map_err(|err| AkitaError::DatabaseError(err.to_string()))?,
            (Some(u), None) => write!(&mut url, "{}@", u).map_err(|err| AkitaError::DatabaseError(err.to_string()))?,
            _ => {}
        }

        if let Some(host) = &info.hostname {
            if let Some(port) = info.port {
                write!(&mut url, "{}:{}", host, port).map_err(|err| AkitaError::DatabaseError(err.to_string()))?;
            } else {
                url.push_str(host);
            }
        }

        if let Some(db) = &info.database {
            write!(&mut url, "/{}", db).map_err(|err| AkitaError::DatabaseError(err.to_string()))?;
        }

        if !info.extra_params.is_empty() {
            url.push('?');
            let params: String = info.extra_params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");
            url.push_str(&params);
        }

        Ok(url)
    }

    fn build_postgres_url(&self, info: &ParsedConnectionInfo) -> Result<String, AkitaError> {
        let mut url = String::from("postgresql://");

        // The username and password section
        match (&info.username, &info.password) {
            (Some(u), Some(p)) => write!(&mut url, "{}:{}@", u, p).map_err(|err| AkitaError::DatabaseError(err.to_string()))?,
            (Some(u), None) => write!(&mut url, "{}@", u).map_err(|err| AkitaError::DatabaseError(err.to_string()))?,
            _ => {}
        }

        // Host port section
        if let Some(host) = &info.hostname {
            if let Some(port) = info.port {
                write!(&mut url, "{}:{}", host, port).map_err(|err| AkitaError::DatabaseError(err.to_string()))?;
            } else {
                url.push_str(host);
            }
        }

        // 数据库部分
        if let Some(db) = &info.database {
            write!(&mut url, "/{}", db).map_err(|err| AkitaError::DatabaseError(err.to_string()))?;
        }

        // 参数部分
        if !info.extra_params.is_empty() {
            url.push('?');
            for (i, (k, v)) in info.extra_params.iter().enumerate() {
                if i > 0 {
                    url.push('&');
                }
                write!(&mut url, "{}={}", k, v).map_err(|err| AkitaError::DatabaseError(err.to_string()))?;
            }
        }

        Ok(url)
    }

    fn build_oracle_url(&self, info: &ParsedConnectionInfo) -> Result<String, AkitaError> {
        let mut url = String::new();

        if let (Some(username), Some(password)) = (&info.username, &info.password) {
            url.push_str(&format!("{}/{}", username, password));
        } else if let Some(username) = &info.username {
            url.push_str(username);
        }

        if info.hostname.is_some() {
            url.push('@');
            if let Some(host) = &info.hostname {
                url.push_str(host);
                if let Some(port) = info.port {
                    url.push_str(&format!(":{}", port));
                }
            }

            if let Some(service) = &info.database {
                url.push_str(&format!("/{}", service));
            }
        }

        Ok(url)
    }

    fn build_sqlserver_url(&self, info: &ParsedConnectionInfo) -> Result<String, AkitaError> {
        let mut parts = Vec::new();

        if let Some(host) = &info.hostname {
            let server_str = if let Some(port) = info.port {
                format!("{},{}", host, port)
            } else {
                host.clone()
            };
            parts.push(format!("Server={}", server_str));
        }

        if let Some(db) = &info.database {
            parts.push(format!("Database={}", db));
        }

        if let Some(username) = &info.username {
            parts.push(format!("User Id={}", username));
        }

        if let Some(password) = &info.password {
            parts.push(format!("Password={}", password));
        }

        for (key, value) in &info.extra_params {
            parts.push(format!("{}={}", key, value));
        }

        Ok(parts.join(";"))
    }
}