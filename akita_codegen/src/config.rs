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
use std::{fs, process};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::OnceLock;
use dialoguer::{Input, Confirm};
use getset::{Getters, Setters};
use regex::Regex;
use serde::{Deserialize, Serialize};
use akita::prelude::comm::{DOT, EMPTY};
use crate::builder::ConfigBuilder;
use crate::constant::{TEMPLATE_CONTROLLER, TEMPLATE_DEFAULT, TEMPLATE_ENTITY, TEMPLATE_MAPPER, TEMPLATE_SERVICE, TEMPLATE_SERVICE_IMPL, UNDERLINE};
use crate::datasource::{DbType, MySqlNameConvert, MySqlQuery, NamingConvert};
use crate::engine::TemplateEngine;
use crate::util::{is_camel_case_with_underscores, is_capital_mode, is_uppercase_naming};


#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct AutoGenerator {
    global: GlobalConfig,
    package: PackageConfig,
    strategy: StrategyConfig,
    datasource: DataSourceConfig,
    template: TemplateConfig,
    plugins: Vec<String>,
}

impl Default for AutoGenerator {
    fn default() -> Self {
        Self {
            global: GlobalConfig::default(),
            package: PackageConfig::default(),
            strategy: StrategyConfig::default(),
            datasource: DataSourceConfig::default(),
            template: TemplateConfig::default(),
            plugins: vec![],
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct StrategyConfig {
    /// Global uppercase naming
    capital_mode: bool,
    /// Table name generation strategy
    naming: NamingStrategy,
    /// Naming policy for mapping database table fields to entities
    /// Execution is not specified to be named
    column_naming: Option<NamingStrategy>,
    /// Whether or not the is prefix should be removed for Boolean fields
    entity_boolean_column_remove_is_prefix: bool,
    ///
    rest_controller_style: bool,
    /// Field annotations are generated when entities are generated or not
    entity_table_field_annotation_enable: bool,
    /// Table prefix
    table_prefix: Vec<String>,
    field_prefix: Vec<String>,
    /// The name of the table to be processed
    include: Vec<String>,
    /// The name of the table to exclude
    exclude: Vec<String>,
    /// Whether or not to skip views
    skip_view: bool,
    /// Table populating fields
    table_fill_list: Vec<TableFill>,
    /// Name conversion
    name_convert: Option<NamingConvert>,
    /// Full name of the custom inherited Mapper class, with package name
    super_mapper_class: Option<String>,
    /// Full name of the custom inherited Service class, with package name
    super_service_class: Option<String>,
    super_entity_class: Option<String>,
    /// Full name of the ServiceImpl class that the custom inherits from, with the package name
    super_service_impl_class: Option<String>,
    /// Full name of the custom inherited Controller class, with package name
    super_controller_class: Option<String>,
}

impl StrategyConfig {

    ///
    /// Uppercase names, fields conform to uppercase alphanumeric names with an underscore
    ///
    /// @param word String to be evaluated
    ///
    pub fn is_capital_mode_naming(&self, word: &str) -> bool {
        self.capital_mode && is_capital_mode(word)
    }

    pub fn get_column_naming_strategy(&self) -> NamingStrategy {
        self.column_naming.clone().unwrap_or(self.naming.clone())
    }

}

#[derive(Debug, Clone, Getters, Setters, Serialize, Deserialize)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct TableFill {
    /// Field name
    field_name: String,
    /// Ignoring types
    field_fill: FieldFill,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldFill {
    /// Not handled by default
    Default,
    /// Fill the field when inserting
    Insert,
    /// Fill in fields when updated
    Update,
    /// Fill in fields when inserting and updating
    InsertUpdate
}

impl FieldFill {
    pub fn name(&self) -> String {
        match self {
            FieldFill::Default => "Default".to_string(),
            FieldFill::Insert => "Insert".to_string(),
            FieldFill::Update => "Update".to_string(),
            FieldFill::InsertUpdate => "InsertUpdate".to_string(),
        }
    }
}



impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            capital_mode: false,
            naming: NamingStrategy::UnderlineToCamel,
            column_naming: Some(NamingStrategy::NoChange),
            entity_boolean_column_remove_is_prefix: false,
            rest_controller_style: false,
            entity_table_field_annotation_enable: false,
            table_prefix: vec![],
            field_prefix: vec![],
            include: vec![],
            exclude: vec![],
            skip_view: false,
            table_fill_list: vec![],
            name_convert: None,
            super_controller_class: None,
            super_mapper_class: None,
            super_service_impl_class: None,
            super_entity_class: None,
            super_service_class: None
        }
    }
}


/// 数据源配置
#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct DataSourceConfig {
    driver_name: String,
    username: String,
    password: String,
    url: String,
    db_type: DbType,
}

impl DataSourceConfig {
    pub fn get_db_query(&self) -> MySqlQuery {
        match self.db_type {
            DbType::Mysql => MySqlQuery {},
            _ => MySqlQuery {},
        }
    }
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            driver_name: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            url: "".to_string(),
            db_type: DbType::Mysql,
        }
    }
}


/// Naming Policy
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, Serialize)]
pub enum NamingStrategy {
    /// The underscore turns the hump name
    UnderlineToCamel,
    /// The camel case is named with an underscore
    CamelToUnderline,
    /// Do not make any changes, output as is
    NoChange
}


impl NamingStrategy {


    /// Remove the underscore prefix and convert the second half to camel case format
    pub fn remove_prefix_and_camel(name: &str, table_prefix: &[&str]) -> String {

        Self::underline_to_camel(&Self::remove_prefix(name, table_prefix))
    }

    /// Removes the specified prefix
    pub fn remove_prefix(name: &str, prefixes: &[&str]) -> String {
        if name.is_empty() {
            return String::new();
        }

        // Convert the set of prefixes to a HashSet for quick lookup
        let prefix_set: HashSet<String> = prefixes.iter().map(ToString::to_string).collect();

        // Iterate over the set of prefixes, looking for a matching prefix
        for prefix in &prefix_set {
            if name.starts_with(prefix) {
                // If a matching prefix is found, the truncated string is returned
                return (&name[prefix.len()..]).to_string();
            }
        }

        // If no matching prefix is found, the original string is returned
        name.to_string()
    }




    pub fn capital_first(name: &str) -> String {
        if !name.is_empty() {
            return name[..1].to_uppercase() + &name[1..name.len()];
        }
        EMPTY.to_string()
    }

    pub fn underline_to_camel(name: &str) -> String {
        // Quick check
        if name.is_empty() {
            // No need to convert
            return EMPTY.to_string();
        }
        let mut temp_name = name.to_string();
        // Uppercase numeric underscore composition is converted to lowercase, allowing blending mode to be converted to lowercase
        if is_uppercase_naming(&name) || is_camel_case_with_underscores(&name) {
            temp_name = name.to_lowercase();
        }
        // Split the original string with underscores
        let camels = temp_name.split(UNDERLINE);
        // Skip the beginning or end swappings or double underscores in the original string
        let mut result = String::new();
        // Deal with real hump segments
        camels.filter(|v| !v.is_empty()).for_each(|v| {
            if result.len() == 0 {
                // The first hump segment, all lowercase letters
                result.push_str(v);
            } else {
                // The other hump segments are capitalized
                result.push_str(&Self::capital_first(v))
            }
        });
        result
    }

    pub fn camel_to_underline(name: &str) -> String {
        if name.is_empty() {
            return String::new();
        }
        let mut result = String::new();
        for (i, c) in name.chars().enumerate() {
            if c.is_uppercase() {
                // If it is not the first character, it is preceded by an underscore
                if i != 0 {
                    result.push('_');
                }
                // Change to lowercase
                result.push(c.to_ascii_lowercase());
            } else {
                result.push(c);
            }
        }
        result
    }
}

/// Global configuration
#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct GlobalConfig {
    /// Output directory
    output_dir: String,
    /// Template Catalog
    template_dir: String,
    /// Whether to overwrite a file
    file_override: bool,
    /// Enable activeRecord mode
    active_record: bool,
    open: bool,
    /// author
    author: String,
    /// Layer filename, for example: %sAction generates UserAction %s as a placeholder
    service_name: String,
    request_name: Option<String>,
    response_name: Option<String>,
    entity_name: String,
    service_impl_name: String,
    mapper_name: String,
    controller_name: String,
    // log_level: Level,
}

impl GlobalConfig {
    pub fn get_template_dir(&self) -> String {
        if self.template_dir.is_empty() {
            TEMPLATE_DEFAULT.to_string()
        } else {
            self.template_dir.to_string()
        }
    }
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            output_dir: "".to_string(),
            template_dir: TEMPLATE_DEFAULT.to_string(),
            file_override: true,
            active_record: false,
            open: false,
            author: "".to_string(),
            service_name: "".to_string(),
            request_name: None,
            response_name: None,
            entity_name: "".to_string(),
            service_impl_name: "".to_string(),
            mapper_name: "".to_string(),
            controller_name: "".to_string(),
            // log_level: Level::Info,
        }
    }
}

/// Template path configuration entry
#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct TemplateConfig {
    entity: String,
    service: String,
    service_impl: String,
    request: Option<String>,
    response: Option<String>,
    controller: String,
    mapper: String,
}

impl TemplateConfig {
    pub fn get_entity(&self) -> &String {
        &self.entity
    }
    pub fn get_service(&self) -> &String {
        &self.service
    }
    pub fn get_service_impl(&self) -> &String {
        &self.service_impl
    }
    pub fn get_request(&self) -> Option<&String> {
        self.request.as_ref()
    }
    pub fn get_response(&self) -> Option<&String> {
        self.response.as_ref()
    }
    pub fn get_controller(&self) -> &String {
        &self.controller
    }
    pub fn get_mapper(&self) -> &String {
        &self.mapper
    }
}

impl Default for TemplateConfig {
    fn default() -> Self {
        Self {
            entity: TEMPLATE_ENTITY.to_string(),
            service: TEMPLATE_SERVICE.to_string(),
            service_impl: TEMPLATE_SERVICE_IMPL.to_string(),
            request: None,
            response: None,
            controller: TEMPLATE_CONTROLLER.to_string(),
            mapper: TEMPLATE_MAPPER.to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct PackageConfig {
    /// Parent package name. If it is empty, the following child package name must be written all, otherwise only the child package name is written
    parent: String,
    /// Parent package module name
    module_name: String,
    /// Entity package name
    entity: String,
    /// Service package name
    service: String,
    /// Request package name
    request: Option<String>,
    /// Response package name
    response: Option<String>,
    /// Service Impl package name
    service_impl: String,
    /// Mapper package name
    mapper: String,
    /// Controller package name
    controller: String,
    /// Path configuration information
    path_info: HashMap<String, String>,
}


impl Default for PackageConfig {
    fn default() -> Self {
        Self {
            parent: "com.snack".to_string(),
            module_name: "".to_string(),
            entity: "entity".to_string(),
            service: "service".to_string(),
            request: Some("request".to_string()),
            response: Some("response".to_string()),
            service_impl: "service.impl".to_string(),
            mapper: "mapper".to_string(),
            controller: "controller".to_string(),
            path_info: HashMap::default(),
        }
    }
}


impl PackageConfig {

    pub fn get_parent(&self) -> String {
        if !self.module_name.is_empty() {
            return format!("{}{}{}", &self.parent, DOT, self.module_name);
        }
        self.parent.to_string()
    }
    pub fn get_module_name(&self) -> String {
        self.module_name.to_string()
    }
    pub fn get_entity(&self) -> String {
        self.entity.to_string()
    }
    pub fn get_response(&self) -> Option<&String> {
        self.response.as_ref()
    }
    pub fn get_request(&self) -> Option<&String> {
        self.request.as_ref()
    }
    pub fn get_service(&self) -> String {
        self.service.to_string()
    }
    pub fn get_service_impl(&self) -> String {
        self.service_impl.to_string()
    }
    pub fn get_mapper(&self) -> String {
        self.mapper.to_string()
    }
    pub fn get_controller(&self) -> String {
        self.controller.to_string()
    }
    pub fn get_path_info(&self) -> HashMap<String, String> {
        self.path_info.clone()
    }
}

impl AutoGenerator {

    /// Load configuration from file or start interactive setup
    pub fn load_or_create_config(config_path: &str) -> AutoGenerator {
        // Attempt to read the configuration file
        let mut cfg = match fs::read_to_string(config_path) {
            Ok(content) => match serde_yaml::from_str::<AutoGenerator>(&content) {
                Ok(config) => {
                    println!("Configuration loaded successfully from '{}'.\n", config_path);
                    config
                }
                Err(err) => {
                    eprintln!("Error: Failed to parse configuration file: {}\n", err);
                    Self::interactive_with_language(config_path)
                }
            },
            Err(_) => {
                eprintln!("Error: Configuration file '{}' not found.\n", config_path);
                Self::interactive_with_language(config_path)
            }
        };

        // Validate configuration fields
        let ds_cfg = cfg.datasource.clone();
        if ds_cfg.url.trim().is_empty() {
            eprintln!(
                "Error: 'datasource - url' is missing or empty.\n\n\
            Example configuration:\n\
            \t- database_url: \"mysql://user:password@localhost/db_name\"\n\
            \t- template_dir: \"./templates\"\n\
            \t- output_dir: \"./output\"\n\
            \t- plugins: [\"plugin1\", \"plugin2\"]"
            );
            process::exit(1);
        }

        // Validate table names
        let mut st_cfg = cfg.strategy;
        if st_cfg.include.is_empty() {
            let tables: String = Input::new()
                .with_prompt("Enter your table_names (like 'table_a,table_b')\n")
                // .default("".into())
                .interact_text()
                .unwrap();
            if !tables.trim().is_empty() {
                st_cfg.include = tables.split(",").map(|v| v.to_string()).collect();
            }
        }

        if st_cfg.include.is_empty() {
            eprintln!(
                "Error: 'StrategyConfig - tableNames' is missing or empty.\n\n\
                Example configuration:\n\
                \t- include: \"table_a,table_b\"\n\
                \t- exclude: \"table_c\""
            );
            process::exit(1);
        }
        cfg.strategy = st_cfg.into();
        cfg.save_config(config_path);
        cfg
    }

    pub fn with_datasource(mut self, datasource: DataSourceConfig) -> Self {
        self.datasource = datasource;
        self
    }

    pub fn with_global(mut self, cfg: GlobalConfig) -> Self {
        self.global = cfg;
        self
    }

    pub fn with_strategy(mut self, strategy: StrategyConfig) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_package(mut self, package: PackageConfig) -> Self {
        self.package = package;
        self
    }

    pub fn with_template(mut self, template: TemplateConfig) -> Self {
        self.template = template;
        self
    }

    fn interactive_with_language(config_path: &str) -> Self {
        if Confirm::new()
            .with_prompt("Would you like to set up the configuration interactively?\n")
            .interact()
            .unwrap()
        {
            let config = Self::interactive_help();
            config.save_config(config_path);
            config
        } else {
            process::exit(1);
        }
    }

    fn create_with_language() -> Self {
        Self::default()
    }

    /// Generating code
    pub fn execute(mut self) {
        // self.init_logger();
        eprintln!("==========================Preparing to generate files...==========================");
        // Initializing the configuration
        let builder = ConfigBuilder::new(self.package, self.datasource, self.strategy, self.template, self.global);
        let engine = TemplateEngine::init(builder);
        // The template engine is initialized to execute file output
        engine.mkdirs().batch_output().open();
        eprintln!("==========================File generation complete！！！==========================");
    }

    /// Save configuration to file
    fn save_config(&self, path: &str) {
        match serde_yaml::to_string(&self) {
            Ok(yaml) => {
                if let Err(err) = fs::write(path, yaml) {
                    eprintln!("Error: Failed to save configuration to '{}': {}\n", path, err);
                } else {
                    println!("Configuration saved to '{}\n'.", path);
                }
            }
            Err(err) => {
                eprintln!("Error: Failed to serialize configuration: {}\n", err);
            }
        }
    }

    fn has_credentials(connection_string: &str) -> bool {
        // Regular matching mysql://user:password@host format
        let re = Regex::new(r"^mysql://[^:]+:[^@]+@").unwrap();
        re.is_match(connection_string)
    }

    fn interactive_help() -> AutoGenerator {
        let tables: String = Input::new()
            .with_prompt("Enter your table_names (like 'table_a,table_b')\n")
            .default("".into())
            .interact_text()
            .unwrap();
        let database_url: String = Input::new()
            .with_prompt("Enter your database URL\n")
            .default("mysql://user:password@localhost/db_name".into())
            .interact_text()
            .unwrap();
        let mut uname = None;
        let mut pwd = None;
        if !Self::has_credentials(&database_url) {
            let username: String = Input::new()
                .with_prompt("Missing database username, Enter...\n")
                .default("username".into())
                .interact_text()
                .unwrap();
            uname = Some(username);
            let password: String = Input::new()
                .with_prompt("Missing database password, Enter...\n")
                .default("password".into())
                .interact_text()
                .unwrap();
            pwd = Some(password);
        }
        Self::validate_url(&database_url);

        let template_dir: String = Input::new()
            .with_prompt("Enter your template directory path\n")
            .default("./templates".into())
            .interact_text()
            .unwrap();

        let output_dir: String = Input::new()
            .with_prompt("Enter your output directory path\n")
            .default("./output".into())
            .interact_text()
            .unwrap();
        let mut global = GlobalConfig::default();
        global.output_dir = output_dir;
        global.template_dir = template_dir;
        let datasource = DataSourceConfig {
            db_type: DbType::Mysql,
            url: database_url,
            driver_name: "".to_string(),
            password: pwd.unwrap_or_default(),
            username: uname.unwrap_or_default(),
        };
        let mut strategy = StrategyConfig::default();
        strategy.include = tables.split(",").map(|v| v.to_string()).collect();
        AutoGenerator {
            template: TemplateConfig::default(),
            global,
            package: PackageConfig::default(),
            datasource,
            strategy: strategy.into(),
            plugins: vec![],
        }
    }

    fn validate_mysql_connection_string(conn_str: &str) -> bool {
        // Build the full validation regex
        let patterns = vec![
            // Mode 1: with username and password, with port, with database
            r"^mysql://[a-zA-Z0-9_.-]+:[^@]+@[a-zA-Z0-9_.-]+:\d+/[a-zA-Z0-9_.-]+(?:\?.*)?$",

            // Mode 2: with username and password, without port, with database
            r"^mysql://[a-zA-Z0-9_.-]+:[^@]+@[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+(?:\?.*)?$",

            // Mode 3: without authentication, with port, with database
            r"^mysql://[a-zA-Z0-9_.-]+:\d+/[a-zA-Z0-9_.-]+(?:\?.*)?$",

            // Mode 4: without authentication, without port, with database
            r"^mysql://[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+(?:\?.*)?$",

            // Pattern 5: Only username, no password
            r"^mysql://[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+(?:/|:\d+/)[a-zA-Z0-9_.-]+(?:\?.*)?$",
        ];
        for pattern in patterns {
            let re = Regex::new(pattern).unwrap();
            if re.is_match(conn_str) {
                return true;
            }
        }

        false
    }

    fn validate_url(conn_str: &str) {
        if conn_str.trim().is_empty() || !Self::validate_mysql_connection_string(conn_str) {
            eprintln!(
                "Error: 'datasource - url' is missing or empty.\n\n\
            Example configuration:\n\
            \t- database_url: \"mysql://user:password@localhost/db_name\"\n\
            \t- template_dir: \"./templates\"\n\
            \t- output_dir: \"./output\"\n\
            \t- plugins: [\"plugin1\", \"plugin2\"]"
            );
            process::exit(1);
        }
    }
}



/// The user is prompted to enter the profile path or use the default path
pub fn prompt_for_config_path_or_default(prompt: &str, default_value: &str) -> Result<String, Box<dyn std::error::Error>> {
    print!("{}", prompt);
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let trimmed = input.trim();

    if trimmed.is_empty() {
        Ok(default_value.to_string()) // default
    } else {
        Ok(trimmed.to_string()) // user input
    }
}