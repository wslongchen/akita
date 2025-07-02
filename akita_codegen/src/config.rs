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
use serde::{Deserialize, Serialize};
use akita::comm::{DOT, EMPTY};
use crate::builder::ConfigBuilder;
use crate::constant::{TEMPLATE_CONTROLLER, TEMPLATE_DEFAULT, TEMPLATE_ENTITY_JAVA, TEMPLATE_MAPPER, TEMPLATE_SERVICE, TEMPLATE_SERVICE_IMPL, UNDERLINE};
use crate::datasource::{DbType, MySqlNameConvert, MySqlQuery, NamingConvert};
use crate::engine::TemplateEngine;
use crate::util::{is_camel_case_with_underscores, is_capital_mode, is_uppercase_naming};


#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct AutoGenerator {
    lang: Option<Language>,
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
            lang: None,
            global: GlobalConfig::default(),
            package: PackageConfig::default(),
            strategy: StrategyConfig::default(),
            datasource: DataSourceConfig::default(),
            template: TemplateConfig::default(),
            plugins: vec![],
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Serialize, PartialEq, Ord, PartialOrd, Eq)]
pub enum Language {
    Java,
    Rust,
    NodeJs,
}

impl Default for Language {
    fn default() -> Self {
        Self::Java
    }
}

impl Language {
    pub fn _suffix(&self) -> &'static str {
        match self {
            Language::Java => ".java",
            Language::Rust => ".rs",
            Language::NodeJs => ".js",
        }
    }

    pub fn _capital_first(&self, name: &str) -> String {
        match self {
            _ => NamingStrategy::capital_first(name),
            // Language::Rust | Language::NodeJs => name.to_string(),
        }
    }
    pub fn _name(&self, name: &str) -> String {
        match self {
            Language::Rust | Language::NodeJs => NamingStrategy::camel_to_underline(name),
            _ => name.to_string(),
        }
    }
    pub fn _service_prefix(&self) -> &'static str {
        match self {
            Language::Rust | Language::NodeJs => "",
            _ => "I",
        }
    }
}

impl From<String> for Language {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Java" => Self::Java,
            "Rust" => Self::Rust,
            "NodeJs" => Self::NodeJs,
            _ => Self::Java,
        }
    }
}

impl From<&str> for Language {
    fn from(value: &str) -> Self {
        match value {
            "Java" => Self::Java,
            "Rust" => Self::Rust,
            "NodeJs" => Self::NodeJs,
            _ => Self::Java,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct StrategyConfig {
    /// 全局大写命名
    capital_mode: bool,
    /// 表名生成策略
    naming: NamingStrategy,
    /// 数据库表字段映射到实体的命名策略
    /// 未指定按照 naming 执行
    column_naming: Option<NamingStrategy>,
    /// Boolean类型字段是否移除is前缀处理
    entity_boolean_column_remove_is_prefix: bool,
    ///
    rest_controller_style: bool,
    /// 是否生成实体时，生成字段注解
    entity_table_field_annotation_enable: bool,
    /// 表前缀
    table_prefix: Vec<String>,
    field_prefix: Vec<String>,
    /// 需要处理的表名
    include: Vec<String>,
    /// 需要排除的表名
    exclude: Vec<String>,
    /// 是否跳过视图
    skip_view: bool,
    /// 表填充字段
    table_fill_list: Vec<TableFill>,
    /// 名称转换
    name_convert: Option<NamingConvert>,
    /// 自定义继承的 Mapper 类全称，带包名
    super_mapper_class: Option<String>,
    /// 自定义继承的 Service 类全称，带包名
    super_service_class: Option<String>,
    super_entity_class: Option<String>,
    /// 自定义继承的 ServiceImpl 类全称，带包名
    super_service_impl_class: Option<String>,
    /// 自定义继承的 Controller 类全称，带包名
    super_controller_class: Option<String>,
}

impl StrategyConfig {

    ///
    /// 大写命名、字段符合大写字母数字下划线命名
    ///
    /// @param word 待判断字符串
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
    /// 字段名称
    field_name: String,
    /// 忽略类型
    field_fill: FieldFill,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldFill {
    /// 默认不处理
    Default,
    /// 插入时填充字段
    Insert,
    /// 更新时填充字段
    Update,
    /// 插入和更新时填充字段
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
            naming: NamingStrategy::NoChange,
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


/// 命名策略
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, Serialize)]
pub enum NamingStrategy {
    /// 下划线转驼峰命名
    UnderlineToCamel,
    /// 驼峰转下划线命名
    CamelToUnderline,
    /// 不做任何改变，原样输出
    NoChange
}


impl NamingStrategy {


    /// 去掉下划线前缀且将后半部分转成驼峰格式
    pub fn remove_prefix_and_camel(name: &str, table_prefix: &[&str]) -> String {

        Self::underline_to_camel(&Self::remove_prefix(name, table_prefix))
    }

    /// 去掉指定的前缀
    pub fn remove_prefix(name: &str, prefixes: &[&str]) -> String {
        if name.is_empty() {
            return String::new();
        }

        // 将前缀集合转换为HashSet，以便快速查找
        let prefix_set: HashSet<String> = prefixes.iter().map(ToString::to_string).collect();

        // 遍历前缀集合，查找匹配的前缀
        for prefix in &prefix_set {
            if name.starts_with(prefix) {
                // 如果找到匹配的前缀，则返回截取后的字符串
                return (&name[prefix.len()..]).to_string();
            }
        }

        // 如果没有找到匹配的前缀，则返回原始字符串
        name.to_string()
    }




    pub fn capital_first(name: &str) -> String {
        if !name.is_empty() {
            return name[..1].to_uppercase() + &name[1..name.len()];
        }
        EMPTY.to_string()
    }

    pub fn underline_to_camel(name: &str) -> String {
        // 快速检查
        if name.is_empty() {
            // 没必要转换
            return EMPTY.to_string();
        }
        let mut temp_name = name.to_string();
        // 大写数字下划线组成转为小写 , 允许混合模式转为小写
        if is_uppercase_naming(&name) || is_camel_case_with_underscores(&name) {
            temp_name = name.to_lowercase();
        }
        // 用下划线将原始字符串分割
        let camels = temp_name.split(UNDERLINE);
        // 跳过原始字符串中开头、结尾的下换线或双重下划线
        let mut result = String::new();
        // 处理真正的驼峰片段
        camels.filter(|v| !v.is_empty()).for_each(|v| {
            if result.len() == 0 {
                // 第一个驼峰片段，全部字母都小写
                result.push_str(v);
            } else {
                // 其他的驼峰片段，首字母大写
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
                // 如果不是第一个字符，前面加下划线
                if i != 0 {
                    result.push('_');
                }
                // 转小写
                result.push(c.to_ascii_lowercase());
            } else {
                result.push(c);
            }
        }
        result
    }
}

/// 全局配置
#[derive(Debug, Clone, serde::Deserialize, Serialize, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct GlobalConfig {
    /// 输出目录
    output_dir: String,
    /// 模版目录
    template_dir: String,
    /// 是否覆盖文件
    file_override: bool,
    /// 开启 activeRecord 模式
    active_record: bool,
    open: bool,
    /// 作者
    author: String,
    /// 各层文件名称方式，例如： %sAction 生成 UserAction %s 为占位符
    service_name: String,
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
            entity_name: "".to_string(),
            service_impl_name: "".to_string(),
            mapper_name: "".to_string(),
            controller_name: "".to_string(),
            // log_level: Level::Info,
        }
    }
}

/// 模板路径配置项
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
            entity: TEMPLATE_ENTITY_JAVA.to_string(),
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
    /// 父包名。如果为空，将下面子包名必须写全部， 否则就只需写子包名
    parent: String,
    /// 父包模块名
    module_name: String,
    /// Entity包名
    entity: String,
    /// Service包名
    service: String,
    /// Request包名
    request: Option<String>,
    /// Response包名
    response: Option<String>,
    /// Service Impl包名
    service_impl: String,
    /// Mapper包名
    mapper: String,
    /// Controller包名
    controller: String,
    /// 路径配置信息
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

fn interactive_help() -> AutoGenerator {

    let language: String = Input::new()
        .with_prompt("Enter your Develop Language (like 'Rust')\n")
        .default("".into())
        .interact_text()
        .unwrap();
    let language: Language = Language::from(language);
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
        password: "".to_string(),
        username: "".to_string(),
    };
    let mut strategy = StrategyConfig::default();
    strategy.include = tables.split(",").map(|v| v.to_string()).collect();
    AutoGenerator {
        template: TemplateConfig::default(),
        lang: Some(language),
        global,
        package: PackageConfig::default(),
        datasource,
        strategy: strategy.into(),
        plugins: vec![],
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
            .with_prompt("Would you like to set up the configuration interactively with language?\n")
            .interact()
            .unwrap()
        {
            // 获取语言选择
            let language: String = Input::new()
                .with_prompt("Please choose a language \n\
                (1: Rust \n\
                (2: Java \n\
                ")
                .default("Rust".into())
                .interact_text()
                .unwrap();
            let trimmed = language.trim();
            let language = match trimmed {
                "1" | "rust" | "Rust" | "" => "Rust".to_string(), // 默认rust
                "2" | "java" | "Java" => "Java".to_string(),
                _ => {
                    eprintln!("Invalid language choice, defaulting to Rust.\n");
                    "Rust".to_string()
                }
            };
            // 显示选择的语言
            println!("Selected language: {}", language);
            let language: Language = Language::from(language);
            let config = Self::create_with_language(language);
            config.save_config(config_path);
            config
        } else {
            process::exit(1);
        }
    }

    fn create_with_language(language: Language) -> Self {
        Self::default()
    }

    /// Validate configuration fields
    fn validate_configuration_fields(self) -> Self {
        let ds_cfg = self.datasource.clone();
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

        let st_cfg = self.strategy.clone();
        if st_cfg.include.is_empty() {
            eprintln!(
                "Error: 'StrategyConfig - tableNames' is missing or empty.\n\n\
                    Example configuration:\n\
                    \t- include: \"table_a,table_b\"\n\
                    \t- exclude: \"table_c\""
            );
            process::exit(1);
        }
        self
    }

    /// 生成代码
    pub fn execute(mut self) {
        // self.init_logger();
        eprintln!("==========================准备生成文件...==========================");
        // 初始化配置
        let builder = ConfigBuilder::new(self.lang.unwrap_or_default(), self.package, self.datasource, self.strategy, self.template, self.global);
        let engine = TemplateEngine::init(builder);
        // 模板引擎初始化执行文件输出
        engine.mkdirs().batch_output().open();
        eprintln!("==========================文件生成完成！！！==========================");
    }


    /*/// Load configuration from file or start interactive setup
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
                    if Confirm::new()
                        .with_prompt("Would you like to set up the configuration interactively?\n")
                        .interact()
                        .unwrap()
                    {
                        let config = interactive_help();
                        config.save_config(config_path);
                        config
                    } else {
                        process::exit(1);
                    }
                }
            },
            Err(_) => {
                eprintln!("Error: Configuration file '{}' not found.\n", config_path);
                if Confirm::new()
                    .with_prompt("Would you like to set up the configuration interactively?\n")
                    .interact()
                    .unwrap()
                {
                    let config = interactive_help();
                    config.save_config(config_path);
                    config
                } else {
                    process::exit(1);
                }
            }
        };

        // Validate configuration fields
        let lang = cfg.lang.as_ref();
        if lang.is_none() {
            // 获取语言选择
            let language: String = Input::new()
                .with_prompt("Please choose a language \n\
                (1: Rust \n\
                (2: Java \n\
                ")
                .default("Rust".into())
                .interact_text()
                .unwrap();
            let trimmed = language.trim();
            let language = match trimmed {
                "1" | "rust" | "Rust" | "" => "Rust".to_string(), // 默认rust
                "2" | "java" | "Java" => "Java".to_string(),
                _ => {
                    eprintln!("Invalid language choice, defaulting to Rust.\n");
                    "Rust".to_string()
                }
            };
            // 显示选择的语言
            println!("Selected language: {}", language);
            let language: Language = Language::from(language);
            cfg.lang = Some(language);
        }
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
        let mut st_cfg = cfg.strategy.unwrap_or_default();
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
        cfg
    }*/


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

}


/// 提示用户输入配置文件路径或使用默认路径
pub fn prompt_for_config_path_or_default(prompt: &str, default_value: &str) -> Result<String, Box<dyn std::error::Error>> {
    print!("{}", prompt);
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let trimmed = input.trim();

    if trimmed.is_empty() {
        Ok(default_value.to_string()) // 默认
    } else {
        Ok(trimmed.to_string()) // 用户输入
    }
}