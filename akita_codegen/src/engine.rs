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

use std::{env, fs, io};
use std::fs::{File, read_to_string};
use std::path::Path;
use std::process::Command;
use std::io::Write;
use getset::{Getters, Setters};
use tera::{Context, Tera};
use akita::{Akita, AkitaConfig, AkitaError, Local, TableName};
use crate::builder::ConfigBuilder;
use crate::config::Language;
use crate::constant::{CONTROLLER_PATH, DOT_JAVA, ENTITY_PATH, MAPPER_PATH, REQUEST_PATH, RESPONSE_PATH, SEPARATOR, SERVICE_IMPL_PATH, SERVICE_PATH};
use crate::datasource::TableInfo;
use crate::plugins::{load_plugins, Plugin};

/// 模版引擎
#[derive(Clone, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct TemplateEngine {
    tera: Tera,
    builder: ConfigBuilder,
    // plugins: Vec<Box<dyn Plugin>>,
}

impl TemplateEngine {
    pub const DOT_SK: &'static str = ".tmpl";
    pub fn init(builder: ConfigBuilder) -> Self {
        let template_dir = builder.global_config().get_template_dir();
        let tera = Tera::new(&format!("{}{}", template_dir, Self::DOT_SK)).expect("模版初始化异常");
        Self {
            tera,
            builder,
        }
    }

    pub fn open(&self) {
        let folder_path = self.builder.global_config().output_dir();
        let is_open = *self.builder.global_config().open();
        let os = std::env::consts::OS;
        if is_open && !os.is_empty() {
            match os {
                "windows" => {
                    // Windows上使用explorer命令
                    Command::new("explorer")
                        .arg(format!("{}/", folder_path)) // 添加斜杠以确保路径被识别为文件夹
                        .spawn()
                        .expect("命令执行异常");
                }
                "macos" => {
                    // macOS上使用open命令
                    Command::new("open")
                        .arg(folder_path)
                        .spawn()
                        .expect("命令执行异常");
                }
                _ => {
                    // 对于其他操作系统，你可能需要不同的命令或者不支持
                    // 这里可以返回一个错误或者执行默认行为
                    panic!("{}",format!("Unsupported OS: {}", os));
                }
            }
        }
    }

    /// 创建文件夹
    pub fn mkdirs(self) -> Self {
        for (_k, v) in self.builder.path_info().iter() {
            let path = Path::new(v);
            // 尝试创建文件夹，包括所有必需的父目录
            if !path.exists() {
                let result = fs::create_dir_all(path);
                if result.is_ok() {
                    eprintln!("创建目录： [{}]", v);
                }
            } else if path.is_dir() {
                eprintln!("Folder already exists: {}", v);
            } else {
                eprintln!("Path exists but is not a directory: {}", v);
            }
        }
        self
    }

    /// 输出文件
    pub fn batch_output(self) -> Self {
        // 所有的表结构
        let table_info_list = self.builder.table_info_list();
        for table_info in table_info_list.iter() {
            let context = self.get_context(table_info);
            let path_info = self.builder.path_info();
            let template = self.builder.template_config();
            let lang = self.builder.lang();
            let suffix = lang._suffix();
            // entity
            let entity_name = lang._name(table_info.entity_name());
            let entity_path = path_info.get(ENTITY_PATH).map(Clone::clone).unwrap_or_default();
            if !entity_name.is_empty() && !entity_path.is_empty() {
                let output_entity_file = format!("{}{}{}{}", entity_path, SEPARATOR, entity_name, suffix);
                if self.is_create(&output_entity_file) {
                    self.writer(&context, self.template_file_path(template.get_entity().to_string()), output_entity_file);
                }
            }

            // DTO
            // Request
            let mut request_name = lang._name(table_info.request_name());
            if request_name.is_empty() {
                request_name = entity_name.to_string();
            }
            let request_path = path_info.get(REQUEST_PATH).map(Clone::clone).unwrap_or_default();
            if !request_name.is_empty() && !request_path.is_empty() {
                let output_dto_file = format!("{}{}{}{}", request_path, SEPARATOR, request_name, suffix);
                if self.is_create(&output_dto_file) {
                    if let Some(request) = template.get_request() {
                        self.writer(&context, self.template_file_path(request.to_string()), output_dto_file.to_string());
                    }
                }
            }

            // Response
            let mut response_name = lang._name(table_info.response_name());
            if response_name.is_empty() {
                response_name = entity_name.to_string();
            }
            let response_path = path_info.get(RESPONSE_PATH).map(Clone::clone).unwrap_or_default();
            if !response_name.is_empty() && !response_path.is_empty() {
                let output_dto_file = format!("{}{}{}{}", response_path, SEPARATOR, response_name, suffix);
                if self.is_create(&output_dto_file) {
                    if let Some(response) = template.get_response() {
                        self.writer(&context, self.template_file_path(response.to_string()), output_dto_file);
                    }

                }
            }

            // Mapper
            let mapper_name = lang._name(table_info.mapper_name());
            let mapper_path = path_info.get(MAPPER_PATH).map(Clone::clone).unwrap_or_default();
            if !mapper_name.is_empty() && !mapper_path.is_empty() {
                let mapper_file = format!("{}{}{}{}", mapper_path, SEPARATOR, mapper_name, suffix);
                if self.is_create(&mapper_file) {
                    self.writer(&context, self.template_file_path(template.get_mapper().to_string()), mapper_file);
                }
            }

            // IService
            let service_name = lang._name(table_info.service_name());
            let service_path = path_info.get(SERVICE_PATH).map(Clone::clone).unwrap_or_default();
            if !service_name.is_empty() && !service_path.is_empty() {
                let service_file = format!("{}{}{}{}", service_path, SEPARATOR, service_name, suffix);
                if self.is_create(&service_file) {
                    self.writer(&context, self.template_file_path(template.get_service().to_string()), service_file);
                }
            }

            // ServiceImpl
            let service_impl_name = lang._name(table_info.service_impl_name());
            let service_impl_path = path_info.get(SERVICE_IMPL_PATH).map(Clone::clone).unwrap_or_default();
            if !service_impl_name.is_empty() && !service_impl_path.is_empty() {
                let service_impl_file = format!("{}{}{}{}", service_impl_path, SEPARATOR, service_impl_name, suffix);
                if self.is_create(&service_impl_file) {
                    self.writer(&context, self.template_file_path(template.get_service_impl().to_string()), service_impl_file);
                }
            }
            // Controller
            let controller_name = lang._name(table_info.controller_name());
            let controller_path = path_info.get(CONTROLLER_PATH).map(Clone::clone).unwrap_or_default();
            if !controller_name.is_empty() && !controller_path.is_empty() {
                let controller_file = format!("{}{}{}{}", controller_path, SEPARATOR, controller_name, suffix);
                if self.is_create(&controller_file) {
                    self.writer(&context, self.template_file_path(template.get_controller().to_string()), controller_file);
                }
            }

        }
        self
    }

    fn is_create(&self, file_path: &str) -> bool {
        let path = Path::new(file_path);
        // 判断文件是否存在
        let exist = path.exists();
        if !exist {
            // 如果文件不存在，则创建所有必需的父目录
            fs::create_dir_all(path.parent().unwrap()).expect("创建文件夹异常");
            eprintln!("Parent directories created for file: {}", file_path);
        } else {
            eprintln!("File already exists: {}", file_path);
        }
        !exist || *self.builder.global_config().file_override()
    }


    fn writer(&self, context: &Context, template_path: String, output_file: String) {
        if template_path.is_empty() {
            return;
        }
        let template_name = self.template_file_name(template_path.to_string());
        // 获取模版
        // let content = read_to_string(&template_path).unwrap_or_default();
        let result = match self.tera().render(&template_name, context) {
            Ok(res) => res,
            Err(err) => {
                eprintln!("模版:{} 生成异常: {:?}", &template_name, err);
                template_name.to_string()
            }
        };

        // 创建或覆盖文件的路径
        let path = Path::new(&output_file);
        // 打开文件进行写入，如果文件不存在则创建它
        let mut file = File::create(path).expect("生成文件创建异常");
        // 将字符串写入文件
        file.write_all(result.as_bytes()).expect("生成文件创建异常");
        // 关闭文件句柄，这通常会在文件被丢弃时自动发生
        // 但在这里我们显式调用 drop 来确保所有内容都被刷新到磁盘
        drop(file);

        eprintln!("已成功生成模板:{};  文件: {} ",&template_path, &output_file);
    }

    pub fn template_file_path(&self, file_path: String) -> String {
        if file_path.is_empty() || file_path.contains(Self::DOT_SK) {
            return file_path;
        }
        return file_path + Self::DOT_SK;
    }

    pub fn template_file_name(&self, mut file_path: String) -> String {
        if file_path.is_empty() {
            return file_path;
        }

        if !file_path.contains(Self::DOT_SK) {
            file_path += Self::DOT_SK
        }

        // 将字符串路径转为 Path 类型
        if let Some(file_name) = Path::new(&file_path).file_name() {
            file_path = file_name.to_string_lossy().to_string();
        }
        file_path
    }

    fn get_context(&self, table_info: &TableInfo) -> Context {
        let mut context = Context::new();
        // context.insert("config", self.builder.clone());
        context.insert("package", self.builder.package_info());
        let global_config = self.builder.global_config();
        context.insert("author", global_config.author());
        context.insert("date", &Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string());
        context.insert("activeRecord", global_config.active_record());
        context.insert("table", table_info);
        context.insert("entity", table_info.entity_name());
        context.insert("superEntityClass", self.builder.super_entity_class());
        context.insert("restControllerStyle", self.builder.strategy_config().rest_controller_style());
        context.insert("superMapperClassPackage", &self.get_super_class_name(self.builder.super_mapper_class()));
        context.insert("superMapperClass", self.builder.super_mapper_class());
        context.insert("superServiceClassPackage", &self.get_super_class_name(self.builder.super_mapper_class()));
        context.insert("superServiceClass", self.builder.super_service_class());
        context.insert("superServiceImplClassPackage", &self.get_super_class_name(self.builder.super_service_impl_class()));
        context.insert("superServiceImplClass", self.builder.super_service_impl_class());
        context.insert("superControllerClassPackage", &self.get_super_class_name(self.builder.super_controller_class()));
        context.insert("superControllerClass", self.builder.super_controller_class());

        context
    }

    fn get_super_class_name(&self, class_path: &str) -> Option<String> {
        if class_path.is_empty() {
            return None;
        }
        // 查找最后一个 '.' 的位置
        if let Some(dot_pos) = class_path.rfind('.') {
            // 从最后一个 '.' 后的位置截取
            Some(class_path[dot_pos + 1..].to_string())
        } else {
            None
        }
    }

    /*pub fn new(cfg: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize template engine
        let global_cfg = cfg.global.clone();
        let tera = Tera::new(&format!("{}/**/*.tmpl", &global_cfg.template_dir))?;

        // Initialize database
        let ds_cfg = cfg.datasource.clone();
        let akita = Akita::new(AkitaConfig::new(&ds_cfg.url))?;

        // Load plugins
        let plugins = load_plugins(&cfg.plugins)?;

        Ok(Self {
            _cfg: cfg,
            plugins,
            akita,
            tera
        })
    }


    pub fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        let tables = self.load_schema()?;
        // Generate code for each table
        for table in tables {
            if table.is_none() {
                continue;
            }
            let table = table.unwrap();
            self.generate_code(&table)?;
        }
        Ok(())
    }

    pub fn context(&self) -> Context {
        let mut context = Context::new();
        let global = self._cfg.global.clone();
        context.insert("author", global.author);
        context.insert("date", Local::now().naive_local().format("%Y-%m-%d %H:%M:%S").to_string());
        context.insert("package", self._cfg.package.clone());
        context.insert("entity", self._cfg.clone());
    }

    fn generate_code(&self, table: &TableInfo) -> Result<(), Box<dyn std::error::Error>> {
        let mut context = Context::new();
        context.insert("table", &table);
        // Apply plugins to modify the context
        for plugin in self.plugins.iter() {
            plugin.process(&table, &mut context);
        }

        // Render and write to output
        let rendered = self.tera.render("crud.tmpl", &context)?;
        let file_path = format!("{}/{}_crud.rs", self._cfg.global.output_dir, table.name.name());
        fs::create_dir_all(Path::new(&file_path).parent().unwrap())?;
        fs::write(&file_path, rendered)?;

        println!("Generated file: {}", file_path);
        Ok(())
    }

    fn load_schema(&self) -> Result<Vec<Option<TableInfo>>, AkitaError> {
        let tables = self._cfg.strategy.clone().unwrap_or_default().include;
        let mut db = self.akita.acquire()?;
        if tables.is_empty() {
            return Err(AkitaError::MissingTable("必须指定表名".to_string()))
        }
        let mut tbs = Vec::new();
        for tbn in tables.iter() {
            let tb = db.get_table(&TableName::from(tbn.as_str()))?;
            tbs.push(tb);
        }

        Ok(tbs)
    }*/
}


#[test]
fn test_os() {
    println!("当前系统类型是: {}", std::env::consts::OS);
}