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
use akita::prelude::{Akita, AkitaConfig, AkitaError, Local, TableName};
use crate::builder::ConfigBuilder;
use crate::constant::{CONTROLLER_PATH, DOT_JAVA, ENTITY_PATH, MAPPER_PATH, REQUEST_PATH, RESPONSE_PATH, SEPARATOR, SERVICE_IMPL_PATH, SERVICE_PATH};
use crate::datasource::TableInfo;
use crate::plugins::{load_plugins, Plugin};

/// Template engine
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
        let tera = Tera::new(&format!("{}{}", template_dir, Self::DOT_SK)).expect("Template initialization is abnormal");
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
                    // On Windows, use the explorer command
                    Command::new("explorer")
                        .arg(format!("{}/", folder_path)) // A slash is added to ensure that the path is recognized as a folder
                        .spawn()
                        .expect("Command execution exception");
                }
                "macos" => {
                    // On macOS, use the open command
                    Command::new("open")
                        .arg(folder_path)
                        .spawn()
                        .expect("Command execution exception");
                }
                _ => {
                    // For other operating systems, you may need a different command or not support it
                    // You can return an error or perform the default behavior
                    panic!("{}",format!("Unsupported OS: {}", os));
                }
            }
        }
    }

    /// Creating folders
    pub fn mkdirs(self) -> Self {
        for (_k, v) in self.builder.path_info().iter() {
            let path = Path::new(v);
            // Try creating folders, including all the required parent directories
            if !path.exists() {
                let result = fs::create_dir_all(path);
                if result.is_ok() {
                    eprintln!("Creating a directory： [{}]", v);
                }
            } else if path.is_dir() {
                eprintln!("Folder already exists: {}", v);
            } else {
                eprintln!("Path exists but is not a directory: {}", v);
            }
        }
        self
    }

    /// Output file
    pub fn batch_output(self) -> Self {
        // All table structures
        let table_info_list = self.builder.table_info_list();
        for table_info in table_info_list.iter() {
            let context = self.get_context(table_info);
            let path_info = self.builder.path_info();
            let template = self.builder.template_config();
            let suffix = self.builder._suffix();
            // entity
            let entity_name = self.builder._name(table_info.entity_name());
            let entity_path = path_info.get(ENTITY_PATH).map(Clone::clone).unwrap_or_default();
            if !entity_name.is_empty() && !entity_path.is_empty() {
                let output_entity_file = format!("{}{}{}{}", entity_path, SEPARATOR, entity_name, suffix);
                if self.is_create(&output_entity_file) {
                    self.writer(&context, self.template_file_path(template.get_entity().to_string()), output_entity_file);
                }
            }

            // DTO
            // Request
            let mut request_name = self.builder._name(table_info.request_name());
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
            let mut response_name = self.builder._name(table_info.response_name());
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
            let mapper_name = self.builder._name(table_info.mapper_name());
            let mapper_path = path_info.get(MAPPER_PATH).map(Clone::clone).unwrap_or_default();
            if !mapper_name.is_empty() && !mapper_path.is_empty() {
                let mapper_file = format!("{}{}{}{}", mapper_path, SEPARATOR, mapper_name, suffix);
                if self.is_create(&mapper_file) {
                    self.writer(&context, self.template_file_path(template.get_mapper().to_string()), mapper_file);
                }
            }

            // IService
            let service_name = self.builder._name(table_info.service_name());
            let service_path = path_info.get(SERVICE_PATH).map(Clone::clone).unwrap_or_default();
            if !service_name.is_empty() && !service_path.is_empty() {
                let service_file = format!("{}{}{}{}", service_path, SEPARATOR, service_name, suffix);
                if self.is_create(&service_file) {
                    self.writer(&context, self.template_file_path(template.get_service().to_string()), service_file);
                }
            }

            // ServiceImpl
            let service_impl_name = self.builder._name(table_info.service_impl_name());
            let service_impl_path = path_info.get(SERVICE_IMPL_PATH).map(Clone::clone).unwrap_or_default();
            if !service_impl_name.is_empty() && !service_impl_path.is_empty() {
                let service_impl_file = format!("{}{}{}{}", service_impl_path, SEPARATOR, service_impl_name, suffix);
                if self.is_create(&service_impl_file) {
                    self.writer(&context, self.template_file_path(template.get_service_impl().to_string()), service_impl_file);
                }
            }
            // Controller
            let controller_name = self.builder._name(table_info.controller_name());
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
        // Checks if the file exists
        let exist = path.exists();
        if !exist {
            // If the file does not exist, all the required parent directories are created
            fs::create_dir_all(path.parent().unwrap()).expect("Folder creation exception");
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
        // Getting the template
        // let content = read_to_string(&template_path).unwrap_or_default();
        let result = match self.tera().render(&template_name, context) {
            Ok(res) => res,
            Err(err) => {
                eprintln!("Template :{} Generates an exception: {:?}", &template_name, err);
                template_name.to_string()
            }
        };

        // The path to create or overwrite the file
        let path = Path::new(&output_file);
        // Open the file for writing, or create the file if it doesn't exist
        let mut file = File::create(path).expect("Generate file creation exception");
        // Writing strings to a file
        file.write_all(result.as_bytes()).expect("Generate file creation exception");
        // Close the file handle, which usually happens automatically when a file is discarded
        // But here we explicitly call drop to make sure everything is flushed to disk
        drop(file);

        eprintln!("The template has been successfully generated:{};  file: {} ",&template_path, &output_file);
    }

    pub fn template_file_path(&self, file_path: String) -> String {
        if file_path.is_empty() || file_path.contains(Self::DOT_SK) {
            return file_path;
        }
        file_path + Self::DOT_SK
    }

    pub fn template_file_name(&self, mut file_path: String) -> String {
        if file_path.is_empty() {
            return file_path;
        }

        if !file_path.contains(Self::DOT_SK) {
            file_path += Self::DOT_SK
        }

        // Convert a string Path to the Path type
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
        // Find the position of the last '.'
        if let Some(dot_pos) = class_path.rfind('.') {
            // Taken from the position after the last '.'
            Some(class_path[dot_pos + 1..].to_string())
        } else {
            None
        }
    }
}


#[test]
fn test_os() {
    println!("The current system type is: {}", std::env::consts::OS);
}