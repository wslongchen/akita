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


use std::collections::{HashMap, HashSet};
use std::time::Duration;
use getset::{Getters, Setters};
use crate::datasource::{INameConvert, TargetLang};
use regex::Regex;
use akita::prelude::{AkitaConfig, AkitaMapper, DBPool, DBPoolWrapper, SyncPool};
use akita::prelude::comm::DOT;
use crate::config::{DataSourceConfig, GlobalConfig, NamingStrategy, PackageConfig, StrategyConfig, TemplateConfig};
use crate::constant::{CONTROLLER, CONTROLLER_PATH, ENTITY, ENTITY_PATH, MAPPER, MAPPER_PATH, MODULE_NAME, PLACE_HOLDER, REQUEST, REQUEST_PATH, RESPONSE, RESPONSE_PATH, SEPARATOR, SERVICE, SERVICE_IMPL, SERVICE_IMPL_PATH, SERVICE_PATH, SUPER_MAPPER_CLASS, SUPER_SERVICE_CLASS, SUPER_SERVICE_IMPL_CLASS, TMPDIR};
use crate::datasource::{IDbQuery, MySqlKeyWordsHandler, MySqlQuery, MySqlTypeConvert, TableField, TableInfo};

#[derive(Clone, Getters, Setters)]
#[getset(get_mut = "pub", get = "pub", set = "pub")]
pub struct ConfigBuilder {
    /// Data source configuration
    datasource: DBPool,
    data_source_config: DataSourceConfig,
    package_config: PackageConfig,
    /// Database Table Configuration
    strategy_config: StrategyConfig,
    template_config: TemplateConfig,
    /// Globally correlated configuration
    global_config: GlobalConfig,
    /// Database table information
    table_info_list: Vec<TableInfo>,
    /// Path configuration information
    path_info: HashMap<String, String>,
    /// Package configuration details
    package_info: HashMap<String, String>,
    super_entity_class: String,
    super_mapper_class: String,
    /// SERVICE SUPERCLASS DEFINITION
    super_service_class: String,
    super_service_impl_class: String,
    super_controller_class: String,
    /// Database queries
    db_query: MySqlQuery,
}


impl ConfigBuilder {

    pub fn new(package_config: PackageConfig, data_source_config: DataSourceConfig, strategy_config: StrategyConfig,
               template_config: TemplateConfig, global_config: GlobalConfig) -> Self {
        let mut pool = Self::handler_data_source(&data_source_config);
        let db_query = data_source_config.get_db_query();
        let mut cb = Self {
            datasource: pool,
            data_source_config,
            strategy_config,
            package_config,
            template_config,
            global_config,
            table_info_list: vec![],
            package_info: HashMap::new(),
            path_info: HashMap::new(),
            db_query,
            super_entity_class: "".to_string(),
            super_controller_class: "".to_string(),
            super_mapper_class: "".to_string(),
            super_service_impl_class: "".to_string(),
            super_service_class: "".to_string()
        };

        cb.handler_strategy();
        // Package configuration
        cb.handler_package();
        cb
    }

    pub fn handler_data_source(datasource: &DataSourceConfig) -> DBPool {
        let mut cfg = AkitaConfig::new().url(datasource.url()).min_idle(1).connection_timeout(Duration::from_secs(6)).max_size(10);
        cfg = cfg.username(datasource.username().to_string()).password(datasource.password().to_string());
        let pool = DBPoolWrapper::new(cfg).expect("Data source initialization error");
        pool.pool()
    }

    pub fn handler_strategy(&mut self) {
        self.process_types();
        self.get_tables_info();
    }

    pub fn process_types(&mut self) {
        // Processing superServiceClass
        self.super_service_class = self.strategy_config.super_service_class().clone().unwrap_or_else(|| SUPER_SERVICE_CLASS.to_string());

        // Processing superServiceImplClass
        self.super_service_impl_class = self.strategy_config.super_service_impl_class().clone().unwrap_or_else(|| SUPER_SERVICE_IMPL_CLASS.to_string());

        // Processing superMapperClass
        self.super_mapper_class = self.strategy_config.super_mapper_class().clone().unwrap_or_else(|| SUPER_MAPPER_CLASS.to_string());

        // Assigned directly, or None if not present
        self.super_entity_class = self.strategy_config.super_entity_class().clone().unwrap_or_default();
        self.super_controller_class = self.strategy_config.super_controller_class().clone().unwrap_or_default();
    }

    pub fn handler_package(&mut self) {
        let output_dir = self.global_config.output_dir();
        // Packet information
        // let mut package_info: HashMap<String, String> = HashMap::new();
        self.package_info.insert(MODULE_NAME.to_string(), self.package_config.get_module_name());

        self.package_info.insert(ENTITY.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_entity()));
        self.package_info.insert(REQUEST.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_request().map(Clone::clone).unwrap_or_default()));
        self.package_info.insert(RESPONSE.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_response().map(Clone::clone).unwrap_or_default()));

        self.package_info.insert(MAPPER.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_mapper()));
        self.package_info.insert(SERVICE.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_service()));
        self.package_info.insert(SERVICE_IMPL.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_service_impl()));
        self.package_info.insert(CONTROLLER.to_string(), Self::join_package(self.package_config.get_parent(), self.package_config.get_controller()));

        // Custom paths
        self.path_info = self.package_config.get_path_info().clone();
        if self.path_info.is_empty() {
            // Generating path information
            self.set_path(self.template_config.clone().get_entity(), ENTITY_PATH.to_string(), ENTITY);
            self.set_path(&self.template_config.clone().get_response().map(Clone::clone).unwrap_or_default(), RESPONSE_PATH.to_string(), RESPONSE);
            self.set_path(&self.template_config.clone().get_request().map(Clone::clone).unwrap_or_default(), REQUEST_PATH.to_string(), REQUEST);
            self.set_path(self.template_config.clone().get_mapper(),MAPPER_PATH.to_string(), MAPPER);
            self.set_path(self.template_config.clone().get_service(), SERVICE_PATH.to_string(), SERVICE);
            self.set_path(self.template_config.clone().get_service_impl(), SERVICE_IMPL_PATH.to_string(), SERVICE_IMPL);
            self.set_path(self.template_config.clone().get_controller(), CONTROLLER_PATH.to_string(), CONTROLLER);
        }
    }


    fn join_package(parent: String, sub_package: String) -> String {
        if parent.is_empty() {
            return sub_package;
        }
        format!("{}{}{}", parent ,DOT, sub_package)
    }

    fn join_path(mut parent_dir: String, mut package_name: String) -> String {
        if parent_dir.is_empty() {
            parent_dir = TMPDIR.to_string();
        }

        if !parent_dir.ends_with("\\") && !parent_dir.ends_with("/") {
            parent_dir += SEPARATOR;
        }

        package_name = package_name.replace("\\.", SEPARATOR).replace(".", SEPARATOR);
        format!("{}{}", parent_dir, package_name)
    }

    fn set_path(&mut self, template: &str, path: String, module: &str) {
        let output_dir = self.global_config.output_dir();
        if !template.is_empty() {
            self.path_info.insert(path, Self::join_path(output_dir.clone(), self.package_info.get(module).map(Clone::clone).unwrap_or_default()));
        }
    }

    /// Handle table/field names
    fn process_name(name: &str, strategy: NamingStrategy, prefix: &[&str]) -> String {

        let mut remove_prefix = false;
        if !prefix.is_empty() && prefix.len() != 0 {
            remove_prefix = true;
        }
        let property_name;
        if remove_prefix {
            if strategy == NamingStrategy::UnderlineToCamel {
                // Remove prefix, underscore to camel case
                property_name = NamingStrategy::remove_prefix_and_camel(name, prefix);
            } else {
                // Remove prefixes
                property_name = NamingStrategy::remove_prefix(name, prefix);
            }
        } else if strategy == NamingStrategy::UnderlineToCamel {
            // The underscore turns the hump
            property_name = NamingStrategy::underline_to_camel(name);
        } else {
            // Leave out
            property_name = name.to_string();
        }
        property_name
    }

    /// Working with field names
    fn process_name_with_naming(&self, name: &str) -> String {
        Self::process_name(name, self.strategy_config.get_column_naming_strategy().clone(), &self.strategy_config.field_prefix().iter().map(|s| s.as_str()).collect::<Vec<&str>>()[..])
    }

    /// Table name matching
    fn table_name_matches(set_table_name: &str, db_table_name: &str) -> bool {
        set_table_name.eq_ignore_ascii_case(db_table_name)
            // || set_table_name.matches(db_table_name).count() > 0
    }

    fn get_tables_info(&mut self) {
        let regex: Regex = Regex::new("[~!/@#$%^&*()-_=+\\\\|[{}];:\\'\\\",<.>/?]+").unwrap();

        let is_include = !self.strategy_config.include().is_empty() && self.strategy_config.include().len() > 0;
        let is_exclude = !self.strategy_config.exclude().is_empty() && self.strategy_config.exclude().len() > 0;
        let mut table_list = Vec::new();
        // Table information that needs to be generated or excluded in reverse
        let mut include_table_list = Vec::new();
        let mut exclude_table_list = Vec::new();
        // Nonexistent table name
        let mut not_exist_tables = HashSet::new();
        let db = &mut self.datasource.database().expect("Database initialization error");

        let mut table_info;

        let tables_sql = self.db_query.tables_sql();
        let mut results = db.exec_iter(tables_sql, ()).unwrap_or_default();
        for row in results.iter() {
            let table_name = row.get_by_column::<String>(&self.db_query.table_name()).unwrap();
            if !table_name.is_empty() {
                table_info = TableInfo::default();
                table_info.set_name(table_name.to_string());

                let table_comment = row.get_by_column::<String>(&self.db_query.table_comment()).unwrap();
                if *self.strategy_config.skip_view() && table_comment.eq("View") {
                    // Skip view
                    continue;
                }
                table_info.set_comment(table_comment);
                if (is_include) {
                    for include_table in self.strategy_config.include() {
                        // Ignore case equal to or regular true
                        if Self::table_name_matches(include_table, &table_name) {
                            include_table_list.push(table_info.clone());
                        } else {
                            //Filter regular table names

                            if !regex.is_match(include_table) {
                                not_exist_tables.insert(include_table);
                            }
                        }
                    }
                } else if is_exclude {
                    for exclude_table in self.strategy_config.exclude() {
                        // Ignore case equal to or regular true
                        if Self::table_name_matches(exclude_table, &table_name) {
                            exclude_table_list.push(table_info.clone());
                        } else {
                            //Filter regular table names
                            if !regex.is_match(exclude_table) {
                                not_exist_tables.insert(exclude_table);
                            }
                        }
                    }
                }
                table_list.push(table_info.clone());
            } else {
                eprintln!("The current database is empty！！！");
            }
        }

        // Remove the existing table and retrieve the table that does not exist in the configured database
        for table_info in table_list.iter() {
            not_exist_tables.remove(table_info.name());
        }
        if not_exist_tables.len() > 0 {
            eprintln!("Table {:? } does not exist in the database！！！", not_exist_tables);
        }

        // Reverse generated table information is required
        if is_exclude {
            table_list.retain(|item| !exclude_table_list.contains(item));
            include_table_list = table_list.clone();
        }
        if !is_include && !is_exclude {
            include_table_list = table_list.clone();
        }
        // Performance optimization, processing only required table fields
        include_table_list = include_table_list.iter().map(|ti| self.convert_table_fields(ti.clone())).collect();

        /*for table in strategy_config.include.iter() {
            match db.get_table(&TableName::from(table)) {
                Ok(Some(t)) => {
                    include_table_list.push(t);
                }
                _ => {
                    not_exist_tables.insert(table);
                }
            }
        }*/

        self.table_info_list = self.process_table(include_table_list);
    }

    /// Associate field information with table information
    pub fn convert_table_fields(&self, mut table_info: TableInfo) -> TableInfo {
        let mut have_id = false;
        let mut field_list: Vec<TableField> = Vec::new();
        let common_field_list: Vec<TableField> = Vec::new();
        let table_name = table_info.name();
        let table_fields_sql = self.db_query.table_fields_sql();
        let table_fields_sql = table_fields_sql.replace(PLACE_HOLDER, table_name);
        let db = &mut self.datasource.database().expect("Database initialization error");
        let results = db.exec_iter(table_fields_sql, ()).unwrap_or_default();
        for row in results.iter() {
            let mut field = TableField::default();
            let column_name = row.get_by_column::<String>(&self.db_query.field_name()).unwrap();
            // Avoid multiple primary keys; for now, just take the first one where the ID is found and placed at index 0 in the list
            let key = row.get_by_column::<String>(&self.db_query.field_key()).unwrap();
            let key_identity_flag = MySqlQuery::is_key_identity(row.as_object());
            let is_id = !key.is_empty() && key.to_uppercase().eq("PRI");
            // Handling ids
            if is_id && !have_id {
                field.set_key_flag(true);
                field.set_key_identity_flag(key_identity_flag);
                have_id = true;
            } else {
                field.set_key_flag(false);
            }
            // Custom field queries
            let fcs = self.db_query.field_custom();
            if !fcs.is_empty() {
                let mut custom_map = HashMap::new();
                for fc in fcs.iter() {
                    custom_map.insert(fc.to_string(), row.get_by_column::<serde_json::Value>(fc).unwrap_or_default());
                }
                field.set_custom_map(custom_map);
            }

            // Processing other information
            field.set_name(column_name.to_string());
            let mut new_column_name = column_name.to_string();
            if MySqlKeyWordsHandler::is_key_words(&column_name) {
                eprintln!("The current table [{}] existence field [{}] is a database keyword or reserved word!", &table_name, &column_name);
                field.set_key_words(true);
                new_column_name = MySqlKeyWordsHandler::format_column(&column_name);
            }

            field.set_column_name(new_column_name);
            field.set_type(row.get_by_column::<String>(&self.db_query.field_type()).unwrap());

            if let Some(name_convert) = self.strategy_config.name_convert() {
                field.set_property_name(name_convert.property_name_convert(&field));
            } else {
                field.set_property_name_with_strategy(&self.strategy_config, self.process_name_with_naming(field.name()));
            }
            // let property_name = Self::process_name_with_naming(field.name(), self.strategy_config.get_column_naming_strategy().clone(), config.clone());
            // field.set_property_name(property_name.to_string());
            field.set_column_type(MySqlTypeConvert::process_type_convert(field.r#type().to_string(), TargetLang::Rust));
            field.set_comment(row.get_by_column::<String>(&self.db_query.field_comment()).unwrap());
            // Sets the name to be capitalized
            field.set_capital_name(field.get_inner_capital_name());
            // Filling logic judgment
            let table_fill_list = self.strategy_config.table_fill_list().clone();
            if !table_fill_list.is_empty() {
                // Ignore the uppercase field problem
                table_fill_list.iter().find(|tf| tf.field_name().eq_ignore_ascii_case(&field.name()))
                    .map(|tf| tf.field_fill().name()) // 获取匹配的FieldFill的name
                    .map(|fill_name| field.set_fill(fill_name));
            }
            field_list.push(field);
        }
        table_info.set_fields(field_list);
        table_info.set_common_fields(common_field_list);
        table_info
    }

    /// The class name corresponding to the processing table
    fn process_table(&self, mut table_list: Vec<TableInfo>) -> Vec<TableInfo> {
        let table_prefix = self.strategy_config.table_prefix();
        for table_info in table_list.iter_mut() {
            let entity_name = self._capital_first(&Self::process_name(table_info.name(), self.strategy_config.naming().clone(), &table_prefix.iter().map(|s| s.as_str()).collect::<Vec<&str>>()[..]));
            
            if !self.global_config.entity_name().is_empty() {
                table_info.set_convert(true);
                table_info.set_entity_name(self.global_config.entity_name().replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_entity_name(entity_name.to_string());
            }

            if !self.global_config.mapper_name().is_empty() {
                table_info.set_mapper_name(self.global_config.mapper_name().replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_mapper_name(format!("{}{}", entity_name, MAPPER));
            }

            if let Some(request_name) = self.global_config.request_name() {
                table_info.set_request_name(request_name.replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_request_name(format!("{}{}", entity_name, REQUEST));
            }

            if let Some(response_name) = self.global_config.response_name() {
                table_info.set_response_name(response_name.replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_response_name(format!("{}{}", entity_name, REQUEST));
            }

            if !self.global_config.service_name().is_empty() {
                table_info.set_service_name(self.global_config.service_name().replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_service_name(format!("{}{}{}", self._service_prefix(), entity_name, SERVICE));
            }

            if !self.global_config.service_impl_name().is_empty() {
                table_info.set_service_impl_name(self.global_config.service_impl_name().replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_service_impl_name(format!("{}{}", entity_name, SERVICE_IMPL));
            }

            if !self.global_config.controller_name().is_empty() {
                table_info.set_controller_name(self.global_config.controller_name().replace(PLACE_HOLDER, &entity_name));
            } else {
                table_info.set_controller_name(format!("{}{}", entity_name, CONTROLLER));
            }

            table_info.set_entity_path_info();
        }
        table_list
    }

    pub fn _suffix(&self) -> &'static str {
        ".rs"
    }

    pub fn _capital_first(&self, name: &str) -> String {
        match self {
            _ => NamingStrategy::capital_first(name),
        }
    }
    pub fn _name(&self, name: &str) -> String {
        NamingStrategy::camel_to_underline(name)
    }
    pub fn _service_prefix(&self) -> &'static str {
        ""
    }
}



#[test]
fn test_table_name_matches() {
    let result = ConfigBuilder::table_name_matches("sys_user_role", "sys_user");
    assert_eq!(result, true);
}


