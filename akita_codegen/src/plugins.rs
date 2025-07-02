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

use tera::Context;
use akita::TableInfo;

pub trait Plugin: Send + Sync {
    fn process(&self, table: &TableInfo, context: &mut Context);
}

pub fn load_plugins(plugin_names: &[String]) -> Result<Vec<Box<dyn Plugin>>, Box<dyn std::error::Error>> {
    let mut plugins: Vec<Box<dyn Plugin>> = Vec::new();

    for name in plugin_names {
        match name.as_str() {
            "log_plugin" => plugins.push(Box::new(LogPlugin {})),
            "additional_fields_plugin" => plugins.push(Box::new(AdditionalFieldsPlugin {})),
            _ => println!("Warning: Unknown plugin {}", name),
        }
    }

    Ok(plugins)
}

// Example Plugin 1: Log table information
pub struct LogPlugin;

impl Plugin for LogPlugin {
    fn process(&self, table: &TableInfo, _: &mut Context) {
        println!("Processing table: {}", table.name.name());
    }
}

// Example Plugin 2: Add additional metadata
pub struct AdditionalFieldsPlugin;

impl Plugin for AdditionalFieldsPlugin {
    fn process(&self, table: &TableInfo, context: &mut Context) {
        context.insert("additional_metadata", &format!("Metadata for table {}", table.name.name()));
    }
}