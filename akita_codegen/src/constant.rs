/*
 *
 *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *
 *   Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *   Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *   Author: SnackCloud
 *
 */



pub const TEMPLATE_DEFAULT: &str = "./templates/**/*";
pub const TEMPLATE_ENTITY_JAVA: &str= "./templates/entity.java";
pub const TEMPLATE_ENTITY_KT: &str = "./templates/entity.kt";
pub const TEMPLATE_ENTITY_RS: &str = "./templates/entity.rs";
pub const TEMPLATE_MAPPER: &str = "./templates/mapper.java";
pub const TEMPLATE_MAPPER_RS: &str = "./templates/mapper.rs";
pub const TEMPLATE_XML: &str = "./templates/mapper.xml";
pub const TEMPLATE_SERVICE: &str = "./templates/service.java";
pub const TEMPLATE_SERVICE_RS: &str = "./templates/service.rs";
pub const TEMPLATE_SERVICE_IMPL: &str = "./templates/serviceImpl.java";
pub const TEMPLATE_SERVICE_IMPL_RS: &str = "./templates/serviceImpl.rs";
pub const TEMPLATE_CONTROLLER: &str = "./templates/controller.java";
pub const TEMPLATE_CONTROLLER_RS: &str = "./templates/controller.rs";


pub const MODULE_NAME: &str = "ModuleName";
pub const ENTITY: &str = "Entity";
pub const REQUEST: &str = "Request";
pub const RESPONSE: &str = "Response";
pub const SERVICE: &str = "Service";
pub const SERVICE_IMPL: &str = "ServiceImpl";
pub const MAPPER: &str = "Mapper";
pub const XML: &str = "Xml";
pub const CONTROLLER: &str = "Controller";
pub const SEPARATOR: &str = "/";
pub const TMPDIR: &str = "./temp";
pub const BACK_SLASH: &str = "\\";


pub const ENTITY_PATH: &str = "entity_path";
pub const REQUEST_PATH: &str = "request_path";
pub const RESPONSE_PATH: &str = "response_path";
pub const SERVICE_PATH: &str = "service_path";
pub const SERVICE_IMPL_PATH: &str = "service_impl_path";
pub const MAPPER_PATH: &str = "mapper_path";
pub const XML_PATH: &str = "xml_path";
pub const CONTROLLER_PATH: &str = "controller_path";

pub const UNDERLINE: &str = "_";
pub const PLACE_HOLDER: &str = "%s";

pub const DOT_JAVA: &str = ".java";
pub const SUPER_MAPPER_CLASS: &str = "com.BaseMapper";
pub const SUPER_SERVICE_CLASS: &str = "com.IService";
pub const SUPER_SERVICE_IMPL_CLASS: &str = "com.ServiceImpl";