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
use akita_core::{SchemaContent, TableInfo, TableName};
use crate::driver::auth::{DataBaseUser};
use crate::driver::{GrantUserPrivilege, Role, UserInfo};
use crate::errors::Result;

pub trait DbManager {
    fn get_table(&self, table_name: &TableName) -> Result<Option<TableInfo>>;

    fn exist_table(&self, table_name: &TableName) -> Result<bool>;

    fn get_grouped_tables(&self) -> Result<Vec<SchemaContent>>;

    fn get_all_tables(&self, schema: &str) -> Result<Vec<TableInfo>>;

    fn get_table_names(&self, schema: &str) -> Result<Vec<TableName>>;
    
    fn get_users(&self) -> Result<Vec<DataBaseUser>>;

    fn exist_user(&self, user: &UserInfo) -> Result<bool>;

    fn get_user_detail(&self, username: &str) -> Result<Vec<DataBaseUser>>;

    fn get_roles(&self, username: &str) -> Result<Vec<Role>>;

    fn create_user(&self, user: &UserInfo) -> Result<()>;

    fn drop_user(&self, user: &UserInfo) -> Result<()>;

    fn update_user_password(&self, user: &UserInfo) -> Result<()>;

    fn lock_user(&self, user: &UserInfo) -> Result<()>;

    fn unlock_user(&self, user: &UserInfo) -> Result<()>;

    fn expire_user_password(&self, user: &UserInfo) -> Result<()>;

    fn grant_privileges(&self, user: &GrantUserPrivilege) -> Result<()>;

    fn revoke_privileges(&self, user: &GrantUserPrivilege) -> Result<()>;

    fn flush_privileges(&self) -> Result<()>;
}
