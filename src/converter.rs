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

/// 字段转换器
#[allow(unused)]
pub trait Converter<T>: Send + Sync {
    fn convert(data: &T) -> T;
    fn revert(data: &T) -> T;
}

// 对 Option<T> 的支持
impl<T, C> Converter<Option<T>> for C
where
    C: Converter<T>,
{
    fn convert(data: &Option<T>) -> Option<T> {
        data.as_ref().map(C::convert)
    }

    fn revert(data: &Option<T>) -> Option<T> {
        data.as_ref().map(C::revert)
    }
}

pub struct UpperCaseConverter;

impl Converter<String> for UpperCaseConverter {

    fn convert(data: &String) -> String {
        data.to_uppercase()
    }

    fn revert(data: &String) -> String {
        data.to_lowercase()
    }
}



#[cfg(test)]
#[allow(unused)]
mod test {
    use crate::{AkitaMapper, Entity, BaseMapper, self as akita, ToValue, IdentifierGenerator};
    use crate::key::SnowflakeGenerator;

    #[test]
    fn test_converter() {
        let user = SystemUsers {
            id: None,
            username: "Longchen".to_string(),
            age: 0,
        };

        println!("ddddd{:?}", user.to_value());
    }

    #[test]
    fn test_id_generater(){
        let generator = SnowflakeGenerator::new();

        let id_as_u64: u64 = generator.next_id();
        // 4194512547840
        println!("u64 ID: {}", id_as_u64);
    }

    #[derive(Debug, Entity, Clone)]
    #[table(name = "t_system_user")]
    struct SystemUsers {
        id: Option<i32>,
        #[id(name = "ffff", id_type = "none", converter="akita::UpperCaseConverter")]
        username: String,
        #[field(name = "ssss")]
        age: i32,
    }
}
