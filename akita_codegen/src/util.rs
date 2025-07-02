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

use regex::Regex;
use once_cell::unsync::Lazy;

/// 是否为大写命名
const CAPITAL_MODE: Lazy<Regex> = Lazy::new(|| {
    Regex::new("[~!/@#$%^&*()-_=+\\\\|[{}];:\\'\\\",<.>/?]+").unwrap()
});

pub fn is_uppercase_naming(s: &str) -> bool {
    s.chars().all(|c| c.is_uppercase())
}

/**
 * 是否为大写命名
 *
 * @param word 待判断字符串
 * @return ignore
 */
pub fn is_capital_mode(word: &str) -> bool {
    !word.is_empty() && CAPITAL_MODE.is_match(word)
}

pub fn remove_is_prefix_if_boolean(name: &str) -> String {
    let re = Regex::new(r"^is([A-Za-z])").unwrap();
    re.replace(name, |caps: &regex::Captures| {
        let first_char = &caps[1];
        first_char.to_uppercase().to_string()
    }).to_string()
}

/// 包含大写字母
pub fn contains_upper_case(word: &str) -> bool {
    for c in word.chars() {
        if c.is_uppercase() {
            return true;
        }
    }
    false
}

pub fn is_camel_case_with_underscores(s: &str) -> bool {
    // 驼峰命名首字母大写，其余部分可能包含大写字母和下划线，但下划线前后必须跟字母或数字
    if s.is_empty() || !s.chars().next().unwrap().is_uppercase() {
        return false;
    }

    let mut prev_was_underscore = false;
    for c in s.chars().skip(1) {
        if c.is_uppercase() {
            // 大写字母前不能是下划线（除非它是字符串的第一个字符）
            if prev_was_underscore {
                return false;
            }
        } else if c == '_' {
            // 下划线前后必须跟字母或数字
            if prev_was_underscore || !s.chars().nth(s.chars().position(|x| x == c).unwrap() - 1).unwrap().is_alphanumeric()
                || !s.chars().nth(s.chars().position(|x| x == c).unwrap() + 1).unwrap().is_alphanumeric() {
                return false;
            }
            prev_was_underscore = true;
        } else if !c.is_alphanumeric() {
            // 非大写字母和非下划线字符不被允许
            return false;
        } else {
            prev_was_underscore = false;
        }
    }

    true
}