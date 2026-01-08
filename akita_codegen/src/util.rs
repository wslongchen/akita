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

/// Whether the name should be uppercase
const CAPITAL_MODE: Lazy<Regex> = Lazy::new(|| {
    Regex::new("[~!/@#$%^&*()-_=+\\\\|[{}];:\\'\\\",<.>/?]+").unwrap()
});

pub fn is_uppercase_naming(s: &str) -> bool {
    s.chars().all(|c| c.is_uppercase())
}

/**
 * Whether the name should be uppercase
 *
 * @param word String to be evaluated
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

/// Contains uppercase letters
pub fn contains_upper_case(word: &str) -> bool {
    for c in word.chars() {
        if c.is_uppercase() {
            return true;
        }
    }
    false
}

pub fn is_camel_case_with_underscores(s: &str) -> bool {
    // The first letter of a camel case name is capitalized, 
    // and the rest of the name may contain capital letters and underscores, 
    // but the underscores must be followed by a letter or number
    if s.is_empty() || !s.chars().next().unwrap().is_uppercase() {
        return false;
    }

    let mut prev_was_underscore = false;
    for c in s.chars().skip(1) {
        if c.is_uppercase() {
            // Uppercase letters cannot be preceded by an underscore (unless it is the first character in the string)
            if prev_was_underscore {
                return false;
            }
        } else if c == '_' {
            // The underscore must be followed by a letter or number
            if prev_was_underscore || !s.chars().nth(s.chars().position(|x| x == c).unwrap() - 1).unwrap().is_alphanumeric()
                || !s.chars().nth(s.chars().position(|x| x == c).unwrap() + 1).unwrap().is_alphanumeric() {
                return false;
            }
            prev_was_underscore = true;
        } else if !c.is_alphanumeric() {
            // Non-capital letters and non-underscore characters are not allowed
            return false;
        } else {
            prev_was_underscore = false;
        }
    }

    true
}