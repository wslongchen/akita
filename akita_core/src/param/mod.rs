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

mod convert;

pub use convert::*;

use indexmap::IndexMap;
use crate::AkitaValue;

#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    None,
    Positional(Vec<AkitaValue>),
    Named(IndexMap<String, AkitaValue>),
}

impl std::fmt::Display for Params {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Params::Positional(v) => {
                if v.is_empty() {
                    write!(f, "==> [Akita]  Parameters: ")
                } else {
                    let params_str = v.iter()
                        .enumerate()
                        .map(|(i, value)| {
                            let value_str = match value {
                                AkitaValue::Text(s) => {
                                    format!("'{}'", s.replace("'", "''"))
                                }
                                AkitaValue::Char(c) => {
                                    format!("'{}'", c)
                                }
                                AkitaValue::Null => "null".to_string(),
                                AkitaValue::Bool(b) => b.to_string(),
                                AkitaValue::Tinyint(n)=> n.to_string(),
                                AkitaValue::Smallint(n)=> n.to_string(),
                                AkitaValue::Int(n)=> n.to_string(),
                                AkitaValue::Bigint(n) => n.to_string(),
                                AkitaValue::Float(n) => format!("{:.6}", n),
                                AkitaValue::Double(n) => format!("{:.6}", n),
                                AkitaValue::BigDecimal(n) => n.to_string(),
                                AkitaValue::Date(date) => {
                                    format!("DATE '{}'", date.format("%Y-%m-%d"))
                                }
                                AkitaValue::Time(time) => {
                                    format!("TIME '{}'", time.format("%H:%M:%S%.3f"))
                                }
                                AkitaValue::DateTime(datetime) => {
                                    format!("DATETIME '{}'", datetime.format("%Y-%m-%d %H:%M:%S"))
                                }
                                AkitaValue::Timestamp(ts) => {
                                    format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.3f"))
                                }
                                AkitaValue::Json(json) => {
                                    let json_str = serde_json::to_string(json)
                                        .unwrap_or_else(|_| "{}".to_string());
                                    if json_str.len() > 50 {
                                        format!("JSON '{}...'", &json_str[..47])
                                    } else {
                                        format!("JSON '{}'", json_str)
                                    }
                                }
                                AkitaValue::Uuid(uuid) => {
                                    format!("UUID '{}'", uuid)
                                }
                                AkitaValue::Blob(blob) => {
                                    if blob.len() <= 20 {
                                        format!("BLOB({} bytes)", blob.len())
                                    } else {
                                        format!("BLOB({} bytes) ...", blob.len())
                                    }
                                }
                                AkitaValue::RawSql(_sql) => {
                                    format!("[RAW_SQL]")
                                }
                                AkitaValue::Column(col) => {
                                    format!("[COLUMN: {}]", col)
                                }
                                // 复杂类型
                                AkitaValue::Array(arr) => {
                                    format!("ARRAY({} items)", arr.len())
                                }
                                AkitaValue::List(list) => {
                                    if list.is_empty() {
                                        "[]".to_string()
                                    } else if list.len() <= 3 {
                                        format!("{:?}", list)
                                    } else {
                                        format!("[{} items]", list.len())
                                    }
                                }
                                AkitaValue::Object(obj) => {
                                    format!("OBJECT({} fields)", obj.len())
                                }
                                AkitaValue::Wrapper(wrapper) => {
                                    format!("[WRAPPER: {}]", wrapper.to_string())
                                }
                                AkitaValue::Interval(interval) => {
                                    interval.to_string()
                                }
                            };

                            format!("{}: {}", i + 1, value_str)
                        })
                        .collect::<Vec<String>>()
                        .join(", ");

                    write!(f, "==>  Parameters: {}", params_str)
                }
            }
            Params::Named(v) => {
                if v.is_empty() {
                    write!(f, "==>  Parameters: ")
                } else {
                    let params_str = v.iter()
                        .map(|(key, value)| {
                            let value_str = match value {
                                AkitaValue::Text(s) => format!("'{}'", s.replace("'", "''")),
                                AkitaValue::Char(c) => format!("'{}'", c),
                                AkitaValue::Null => "null".to_string(),
                                AkitaValue::Bool(b) => b.to_string(),
                                AkitaValue::Tinyint(n) => n.to_string(),
                                AkitaValue::Smallint(n) => n.to_string(),
                                AkitaValue::Int(n) => n.to_string(),
                                AkitaValue::Bigint(n) => n.to_string(),
                                AkitaValue::Float(n) => format!("{:.6}", n),
                                AkitaValue::Double(n) => format!("{:.6}", n),
                                AkitaValue::BigDecimal(n) => n.to_string(),
                                AkitaValue::Date(date) => {
                                    format!("DATE '{}'", date.format("%Y-%m-%d"))
                                }
                                AkitaValue::Time(time) => {
                                    format!("TIME '{}'", time.format("%H:%M:%S%.3f"))
                                }
                                AkitaValue::DateTime(datetime) => {
                                    format!("DATETIME '{}'", datetime.format("%Y-%m-%d %H:%M:%S"))
                                }
                                AkitaValue::Timestamp(ts) => {
                                    format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.3f"))
                                }
                                AkitaValue::Json(json) => {
                                    let json_str = serde_json::to_string(json)
                                        .unwrap_or_else(|_| "{}".to_string());
                                    if json_str.len() > 50 {
                                        format!("JSON '{}...'", &json_str[..47])
                                    } else {
                                        format!("JSON '{}'", json_str)
                                    }
                                }
                                AkitaValue::Uuid(uuid) => {
                                    format!("UUID '{}'", uuid)
                                }
                                AkitaValue::Blob(blob) => {
                                    if blob.len() <= 20 {
                                        format!("BLOB({} bytes)", blob.len())
                                    } else {
                                        format!("BLOB({} bytes) ...", blob.len())
                                    }
                                }
                                AkitaValue::RawSql(_sql) => format!("[RAW_SQL]"),
                                AkitaValue::Column(col) => format!("[COLUMN: {}]", col),
                                AkitaValue::Array(arr) => format!("ARRAY({} items)", arr.len()),
                                AkitaValue::List(list) => {
                                    if list.is_empty() {
                                        "[]".to_string()
                                    } else if list.len() <= 3 {
                                        format!("{:?}", list)
                                    } else {
                                        format!("[{} items]", list.len())
                                    }
                                }
                                AkitaValue::Object(obj) => format!("OBJECT({} fields)", obj.len()),
                                AkitaValue::Wrapper(wrapper) => {
                                    format!("[WRAPPER: {}]", wrapper.to_string())
                                }
                                AkitaValue::Interval(interval) => interval.to_string(),
                            };
                            format!("{}: {}", key, value_str)
                        })
                        .collect::<Vec<String>>()
                        .join(", ");

                    write!(f, "==>  Parameters: {}", params_str)
                }
            }
            Params::None => {
                write!(f, "==>  Parameters: ")
            }
        }
    }
}

impl IntoIterator for Params {
    type Item = (Option<String>, AkitaValue);
    type IntoIter = ParamsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        ParamsIntoIter::new(self)
    }
}

pub struct ParamsIntoIter {
    params: Params,
    index: usize,
    named_iter: Option<indexmap::map::IntoIter<String, AkitaValue>>,
}

impl ParamsIntoIter {
    fn new(params: Params) -> Self {
        Self {
            params,
            index: 0,
            named_iter: None,
        }
    }
}

impl Iterator for ParamsIntoIter {
    type Item = (Option<String>, AkitaValue);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.params {
            Params::None => None,
            Params::Positional(vec) => {
                if self.index < vec.len() {
                    let value = vec[self.index].clone();
                    self.index += 1;
                    Some((None, value))
                } else {
                    None
                }
            }
            Params::Named(_map) => {
                // If it's the first iteration of the Named type, initialize the iterator of the hashmap
                if self.named_iter.is_none() {
                    // Ownership is required here, so replace the entire map
                    let mut new_params = Params::None;
                    std::mem::swap(&mut self.params, &mut new_params);
                    if let Params::Named(map) = new_params {
                        self.named_iter = Some(map.into_iter());
                    }
                }

                if let Some(ref mut iter) = self.named_iter {
                    iter.next().map(|(key, value)| (Some(key), value))
                } else {
                    None
                }
            }
        }
    }
}

impl<'a> IntoIterator for &'a Params {
    type Item = (Option<String>, &'a AkitaValue);
    type IntoIter = ParamsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ParamsIter::new(self)
    }
}

pub struct ParamsIter<'a> {
    params: &'a Params,
    index: usize,
    named_iter: Option<indexmap::map::Iter<'a, String, AkitaValue>>,
}

impl<'a> ParamsIter<'a> {
    fn new(params: &'a Params) -> Self {
        Self {
            params,
            index: 0,
            named_iter: None,
        }
    }
}

impl<'a> Iterator for ParamsIter<'a> {
    type Item = (Option<String>, &'a AkitaValue);

    fn next(&mut self) -> Option<Self::Item> {
        match self.params {
            Params::None => None,
            Params::Positional(vec) => {
                if self.index < vec.len() {
                    let value = &vec[self.index];
                    self.index += 1;
                    Some((None, value))
                } else {
                    None
                }
            }
            Params::Named(map) => {
                if self.named_iter.is_none() {
                    self.named_iter = Some(map.iter());
                }

                self.named_iter
                    .as_mut()
                    .and_then(|iter| iter.next())
                    .map(|(key, value)| (Some(key.clone()), value))
            }
        }
    }
}

impl<'a> IntoIterator for &'a mut Params {
    type Item = (Option<String>, &'a mut AkitaValue);
    type IntoIter = ParamsIterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ParamsIterMut::new(self)
    }
}

pub struct ParamsIterMut<'a> {
    positional_iter: Option<std::slice::IterMut<'a, AkitaValue>>,
    named_iter: Option<indexmap::map::IterMut<'a, String, AkitaValue>>,
}

impl <'a> ParamsIterMut<'a> {
    fn new(params: &'a mut Params) -> Self {
        match params {
            Params::None => Self {
                positional_iter: None,
                named_iter: None,
            },
            Params::Positional(vec) => Self {
                positional_iter: Some(vec.iter_mut()),
                named_iter: None,
            },
            Params::Named(map) => Self {
                positional_iter: None,
                named_iter: Some(map.iter_mut()),
            },
        }
    }
}
impl<'a> Iterator for ParamsIterMut<'a> {
    type Item = (Option<String>, &'a mut AkitaValue);

    fn next(&mut self) -> Option<Self::Item> {
        // Position parameters are processed first
        if let Some(ref mut iter) = self.positional_iter {
            return iter.next().map(|value| (None, value));
        }

        // Then the named parameters are processed
        if let Some(ref mut iter) = self.named_iter {
            return iter.next().map(|(key, value)| (Some(key.clone()), value));
        }

        None
    }
}

#[allow(mismatched_lifetime_syntaxes)]
impl Params {
    /// Create null arguments (compatible with original nulls)
    pub fn null() -> Self {
        Self::None
    }

    /// Create position parameters (compatible with the original Vector)
    pub fn vector(values: Vec<AkitaValue>) -> Self {
        Self::Positional(values)
    }

    /// Create a named parameter (compatible with the original Custom)
    pub fn custom(pairs: Vec<(String, AkitaValue)>) -> Self {
        let named_params: IndexMap<String, AkitaValue> = pairs.into_iter().collect();
        Self::Named(named_params)
    }

    /// Convert to position parameters (core conversion logic)
    pub fn into_positional(self, sql: &str) -> (String, Vec<AkitaValue>) {
        match self {
            Self::None => (sql.to_string(), vec![]),
            Self::Positional(values) => (sql.to_string(), values),
            Self::Named(named_params) => Self::process_named_params(sql, named_params),
        }
    }

    /// Handle named parameter conversions
    fn process_named_params(sql: &str, named_params: IndexMap<String, AkitaValue>) -> (String, Vec<AkitaValue>) {
        let mut prepared_sql = String::with_capacity(sql.len());
        let mut prepared_params = Vec::new();

        let mut chars = sql.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == ':' {
                // 解析参数名
                let mut param_name = String::new();
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_alphanumeric() || next_ch == '_' {
                        param_name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                if !param_name.is_empty() {
                    if let Some(value) = named_params.get(&param_name) {
                        prepared_params.push(value.clone());
                    } else {
                        prepared_params.push(AkitaValue::Null);
                    }
                    prepared_sql.push('?');
                } else {
                    prepared_sql.push(ch);
                }
            } else {
                prepared_sql.push(ch);
            }
        }

        (prepared_sql, prepared_params)
    }

    pub fn from_map(map: IndexMap<String, AkitaValue>) -> Self {
        Self::Named(map)
    }

    // Maintain compatibility with the original method
    pub fn is_null(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Positional(vec) => vec.len(),
            Self::Named(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the iterator for all key-value pairs (borrowed)
    pub fn iter(&self) -> ParamsIter {
        self.into_iter()
    }

    /// Get a variable iterator for all key-value pairs
    pub fn iter_mut(&mut self) -> ParamsIterMut {
        self.into_iter()
    }

    /// Convert to a key-value pair vector
    pub fn to_vec(&self) -> Vec<(Option<String>, AkitaValue)> {
        self.clone().into_iter().collect()
    }

    /// Convert to key-value pair vector (contains values only)
    pub fn values(&self) -> Vec<AkitaValue> {
        match self {
            Params::None => vec![],
            Params::Positional(vec) => vec.clone(),
            Params::Named(map) => map.values().cloned().collect(),
        }
    }

    /// Get all parameter names (Named type)
    pub fn keys(&self) -> Vec<Option<String>> {
        match self {
            Params::None => vec![],
            Params::Positional(vec) => vec![None; vec.len()],
            Params::Named(map) => map.keys().map(|k| Some(k.clone())).collect(),
        }
    }

    /// Get parameter values by name
    pub fn get(&self, key: &str) -> Option<&AkitaValue> {
        match self {
            Self::Named(map) => map.get(key),
            _ => None,
        }
    }

    /// Get variable parameter values by name
    pub fn get_mut(&mut self, key: &str) -> Option<&mut AkitaValue> {
        match self {
            Self::Named(map) => map.get_mut(key),
            _ => None,
        }
    }

    /// Insert or update named parameters (keep order)
    pub fn insert(&mut self, key: String, value: AkitaValue) -> Option<AkitaValue> {
        match self {
            Self::Named(map) => map.insert(key, value),
            _ => {
                // If it is not of type Named, convert to Named
                *self = Self::custom(vec![(key, value)]);
                None
            }
        }
    }

    /// Remove named parameters (keep the remaining order)
    pub fn remove(&mut self, key: &str) -> Option<AkitaValue> {
        match self {
            Self::Named(map) => map.shift_remove(key),
            _ => None,
        }
    }

    /// Check if the specified parameters are included
    pub fn contains_key(&self, key: &str) -> bool {
        match self {
            Self::Named(map) => map.contains_key(key),
            _ => false,
        }
    }

    /// Convert to Map (if Named type)
    pub fn to_map(&self) -> Option<&IndexMap<String, AkitaValue>> {
        match self {
            Self::Named(map) => Some(map),
            _ => None,
        }
    }

    /// Get the position of the parameter in the order (Named type)
    pub fn index_of(&self, key: &str) -> Option<usize> {
        match self {
            Self::Named(map) => map.get_index_of(key),
            _ => None,
        }
    }

    /// Get key-value pairs for the specified location (Named type, keep order)
    pub fn get_index(&self, index: usize) -> Option<(&String, &AkitaValue)> {
        match self {
            Self::Named(map) => map.get_index(index),
            _ => None,
        }
    }

    /// Get a variable key-value pair (Named type) for a specified location
    pub fn get_index_mut(&mut self, index: usize) -> Option<(&String, &mut AkitaValue)> {
        match self {
            Self::Named(map) => map.get_index_mut(index),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexmap_preserves_order() {
        let params = Params::custom(vec![
            ("z".to_string(), AkitaValue::Int(3)),
            ("a".to_string(), AkitaValue::Int(1)),
            ("m".to_string(), AkitaValue::Int(2)),
        ]);

        // Using IndexMap should keep the insertion order
        let keys: Vec<_> = params.keys().into_iter().flatten().collect();
        assert_eq!(keys, vec!["z", "a", "m"]);

        let values: Vec<_> = params.values();
        assert_eq!(
            values,
            vec![
                AkitaValue::Int(3),
                AkitaValue::Int(1),
                AkitaValue::Int(2),
            ]
        );
    }

    #[test]
    fn test_iteration_order() {
        let params = Params::custom(vec![
            ("first".to_string(), AkitaValue::Text("hello".to_string())),
            ("second".to_string(), AkitaValue::Text("world".to_string())),
            ("third".to_string(), AkitaValue::Text("!".to_string())),
        ]);

        // The iteration should keep the insertion order
        let mut iter = params.iter();
        assert_eq!(
            iter.next(),
            Some((Some("first".to_string()), &AkitaValue::Text("hello".to_string())))
        );
        assert_eq!(
            iter.next(),
            Some((Some("second".to_string()), &AkitaValue::Text("world".to_string())))
        );
        assert_eq!(
            iter.next(),
            Some((Some("third".to_string()), &AkitaValue::Text("!".to_string())))
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_mut_iteration_preserves_order() {
        let mut params = Params::custom(vec![
            ("a".to_string(), AkitaValue::Int(1)),
            ("b".to_string(), AkitaValue::Int(2)),
            ("c".to_string(), AkitaValue::Int(3)),
        ]);

        // Modify the parameter value
        for (key, value) in params.iter_mut() {
            if let (Some(k), AkitaValue::Int(num)) = (key, value) {
                match k.as_str() {
                    "a" => *num = 10,
                    "b" => *num = 20,
                    "c" => *num = 30,
                    _ => {}
                }
            }
        }

        // The order of verification remains the same
        let keys: Vec<_> = params.keys().into_iter().flatten().collect();
        assert_eq!(keys, vec!["a", "b", "c"]);

        let values = params.values();
        assert_eq!(
            values,
            vec![AkitaValue::Int(10), AkitaValue::Int(20), AkitaValue::Int(30)]
        );
    }

    #[test]
    fn test_get_index_and_position() {
        let params = Params::custom(vec![
            ("apple".to_string(), AkitaValue::Text("red".to_string())),
            ("banana".to_string(), AkitaValue::Text("yellow".to_string())),
            ("cherry".to_string(), AkitaValue::Text("red".to_string())),
        ]);

        // 测试获取索引
        assert_eq!(params.index_of("apple"), Some(0));
        assert_eq!(params.index_of("banana"), Some(1));
        assert_eq!(params.index_of("cherry"), Some(2));
        assert_eq!(params.index_of("nonexistent"), None);

        // Tests are taken by index
        assert_eq!(
            params.get_index(0),
            Some((&"apple".to_string(), &AkitaValue::Text("red".to_string())))
        );
        assert_eq!(
            params.get_index(1),
            Some((&"banana".to_string(), &AkitaValue::Text("yellow".to_string())))
        );
        assert_eq!(params.get_index(3), None);
    }

    #[test]
    fn test_insert_preserves_order() {
        let mut params = Params::custom(vec![
            ("a".to_string(), AkitaValue::Int(1)),
            ("c".to_string(), AkitaValue::Int(3)),
        ]);

        // Insert new parameters
        params.insert("b".to_string(), AkitaValue::Int(2));
        params.insert("d".to_string(), AkitaValue::Int(4));

        // Verify the order
        let keys: Vec<_> = params.keys().into_iter().flatten().collect();
        assert_eq!(keys, vec!["a", "c", "b", "d"]);

        // Updating existing parameters does not change the position
        params.insert("a".to_string(), AkitaValue::Int(100));
        let keys_after: Vec<_> = params.keys().into_iter().flatten().collect();
        assert_eq!(keys_after, vec!["a", "c", "b", "d"]);

        // The validation value is updated
        assert_eq!(params.get("a"), Some(&AkitaValue::Int(100)));
    }

    #[test]
    fn test_remove_preserves_order() {
        let mut params = Params::custom(vec![
            ("a".to_string(), AkitaValue::Int(1)),
            ("b".to_string(), AkitaValue::Int(2)),
            ("c".to_string(), AkitaValue::Int(3)),
            ("d".to_string(), AkitaValue::Int(4)),
        ]);

        // Remove the element in the middle
        let removed = params.remove("b");
        assert_eq!(removed, Some(AkitaValue::Int(2)));

        // Verify the order of the remaining elements
        let keys: Vec<_> = params.keys().into_iter().flatten().collect();
        assert_eq!(keys, vec!["a", "c", "d"]);

        // Remove elements that don't exist
        let removed = params.remove("nonexistent");
        assert_eq!(removed, None);
    }

    #[test]
    fn test_positional_params() {
        let params = Params::Positional(vec![
            AkitaValue::Text("hello".to_string()),
            AkitaValue::Int(42),
            AkitaValue::Bool(true),
        ]);

        // Test iterations
        let mut iter = params.iter();
        assert_eq!(iter.next(), Some((None, &AkitaValue::Text("hello".to_string()))));
        assert_eq!(iter.next(), Some((None, &AkitaValue::Int(42))));
        assert_eq!(iter.next(), Some((None, &AkitaValue::Bool(true))));
        assert_eq!(iter.next(), None);

        // Test variable iterations
        let mut params = Params::Positional(vec![AkitaValue::Int(1), AkitaValue::Int(2)]);
        for (_, value) in params.iter_mut() {
            if let AkitaValue::Int(num) = value {
                *num *= 2;
            }
        }
        assert_eq!(params.values(), vec![AkitaValue::Int(2), AkitaValue::Int(4)]);
    }

    #[test]
    fn test_conversion_methods() {
        // From conversion
        let params1: Params = vec![AkitaValue::Int(1), AkitaValue::Int(2)].into();
        assert!(matches!(params1, Params::Positional(_)));

        let params2: Params = vec![
            ("x".to_string(), AkitaValue::Int(10)),
            ("y".to_string(), AkitaValue::Int(20)),
        ].into();
        assert!(matches!(params2, Params::Named(_)));

        // 转换为 IndexMap
        if let Some(map) = params2.to_map() {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("x"), Some(&AkitaValue::Int(10)));
        }
    }
}