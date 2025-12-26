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
use std::any::type_name;
use indexmap::IndexMap;
use crate::{AkitaDataError, AkitaValue, ConversionError, FromAkitaValue, IntoAkitaValue};

/// Implement the FromAkitaValue trait for the (N, V) tuple
impl<N, V> FromAkitaValue for (N, V)
where
    N: FromAkitaValue,
    V: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::List(elements) => {
                // Make sure the list has two elements
                if elements.len() != 2 {
                    return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                        message: format!(
                        "Expected tuple with 2 elements, got {} elements",
                        elements.len()
                    )}));
                }

                // Try converting the first element to N
                let n = match N::from_value_opt(&elements[0]) {
                    Ok(val) => val,
                    Err(e) => {
                        return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                        message: format!(
                            "Failed to convert first element to {}: {}",
                            type_name::<N>(),
                            e
                        )}))
                    }
                };

                // Try converting the second element to V
                let v = match V::from_value_opt(&elements[1]) {
                    Ok(val) => val,
                    Err(e) => {
                        return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                        message: format!(
                            "Failed to convert second element to {}: {}",
                            type_name::<V>(),
                            e
                        )}))
                    }
                };

                Ok((n, v))
            }

            // If it's an object type, try to convert from a key-value pair
            AkitaValue::Object(map) if map.len() == 2 => {
                
                let n = match map.get_index(0) {
                    Some((_, val)) => match N::from_value_opt(val) {
                        Ok(val) => val,
                        Err(e) => {
                            return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                        message: format!(
                                "Failed to convert key '_1' to {}: {}",
                                type_name::<N>(),
                                e
                            )}))
                        }
                    },
                    None => {
                        return Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                            message: "Missing key 0 in object for tuple conversion".to_string()
                        }));
                    }
                };

                let v = match map.get_index(1) {
                    Some((_, val)) => match V::from_value_opt(val) {
                        Ok(val) => val,
                        Err(e) => {
                            return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                                message: format!(
                                "Failed to convert key 1 to {}: {}",
                                type_name::<V>(),
                                e
                            )}))
                        }
                    },
                    None => {
                        return Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                                message: format!("{}", "Missing key 1 in object for tuple conversion".to_string())
                        }));
                    }
                };

                Ok((n, v))
            }

            // Other cases: Error
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError { 
                        message: format!(
                "Cannot convert {:?} to tuple ({}, {})",
                value,
                type_name::<N>(),
                type_name::<V>()
            )})),
        }
    }
}

/// For (N, V) TUPLE IMPLEMENTATION IntoAkitaValue trait
impl<N, V> IntoAkitaValue for (N, V)
where
    N: IntoAkitaValue,
    V: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
        ])
    }
}

/// IS A REFERENCE IMPLEMENTATION FOR TUPLES IntoAkitaValue trait
impl<N, V> IntoAkitaValue for &mut (N, V)
where
    N: IntoAkitaValue + Clone,
    V: IntoAkitaValue + Clone,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.clone().into_value(),
            self.1.clone().into_value(),
        ])
    }
}

/// Implemented for tuple slices IntoAkitaValue trait
impl<N, V> IntoAkitaValue for [(N, V)]
where
    N: IntoAkitaValue + Clone,
    V: IntoAkitaValue + Clone,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(
            self.iter()
                .map(|(n, v)| AkitaValue::List(vec![n.clone().into_value(), v.clone().into_value()]))
                .collect()
        )
    }
}

/// IS IMPLEMENTED AS A TUPLE VECTOR IntoAkitaValue trait
impl<N, V> IntoAkitaValue for Vec<(N, V)>
where
    N: IntoAkitaValue + Clone,
    V: IntoAkitaValue + Clone,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(
            self.iter()
                .map(|(n, v)| AkitaValue::List(vec![n.clone().into_value(), v.clone().into_value()]))
                .collect()
        )
    }
}

/// Key-value pair type aliases
pub type KeyValue<K, V> = (K, V);

/// AkitaValue Converted to a helper function for key-value pairs
pub fn try_into_key_value<K, V>(value: &AkitaValue) -> Result<(K, V), AkitaDataError>
where
    K: FromAkitaValue,
    V: FromAkitaValue,
{
    <(K, V)>::from_value_opt(value)
}

/// A helper function that converts from a key-value pair to AkitaValue
pub fn from_key_value<K, V>(key: K, value: V) -> AkitaValue
where
    K: IntoAkitaValue,
    V: IntoAkitaValue,
{
    AkitaValue::List(vec![key.into_value(), value.into_value()])
}

/// Convert the AkitaValue of the object type to a key-value pair vector
pub fn object_to_tuples<K, V>(value: &AkitaValue) -> Result<Vec<(K, V)>, AkitaDataError>
where
    K: FromAkitaValue + From<String>,
    V: FromAkitaValue,
{
    match value {
        AkitaValue::Object(map) => {
            let mut result = Vec::with_capacity(map.len());

            for (key, val) in map {
                let k = K::from(key.clone());
                let v = V::from_value_opt(val)?;
                result.push((k, v));
            }

            Ok(result)
        }
        _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
            message: format!("Cannot convert {:?} to key-value pairs", value)
        }))
    }
}

/// Converts key-value pair vectors to object types AkitaValue
pub fn tuples_to_object<K, V, I>(pairs: I) -> AkitaValue
where
    K: Into<String> + Clone,
    V: IntoAkitaValue + Clone,
    I: IntoIterator<Item = (K, V)>,
{
    let mut map = IndexMap::new();

    for (key, value) in pairs.into_iter() {
        map.insert(key.into(), value.into_value());
    }

    AkitaValue::Object(map)
}


impl IntoAkitaValue for (String, AkitaValue) {
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.clone().into_value(),
            self.1.clone(),
        ])
    }
}

impl<A, B, C> FromAkitaValue for (A, B, C)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::List(elements) if elements.len() == 3 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 3-tuple", value)
            })),
        }
    }
}

impl<A, B, C> IntoAkitaValue for (A, B, C)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
        ])
    }
}

/// Implemented for 4 tuples
impl<A, B, C, D> FromAkitaValue for (A, B, C, D)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::List(elements) if elements.len() == 4 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 4-tuple", value)
            })),
        }
    }
}

impl<A, B, C, D> IntoAkitaValue for (A, B, C, D)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
        ])
    }
}