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


macro_rules! take_or_place {
    ($row:expr, $index:expr, $t:ident) => (
        match $row.take_obj_raw($index) {
            Some(v) => v,
            None => return Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", $row))),
        }
    );
    ($row:expr, $index:expr, $t:ident, $( [$idx:expr, $ir:expr] ),*) => (
        match $row.take_obj_raw($index) {
            Some(v) => v,
            None => return Err(AkitaDataError::NoSuchValueError(format!("{:?} can not get value", $row))),
        }
    );
}


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

impl FromAkitaValue for () {
    fn from_value_opt(v: &AkitaValue) -> Result<Self, AkitaDataError> {
        match v {
            AkitaValue::Null => Ok(()),
            _ => Err(AkitaDataError::not_supported_error(
                format!("{:?}", v),
                "Vec<String>".to_string(),
            )),
        }
    }
}

impl <A> FromAkitaValue for (A,) where A: FromAkitaValue {
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 1 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 1-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                Ok((ir1,))
            },
            AkitaValue::List(elements) if elements.len() == 1 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 3-tuple", value)
            })),
        }
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
            AkitaValue::Object(obj) => {
                if obj.len() != 3 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 3-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                Ok((ir1, ir2, ir3,))
            },
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

impl<A> IntoAkitaValue for (A,)
where
    A: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
        ])
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

impl<A, B, C, D, E> IntoAkitaValue for (A, B, C, D, E)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
        ])
    }
}


impl<A, B, C, D, E, F> IntoAkitaValue for (A, B, C, D, E, F)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
        ])
    }
}


impl<A, B, C, D, E, F, G> IntoAkitaValue for (A, B, C, D, E, F, G)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H> IntoAkitaValue for (A, B, C, D, E, F, G, H)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H, I> IntoAkitaValue for (A, B, C, D, E, F, G, H, I)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
        ])
    }
}


impl<A, B, C, D, E, F, G, H, I, J> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H, I, J, K> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
        ])
    }
}


impl<A, B, C, D, E, F, G, H, I, J, K, L> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
    L: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
            self.11.into_value(),
        ])
    }
}



impl<A, B, C, D, E, F, G, H, I, J, K, L, M> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
    L: IntoAkitaValue,
    M: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
            self.11.into_value(),
            self.12.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
    L: IntoAkitaValue,
    M: IntoAkitaValue,
    N: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
            self.11.into_value(),
            self.12.into_value(),
            self.13.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
    L: IntoAkitaValue,
    M: IntoAkitaValue,
    N: IntoAkitaValue,
    O: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
            self.11.into_value(),
            self.12.into_value(),
            self.13.into_value(),
            self.14.into_value(),
        ])
    }
}

impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P> IntoAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)
where
    A: IntoAkitaValue,
    B: IntoAkitaValue,
    C: IntoAkitaValue,
    D: IntoAkitaValue,
    E: IntoAkitaValue,
    F: IntoAkitaValue,
    G: IntoAkitaValue,
    H: IntoAkitaValue,
    I: IntoAkitaValue,
    J: IntoAkitaValue,
    K: IntoAkitaValue,
    L: IntoAkitaValue,
    M: IntoAkitaValue,
    N: IntoAkitaValue,
    O: IntoAkitaValue,
    P: IntoAkitaValue,
{
    fn into_value(&self) -> AkitaValue {
        AkitaValue::List(vec![
            self.0.into_value(),
            self.1.into_value(),
            self.2.into_value(),
            self.3.into_value(),
            self.4.into_value(),
            self.5.into_value(),
            self.6.into_value(),
            self.7.into_value(),
            self.8.into_value(),
            self.9.into_value(),
            self.10.into_value(),
            self.11.into_value(),
            self.12.into_value(),
            self.13.into_value(),
            self.14.into_value(),
            self.15.into_value(),
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
            AkitaValue::Object(obj) => {
                if obj.len() != 4 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 4-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                Ok((ir1, ir2, ir3, ir4))
            },
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

/// Implemented for 5 tuples
impl<A, B, C, D, E> FromAkitaValue for (A, B, C, D, E)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 5 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 5-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                Ok((ir1, ir2, ir3, ir4, ir5))
            },
            AkitaValue::List(elements) if elements.len() == 5 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 5-tuple", value)
            })),
        }
    }
}

/// Implemented for 6 tuples
impl<A, B, C, D, E, F> FromAkitaValue for (A, B, C, D, E, F)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 6 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 6-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6))
            },
            AkitaValue::List(elements) if elements.len() == 6 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 6-tuple", value)
            })),
        }
    }
}

/// Implemented for 7 tuples
impl<A, B, C, D, E, F, G> FromAkitaValue for (A, B, C, D, E, F, G)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 7 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 7-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7))
            },
            AkitaValue::List(elements) if elements.len() == 7 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 7-tuple", value)
            })),
        }
    }
}

/// Implemented for 8 tuples
impl<A, B, C, D, E, F, G, H> FromAkitaValue for (A, B, C, D, E, F, G, H)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 8 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 8-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8))
            },
            AkitaValue::List(elements) if elements.len() == 8 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 8-tuple", value)
            })),
        }
    }
}

/// Implemented for 9 tuples
impl<A, B, C, D, E, F, G, H, I> FromAkitaValue for (A, B, C, D, E, F, G, H, I)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 9 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 9-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9))
            },
            AkitaValue::List(elements) if elements.len() == 9 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 9-tuple", value)
            })),
        }
    }
}

/// Implemented for 10 tuples
impl<A, B, C, D, E, F, G, H, I, J> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 10 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 10-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10))
            },
            AkitaValue::List(elements) if elements.len() == 10 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 10-tuple", value)
            })),
        }
    }
}

/// Implemented for 11 tuples
impl<A, B, C, D, E, F, G, H, I, J, K> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 11 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 11-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11))
            },
            AkitaValue::List(elements) if elements.len() == 11 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 11-tuple", value)
            })),
        }
    }
}

/// Implemented for 12 tuples (A-L)
impl<A, B, C, D, E, F, G, H, I, J, K, L> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
    L: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 12 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 12-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                let ir12 = take_or_place!(value, 11, L, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12))
            },
            AkitaValue::List(elements) if elements.len() == 12 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                    L::from_value_opt(&elements[11])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 12-tuple", value)
            })),
        }
    }
}

/// Implemented for 13 tuples (A-M)
impl<A, B, C, D, E, F, G, H, I, J, K, L, M> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
    L: FromAkitaValue,
    M: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 13 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 13-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                let ir12 = take_or_place!(value, 11, L, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
                let ir13 = take_or_place!(value, 12, M, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12, ir13))
            },
            AkitaValue::List(elements) if elements.len() == 13 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                    L::from_value_opt(&elements[11])?,
                    M::from_value_opt(&elements[12])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 13-tuple", value)
            })),
        }
    }
}

/// Implemented for 14 tuples (A-N)
impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
    L: FromAkitaValue,
    M: FromAkitaValue,
    N: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 14 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 14-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                let ir12 = take_or_place!(value, 11, L, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
                let ir13 = take_or_place!(value, 12, M, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12]);
                let ir14 = take_or_place!(value, 13, N, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12, ir13, ir14))
            },
            AkitaValue::List(elements) if elements.len() == 14 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                    L::from_value_opt(&elements[11])?,
                    M::from_value_opt(&elements[12])?,
                    N::from_value_opt(&elements[13])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 14-tuple", value)
            })),
        }
    }
}

/// Implemented for 15 tuples (A-O)
impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
    L: FromAkitaValue,
    M: FromAkitaValue,
    N: FromAkitaValue,
    O: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 15 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 15-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                let ir12 = take_or_place!(value, 11, L, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
                let ir13 = take_or_place!(value, 12, M, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12]);
                let ir14 = take_or_place!(value, 13, N, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13]);
                let ir15 = take_or_place!(value, 14, O, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13], [13, ir14]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12, ir13, ir14, ir15))
            },
            AkitaValue::List(elements) if elements.len() == 15 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                    L::from_value_opt(&elements[11])?,
                    M::from_value_opt(&elements[12])?,
                    N::from_value_opt(&elements[13])?,
                    O::from_value_opt(&elements[14])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 15-tuple", value)
            })),
        }
    }
}

/// Implemented for 16 tuples (A-P)
impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P> FromAkitaValue for (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)
where
    A: FromAkitaValue,
    B: FromAkitaValue,
    C: FromAkitaValue,
    D: FromAkitaValue,
    E: FromAkitaValue,
    F: FromAkitaValue,
    G: FromAkitaValue,
    H: FromAkitaValue,
    I: FromAkitaValue,
    J: FromAkitaValue,
    K: FromAkitaValue,
    L: FromAkitaValue,
    M: FromAkitaValue,
    N: FromAkitaValue,
    O: FromAkitaValue,
    P: FromAkitaValue,
{
    fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
        match value {
            AkitaValue::Object(obj) => {
                if obj.len() != 16 {
                    return Err(AkitaDataError::NoSuchValueError(format!("Cannot convert object with {} fields to 16-tuple", obj.len())))
                }
                let ir1 = take_or_place!(value, 0, A);
                let ir2 = take_or_place!(value, 1, B, [0, ir1]);
                let ir3 = take_or_place!(value, 2, C, [0, ir1], [1, ir2]);
                let ir4 = take_or_place!(value, 3, D, [0, ir1], [1, ir2], [2, ir3]);
                let ir5 = take_or_place!(value, 4, E, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
                let ir6 = take_or_place!(value, 5, F, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
                let ir7 = take_or_place!(value, 6, G, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6]);
                let ir8 = take_or_place!(value, 7, H, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7]);
                let ir9 = take_or_place!(value, 8, I, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8]);
                let ir10 = take_or_place!(value, 9, J, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9]);
                let ir11 = take_or_place!(value, 10, K, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10]);
                let ir12 = take_or_place!(value, 11, L, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11]);
                let ir13 = take_or_place!(value, 12, M, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12]);
                let ir14 = take_or_place!(value, 13, N, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13]);
                let ir15 = take_or_place!(value, 14, O, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13], [13, ir14]);
                let ir16 = take_or_place!(value, 15, P, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5], [5, ir6], [6, ir7], [7, ir8], [8, ir9], [9, ir10], [10, ir11], [11, ir12], [12, ir13], [13, ir14], [14, ir15]);
                Ok((ir1, ir2, ir3, ir4, ir5, ir6, ir7, ir8, ir9, ir10, ir11, ir12, ir13, ir14, ir15, ir16))
            },
            AkitaValue::List(elements) if elements.len() == 16 => {
                Ok((
                    A::from_value_opt(&elements[0])?,
                    B::from_value_opt(&elements[1])?,
                    C::from_value_opt(&elements[2])?,
                    D::from_value_opt(&elements[3])?,
                    E::from_value_opt(&elements[4])?,
                    F::from_value_opt(&elements[5])?,
                    G::from_value_opt(&elements[6])?,
                    H::from_value_opt(&elements[7])?,
                    I::from_value_opt(&elements[8])?,
                    J::from_value_opt(&elements[9])?,
                    K::from_value_opt(&elements[10])?,
                    L::from_value_opt(&elements[11])?,
                    M::from_value_opt(&elements[12])?,
                    N::from_value_opt(&elements[13])?,
                    O::from_value_opt(&elements[14])?,
                    P::from_value_opt(&elements[15])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 16-tuple", value)
            })),
        }
    }
}