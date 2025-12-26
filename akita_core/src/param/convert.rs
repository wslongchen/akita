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
use crate::{Params, AkitaValue, IntoAkitaValue};
use indexmap::IndexMap;

// Implement for basic numeric types From<T> for Params
macro_rules! impl_from_numeric_for_params {
    ($($type:ty => $variant:ident),*) => {
        $(
            impl From<$type> for Params {
                fn from(x: $type) -> Params {
                    Params::Positional(vec![AkitaValue::$variant(x)])
                }
            }

            // 为 &T 实现
            impl From<&$type> for Params {
                fn from(x: &$type) -> Params {
                    Params::Positional(vec![AkitaValue::$variant(*x)])
                }
            }
        )*
    };
}

// Implement conversion for unsigned integer types (type conversion required)
macro_rules! impl_from_unsigned_for_params {
    ($($type:ty => $target:ty => $variant:ident),*) => {
        $(
            impl From<$type> for Params {
                fn from(x: $type) -> Params {
                    Params::Positional(vec![AkitaValue::$variant(x as $target)])
                }
            }

            // 为 &T 实现
            impl From<&$type> for Params {
                fn from(x: &$type) -> Params {
                    Params::Positional(vec![AkitaValue::$variant(*x as $target)])
                }
            }
        )*
    };
}

// Implement for the Option type From<Option<T>> for Params
macro_rules! impl_from_option_for_params {
    ($($type:ty => $variant:ident),*) => {
        $(
            impl From<Option<$type>> for Params {
                fn from(x: Option<$type>) -> Params {
                    match x {
                        Some(val) => Params::Positional(vec![AkitaValue::$variant(val)]),
                        None => Params::Positional(vec![AkitaValue::Null]),
                    }
                }
            }

            // Implemented for Option<&T>
            impl From<Option<&$type>> for Params {
                fn from(x: Option<&$type>) -> Params {
                    match x {
                        Some(val) => Params::Positional(vec![AkitaValue::$variant(*val)]),
                        None => Params::Positional(vec![AkitaValue::Null]),
                    }
                }
            }
        )*
    };
}

// Implement a conversion for the Option unsigned integer type
macro_rules! impl_from_option_unsigned_for_params {
    ($($type:ty => $target:ty => $variant:ident),*) => {
        $(
            impl From<Option<$type>> for Params {
                fn from(x: Option<$type>) -> Params {
                    match x {
                        Some(val) => Params::Positional(vec![AkitaValue::$variant(val as $target)]),
                        None => Params::Positional(vec![AkitaValue::Null]),
                    }
                }
            }

            // 为 Option<&T> 实现
            impl From<Option<&$type>> for Params {
                fn from(x: Option<&$type>) -> Params {
                    match x {
                        Some(val) => Params::Positional(vec![AkitaValue::$variant(*val as $target)]),
                        None => Params::Positional(vec![AkitaValue::Null]),
                    }
                }
            }
        )*
    };
}

impl_from_numeric_for_params! {
    i8 => Tinyint,
    i16 => Smallint,
    i32 => Int,
    i64 => Bigint,
    f32 => Float,
    f64 => Double,
    bool => Bool,
    char => Char
}

impl_from_unsigned_for_params! {
    u8 => i32 => Int,      // u8 -> i32
    u16 => i32 => Int,     // u16 -> i32
    u32 => i64 => Bigint,  // u32 -> i64
    u64 => i64 => Bigint   // u64 -> i64
}

macro_rules! impl_from_str_for_params {
    ($($type:ty),*) => {
        $(
            impl From<$type> for Params {
                fn from(value: $type) -> Params {
                    Params::Positional(vec![AkitaValue::Text(value.to_string())])
                }
            }
        )*
    };
}

macro_rules! impl_from_string_for_params {
    ($($type:ty),*) => {
        $(
            impl From<$type> for Params {
                fn from(value: $type) -> Params {
                    Params::Positional(vec![AkitaValue::Text(value.to_string())])
                }
            }
        )*
    };
}

macro_rules! impl_from_bytes_for_params {
    ($($type:ty),*) => {
        $(
            impl From<$type> for Params {
                fn from(value: $type) -> Params {
                    Params::Positional(vec![AkitaValue::Blob(value.to_vec())])
                }
            }
        )*
    };
}

impl_from_str_for_params! {
    &str,
    &&str
}

impl_from_string_for_params! {
    String,
    &String,
    &&String
}

impl_from_bytes_for_params! {
    &[u8],
    &&[u8],
    Vec<u8>
}

impl_from_option_for_params! {
    i8 => Tinyint,
    i16 => Smallint,
    i32 => Int,
    i64 => Bigint,
    f32 => Float,
    f64 => Double,
    bool => Bool,
    char => Char
}

impl_from_option_unsigned_for_params! {
    u8 => i32 => Int,      // u8 -> i32
    u16 => i32 => Int,     // u16 -> i32
    u32 => i64 => Bigint,  // u32 -> i64
    u64 => i64 => Bigint   // u64 -> i64
}

impl From<&chrono::NaiveDate> for Params {
    fn from(x: &chrono::NaiveDate) -> Params {
        Params::Positional(vec![AkitaValue::Date(*x)])
    }
}

impl From<&chrono::NaiveTime> for Params {
    fn from(x: &chrono::NaiveTime) -> Params {
        Params::Positional(vec![AkitaValue::Time(*x)])
    }
}

impl From<&chrono::NaiveDateTime> for Params {
    fn from(x: &chrono::NaiveDateTime) -> Params {
        Params::Positional(vec![AkitaValue::DateTime(*x)])
    }
}

impl From<&chrono::DateTime<chrono::Utc>> for Params {
    fn from(x: &chrono::DateTime<chrono::Utc>) -> Params {
        Params::Positional(vec![AkitaValue::Timestamp(*x)])
    }
}

impl From<&chrono::DateTime<chrono::Local>> for Params {
    fn from(x: &chrono::DateTime<chrono::Local>) -> Params {
        Params::Positional(vec![AkitaValue::Timestamp(x.with_timezone(&chrono::Utc))])
    }
}

impl From<&uuid::Uuid> for Params {
    fn from(x: &uuid::Uuid) -> Params {
        Params::Positional(vec![AkitaValue::Uuid(*x)])
    }
}

impl From<&bigdecimal::BigDecimal> for Params {
    fn from(x: &bigdecimal::BigDecimal) -> Params {
        Params::Positional(vec![AkitaValue::BigDecimal(x.clone())])
    }
}

impl From<Option<String>> for Params {
    fn from(x: Option<String>) -> Params {
        match x {
            Some(val) => Params::Positional(vec![AkitaValue::Text(val)]),
            None => Params::Positional(vec![AkitaValue::Null]),
        }
    }
}

impl From<Option<&str>> for Params {
    fn from(x: Option<&str>) -> Params {
        match x {
            Some(val) => Params::Positional(vec![AkitaValue::Text(val.to_string())]),
            None => Params::Positional(vec![AkitaValue::Null]),
        }
    }
}

impl From<Option<&String>> for Params {
    fn from(x: Option<&String>) -> Params {
        match x {
            Some(val) => Params::Positional(vec![AkitaValue::Text(val.clone())]),
            None => Params::Positional(vec![AkitaValue::Null]),
        }
    }
}

impl From<&Vec<AkitaValue>> for Params {
    fn from(x: &Vec<AkitaValue>) -> Params {
        Params::Positional(x.clone())
    }
}

impl<const N: usize> From<&[AkitaValue; N]> for Params {
    fn from(x: &[AkitaValue; N]) -> Params {
        Params::Positional(x.to_vec())
    }
}

pub trait ToParams {
    fn to_params(self) -> Params;
}

impl<T: IntoAkitaValue> ToParams for T {
    fn to_params(self) -> Params {
        let v = self.into_value();
        match v {
            AkitaValue::Null => Params::None,
            _ => Params::Positional(vec![v]),
        }
    }
}

impl From<AkitaValue> for Params {
    fn from(v: AkitaValue) -> Params {
        Params::Positional(vec![v])
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::Positional(Vec::new())
    }
}

impl From<Vec<AkitaValue>> for Params {
    fn from(v: Vec<AkitaValue>) -> Params {
        Params::Positional(v)
    }
}

#[macro_export]
macro_rules! into_params_ref_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Clone + Into<AkitaValue>,)*> From<&($($A,)*)> for Params {
            fn from(tuple: &($($A,)*)) -> Params {
                let ($($a,)*) = tuple.clone();
                let mut params = Vec::new();
                $(params.push($a.into());)*
                Params::Positional(params)
            }
        }
    );
}
// 为不同长度的元组引用实现

into_params_ref_impl!([A, a]);
into_params_ref_impl!([A, a], [B, b]);
into_params_ref_impl!([A, a], [B, b], [C, c]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y]);
into_params_ref_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y], [Z, z]);

#[macro_export]
macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Into<AkitaValue>,)*> From<($($A,)*)> for Params {
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                let mut params = Vec::new();
                $(params.push($a.into());)*
                Params::Positional(params)
            }
        }
    );
}


impl<N, V> From<Vec<(N, V)>> for Params
where
    String: From<N>,
    AkitaValue: From<V>,
{
    fn from(x: Vec<(N, V)>) -> Params {
        let mut params = Vec::new();
        for (name, value) in x.into_iter() {
            let name = String::from(name);
            params.push((name, AkitaValue::from(value)));
        }
        // Custom -> Named (Requires converting Vec to HashMap.))
        let named_params: IndexMap<String, AkitaValue> = params.into_iter().collect();
        Params::Named(named_params)
    }
}

impl From<IndexMap<String, AkitaValue>> for Params {
    fn from(map: IndexMap<String, AkitaValue>) -> Self {
        Self::Named(map)
    }
}

impl <'a> From<&'a dyn IntoAkitaValue> for Params {
    fn from(x: &'a dyn IntoAkitaValue) -> Params {
        let v = x.into_value();
        match v {
            AkitaValue::Null => Params::None,  // Null -> None
            _ => Params::Positional(vec![v]),  // Vector -> Positional
        }
    }
}

impl<'a> From<&'a [&'a dyn IntoAkitaValue]> for Params {
    fn from(x: &'a [&'a dyn IntoAkitaValue]) -> Params {
        let values = x.iter().map(|p| p.into_value()).collect::<Vec<AkitaValue>>();
        Params::Positional(values)
    }
}

into_params_impl!([A, a]);
into_params_impl!([A, a], [B, b]);
into_params_impl!([A, a], [B, b], [C, c]);
into_params_impl!([A, a], [B, b], [C, c], [D, d]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g], [H, h], [I, i], [J, j], [K, k], [L, l], [M, m], [N, n], [O, o], [P, p], [Q, q], [R, r], [S, s], [T, t], [U, u], [V, v], [W, w], [X, x], [Y, y], [Z, z]);
