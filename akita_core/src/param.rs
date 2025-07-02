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

use crate::value::{Value, ToValue};



#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    Null,
    Vector(Vec<Value>),
    Custom(Vec<(String, Value)>),
}

impl std::fmt::Display for Params {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Params::Vector(v) => {
                let v = v.iter().map(|value| format!("{}", value)).collect::<Vec<String>>().join(", ");
                write!(f, "==> Parameters: {}",  v)
            }
            Params::Custom(v) => {
                let v = v.iter().map(|(_, value)| format!("{}", value)).collect::<Vec<String>>().join(", ");
                write!(f, "==> Parameters: {}", v)
            }
            Params::Null => {write!(f, "==> Parameters: Null")}
        }
    }
}

impl From<Vec<Value>> for Params {
    fn from(x: Vec<Value>) -> Params {
        Params::Vector(x)
    }
}


impl <T: ToValue> From<T> for Params {
    fn from(x: T) -> Params {
        let v = x.to_value();
        match v {
            Value::Null => Params::Null,
            _ => Params::Vector(vec![v.to_owned()]),
        }
        
    }
}

impl<'a> From<&'a [&'a dyn ToValue]> for Params {
    fn from(x: &'a [&'a dyn ToValue]) -> Params {
        let values = x.iter().map(|p| p.to_value()).collect::<Vec<Value>>();
        Params::Vector(values)
    }
}

impl<N, V> From<Vec<(N, V)>> for Params
where
    String: From<N>,
    Value: From<V>,
{
    fn from(x: Vec<(N, V)>) -> Params {
        let mut params = Vec::new();
        for (name, value) in x.into_iter() {
            let name = String::from(name);
            params.push((name, Value::from(value)));
        }
        Params::Custom(params)
    }
}

impl From<Value> for Params {
    fn from(x: Value) -> Params {
        match x {
            Value::Null => Params::Null,
            _ => Params::Vector(vec![x]),
        }
    }
}

impl <'a> From<&'a dyn ToValue> for Params {
    fn from(x: &'a dyn ToValue) -> Params {
        
        let v = x.to_value();
        match v {
            Value::Null => Params::Null,
            _ => Params::Vector(vec![v.to_owned()]),
        }
    }
}

macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A: Into<Value>,)*> From<($($A,)*)> for Params {
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                let mut params = Vec::new();
                $(params.push($a.into());)*
                Params::Vector(params)
            }
        }
    );
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
