use crate::value::{Value, ToValue};



#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    Nil, // no params
    Vector(Vec<Value>), // vec
    Custom(Vec<(String, Value)>), // custom params
}

impl std::fmt::Display for Params {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Params::Vector(v) => {
                write!(f, "{:?}",  v)
            }
            Params::Custom(v) => {
                write!(f, "{:?}", v)
            }
            Params::Nil => {write!(f, "Nil")}
        }
    }
}

// pub trait ToParam {
//     fn to_param(&self) -> Params;
// }

impl From<Vec<Value>> for Params {
    fn from(x: Vec<Value>) -> Params {
        Params::Vector(x)
    }
}


impl <T: ToValue> From<T> for Params {
    fn from(x: T) -> Params {
        let v = x.to_value();
        match v {
            Value::Nil => Params::Nil,
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
            Value::Nil => Params::Nil,
            _ => Params::Vector(vec![x]),
        }
    }
}

impl <'a> From<&'a dyn ToValue> for Params {
    fn from(x: &'a dyn ToValue) -> Params {
        
        let v = x.to_value();
        match v {
            Value::Nil => Params::Nil,
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
