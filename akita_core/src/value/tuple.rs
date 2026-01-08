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

use crate::{from_akita_value_opt, AkitaDataError, AkitaValue, ConversionError, FromAkitaValue, IntoAkitaValue};


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
            AkitaValue::List(elements) if elements.len() == 1 => {
                Ok((
                    from_akita_value_opt(&elements[0])?,
                ))
            }
            _ => Err(AkitaDataError::ConversionError(ConversionError::ConversionError {
                message: format!("Cannot convert {:?} to 3-tuple", value)
            })),
        }
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

macro_rules! impl_tuple_for_akita {
    ($($t:ident),+) => {
        impl<$($t),+> FromAkitaValue for ($($t),+)
        where
            $($t: FromAkitaValue),+
        {
            fn from_value_opt(value: &AkitaValue) -> Result<Self, AkitaDataError> {
                const TUPLE_SIZE: usize = count_idents!($($t)+);

                match value {
                    AkitaValue::List(elements) => {
                        if elements.len() != TUPLE_SIZE {
                            return Err(AkitaDataError::NoSuchValueError(format!(
                                "Cannot convert list with {} elements to {}-tuple",
                                elements.len(),
                                TUPLE_SIZE
                            )));
                        }

                        let mut iter = elements.iter();

                        Ok((
                            $(
                                <$t as FromAkitaValue>::from_value_opt(
                                    iter.next().expect("checked tuple size")
                                )?,
                            )+
                        ))
                    }

                    AkitaValue::Object(obj) => {
                        if obj.len() != TUPLE_SIZE {
                            return Err(AkitaDataError::NoSuchValueError(format!(
                                "Cannot convert object with {} fields to {}-tuple",
                                obj.len(),
                                TUPLE_SIZE
                            )));
                        }

                        let mut index = 0;
                        Ok((
                            $(
                                {
                                    let value = take_or_place!(value, index, $t);
                                    index += 1;
                                    value
                                },
                            )+
                        ))
                    }

                    _ => Err(AkitaDataError::ConversionError(
                        ConversionError::ConversionError {
                            message: format!(
                                "Cannot convert {:?} to {}-tuple",
                                value,
                                TUPLE_SIZE
                            ),
                        },
                    )),
                }
            }
        }

        impl<$($t),+> IntoAkitaValue for ($($t),+)
        where
            $($t: IntoAkitaValue),+
        {
            fn into_value(&self) -> AkitaValue {
                let ($($t),+) = self;

                AkitaValue::List(vec![
                    $(
                        $t.into_value(),
                    )+
                ])
            }
        }
    };
}

macro_rules! count_idents {
    () => { 0 };
    ($head:ident $($tail:ident)*) => {
        1 + count_idents!($($tail)*)
    };
}


impl_tuple_for_akita!(A, B);
impl_tuple_for_akita!(A, B, C);
impl_tuple_for_akita!(A, B, C, D);
impl_tuple_for_akita!(A, B, C, D, E);
impl_tuple_for_akita!(A, B, C, D, E, F);
impl_tuple_for_akita!(A, B, C, D, E, F, G);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y);
impl_tuple_for_akita!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z);