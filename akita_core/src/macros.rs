#[macro_export(local_inner_macros)]
macro_rules! akita_value {
    // Hide distracting implementation details from the generated rustdoc.
    ($($json:tt)+) => {
        akita_internal!($($json)+)
    };
}

// Rocket relies on this because they export their own `akita_value!` with a different
// doc comment than ours, and various Rust bugs prevent them from calling our
// `akita_value!` from their `akita_value!` so they call `akita_internal!` directly. Check with
//
// Changes are fine as long as `akita_internal!` does not call any new helper
// macros and can still be invoked as `akita_internal!($($json)+)`.
#[macro_export(local_inner_macros)]
#[doc(hidden)]
macro_rules! akita_internal {
    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an array [...]. Produces a vec![...]
    // of the elements.
    //
    // Must be invoked as: akita_internal!(@array [] $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    (@array [$($elems:expr,)*]) => {
        akita_internal_vec![$($elems,)*]
    };

    // Done without trailing comma.
    (@array [$($elems:expr),*]) => {
        akita_internal_vec![$($elems),*]
    };

    // Next element is `null`.
    (@array [$($elems:expr,)*] null $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!(null)] $($rest)*)
    };

    // Next element is `true`.
    (@array [$($elems:expr,)*] true $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!(true)] $($rest)*)
    };

    // Next element is `false`.
    (@array [$($elems:expr,)*] false $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!(false)] $($rest)*)
    };

    // Next element is an array.
    (@array [$($elems:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!([$($array)*])] $($rest)*)
    };

    // Next element is a map.
    (@array [$($elems:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!({$($map)*})] $($rest)*)
    };

    // Next element is an expression followed by comma.
    (@array [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)* akita_internal!($next),] $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    (@array [$($elems:expr,)*] $last:expr) => {
        akita_internal!(@array [$($elems,)* akita_internal!($last)])
    };

    // Comma after the most recent element.
    (@array [$($elems:expr),*] , $($rest:tt)*) => {
        akita_internal!(@array [$($elems,)*] $($rest)*)
    };

    // Unexpected token after most recent element.
    (@array [$($elems:expr),*] $unexpected:tt $($rest:tt)*) => {
        akita_unexpected!($unexpected)
    };

    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an object {...}. Each entry is
    // inserted into the given map variable.
    //
    // Must be invoked as: akita_internal!(@object $map () ($($tt)*) ($($tt)*))
    //
    // We require two copies of the input tokens so that we can match on one
    // copy and trigger errors on the other copy.
    //////////////////////////////////////////////////////////////////////////

    // Done.
    (@object $object:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        let _ = $object.insert(($($key)+).into(), $value);
        akita_internal!(@object $object () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    (@object $object:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        akita_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr)) => {
        let _ = $object.insert(($($key)+).into(), $value);
    };

    // Next value is `null`.
    (@object $object:ident ($($key:tt)+) (: null $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!(null)) $($rest)*);
    };

    // Next value is `true`.
    (@object $object:ident ($($key:tt)+) (: true $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!(true)) $($rest)*);
    };

    // Next value is `false`.
    (@object $object:ident ($($key:tt)+) (: false $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!(false)) $($rest)*);
    };

    // Next value is an array.
    (@object $object:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@object $object:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!({$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        akita_internal!(@object $object [$($key)+] (akita_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        akita_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@object $object:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        akita_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@object $object:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        akita_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        akita_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@object $object:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    (@object $object:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        akita_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    (@object $object:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        akita_internal!(@object $object ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: akita_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    (null) => {
        $crate::Value::Null
    };

    (true) => {
        $crate::Value::Bool(true)
    };

    (false) => {
        $crate::Value::Bool(false)
    };

    ([]) => {
        $crate::Value::Array(akita_internal_vec![])
    };

    ([ $($tt:tt)+ ]) => {
        $crate::Value::Array(akita_internal!(@array [] $($tt)+))
    };

    ({}) => {
        $crate::Value::Object($crate::Map::new())
    };

    ({ $($tt:tt)+ }) => {
        $crate::Value::Object({
            let mut object = $crate::Map::new();
            akita_internal!(@object object () ($($tt)+) ($($tt)+));
            object
        })
    };

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        $crate::to_value(&$other).unwrap()
    };
}

// The akita_internal macro above cannot invoke vec directly because it uses
// local_inner_macros. A vec invocation there would resolve to $crate::vec.
// Instead invoke vec here outside of local_inner_macros.
#[macro_export]
#[doc(hidden)]
macro_rules! akita_internal_vec {
    ($($content:tt)*) => {
        vec![$($content)*]
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! akita_unexpected {
    () => {};
}

#[macro_export]
#[doc(hidden)]
macro_rules! akita_expect_expr_comma {
    ($e:expr , $($tt:tt)*) => {};
}



#[macro_export]
macro_rules! cfg_if {
    // match if/else chains with a final `else`
    (
        $(
            if #[cfg( $i_meta:meta )] { $( $i_tokens:tt )* }
        ) else+
        else { $( $e_tokens:tt )* }
    ) => {
        $crate::cfg_if! {
            @__items () ;
            $(
                (( $i_meta ) ( $( $i_tokens )* )) ,
            )+
            (() ( $( $e_tokens )* )) ,
        }
    };

    // match if/else chains lacking a final `else`
    (
        if #[cfg( $i_meta:meta )] { $( $i_tokens:tt )* }
        $(
            else if #[cfg( $e_meta:meta )] { $( $e_tokens:tt )* }
        )*
    ) => {
        $crate::cfg_if! {
            @__items () ;
            (( $i_meta ) ( $( $i_tokens )* )) ,
            $(
                (( $e_meta ) ( $( $e_tokens )* )) ,
            )*
        }
    };

    // Internal and recursive macro to emit all the items
    //
    // Collects all the previous cfgs in a list at the beginning, so they can be
    // negated. After the semicolon is all the remaining items.
    (@__items ( $( $_:meta , )* ) ; ) => {};
    (
        @__items ( $( $no:meta , )* ) ;
        (( $( $yes:meta )? ) ( $( $tokens:tt )* )) ,
        $( $rest:tt , )*
    ) => {
        // Emit all items within one block, applying an appropriate #[cfg]. The
        // #[cfg] will require all `$yes` matchers specified and must also negate
        // all previous matchers.
        #[cfg(all(
            $( $yes , )?
            not(any( $( $no ),* ))
        ))]
        $crate::cfg_if! { @__identity $( $tokens )* }

        // Recurse to emit all other items in `$rest`, and when we do so add all
        // our `$yes` matchers to the list of `$no` matchers as future emissions
        // will have to negate everything we just matched as well.
        $crate::cfg_if! {
            @__items ( $( $no , )* $( $yes , )? ) ;
            $( $rest , )*
        }
    };

    // Internal macro to make __apply work out right for different match types,
    // because of how macros match/expand stuff.
    (@__identity $( $tokens:tt )* ) => {
        $( $tokens )*
    };
}





/// This macro is a convenient way to pass named parameters to a statement.
///
/// ```ignore
/// let foo = 42;
/// params! {
///     foo,
///     "foo2x" => foo * 2,
/// });
/// ```
#[macro_export]
macro_rules! params {
    () => {};
    (@to_pair $name:expr => $value:expr) => (
        (std::string::String::from($name), akita_core::Value::from($value))
    );
    (@to_pair $name:ident) => (
        (std::string::String::from(stringify!($name)), akita_core::Value::from($name))
    );
    (@expand $vec:expr;) => {};
    (@expand $vec:expr; $name:expr => $value:expr, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:expr => $value:expr $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    ($i:ident, $($tail:tt)*) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i, $($tail)*);
            output
        }
    };
    ($i:expr => $($tail:tt)*) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i => $($tail)*);
            output
        }
    };
    ($i:ident) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i);
            output
        }
    }
}

//
// /// This macro is a convenient way to pass named parameters to a statement.
// ///
// /// ```ignore
// /// let a: StructA = StructA {
// ///     field: "name"
// /// };
// /// let b: StructB = copy_properties!(a, StructB);
// /// ```
// #[macro_export]
// macro_rules! copy_properties {
//     () => {
//         panic!("element can not be empty!")
//     };
//     (@to_pair $name:expr => $value:expr) => (
//         (std::string::String::from($name), akita_core::Value::from($value))
//     );
//     (@to_pair $name:ident) => (
//         (std::string::String::from(stringify!($name)), akita_core::Value::from($name))
//     );
//     (@expand $vec:expr;) => {};
//     (@expand $vec:expr; $name:expr => $value:expr, $($tail:tt)*) => {
//         $vec.push(params!(@to_pair $name => $value));
//         params!(@expand $vec; $($tail)*);
//     };
//     (@expand $vec:expr; $name:expr => $value:expr $(, $tail:tt)*) => {
//         $vec.push(params!(@to_pair $name => $value));
//         params!(@expand $vec; $($tail)*);
//     };
//     (@expand $vec:expr; $name:ident, $($tail:tt)*) => {
//         $vec.push(params!(@to_pair $name));
//         params!(@expand $vec; $($tail)*);
//     };
//     (@expand $vec:expr; $name:ident $(, $tail:tt)*) => {
//         $vec.push(params!(@to_pair $name));
//         params!(@expand $vec; $($tail)*);
//     };
//     ($i:ident, $($tail:tt)*) => {
//         {
//             let mut output = std::vec::Vec::new();
//             params!(@expand output; $i, $($tail)*);
//             output
//         }
//     };
//     ($i:expr => $($tail:tt)*) => {
//         {
//             let mut output = std::vec::Vec::new();
//             params!(@expand output; $i => $($tail)*);
//             output
//         }
//     };
//     ($i:ident) => {
//         {
//             let mut output = std::vec::Vec::new();
//             params!(@expand output; $i);
//             output
//         }
//     }
// }