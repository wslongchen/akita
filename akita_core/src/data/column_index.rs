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


/// Things that may be used as an index of a row column.
pub trait ColumnIndex {
    fn idx(&self, columns: &[String]) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn idx(&self, columns: &[String]) -> Option<usize> {
        if *self < columns.len() {
            Some(*self)
        } else {
            None
        }
    }
}

impl<'a> ColumnIndex for &'a str {
    fn idx(&self, columns: &[String]) -> Option<usize> {
        columns.iter().position(|c| c == *self)
    }
}

impl ColumnIndex for String {
    fn idx(&self, columns: &[String]) -> Option<usize> {
        columns.iter().position(|c| c == self)
    }
}