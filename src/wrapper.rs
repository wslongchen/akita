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

//!
//! Generate Wrapper.
//! ```ignore
//! 
//! let mut wrapper = Wrapper::new();
//! wrapper.like(true, "column1", "ffff");
//! wrapper.eq(true, "column2", 12);
//! wrapper.eq(true, "column3", "3333");
//! wrapper.in_(true, "column4", vec![1,44,3]);
//! wrapper.not_between(true, "column5", 2, 8);
//! wrapper.set(true, "column1", 4);
//! match wrapper.get_target_sql("t_user") {
//!     Ok(sql) => {println!("ok:{}", sql);}
//!     Err(err) => {println!("err:{}", err);}
//! }
//! ```
//!
//!
use crate::{segment::{MergeSegments, Segment, SqlKeyword, SqlLike, ToSegment, ISegment}, comm::*, AkitaError};
use crate::errors::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct Wrapper{
    /// 表名
    pub table: Option<String>,
    /// 必要度量
    pub param_name_seq: i32,
    /// SQL set字段
    pub sql_set: Vec<String>,
    /// set 字段
    pub fields_set: Vec<(String, Segment)>,
    /// SQL查询字段
    pub sql_select: Option<String>,
    /// SQL注释
    pub sql_comment: Option<String>,
    /// SQL起始语句
    pub sql_first: Option<String>,
    /// SQL结束语句
    pub last_sql: Option<String>,
    pub expression: MergeSegments,
}

impl ISegment for Wrapper {
    fn get_sql_segment(&mut self) -> String {
        let mut sql =  self.sql_first.to_owned().unwrap_or_default();
        sql.push_str(SPACE);
        let condition = self.expression.get_sql_segment();
        if !condition.is_empty() {
            sql.push_str(&self.expression.get_sql_segment());
        }
        if sql.trim().is_empty() {
            sql.push_str("(1 = 1)")
        }
        sql.push_str(SPACE);
        sql.push_str(&self.last_sql.to_owned().unwrap_or_default());
        sql
    }
}

impl Wrapper{

    pub fn new() -> Self {
        Self { table: None, sql_set: Vec::new(), expression: MergeSegments::default(), param_name_seq: 0, sql_first: None, last_sql: None, sql_comment: None, sql_select: None, fields_set: Vec::new() }
    }

    pub fn set<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self {
        self.set_condition(true, column, val)
    }

    pub fn set_condition<S: Into<String>, U: ToSegment>(mut self,condition: bool, column: S, val: U) -> Self {
        if condition {
            let col: String = column.into();
            self.sql_set.push(col.to_owned() + EQUALS + val.to_segment().get_sql_segment().as_str());
            self.fields_set.push((col.to_owned(), val.to_segment()));
        }
        self
    }

    pub fn set_sql<S: Into<String>>(mut self, sql: S) -> Self {
        let sql: String = sql.into();
        if !sql.is_empty() {
            self.sql_set.push(sql);
        }
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        let table: String = table.into();
        self.table = table.into();
        self
    }


    pub fn get_set_sql(&mut self) -> Option<String> {
        if self.sql_set.is_empty() {
            None
        } else {
            self.sql_set.join(COMMA).into()
        }
    }

    pub fn clear(mut self) {
        self.expression.clear();
        self.sql_set.clear();
    }

    pub fn get_update_sql(&mut self) -> Result<String> {
        let set_fields = if let Some(set) = self.get_set_sql() {
            set.to_owned()
        } else {
            return Err(AkitaError::DataError("update fields is empty!!!".to_string()))
        };
        let table = self.table.to_owned().unwrap_or_default();
        if table.is_empty() {
            Err(AkitaError::DataError("table name is empty!!!".to_string()))
        } else {
            let condition = self.expression.get_sql_segment();
            let mut sql = format!("update {} set {} ", table, set_fields);
            if !condition.is_empty() {
                sql.push_str(" where ");
                sql.push_str(&condition);
            }
            Ok(sql)
        }
    }

    pub fn get_query_sql(mut self) -> Result<String> {
        let select_fields = self.get_select_sql();
        let table = self.table.unwrap_or_default();
        if table.is_empty() {
            Err(AkitaError::DataError("table name is empty!!!".to_string()))
        } else {
            let condition = self.expression.get_sql_segment();
            let mut sql = format!("select {} from {}", select_fields, table);
            if !condition.is_empty() {
                sql.push_str(" where ");
                sql.push_str(&condition);
            }
            Ok(sql)
        }
    }

    pub fn eq<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self {
        self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::EQ, val.into())
    }
    pub fn ne<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::NE, val.into()) }
    pub fn gt<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::GT, val.into()) }
    pub fn ge<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::GE, val.into()) }
    pub fn lt<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::LT, val.into()) }
    pub fn le<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.add_condition(true, Segment::ColumnField(column.into()), SqlKeyword::LE, val.into()) }
    pub fn eq_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::EQ, val.into()) }
    pub fn ne_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::NE, val.into()) }
    pub fn gt_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::GT, val.into()) }
    pub fn ge_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::GE, val.into()) }
    pub fn lt_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::LT, val.into()) }
    pub fn le_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::LE, val.into()) }
    pub fn first<S: Into<String>>(self, sql: S) -> Self { self.first_condition(true, sql) }
    pub fn last<S: Into<String>>(self, sql: S) -> Self { self.last_condition(true, sql) }
    pub fn first_condition<S: Into<String>>(mut self, condition: bool, sql: S) -> Self { if condition { self.sql_first = format!("{}{}", sql.into(), SPACE ).into(); } self }
    pub fn last_condition<S: Into<String>>(mut self, condition: bool, sql: S) -> Self { if condition { self.last_sql = format!("{}{}", SPACE , sql.into()).into(); } self }
    pub fn inside<S: Into<String>, U: ToSegment + Clone>(self, column: S, vals: Vec<U>) -> Self { self.in_condition(true, column, vals) }
    pub fn not_in<S: Into<String>, U: ToSegment + Clone>(self, column: S, vals: Vec<U>) -> Self { self.not().inside(column, vals) }
    pub fn not_in_condition<S: Into<String>, U: ToSegment + Clone>(self, condition: bool, column: S, vals: Vec<U>) -> Self { self.not_condition(condition).in_condition(condition, column, vals) }
    pub fn in_condition<S: Into<String>, U: ToSegment + Clone>(mut self, condition: bool, column: S, vals: Vec<U>) -> Self { let segs: Vec<Segment> = vals.iter().map(|val|val.to_owned().into()).collect::<Vec<Segment>>(); if condition { self.append_sql_segments(vec![Segment::ColumnField(column.into()), SqlKeyword::IN.into(), Self::in_expression(segs)]) }; self }
    pub fn append_sql_segments(&mut self, sql_segments: Vec<Segment>) { self.expression.add(sql_segments); }
    pub fn do_it(mut self, condition: bool, segments: Vec<Segment>) -> Self {
        if condition {
            self.expression.add(segments); 
        } 
        self 
    }
    pub fn comment<S: Into<String>>(self, comment: S) -> Self { self.comment_condition(true, comment) }
    pub fn comment_condition<S: Into<String>>(mut self, condition: bool, comment: S) -> Self { if condition { self.sql_comment = comment.into().into(); } self }
    pub fn get_select_sql(&mut self) -> String { if let Some(select) = &self.sql_select { select.to_owned() } else { "*".to_string() } }
    pub fn select(mut self, columns: Vec<String>) -> Self { if !columns.is_empty() { self.sql_select = columns.join(",").into(); } self }
    pub fn like<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.like_value(true, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    pub fn like_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    pub fn not_like<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.not().like_value(true, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    pub fn not_like_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.not_condition(condition).like_value(condition, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    pub fn like_left<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.like_value(true, Segment::ColumnField(column.into()), SqlLike::LEFT, val.into()) }
    pub fn like_left_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::LEFT, val.into()) }
    pub fn like_right<S: Into<String>, U: ToSegment>(self, column: S, val: U) -> Self { self.like_value(true, Segment::ColumnField(column.into()), SqlLike::RIGHT, val.into()) }
    pub fn like_right_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val: U) -> Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::RIGHT, val.into()) }
    pub fn in_expression(mut vals: Vec<Segment>) -> Segment { 
        if vals.is_empty() { 
            Segment::Str("()") 
        } 
        else {  
            Segment::Text(LEFT_BRACKET.to_string() + vals.iter_mut().map(|val| val.get_sql_segment()).collect::<Vec<String>>().join(COMMA).as_str() + RIGHT_BRACKET) 
        } 
    }
    pub fn between<S: Into<String>, U: ToSegment>(self, column: S, val1: U, val2: U) -> Self { self.do_it(true, vec![Segment::ColumnField(column.into()), SqlKeyword::BETWEEN.into(), val1.into(), SqlKeyword::AND.into(), val2.into() ]) }
    pub fn between_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val1: U, val2: U) -> Self { self.do_it(condition, vec![Segment::ColumnField(column.into()), SqlKeyword::BETWEEN.into(), val1.into(), SqlKeyword::AND.into(), val2.into() ]) }
    pub fn not_between<S: Into<String>, U: ToSegment>(self, column: S, val1: U, val2: U) -> Self { self.not().between(column, val1, val2) }
    pub fn not_between_condition<S: Into<String>, U: ToSegment>(self, condition: bool, column: S, val1: U, val2: U) -> Self { self.not_condition(condition).between_condition(condition, column, val1, val2) }
    pub fn add_condition(self, condition: bool, column: Segment, sql_keword: SqlKeyword, val: Segment) -> Self { 
        self.do_it(condition, vec![column, sql_keword.into(), val]) 
    }
    pub fn like_value(self, condition: bool, column: Segment, sql_like: SqlLike, val: Segment) -> Self { self.do_it(condition, vec![column, SqlKeyword::LIKE.into(), sql_like.concat_like(val)]) }
    pub fn not(self) -> Self { self.do_it(true, vec![ SqlKeyword::NOT.into() ]) }
    pub fn and<F: FnOnce(Self) -> Self>(self, f: F) -> Self {
        self.and_inner().add_nested_condition(true, f)
    }
    pub fn or<F: FnOnce(Self) -> Self>(self, f: F) -> Self {
        self.or_inner().add_nested_condition(true, f)
    }

    fn add_nested_condition<F: FnOnce(Self) -> Self>(self, condition: bool, f: F) -> Self {
        if condition {
            let instance = f(Self::new());
            //let mut segs = instance.expression.get_normal();
            // segs.insert(0, SqlKeyword::BRACKET.into());
            self.do_it(true, vec![SqlKeyword::BRACKET.into(), instance.into()])
        } else {
            self
        }
    }

    fn and_inner(self) -> Self { self.do_it(true, vec![SqlKeyword::AND.into()]) }
    fn or_inner(self) -> Self { self.do_it(true, vec![SqlKeyword::OR.into()]) }
    pub fn not_condition(self, condition: bool) -> Self { self.do_it(condition, vec![ SqlKeyword::NOT.into() ]) }
    pub fn and_condition(self, condition: bool) -> Self { self.do_it(condition, vec![SqlKeyword::AND.into()]) }
    pub fn and_direct(self) -> Self { self.do_it(true, vec![SqlKeyword::AND.into()]) }
    pub fn or_condition(self, condition: bool) -> Self { self.do_it(condition, vec![SqlKeyword::OR.into()]) }
    pub fn or_direct(self) -> Self { self.do_it(true, vec![SqlKeyword::OR.into()]) }
    pub fn apply<S: Into<String>>(self, apply_sql: S) -> Self { self.do_it(true, vec![SqlKeyword::APPLY.into(), Segment::Extensions(apply_sql.into())]) }
    pub fn apply_condition<S: Into<String>>(self, condition: bool, apply_sql: S) -> Self { self.do_it(condition, vec![SqlKeyword::APPLY.into(), Segment::Extensions(apply_sql.into())]) }
    pub fn is_null<S: Into<String>>(self, column: S) -> Self { self.do_it(true, vec![Segment::ColumnField(column.into()), SqlKeyword::IS_NULL.into() ]) }
    pub fn is_null_condition<S: Into<String>>(self, condition: bool, column: S) -> Self { self.do_it(condition, vec![Segment::ColumnField(column.into()), SqlKeyword::IS_NULL.into() ]) }
    pub fn is_not_null<S: Into<String>>(self, column: S) -> Self { self.do_it(true, vec![ Segment::ColumnField(column.into()), SqlKeyword::IS_NOT_NULL.into() ]) }
    pub fn is_not_null_condition<S: Into<String>>(self, condition: bool, column: S) -> Self { self.do_it(condition, vec![Segment::ColumnField(column.into()), SqlKeyword::IS_NOT_NULL.into() ]) }
    pub fn not_exists<S: Into<String>>(self, not_exists_sql: S) -> Self  { self.not().exists(not_exists_sql) }
    pub fn not_exists_condition<S: Into<String>>(self, condition: bool, not_exists_sql: S) -> Self  { self.not_condition(condition).exists_condition(condition, not_exists_sql) }
    pub fn exists<S: Into<String>>(self, exists_sql: S) -> Self { self.do_it(true, vec![SqlKeyword::EXISTS.into(), Segment::Extensions(format!("({})", exists_sql.into()))]) }
    pub fn exists_condition<S: Into<String>>(self, condition: bool, exists_sql: S) -> Self { self.do_it(condition, vec![SqlKeyword::EXISTS.into(), Segment::Extensions(format!("({})", exists_sql.into()))]) }
    pub fn in_sql<S: Into<String>, U: Into<String>>(self, column: S, in_val: U) -> Self { self.do_it(true, vec![column.into().into() ,SqlKeyword::IN.into(), Segment::Extensions(format!("({})", in_val.into()))]) }
    pub fn in_sql_condition<S: Into<String>, U: Into<String>>(self, condition: bool, column: S, in_val: U) -> Self { self.do_it(condition, vec![column.into().into() ,SqlKeyword::IN.into(), Segment::Extensions(format!("({})", in_val.into()))]) }
    pub fn group_by<S: Into<String> + Clone>(self, columns: Vec<S>) -> Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { self.do_it(true, vec![SqlKeyword::GROUP_BY.into(), Segment::ColumnField(cols.join(COMMA))]) } }
    pub fn group_by_condition<S: Into<String> + Clone>(self, condition: bool, columns: Vec<S>) -> Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { self.do_it(condition, vec![SqlKeyword::GROUP_BY.into(), Segment::ColumnField(cols.join(COMMA))]) } }
    pub fn having<S: Into<String>>(self, sql_having: S) -> Self { self.do_it(true, vec![SqlKeyword::HAVING.into(), sql_having.into().into()]) }
    pub fn having_condition<S: Into<String>>(self, condition: bool, sql_having: S) -> Self { self.do_it(condition, vec![SqlKeyword::HAVING.into(), sql_having.into().into()]) }
    pub fn order_by<S: Into<String> + Clone>(self, is_asc: bool, columns: Vec<S>) -> Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { let mode = if is_asc { SqlKeyword::ASC } else { SqlKeyword::DESC }; self.do_it(true, vec![ SqlKeyword::ORDER_BY.into(), Segment::ColumnField(cols.join(COMMA)), mode.into() ]) } }
    pub fn asc_by<S: Into<String> + Clone>(self, columns: Vec<S>) -> Self { self.order_by(true, columns) }
    pub fn desc_by<S: Into<String> + Clone>(self, columns: Vec<S>) -> Self { self.order_by(false, columns) }
    pub fn order_by_condition<S: Into<String> + Clone>(self, condition: bool, is_asc: bool, columns: Vec<S>) -> Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { let mode = if is_asc { SqlKeyword::ASC } else { SqlKeyword::DESC }; self.do_it(condition, vec![ SqlKeyword::ORDER_BY.into(), Segment::ColumnField(cols.join(COMMA)), mode.into() ]) } }
    pub fn asc_by_condition<S: Into<String> + Clone>(self, condition: bool, columns: Vec<S>) -> Self { self.order_by_condition(condition, true, columns) }
    pub fn desc_by_condition<S: Into<String> + Clone>(self, condition: bool, columns: Vec<S>) -> Self { self.order_by_condition(condition, false, columns) }
}


#[test]
#[allow(unused)]
fn basic_test() {
    let s : Option<String> = Some("ffffa".to_string());
    let d: Option<i32> = None;
    let mut wrapper = Wrapper::new().set_sql("a='b'").eq("a", "bn").last("limit 1");
        //.not_in("vecs", vec!["a","f","g"]);
    println!("{}", wrapper.get_set_sql().unwrap_or_default());
}

#[test]
fn test_params() {
    let foo = 42;
    let v = params!{
        foo,
        "foo2x" => foo * 2,
    };
    println!("{:?}", v);
}