//! 
//! Generate Wrapper.
//! ```ignore
//! 
//! let mut wrapper = UpdateWrapper::new();
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
use std::{sync::atomic::AtomicI32};

use crate::{comm::*, segment::{MergeSegments, Segment, SqlKeyword, SqlLike}};

pub trait Wrapper {
    fn eq<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn ne<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn gt<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn ge<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn lt<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn le<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn in_<S: Into<String>, U: Into<Segment> + Clone>(&mut self, condition: bool, column: S, vals: Vec<U>) -> &mut Self;
    fn not_in<S: Into<String>, U: Into<Segment> + Clone>(&mut self, condition: bool, column: S, vals: Vec<U>) -> &mut Self { self.not(condition).in_(condition, column, vals) }
    // fn between<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val1: U, val2: U) -> &mut Self;
    fn first<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self;
    fn last<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self;
    // fn like<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self;
    fn like<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    fn not_like<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.not(condition).like_value(condition, Segment::ColumnField(column.into()), SqlLike::DEFAULT, val.into()) }
    fn like_left<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::LEFT, val.into()) }
    fn like_right<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.like_value(condition, Segment::ColumnField(column.into()), SqlLike::RIGHT, val.into()) }
    fn append_sql_segments(&mut self, sql_segments: Vec<Segment>);
    // fn add_condition(&mut self, condition: bool, column: Segment, sql_keword: SqlKeyword, val: Segment) -> &mut Self;
    // fn like_value(&mut self, condition: bool, column: Segment, sql_like: SqlLike, val: Segment) -> &mut Self;
    fn do_it(&mut self, condition: bool, segments: Vec<Segment>) -> &mut Self;
    fn get_sql_segment(&mut self) -> String;
    fn in_expression(&self, vals: Vec<Segment>) -> Segment { if vals.is_empty() { Segment::Str("()") } else {  Segment::Text(LEFT_BRACKET.to_string() + vals.iter().map(|val| val.get_sql_segment()).collect::<Vec<String>>().join(COMMA).as_str() + RIGHT_BRACKET) } }
    fn between<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val1: U, val2: U) -> &mut Self { self.do_it(condition, vec![column.into().into(), SqlKeyword::BETWEEN.into(), val1.into().into(), SqlKeyword::AND.into(), val2.into().into() ]) }
    fn not_between<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val1: U, val2: U) -> &mut Self { self.not(condition).between(condition, column, val1, val2) }
    fn add_condition(&mut self, condition: bool, column: Segment, sql_keword: SqlKeyword, val: Segment) -> &mut Self { self.do_it(condition, vec![column, sql_keword.into(), val]) }
    fn like_value(&mut self, condition: bool, column: Segment, sql_like: SqlLike, val: Segment) -> &mut Self { self.do_it(condition, vec![column, SqlKeyword::LIKE.into(), Segment::Extenssion(sql_like.concat_like(val))]) }
    fn not(&mut self, condition: bool) -> &mut Self { self.do_it(condition, vec![ SqlKeyword::NOT.into() ]) }
    fn and(&mut self, condition: bool) -> &mut Self { self.do_it(condition, vec![SqlKeyword::AND.into()]) }
    fn or(&mut self, condition: bool) -> &mut Self { self.do_it(condition, vec![SqlKeyword::OR.into()]) }
    fn is_null<S: Into<String>>(&mut self, condition: bool, column: S) -> &mut Self { self.do_it(condition, vec![ column.into().into(), SqlKeyword::IS_NULL.into() ]) }
    fn is_not_null<S: Into<String>>(&mut self, condition: bool, column: S) -> &mut Self { self.do_it(condition, vec![ column.into().into(), SqlKeyword::IS_NOT_NULL.into() ]) }
    fn not_exists<S: Into<String>>(&mut self, condition: bool, not_exists_sql: S) -> &mut Self  { self.not(condition).exists(condition, not_exists_sql) }
    fn exists<S: Into<String>>(&mut self, condition: bool, exists_sql: S) -> &mut Self { self.do_it(condition, vec![SqlKeyword::EXISTS.into(), Segment::Extenssion(format!("({})", exists_sql.into()))]) }
    fn in_sql<S: Into<String>, U: Into<String>>(&mut self, condition: bool, column: S, in_val: U) -> &mut Self { self.do_it(condition, vec![column.into().into() ,SqlKeyword::IN.into(), Segment::Extenssion(format!("({})", in_val.into()))]) }
    fn group_by<S: Into<String> + Clone>(&mut self, condition: bool, columns: Vec<S>) -> &mut Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { self.do_it(condition, vec![SqlKeyword::GROUP_BY.into(), cols.join(COMMA).into()]) } }
    fn having<S: Into<String>>(&mut self, condition: bool, sql_having: S) -> &mut Self { self.do_it(condition, vec![SqlKeyword::HAVING.into(), sql_having.into().into()]) }
    fn order_by<S: Into<String> + Clone>(&mut self, condition: bool, is_asc: bool, columns: Vec<S>) -> &mut Self { let cols: Vec<String> = columns.iter().map(|col|col.to_owned().into()).collect::<Vec<String>>();if columns.is_empty() { self } else { let mode = if is_asc { SqlKeyword::ASC } else { SqlKeyword::DESC }; self.do_it(condition, vec![ SqlKeyword::ORDER_BY.into(), cols.join(COMMA).into(), mode.into() ]) } }
    fn comment<S: Into<String>>(&mut self, condition: bool, comment: S) -> &mut Self;
    fn get_select_sql(&mut self) -> String;
    fn select(&mut self, columns: Vec<String>) -> &mut Self;
}


pub struct QueryWrapper{
    /// 必要度量
    pub param_name_seq: AtomicI32,
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

pub struct UpdateWrapper{
    /// 必要度量
    pub param_name_seq: AtomicI32,
    /// SQL set字段
    pub sql_set: Vec<String>,
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


impl UpdateWrapper {

    pub fn new() -> Self {
        Self { sql_set: Vec::new(), expression: MergeSegments::default(), param_name_seq: AtomicI32::new(0), sql_first: None, last_sql: None, sql_comment: None, sql_select: None }
    }

    pub fn set<S: Into<String>, U: Into<Segment>>(&mut self,condition: bool, column: S, val: U) -> &mut Self {
        if condition {
            self.sql_set.push(column.into() + EQUALS + val.into().get_sql_segment().as_str());
        }
        self
    }

    pub fn set_sql<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self {
        let sql: String = sql.into();
        if condition && !&sql.is_empty() {
            self.sql_set.push(sql);
        }
        self
    }


    pub fn get_set_sql(&mut self) -> Option<String> {
        if self.sql_set.is_empty() {
            None
        } else {
            self.sql_set.join(COMMA).into()
        }
    }

    pub fn clear(&mut self) {
        self.expression.clear();
        self.sql_set.clear();
    }

    pub fn get_target_sql(&mut self, table_name: &'static str) -> Result<String, &str> {
        let set_fields = if let Some(set) = self.get_set_sql() {
            set.to_owned()
        } else {
            return Err("update fields is empty!!!")
        };
        if table_name.is_empty() {
            Err("table name is empty!!!")
        } else {
            Ok(format!("update {} set {} where {}", table_name, set_fields, self.expression.get_sql_segment()))
        }
    }

}

impl QueryWrapper {

    pub fn new() -> Self {
        Self { sql_select: None, expression: MergeSegments::default(), param_name_seq: AtomicI32::new(0), sql_first: None, last_sql: None, sql_comment: None }
    }

    pub fn get_target_sql(&mut self, table_name: &'static str) -> Result<String, &str> {
        let select_fields = self.get_select_sql();
        if table_name.is_empty() {
            Err("table name is empty!!!")
        } else {
            Ok(format!("select {} from {} where {}", select_fields, table_name, self.expression.get_sql_segment()))
        }
    }
}

macro_rules! impl_wrapper {
    ($e: ty) => {
        impl Wrapper for $e {
            fn eq<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::EQ, val.into()) }
            fn ne<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::NE, val.into()) }
            fn gt<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::GT, val.into()) }
            fn ge<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::GE, val.into()) }
            fn lt<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::LT, val.into()) }
            fn le<S: Into<String>, U: Into<Segment>>(&mut self, condition: bool, column: S, val: U) -> &mut Self { self.add_condition(condition, Segment::ColumnField(column.into()), SqlKeyword::LE, val.into()) }
            fn first<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self { if condition { self.last_sql = format!("{}{}", SPACE , sql.into()).into(); } self }
            fn last<S: Into<String>>(&mut self, condition: bool, sql: S) -> &mut Self { if condition { self.sql_first = format!("{}{}", SPACE , sql.into()).into(); } self }
            fn in_<S: Into<String>, U: Into<Segment> + Clone>(&mut self, condition: bool, column: S, vals: Vec<U>) -> &mut Self { let segs: Vec<Segment> = vals.iter().map(|val|val.to_owned().into()).collect::<Vec<Segment>>(); if condition { self.append_sql_segments(vec![Segment::ColumnField(column.into()), SqlKeyword::IN.into(), self.in_expression(segs)]) } self }
            fn append_sql_segments(&mut self, sql_segments: Vec<Segment>) { self.expression.add(sql_segments); }
            fn do_it(&mut self, condition: bool, segments: Vec<Segment>) -> &mut Self { if condition { self.expression.add(segments); } self }
            fn get_sql_segment(&mut self) -> String {self.expression.get_sql_segment() }
            fn comment<S: Into<String>>(&mut self, condition: bool, comment: S) -> &mut Self { if condition { self.sql_comment = comment.into().into(); } self }
            fn get_select_sql(&mut self) -> String { if let Some(select) = &self.sql_select { select.to_owned() } else { "*".to_string() } }
            fn select(&mut self, columns: Vec<String>) -> &mut Self { if !columns.is_empty() { self.sql_select = columns.join(",").into(); } self }
        }
    };
}

impl_wrapper!(QueryWrapper);
impl_wrapper!(UpdateWrapper);