//! 
//! Common Use.
//! 
use r2d2::{Pool, PooledConnection};
pub type PooledConn = PooledConnection<MysqlConnectionManager>;
pub type r2d2Pool = Pool<MysqlConnectionManager>;
pub use crate::{FromRowExt, from_long_row, wrapper::{QueryWrapper, UpdateWrapper, Wrapper}, BaseMapper, IPage, segment::SqlSegment, errors::AkitaError, ConnMut};
pub use mysql::{params, prelude::*};
pub use mysql::prelude::Queryable;
pub use mysql::error::Error;
use mysql::{Conn, Opts, OptsBuilder};
use std::result::Result;

#[derive(Clone, Debug)]
pub struct MysqlConnectionManager {
    params: Opts,
}

impl MysqlConnectionManager {
    pub fn new(params: OptsBuilder) -> MysqlConnectionManager {
        MysqlConnectionManager {
            params: Opts::from(params),
        }
    }
}

impl r2d2::ManageConnection for MysqlConnectionManager {
    type Connection = Conn;
    type Error = Error;

    fn connect(&self) -> Result<Conn, Error> {
        Conn::new(self.params.to_owned())
    }

    fn is_valid(&self, conn: &mut Conn) -> Result<(), Error> {
        conn.query_drop("SELECT version()")
    }

    fn has_broken(&self, conn: &mut Conn) -> bool {
        self.is_valid(conn).is_err()
    }
}

/// 格式化数值
pub trait FormatNum {
    type Item;
    fn format_num(&mut self,precision: i32) -> Option<Self::Item>;
    fn format_num_custom(&mut self, precision: &String, number_rule: u8) -> Option<Self::Item>;
}

impl FormatNum for f64 {
    type Item = f64;

    fn format_num(&mut self, precision: i32) -> Option<f64> {
        let mut num = 1;
        for _i in 0..precision {
            num = num * 10;
        }
        Some(((( *self * num as f64).round() as i32) as f64 )/ num as f64)
    }
    
    fn format_num_custom(&mut self, precision: &String, number_rule: u8) -> Option<Self::Item> {
        let num = if precision.eq("double") {
            *self * 100.00
        }else {
            *self
        };
        
        let num = match number_rule {
            1 => {
                // 四舍五入
                num.round()
            }
            2 => {
                // 最后一位不计
                num.floor()
            }
            3 => {
                // 逢一进十
                num.ceil()
            }
            _ => { num }
        };

        if precision.eq("double") {
            (num / 100.00).into()
        }else {
            num.into()
        }
    }
}

// The Serialize trait is not impl'd for NaiveDateTime
// This is a custom wrapper type to get around that
/* #[derive(Debug, PartialEq)]
pub struct CustomDateTime(pub NaiveDateTime);

impl Serialize for CustomDateTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = self.0.format("%Y-%m-%dT%H:%M:%S.%3fZ");
        serializer.serialize_str(&s.to_string())
    }
} */