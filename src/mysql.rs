//! 
//! MySQL modules.
//! 
use mysql::{Conn, Error, Opts, OptsBuilder, Row, prelude::Queryable};
use r2d2::{Pool, PooledConnection};
use std::result::Result;

pub type PooledConn = PooledConnection<MysqlConnectionManager>;
pub type r2d2Pool = Pool<MysqlConnectionManager>;

pub trait FromRowExt {
    fn from_long_row(row: mysql::Row) -> Self;
    fn from_long_row_opt(row: mysql::Row) -> Result<Self, mysql::FromRowError>
    where
        Self: Sized;
}

#[inline]
#[allow(unused)]
pub fn from_long_row<T: FromRowExt>(row: Row) -> T {
    FromRowExt::from_long_row(row)
}

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

///
/// 创建连接池
/// database_url 连接地址
/// max_size 最大连接数量
/// 
pub fn new_pool<S: Into<String>>(database_url: S, max_size: u32) -> Result<r2d2Pool, r2d2::Error> {
    let opts = Opts::from_url(&database_url.into()).expect("database url is empty.");
    let builder = OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let pool = Pool::builder().max_size(max_size).build(manager)?;
    Ok(pool)
}
