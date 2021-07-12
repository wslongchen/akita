use std::convert::TryFrom;

use mysql::{Conn, Transaction};

use crate::{AkitaError, UpdateWrapper, Wrapper, mysql::{PooledConn, r2d2Pool}};


pub enum ConnMut<'c, 't, 'tc> {
    Mut(&'c mut Conn),
    TxMut(&'t mut Transaction<'tc>),
    Owned(Conn),
    Pooled(mysql::PooledConn),
    R2d2Polled(PooledConn)
}

impl From<Conn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: Conn) -> Self {
        ConnMut::Owned(conn)
    }
}

impl From<mysql::PooledConn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: mysql::PooledConn) -> Self {
        ConnMut::Pooled(conn)
    }
}

impl From<PooledConn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: PooledConn) -> Self {
        ConnMut::R2d2Polled(conn)
    }
}

impl<'a> From<&'a mut Conn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut Conn) -> Self {
        ConnMut::Mut(conn)
    }
}

impl<'a> From<&'a mut mysql::PooledConn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut mysql::PooledConn) -> Self {
        ConnMut::Mut(conn.as_mut())
    }
}

impl<'t, 'tc> From<&'t mut Transaction<'tc>> for ConnMut<'static, 't, 'tc> {
    fn from(tx: &'t mut Transaction<'tc>) -> Self {
        ConnMut::TxMut(tx)
    }
}


impl TryFrom<&mysql::Pool> for ConnMut<'static, 'static, 'static> {
    type Error = mysql::Error;

    fn try_from(pool: &mysql::Pool) -> Result<Self, Self::Error> {
        pool.get_conn().map(From::from)
    }
}

impl TryFrom<&r2d2Pool> for ConnMut<'static, 'static, 'static> {
    type Error = r2d2::Error;

    fn try_from(pool: &r2d2Pool) -> Result<Self, Self::Error> {
        pool.get().map(From::from)
    }
}



#[derive(Clone)]
pub struct IPage <T> 
    where T: Sized + Clone {
    pub total: usize,
    pub size: usize,
    pub current: usize,
    pub records: Vec<T>
}

impl <T> IPage <T> 
where T: Sized + Clone{
    pub fn new(current: usize, size: usize, total: usize, records: Vec<T>) -> Self {
        Self {
            total,
            size,
            current,
            records,
        }
    }

    pub fn offset(&self) -> usize {
        if self.current > 0 { (self.current - 1) * self.size } else { 0 }
    }
}



pub trait BaseMapper{
    type Item;
    /// Insert Data.
    fn insert<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<u64>, AkitaError>;

    /// Update Data With Wrapper.
    fn update<'a, 'b, 'c>(&self, wrapper: &mut UpdateWrapper, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    fn list<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Vec<Self::Item>, AkitaError> where Self::Item: Clone;

    fn page<'a, 'b, 'c, W: Wrapper>(&self, page: usize, size: usize, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<IPage<Self::Item>, AkitaError> where Self::Item: Clone;

    /// Find One With Wrapper.
    fn find_one<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError>;

    /// Find Data With Table's Ident.
    fn find_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<Option<Self::Item>, AkitaError>;

    /// Update Data With Table's Ident.
    fn update_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Delete Data With Wrapper.
    fn delete<'a, 'b, 'c, W: Wrapper>(&self, wrapper: &mut W, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Delete Data With Table's Ident.
    fn delete_by_id<'a, 'b, 'c>(&self, conn: &mut ConnMut<'a, 'b, 'c>) -> Result<bool, AkitaError>;

    /// Get the Table Fields.
    fn get_table_fields(&self) -> Result<String, AkitaError>;

    /// Get Table Idents.
    fn get_table_idents(&self) -> Result<String, AkitaError>;

    /// Get Condition Fields.
    fn get_update_fields(&self, set_sql: Option<String>) -> Result<String, AkitaError>;

    /// Get Table Name.
    fn get_table_name(&self) -> Result<String, AkitaError>;
}
