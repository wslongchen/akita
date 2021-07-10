//! 
//! Tests.
//! 
use akita::prelude:: * ;
use mysql::{Opts, OptsBuilder, Transaction, TxOpts};
use r2d2::Pool;

#
[macro_use]
extern crate akita_derive;

#[derive(Table, Clone)]
#[table(name = "t_system_user")]
pub struct User {
    #[id(name="id")]
    pub pk: i64,
    pub id: String,
    pub name: String,
    pub headline: String,
    pub avatar_url: String,
    pub gender: i32,
    pub is_org: bool, 
    #[column(name="token")]
    pub url_token: String,
    pub user_type: String,
}

# [test]
fn basic_test() {
    let mut wrapper = UpdateWrapper::new();
    wrapper.like(true, "username", "ffff");
    wrapper.eq(true, "username", 12);
    wrapper.eq(true, "username", "3333");
    wrapper.in_(true, "username", vec![1, 44, 3]);
    wrapper.not_between(true, "username", 2, 8);
    wrapper.set(true, "username", 4);
    let opts = Opts::from_url("mysql://root:MIMAlongchen520.@47.94.194.242:3306/dog_cloud").expect("database url is empty.");
    let builder = OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let pool = Pool::builder().max_size(4).build(manager).unwrap();
    let mut conn = pool.get().unwrap();
    
    let user = User {
        id: "2".to_string(),
        pk: 0,
        name: "name".to_string(),
        headline: "name".to_string(),
        avatar_url: "name".to_string(),
        gender: 0,
        is_org: false,
        url_token: "name".to_string(),
        user_type: "name".to_string(),
    };
    conn.start_transaction(TxOpts::default()).map(|mut transaction| {
        match user.update( & mut wrapper, ConnMut::TxMut(&mut transaction)) {
            Ok(res) => {}
            Err(err) => {
                println!("error : {:?}", err);
            }
        }
    });
    
    match user.update_by_id(ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.delete_by_id(ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.delete:: < UpdateWrapper > ( & mut wrapper, ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.insert(ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }

    match user.find_by_id(ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }

    match user.find_one::<UpdateWrapper>(&mut wrapper, ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.page::<UpdateWrapper>(1, 10,&mut wrapper, ConnMut::Pooled(&mut conn)) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
}