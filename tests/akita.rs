//!
//! Tests.
//!
use akita::prelude::*;
use akita::*;
use mysql::chrono::NaiveDateTime;
use mysql::TxOpts;

#[derive(Table, Clone)]
#[table(name = "t_system_user")]
pub struct User {
    #[id(name = "id")]
    pub pk: i64,
    pub id: String,
    pub name: String,
    pub headline: NaiveDateTime,
    pub avatar_url: Option<String>,
    pub gender: i32,
    pub birthday: Option<NaiveDate>,
    #[column(exist = "false")]
    pub is_org: bool,
    #[column(name = "token")]
    pub url_token: String,
    pub user_type: String,
}

#[test]
fn basic_test() {
    let mut wrapper = UpdateWrapper::new();
    wrapper.like(true, "username", "ffff");
    wrapper.eq(true, "username", 12);
    wrapper.eq(true, "username", "3333");
    wrapper.in_(true, "username", vec![1, 44, 3]);
    wrapper.not_between(true, "username", 2, 8);
    wrapper.set(true, "username", 4);
    let opts = Opts::from_url("mysql://root:MIMAlongchen520.@47.94.194.242:3306/dog_cloud")
        .expect("database url is empty.");
    let pool = new_pool(
        "mysql://root:MIMAlongchen520.@47.94.194.242:3306/dog_cloud",
        4,
    )
    .unwrap();
    let mut conn = pool.get().unwrap();

    let user = User {
        id: "2".to_string(),
        pk: 0,
        name: "name".to_string(),
        headline: mysql::chrono::Local::now().naive_local(),
        avatar_url: "name".to_string().into(),
        gender: 0,
        birthday: mysql::chrono::Local::now().naive_local().date().into(),
        is_org: false,
        url_token: "name".to_string(),
        user_type: "name".to_string(),
    };
    conn.start_transaction(TxOpts::default())
        .map(|mut transaction| {
            match user.update(&mut wrapper, &mut ConnMut::TxMut(&mut transaction)) {
                Ok(res) => {}
                Err(err) => {
                    println!("error : {:?}", err);
                }
            }
        });
    let mut pool = ConnMut::R2d2Polled(conn);
    match user.update_by_id(&mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.delete_by_id(&mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.delete::<UpdateWrapper>(&mut wrapper, &mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.insert(&mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }

    match user.find_by_id(&mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }

    match user.find_one::<UpdateWrapper>(&mut wrapper, &mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    match user.page::<UpdateWrapper>(1, 10, &mut wrapper, &mut pool) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
}
