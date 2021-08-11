//!
//! Tests.
//!
use akita::prelude::*;
use akita::*;
use mysql::chrono::NaiveDateTime;
use mysql::TxOpts;

#[derive(Table, Clone, ToAkita, FromAkita)]
#[table(name = "t_system_user")]
pub struct User {
    #[table_id(name = "id")]
    pub pk: i64,
    pub id: String,
    pub name: String,
    pub headline: NaiveDateTime,
    pub avatar_url: Option<String>,
    /// 状态
    pub status: u8,
    /// 用户等级 0.普通会员 1.VIP会员
    pub level: u8,
    /// 生日
    pub birthday: Option<NaiveDate>,
    /// 性别
    pub gender: u8,
    #[field(exist = "false")]
    pub is_org: bool,
    #[field(name = "token")]
    pub url_token: String,
    pub data: Vec<String>,
    pub user_type: String,
    pub inner_tuple: (String),
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            pk: 0,
            name: "".to_string(),
            headline: mysql::chrono::Local::now().naive_local(),
            avatar_url: "".to_string().into(),
            gender: 0,
            birthday: mysql::chrono::Local::now().naive_local().date().into(),
            is_org: false,
            url_token: "".to_string(),
            user_type: "".to_string(),
            status: 0,
            level: 1,
            data: vec![],
            inner_tuple: ("".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct TestInnerStruct {
    pub id: String,
}

#[derive(Clone)]
pub enum TestInnerEnum {
    Field,
}

// #[test]
// fn basic_test() {
//     let mut wrapper = UpdateWrapper::new();
//     wrapper.like(true, "username", "ffff");
//     wrapper.eq(true, "username", 12);
//     wrapper.eq(true, "username", "3333");
//     wrapper.in_(true, "username", vec![1, 44, 3]);
//     wrapper.not_between(true, "username", 2, 8);
//     wrapper.set(true, "username", 4);
//     let opts = Opts::from_url("mysql://root:127.0.0.1:3306/test").expect("database url is empty.");
//     let pool = new_pool("mysql://root:127.0.0.1:3306/test", 4).unwrap();
//     let mut conn = pool.get().unwrap();
//     let user = User {
//         id: "2".to_string(),
//         pk: 0,
//         name: "name".to_string(),
//         headline: mysql::chrono::Local::now().naive_local(),
//         avatar_url: "name".to_string().into(),
//         gender: 0,
//         birthday: mysql::chrono::Local::now().naive_local().date().into(),
//         is_org: false,
//         url_token: "name".to_string(),
//         user_type: "name".to_string(),
//         status: 0,
//         level: 1,
//         data: vec![],
//         inner_struct: Some(TestInnerStruct { id: "".to_string() }),
//         inner_tuple: ("".to_string()),
//         inner_enum: TestInnerEnum::Field,
//     };
//     conn.start_transaction(TxOpts::default())
//         .map(|mut transaction| {
//             match user.update(&mut wrapper, &mut ConnMut::TxMut(&mut transaction)) {
//                 Ok(res) => {}
//                 Err(err) => {
//                     println!("error : {:?}", err);
//                 }
//             }
//         });
//     let mut pool = ConnMut::R2d2Polled(conn);
//     match user.update_by_id(&mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }
//     match user.delete_by_id(&mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }
//     match user.delete::<UpdateWrapper>(&mut wrapper, &mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }
//     match user.insert(&mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }

//     match user.find_by_id(&mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }

//     match User::find_one::<UpdateWrapper>(&mut wrapper, &mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }
//     match User::page::<UpdateWrapper>(1, 10, &mut wrapper, &mut pool) {
//         Ok(res) => {}
//         Err(err) => {
//             println!("error : {:?}", err);
//         }
//     }
// }

// #[test]
// fn basic_wrapper() {
//     let mut wrapper = UpdateWrapper::new();
//     wrapper.like(true, "username", "ffff");
//     wrapper.eq(true, "username", 12);
//     wrapper.eq(true, "username", "3333");
//     wrapper.in_(true, "username", vec![1, 44, 3]);
//     // wrapper.not_between(true, "username", 2, 8);
//     wrapper.set(true, "username", 4);
//     wrapper.apply(true, "FIND_IN_SET(1,category_ids)");
//     wrapper.order_by(true, true, vec!["name","age"]);
//     wrapper.group_by(true, vec!["name","age"]);
//     let sql = wrapper.get_target_sql("table_name").unwrap();
//     println!("format sql: {}", sql);
// }