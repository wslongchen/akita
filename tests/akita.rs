//!
//! Tests.
//!
use std::time::Duration;
use akita::*;
use chrono::NaiveDateTime;

#[derive(AkitaTable, Clone, Default)]
#[table(name = "t_system_user")]
pub struct User {
    #[table_id(name = "id")]
    pub pk: i64,
    pub id: String,
    pub headline: Option<NaiveDateTime>,
    /// 状态
    pub status: u8,
    /// 用户等级 0.普通会员 1.VIP会员
    pub level: u8,
    /// 生日
    pub birthday: Option<NaiveDate>,
    /// 性别
    pub gender: u8,
    #[field(exist = false)]
    pub is_org: bool,
    #[field(name = "token")]
    pub url_token: String,
}


fn main() {
    let db_url = String::from("mysql://root:password@localhost:3306/akita");
    let cfg = AkitaConfig::new(db_url).set_connection_timeout(Duration::from_secs(6))
        .set_log_level(LogLevel::Debug).set_max_size(6);
    let mut pool = Pool::new(cfg).expect("must be ok");
    let mut entity_manager = pool.entity_manager().expect("must be ok");
    // The Wrapper to build query condition
    let wrapper = Wrapper::new()
        .eq("username", "ussd") // username = 'ussd'
        .gt("age", 1) // age > 1
        .lt("age", 10) // age < 10
        .inside("user_type", vec!["admin", "super"]); // user_type in ('admin', 'super')
    // CRUD with EntityManager
    let insert_id: Option<i32> = entity_manager.save(&User::default()).unwrap();
    let insert_ids: Vec<Option<i32>>= entity_manager.save_batch(&[&User::default()]).unwrap();
    // Update with wrapper
    let res = entity_manager.update(&User::default(), Wrapper::new().eq("name", "Jack")).unwrap();
    // Update with primary id
    let res = entity_manager.update_by_id(&User::default());
    // Query return List
    let list: Vec<User> = entity_manager.list(Wrapper::new().eq("name", "Jack")).unwrap();
    // Query return Page
    let pageNo = 1;
    let pageSize = 10;
    let page: IPage<User> = entity_manager.page(pageNo, pageSize, Wrapper::new().eq("name", "Jack")).unwrap();
    // Remove with wrapper
    let res = entity_manager.remove::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Remove with primary id
    let res = entity_manager.remove_by_id::<User,_>(0).unwrap();
    // Get the record count
    let count = entity_manager.count::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Query with original sql
    let user: User = entity_manager.execute_first("select * from t_system_user where name = ? and id = ?", ("Jack", 1)).unwrap();
    // Or
    let user: User = entity_manager.execute_first("select * from t_system_user where name = :name and id = :id", params! {
        "name" => "Jack",
        "id" => 1
    }).unwrap();
    let res = entity_manager.execute_drop("select now()", ()).unwrap();

    // CRUD with Entity
    let model = User::default();
    // insert
    let insert_id = model.insert::<Option<i32>, _>(&mut entity_manager).unwrap();
    // update
    let res = model.update_by_id::<_>(&mut entity_manager).unwrap();
    // delete
    let res = model.delete_by_id::<i32,_>(&mut entity_manager, 1).unwrap();
    // list
    let list = User::list::<_>(Wrapper::new().eq("name", "Jack"), &mut entity_manager).unwrap();
    // page
    let page = User::page::<_>(pageNo, pageSize, Wrapper::new().eq("name", "Jack"), &mut entity_manager).unwrap();

    // Fast with Akita
    let list: Vec<User> = Akita::new().conn(pool.database().unwrap())
        .table("t_system_user")
        .wrapper(Wrapper::new().eq("name", "Jack"))
        .list::<User>().unwrap();

    let page: IPage<User> = Akita::new().conn(pool.database().unwrap())
        .table("t_system_user")
        .wrapper(Wrapper::new().eq("name", "Jack"))
        .page::<User>(1, 10).unwrap();

    // ...

    // Transaction
    entity_manager.start_transaction().and_then(|mut transaction| {
        let list: Vec<User> = transaction.list(Wrapper::new().eq("name", "Jack"))?;
        let insert_id: Option<i32> = transaction.save(&User::default())?;
        transaction.commit()
    }).unwrap();
}