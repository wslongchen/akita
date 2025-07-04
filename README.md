# Akita &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![akita: rustc 1.13+]][Rust 1.13] [![akita_derive: rustc 1.31+]][Rust 1.31]

<div>
    <p align="center">
      <img src="http://img.snackcloud.cn/snackcloud/shop/snack_logo.png" alt="猫狗试验室" width="248" height="248">
    </p>
    <p align="center"><strong>猫狗试验室出品</strong></p>
    <p align="center">
      这是一个热爱🫶技术，热爱🔥生活的团队. <br>很欢迎大家能够一起学习📑、沟通💬 .
    </p>
  </div>

[Build Status]: https://img.shields.io/docsrs/akita/0.5.0?style=plastic
[actions]: https://github.com/wslongchen/akita/actions?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/akita?style=plastic
[crates.io]: https://crates.io/crates/akita
[akita: rustc 1.13+]: https://img.shields.io/badge/akita-rustc__1.31%2B-lightgrey
[akita_derive: rustc 1.31+]: https://img.shields.io/badge/akita__derive-rustc__1.31%2B-lightgrey
[Rust 1.13]: https://blog.rust-lang.org/2016/11/10/Rust-1.13.html
[Rust 1.31]: https://blog.rust-lang.org/2018/12/06/Rust-1.31-and-rust-2018.html

```Akita - Mini orm for rust ```

This Crate offers:
* MySql database's helper in pure rust;
* SQLite database's helper in pure rust;
* A mini orm framework (With MySQL/SQLite)。

Features:

* Other Database support, i.e. support Oracle, MSSQL...;
* support of named parameters for custom condition;
---

You may be looking for:

- [An overview of Akita](https://crates.io/crates/akita)
- [Examples](https://github.com/wslongchen/akita/blob/0.4.0/example/simple.rs)
- [API documentation](https://docs.rs/akita/0.4.0/akita/)
- [Release notes](https://github.com/wslongchen/akita/releases)

## Akita in action

<details>
<summary>
Click to show Cargo.toml.
<a href="https://play.rust-lang.org/?version=nightly&mode=debug&edition=2018&gist=bc95328e2b8691b4396222b080fdb1c3" target="_blank">Run this code in the playground.</a>
</summary>

```toml
[dependencies]

# The core APIs, including the Table traits. Always
# required when using Akita. using #[derive(Entity)] 
# to make Akita work with structs defined in your crate.
akita = { version = "0.4.0", features = ["akita-mysql"] }

```

</details>
<p></p>

## API Documentation

```rust
use akita::*;

/// Annotion Support: Entity、id、field (name, exist, fill(function, mode))
#[derive(Entity, Clone, Default)]
#[table(name = "t_system_user")]
pub struct User {
    #[id(name = "id")]
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
    #[field(exist = "false", fill="is_org_build")]
    pub is_org: bool,
    #[field(name = "token", fill(function = "token_build", mode="default"))]
    pub url_token: String,
}

static area: &str = "china"; 

fn is_org_build() -> bool {
    area.eq("china")
}

fn token_build() -> String {
    // generate the token
    todo!()
}

```
 ### CRUD with Akita
```rust


fn main() {
    let cfg = AkitaConfig::new(String::from("mysql://root:password@localhost:3306/akita"))
        .set_connection_timeout(Duration::from_secs(6))
        .set_log_level(LogLevel::Info).set_max_size(6);
    let akita = Akita::new(cfg).expect("must be ok");
    // The Wrapper to build query condition
    let wrapper = Wrapper::new()
        .eq("username", "ussd") // username = 'ussd'
        .gt("age", 1) // age > 1
        .lt("age", 10) // age < 10
        .inside("user_type", vec!["admin", "super"]) // user_type in ('admin', 'super')
        .and(|wrapper| { // or
            wrapper.like("username", &name)
                .or_direct().like("username", &name)
        });
    // CRUD with Akita
    let insert_id: Option<i32> = akita.save(&User::default()).unwrap();
    let _ = akita.save_batch(&[&User::default()]).unwrap();
    // Update with wrapper
    let res = akita.update(&User::default(), Wrapper::new().eq("name", "Jack")).unwrap();
    // Update with primary id
    let res = akita.update_by_id(&User::default());
    // Query return List
    let list: Vec<User> = akita.list(Wrapper::new().eq("name", "Jack")).unwrap();
    // Query return Page
    let pageNo = 1;
    let pageSize = 10;
    let page: IPage<User> = akita.page(pageNo, pageSize, Wrapper::new().eq("name", "Jack")).unwrap();
    // Remove with wrapper
    let res = akita.remove::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Remove with primary id
    let res = akita.remove_by_id::<User,_>(0).unwrap();
    // Get the record count
    let count = akita.count::<User>(Wrapper::new().eq("name", "Jack")).unwrap();
    // Query with original sql
    let user: User = akita.exec_first("select * from t_system_user where name = ? and id = ?", ("Jack", 1)).unwrap();
    // Or
    let user: User = akita.exec_first("select * from t_system_user where name = :name and id = :id", params! {
        "name" => "Jack",
        "id" => 1
    }).unwrap();
    let res = akita.exec_drop("select now()", ()).unwrap();

    // Transaction
    akita.start_transaction().and_then(|mut transaction| {
        let list: Vec<User> = transaction.list(Wrapper::new().eq("name", "Jack"))?;
        let insert_id: Option<i32> = transaction.save(&User::default())?;
        transaction.commit()
    }).unwrap();
}

```
 ### CRUD with Entity
```rust


fn main() {
    let cfg = AkitaConfig::new(String::from("mysql://root:password@localhost:3306/akita"))
        .set_connection_timeout(Duration::from_secs(6))
        .set_log_level(LogLevel::Info).set_max_size(6);
    let akita = Akita::new(cfg).expect("must be ok");
    // CRUD with Entity
    let model = User::default();
    // insert
    let insert_id = model.insert::<Option<i32>, _>(&akita).unwrap();
    // update
    let res = model.update_by_id::<_>(&akita).unwrap();
    // delete
    let res = model.delete_by_id::<i32,_>(&akita, 1).unwrap();
    // list
    let list = User::list::<_>(Wrapper::new().eq("name", "Jack"), &akita).unwrap();
    // page
    let page = User::page::<_>(pageNo, pageSize, Wrapper::new().eq("name", "Jack"), &akita).unwrap();
}
```
 ### Fast with sql
```rust


fn main() {
    pub static AK:Lazy<Akita> = Lazy::new(|| {
        let mut cfg = AkitaConfig::new("xxxx".to_string()).set_max_size(5).set_connection_timeout(Duration::from_secs(5)).set_log_level(LogLevel::Info);
        Akita::new(cfg).unwrap()
    });
    
    #[sql(AK,"select * from mch_info where mch_no = ?")]
    fn select_example(name: &str) -> Vec<MchInfo> { todo!() }
        
    // or:
    #[sql(AK,"select * from mch_info where mch_no = ?")]
    fn select_example2(ak: &AKita, name: &str) -> Vec<MchInfo> { todo!() }
    // ...
    
}

```
 ### Wrapper
 ```ignore

 let mut wrapper = Wrapper::new().like(true, "column1", "ffff")
 .eq(true, "column2", 12)
 .eq(true, "column3", "3333")
 .inside(true, "column4", vec![1,44,3])
 .not_between(true, "column5", 2, 8)
 .set(true, "column1", 4);
 
```
## Feature.

* ```akita-mysql``` - to use mysql
* ```akita-sqlite``` - to use sqlite
* ```akita-auth``` - to use some auth mehod
* ```akita-fuse``` - to use some fuse feature

## Annotions.

* ```Entity``` - to make Akita work with structs
* ```FromValue``` - from value with akita
* ```ToValue``` - to value with akita
* ```id``` - to make Table Ident
* ```field``` - to make struct field with own database.
* ```name``` - work with column, make the table's field name. default struct' field name.
* ```exist``` - ignore struct's field with table. default true.

## Support Field Types.
 
* ```Option<T>```
* ```u8, u32, u64```
* ```i32, i64```
* ```usize```
* ```bool```
* ```f32, f64```
* ```str, String```
* ```serde_json::Value```
* ```NaiveDate, NaiveDateTime```
 
## Developing

To setup the development envrionment run `cargo run`.

## Contributers

	MrPan <1049058427@qq.com>

## Getting help

Akita is a personal project. At the beginning, I just like Akita dog because of my hobbies.
I hope this project will grow more and more lovely. Many practical database functions will 
be added in the future. I hope you can actively help this project grow and put forward suggestions.
I believe the future will be better and better.

[#general]: https://discord.com/channels/273534239310479360/274215136414400513
[#beginners]: https://discord.com/channels/273534239310479360/273541522815713281
[#rust-usage]: https://discord.com/channels/442252698964721669/443150878111694848
[zulip]: https://rust-lang.zulipchat.com/#narrow/stream/122651-general
[stackoverflow]: https://stackoverflow.com/questions/tagged/rust
[/r/rust]: https://www.reddit.com/r/rust
[discourse]: https://users.rust-lang.org

<br>

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Akita by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
</sub>
