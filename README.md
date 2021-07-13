# Akita &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![akita: rustc 1.13+]][Rust 1.13] [![akita_derive: rustc 1.31+]][Rust 1.31]

[Build Status]: https://img.shields.io/docsrs/akita/0.1.2?style=plastic
[actions]: https://github.com/wslongchen/akita/actions?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/akita?style=plastic
[crates.io]: https://crates.io/crates/akita
[akita: rustc 1.13+]: https://img.shields.io/badge/akita-rustc__1.31%2B-lightgrey
[akita_derive: rustc 1.31+]: https://img.shields.io/badge/akita__derive-rustc__1.31%2B-lightgrey
[Rust 1.13]: https://blog.rust-lang.org/2016/11/10/Rust-1.13.html
[Rust 1.31]: https://blog.rust-lang.org/2018/12/06/Rust-1.31-and-rust-2018.html

**Akita is a mini framework for MySQL.**

---

You may be looking for:

- [An overview of Akita (Coming Soon...)]()
- [Examples](https://github.com/wslongchen/akita/blob/master/tests/akita.rs)
- [API documentation](https://docs.rs/akita/0.1.3/akita/)
- [Release notes](https://github.com/wslongchen/akita/releases)

## Akita in action

<details>
<summary>
Click to show Cargo.toml.
<a href="https://play.rust-lang.org/?edition=2018&gist=72755f28f99afc95e01d63174b28c1f5" target="_blank">Run this code in the playground.</a>
</summary>

```toml
[dependencies]

# The core APIs, including the Table traits. Always
# required when using Akita. using #[derive(Table)] 
# to make Akita work with structs and enums defined in your crate.
akita = { version = "1.0", features = ["derive"] }

```

</details>
<p></p>

```rust
use akita::prelude::*;
use mysql::{Opts, OptsBuilder, Transaction, TxOpts};
use r2d2::Pool;

/// Annotion Support: Table、Id、column
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

fn main() {
    // use r2d2 pool
    let opts = Opts::from_url("mysql://root:127.0.0.1:3306/test").expect("database url is empty.");
    let pool = Pool::builder().max_size(4).build(MysqlConnectionManager::new(OptsBuilder::from_opts(opts))).unwrap();
    let mut conn = pool.get().unwrap();
 
    /// build the wrapper.
    let mut wrapper = UpdateWrapper::new()
        .like(true, "username", "ffff");
        .eq(true, "username", 12);
        .eq(true, "username", "3333");
        .in_(true, "username", vec![1,44,3]);
        .not_between(true, "username", 2, 8);
        .set(true, "username", 4);
    
    let user = User{
        id: 2,
        username: "username".to_string(),
        mobile: "mobile".to_string(),
        password: "password".to_string()
    };
    let mut conn = ConnMut::Pooled(&mut conn);
    // Transaction
    conn.start_transaction(TxOpts::default()).map(|mut transaction| {
        match user.update( & mut wrapper, &mut ConnMut::TxMut(&mut transaction)) {
            Ok(res) => {}
            Err(err) => {
                println!("error : {:?}", err);
            }
        }
    });
    
    /// update by identify
    match user.update_by_id(&mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    /// delete by identify
    match user.delete_by_id(&mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    /// delete by condition
    match user.delete:: < UpdateWrapper > ( & mut wrapper, &mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    /// insert data
    match user.insert(&mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    /// find by identify
    match user.find_by_id(&mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    
    /// find one by condition
    match user.find_one::<UpdateWrapper>(&mut wrapper, &mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
    
    /// find page by condition
    match user.page::<UpdateWrapper>(1, 10,&mut wrapper, &mut conn) {
        Ok(res) => {}
        Err(err) => {
            println!("error : {:?}", err);
        }
    }
}
```

## Support Field Types.
 
* ```Option<T>```
* ```u8, u32, u64```
* ```i32, i64```
* ```usize```
* ```f32, f64```
* ```str, String```
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
