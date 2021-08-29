# Akita &emsp; [![Build Status]][actions] [![Latest Version]][crates.io] [![akita: rustc 1.13+]][Rust 1.13] [![akita_derive: rustc 1.31+]][Rust 1.31]

[Build Status]: https://img.shields.io/docsrs/akita/0.2.8?style=plastic
[actions]: https://github.com/wslongchen/akita/actions?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/akita?style=plastic
[crates.io]: https://crates.io/crates/akita
[akita: rustc 1.13+]: https://img.shields.io/badge/akita-rustc__1.31%2B-lightgrey
[akita_derive: rustc 1.31+]: https://img.shields.io/badge/akita__derive-rustc__1.31%2B-lightgrey
[Rust 1.13]: https://blog.rust-lang.org/2016/11/10/Rust-1.13.html
[Rust 1.31]: https://blog.rust-lang.org/2018/12/06/Rust-1.31-and-rust-2018.html

**Akita is a mini orm framework for MySQL.**

---

You may be looking for:

- [An overview of Akita (Coming Soon...)]()
- [Examples](https://github.com/wslongchen/akita/blob/0.2.0/example/simple.rs)
- [API documentation](https://docs.rs/akita/0.1.6/akita/)
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
# required when using Akita. using #[derive(Table)] 
# to make Akita work with structs defined in your crate.
akita = { version = "0.2.0"] }

```

</details>
<p></p>

```rust
use akita::*;
use akita::prelude::*;

/// Annotion Support: Table、table_id、field (name, exist)
#[derive(Debug, FromAkita, ToAkita, Table, Clone)]
#[table(name="t_system_user")]
struct SystemUser {
    #[field = "name"]
    id: Option<i32>,
    #[table_id]
    username: String,
    #[field(name="ages", exist = "false")]
    age: i32,
}

fn main() {
    let db_url = String::from("mysql://root:password@localhost:3306/akita");
    let mut pool = Pool::new(AkitaConfig{ max_size: None, url: db_url, log_level: None }).unwrap();
    let mut em = pool.entity_manager().expect("must be ok");
    let mut wrap = UpdateWrapper::new();
    wrap.eq(true, "username", "'ussd'");
    match em.count::<SystemUser, UpdateWrapper>(&mut wrap) {
        Ok(res) => {
            println!("success count data!");
        }
        Err(err) => {
            println!("error:{:?}",err);
        }
    }
}
```


## Annotions.

* ```Table``` - to make Akita work with structs
* ```table_id``` - to make Table Ident
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
