//!
//! Tests.
//!
use akita::prelude::*;
use akita::*;
use chrono::NaiveDateTime;

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
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            pk: 0,
            name: "".to_string(),
            headline: chrono::Local::now().naive_local(),
            avatar_url: "".to_string().into(),
            gender: 0,
            birthday: chrono::Local::now().naive_local().date().into(),
            is_org: false,
            url_token: "".to_string(),
            user_type: "".to_string(),
            status: 0,
            level: 1,
            data: vec![],
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