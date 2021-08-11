//! 
//! SQL Segments.
//! 
use crate::comm::*;

pub trait SqlSegment {
    fn get_sql_segment(&self) -> String;
}

#[derive(Clone, Debug)]
pub enum Segment{
    Keyword(SqlKeyword),
    Float(f64),
    ColumnField(String),
    Extenssion(String),
    Text(String),
    Int32(i32),
    Int64(i64),
    Usize(usize),
    U8(u8),
    Int8(i8),
    U32(u32),
    U64(u64),
    Str(&'static str),
    Nil,
}

#[derive(Clone, Debug)]
pub enum SegmentType{
    GroupBy,
    Having,
    OrderBy,
    Normal,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum SqlKeyword {
    AND,
    OR,
    IN,
    NOT,
    LIKE,
    LIKE_LEFT,
    LIKE_RIGHT,
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE,
    IS_NULL,
    IS_NOT_NULL,
    GROUP_BY,
    HAVING,
    APPLY,
    ORDER_BY,
    EXISTS,
    BETWEEN,
    ASC,
    DESC
}

pub enum SqlLike {
    /**
     * %值
     */
    LEFT,
    /**
     * 值%
     */
    RIGHT,
    /**
     * %值%
     */
    DEFAULT
}


pub struct MergeSegments{
    pub normal: SegmentList,
    pub group_by: SegmentList,
    pub order_by: SegmentList,
    pub having: SegmentList,
}


pub struct SegmentList {
    pub seg_type: SegmentType,
    pub last_value: Option<Segment>,
    pub execute_not: bool,
    pub flush_last_value: bool,
    pub sql_segment: String,
    pub segments: Vec<Segment>,
}

impl Segment {
    pub fn get_sql_segment(&self) -> String {
        match self {
            Segment::Keyword(keyword) => keyword.get_sql_segment(),
            Segment::ColumnField(val) => format!("{}", val),
            Segment::Float(val) => format!("{}", val),
            Segment::Extenssion(val) => format!("{}", val),
            Segment::Text(val) => format!("{}", val),
            Segment::Int32(val) => format!("{}", val),
            Segment::Nil => String::default().to_string(),
            Segment::Int64(val) => format!("{}", val),
            Segment::Usize(val) => format!("{}", val),
            Segment::U32(val) => format!("{}", val),
            Segment::U64(val) => format!("{}", val),
            Segment::Str(val) => format!("{}", val),
            Segment::U8(val) => format!("{}", val),
            Segment::Int8(val) => format!("{}", val),
        }
    }
}


impl Into<Segment> for i32 {
    fn into(self) -> Segment {
        Segment::Int32(self)
    }
}

impl Into<Segment> for i64 {
    fn into(self) -> Segment {
        Segment::Int64(self)
    }
}

impl Into<Segment> for i8 {
    fn into(self) -> Segment {
        Segment::Int8(self)
    }
}

impl Into<Segment> for u64 {
    fn into(self) -> Segment {
        Segment::U64(self)
    }
}

impl Into<Segment> for u8 {
    fn into(self) -> Segment {
        Segment::U8(self)
    }
}

impl Into<Segment> for u32 {
    fn into(self) -> Segment {
        Segment::U32(self)
    }
}


impl Into<Segment> for String {
    fn into(self) -> Segment {
        Segment::Text(self)
    }
}


impl Into<Segment> for usize {
    fn into(self) -> Segment {
        Segment::Usize(self)
    }
}

impl Into<Segment> for f64 {
    fn into(self) -> Segment {
        Segment::Float(self)
    }
}

impl Into<Segment> for &'static str {
    fn into(self) -> Segment {
        Segment::Str(self)
    }
}


impl Into<Segment> for SqlKeyword {
    fn into(self) -> Segment {
        Segment::Keyword(self)
    }
}

#[allow(non_camel_case_types, unused)]
pub enum MatchSegment {
    GROUP_BY,
    ORDER_BY,
    NOT,
    AND,
    OR,
    AND_OR,
    EXISTS,
    HAVING,
    APPLY
}

impl MatchSegment {
    fn matches(&self, seg: &Segment) -> bool {
        match seg {
            Segment::Keyword(keyword) => {
                let keyword = keyword.format().to_lowercase();
                match *self {
                    MatchSegment::GROUP_BY => keyword.eq("group by"),
                    MatchSegment::ORDER_BY => keyword.eq("order by"),
                    MatchSegment::NOT => keyword.eq("not"),
                    MatchSegment::AND => keyword.eq("and"),
                    MatchSegment::OR => keyword.eq("or"),
                    MatchSegment::AND_OR => keyword.eq("and") || keyword.eq("or"),
                    MatchSegment::EXISTS => keyword.eq("exists"),
                    MatchSegment::HAVING => keyword.eq("having"),
                    MatchSegment::APPLY => keyword.eq("apply"),
                }
            },
            _ => {
                false
            }
        }
    }
}

impl SegmentList {
    pub fn get_sql_segment(&mut self) -> String {
        if self.is_empty() {
            return String::default();
        }
        match self.seg_type {
            SegmentType::GroupBy => {
                SPACE.to_string() + SqlKeyword::GROUP_BY.get_sql_segment().as_str() + SPACE  + self.segments.iter().map(|seg| seg.get_sql_segment()).collect::<Vec<String>>().join(COMMA).as_str()
            },
            SegmentType::Having => {
                SPACE.to_string() + SqlKeyword::HAVING.get_sql_segment().as_str() + SPACE + self.segments.iter().map(|seg| seg.get_sql_segment()).collect::<Vec<String>>().join(SPACE).as_str()
            },
            SegmentType::OrderBy => {
                SPACE.to_string() + SqlKeyword::ORDER_BY.get_sql_segment().as_str() + SPACE + self.segments.iter().map(|seg| seg.get_sql_segment()).collect::<Vec<String>>().join(SPACE).as_str()
            },
            SegmentType::Normal => {
                if MatchSegment::AND_OR.matches(&self.last_value.as_ref().unwrap_or(&Segment::Nil)) {
                    self.remove_and_flush_last();
                }
                LEFT_BRACKET.to_string() + self.segments.iter().map(|seg| match seg {
                    Segment::Text(val) => format!("{}", val),
                    Segment::Str(val) => format!("{}", val),
                     _ => seg.get_sql_segment(),
                }).collect::<Vec<String>>().join(SPACE).as_str() + RIGHT_BRACKET
            },
        }
        
    }
}

impl SegmentList {
    pub fn add_all(&mut self, segs: Vec<Segment>) -> bool {
        let seg_type = self.seg_type.to_owned();
        let first = segs.first();
        let last = segs.last();
       
        let mut segments = segs.to_vec();
        let goon = self.transform_list(&seg_type, &mut segments, first, last);
        if goon {
            if self.flush_last_value {
                self.remove_and_flush_last()
            }
            self.segments.extend_from_slice(segments.as_slice());
            true
        } else { false }
    }

    /**
     * 刷新属性 lastValue
     */
    fn _flush_last_value(&mut self) {
        self.last_value = self.segments.last().map(|seg| seg.to_owned());
    }

    fn clear(&mut self) {
        self.segments.clear();
        self.last_value = None;
        self.sql_segment.clear();
    }

    fn is_empty(&mut self) -> bool {
        self.segments.is_empty()
    }

    fn new(seg_type: SegmentType) -> Self {
        Self { seg_type, last_value: None, execute_not: true, flush_last_value: false, sql_segment: String::default(), segments: Vec::new() }
    }

    /**
     * 删除元素里最后一个值</br>
     * 并刷新属性 lastValue
     */
     fn remove_and_flush_last(&mut self) {
        self.segments.remove(self.segments.len() - 1);
        self.last_value = self.segments.last().map(|seg| seg.to_owned());
    }

    fn transform_list(&mut self, seg_type: &SegmentType, list: &mut Vec<Segment>, first: Option<&Segment>, last: Option<&Segment>) -> bool {
        match seg_type {
            SegmentType::GroupBy => { list.remove(0); true },
            SegmentType::Having => { if !list.is_empty() { list.push(SqlKeyword::AND.into()); } list.remove(0); true },
            SegmentType::OrderBy => { 
                list.remove(0); 
                // let sql = list.iter().map(|seg| seg.get_sql_segment()).collect::<Vec<String>>().join(SPACE);
                // list.clear(); 
                // list.push(Segment::Extenssion(sql));
                true
            },
            SegmentType::Normal => {
                let first = first.unwrap_or(&Segment::Nil);
                let last = last.unwrap_or(&Segment::Nil);
                if list.len() == 1 {
                    /* 只有 and() 以及 or() 以及 not() 会进入 */
                    if !MatchSegment::NOT.matches(first) {
                        //不是 not
                        if self.segments.is_empty() {
                            //sqlSegment是 and 或者 or 并且在第一位,不继续执行
                            return false;
                        }
                        let match_last_and = MatchSegment::AND.matches(last);
                        let match_last_or = MatchSegment::OR.matches(last);
                        if match_last_and || match_last_or {
                            //上次最后一个值是 and 或者 or
                            if match_last_and && MatchSegment::AND.matches(first) {
                                return false;
                            } else if match_last_or && MatchSegment::OR.matches(first) {
                                return false;
                            } else {
                                //和上次的不一样
                                self.remove_and_flush_last();
                            }
                        }
                    } else {
                        self.execute_not = false;
                        return false;
                    }
                } else {
                    if MatchSegment::APPLY.matches(first) {
                        list.remove(0);
                    }
                    if !MatchSegment::AND_OR.matches(last) && !self.segments.is_empty() {
                        self.segments.push(SqlKeyword::AND.into());
                    }
                    if !self.execute_not {
                        list.insert(0, SqlKeyword::NOT.into());
                        self.execute_not = true;
                    }
                }
                true
            },
        }
        
    }
}



impl MergeSegments {
    pub fn add(&mut self, segments: Vec<Segment>) {
        if !segments.is_empty() {
            let segment = &segments[0];
            if MatchSegment::ORDER_BY.matches(&segment) {
                self.order_by.add_all(segments);
            } else if MatchSegment::GROUP_BY.matches(&segment) {
                self.group_by.add_all(segments);
            } else if MatchSegment::HAVING.matches(&segment) {
                self.having.add_all(segments);
            } else {
                self.normal.add_all(segments);
            }
        }        
    }

    pub fn default() -> Self {
        Self { normal: SegmentList::new(SegmentType::Normal), group_by: SegmentList::new(SegmentType::GroupBy), order_by: SegmentList::new(SegmentType::OrderBy), having: SegmentList::new(SegmentType::Having) }
    }

    pub fn clear(&mut self) {
        self.normal.clear();
        self.group_by.clear();
        self.order_by.clear();
        self.having.clear();
    }
}

impl MergeSegments {
    pub fn get_sql_segment(&mut self) -> String {
        if self.normal.is_empty() {
            if !self.group_by.is_empty() || !self.order_by.is_empty() {
                self.group_by.get_sql_segment() + self.get_sql_segment().as_str() + self.get_sql_segment().as_str()
            } else {
                "".to_string()
            }
        } else {
            self.normal.get_sql_segment() + self.group_by.get_sql_segment().as_str() + self.having.get_sql_segment().as_str() + self.order_by.get_sql_segment().as_str()
        }
    }
}


impl SqlLike {
    pub fn concat_like(&self, val:Segment) -> String {
        let val = val.get_sql_segment().replace(SINGLE_QUOTE, EMPTY);
        match *self {
            SqlLike::DEFAULT => format!("'%{}%'", val),
            SqlLike::LEFT => format!("'%{}'", val),
            SqlLike::RIGHT => format!("'{}%'", val),
        }
    }
}

impl SqlSegment for SqlKeyword  {
    fn get_sql_segment(&self) -> String {
        match *self {
            Self::AND => "and",
            Self::OR => "or",
            Self::IN => "in",
            Self::NOT => "not",
            Self::LIKE => "like",
            Self::LIKE_LEFT => "like",
            Self::LIKE_RIGHT => "like",
            Self::EQ => "=",
            Self::NE => "<>",
            Self::GT => ">",
            Self::GE => ">=",
            Self::LT => "<",
            Self::LE => "<=",
            Self::IS_NULL => "is null",
            Self::IS_NOT_NULL => "is not null",
            Self::GROUP_BY => "group by",
            Self::HAVING => "having",
            Self::ORDER_BY => "order by",
            Self::EXISTS => "exists",
            Self::BETWEEN => "between",
            Self::ASC => "asc",
            Self::DESC => "desc",
            Self::APPLY => "apply",
        }.to_string()
    }
}

impl SqlKeyword {
    pub fn format(&self) -> &'static str {
        match *self {
            Self::AND => "and",
            Self::OR => "or",
            Self::IN => "in",
            Self::NOT => "not",
            Self::LIKE => "like",
            Self::LIKE_LEFT => "like",
            Self::LIKE_RIGHT => "like",
            Self::EQ => "=",
            Self::NE => "<>",
            Self::GT => ">",
            Self::GE => ">=",
            Self::LT => "<",
            Self::LE => "<=",
            Self::IS_NULL => "is null",
            Self::IS_NOT_NULL => "is not null",
            Self::GROUP_BY => "group by",
            Self::HAVING => "having",
            Self::ORDER_BY => "order by",
            Self::EXISTS => "exists",
            Self::BETWEEN => "between",
            Self::ASC => "asc",
            Self::DESC => "desc",
            Self::APPLY => "apply",
        }
    }
}