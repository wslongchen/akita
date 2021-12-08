//! 
//! Common Params
//! 

use crate::information::Capacity;

pub static AMPERSAND: &str = "&";
pub static AND: &str = "and";
pub static AT: &str = "@";
pub static ASTERISK: &str = "*";
pub static STAR: &str = ASTERISK;
pub static BACK_SLASH: &str = "\\";
pub static COLON: &str = ":";
pub static COMMA: &str = ",";
pub static DASH: &str = "-";
pub static DOLLAR: &str = "$";
pub static DOT: &str = ".";
pub static DOTDOT: &str = "..";
pub static DOT_CLASS: &str = ".class";
pub static DOT_JAVA: &str = ".java";
pub static DOT_XML: &str = ".xml";
pub static EMPTY: &str = "";
pub static EQUALS: &str = "=";
pub static FALSE: &str = "false";
pub static SLASH: &str = "/";
pub static HASH: &str = "#";
pub static HAT: &str = "^";
pub static LEFT_BRACE: &str = "{";
pub static LEFT_BRACKET: &str = "(";
pub static LEFT_CHEV: &str = "<";
pub static DOT_NEWLINE: &str = ",\n";
pub static NEWLINE: &str = "\n";
pub static N: &str = "n";
pub static NO: &str = "no";
pub static NULL: &str = "null";
pub static OFF: &str = "off";
pub static ON: &str = "on";
pub static PERCENT: &str = "%";
pub static PIPE: &str = "|";
pub static PLUS: &str = "+";
pub static QUESTION_MARK: &str = "?";
pub static EXCLAMATION_MARK: &str = "!";
pub static QUOTE: &str = "\"";
pub static RETURN: &str = "\r";
pub static TAB: &str = "\t";
pub static RIGHT_BRACE: &str = "}";
pub static RIGHT_BRACKET: &str = ")";
pub static RIGHT_CHEV: &str = ">";
pub static SEMICOLON: &str = ";";
pub static SINGLE_QUOTE: &str = "'";
pub static BACKTICK: &str = "`";
pub static SPACE: &str = " ";
pub static TILDA: &str = "~";
pub static LEFT_SQ_BRACKET: &str = "[";
pub static RIGHT_SQ_BRACKET: &str = "]";
pub static TRUE: &str = "true";
pub static UNDERSCORE: &str = "_";
pub static UTF_8: &str = "UTF-8";
pub static US_ASCII: &str = "US-ASCII";
pub static ISO_8859_1: &str = "ISO-8859-1";
pub static Y: &str = "y";
pub static YES: &str = "yes";
pub static ONE: &str = "1";
pub static ZERO: &str = "0";
pub static DOLLAR_LEFT_BRACE: &str = "${";
pub static HASH_LEFT_BRACE: &str = "#{";
pub static CRLF: &str = "\r\n";

pub static HTML_NBSP: &str = "&nbsp;";
pub static HTML_AMP: &str = "&amp";
pub static HTML_QUOTE: &str = "&quot;";
pub static HTML_LT: &str = "&lt;";
pub static HTML_GT: &str = "&gt;";
pub static WRAPPER_PARAM: &str = "MPGENVAL";
pub static PRIMARY_KEY: &str = "primary key";
pub static UNIQUE_KEY: &str = "unique key";
pub static KEY: &str = "key";
pub static CONSTRAINT: &str = "key";
pub static FOREIGN_KEY: &str = "FOREIGN KEY";
pub static REFERENCES: &str = "REFERENCES";
pub static DELETE: &str = "DELETE";
pub static UPDATE: &str = "UPDATE";
pub static CASCADE: &str = "CASCADE";


pub fn extract_datatype_with_capacity(data_type: &str) -> (String, Option<Capacity>) {
    let start = data_type.find('(');
    let end = data_type.find(')');
    if let Some(start) = start {
        if let Some(end) = end {
            let dtype = &data_type[0..start];
            let range = &data_type[start + 1..end];
            let capacity = if range.contains(',') {
                let splinters = range.split(',').collect::<Vec<&str>>();
                assert!(splinters.len() == 2, "There should only be 2 parts");
                let range1: Result<i32, _> = splinters[0].parse();
                let range2: Result<i32, _> = splinters[1].parse();
                match range1 {
                    Ok(r1) => match range2 {
                        Ok(r2) => Some(Capacity::Range(r1, r2)),
                        Err(_e) => {
                            None
                        }
                    },
                    Err(_e) => {
                        None
                    }
                }
            } else {
                let limit: Result<i32, _> = range.parse();
                match limit {
                    Ok(limit) => Some(Capacity::Limit(limit)),
                    Err(_e) => {
                        None
                    }
                }
            };
            (dtype.to_owned(), capacity)
        } else {
            (data_type.to_owned(), None)
        }
    } else {
        (data_type.to_owned(), None)
    }
}


fn trim_parenthesis(arg: &str) -> &str {
    arg.trim_start_matches('(').trim_end_matches(')')
}

pub fn maybe_trim_parenthesis(arg: &str) -> &str {
    if arg.starts_with('(') && arg.ends_with(')') {
        trim_parenthesis(arg)
    } else {
        arg
    }
}

fn is_keyword(s: &str) -> bool {
    let keywords = ["user", "role"];
    keywords.contains(&s)
}

pub fn keywords_safe(s: &str) -> String {
    if is_keyword(s) {
        format!("\"{}\"", s)
    } else {
        s.to_string()
    }
}