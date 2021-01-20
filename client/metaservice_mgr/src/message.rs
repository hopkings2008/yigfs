extern crate serde;

use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqReadDir {
    pub region: String,
    pub bucket: String,
    pub ino: u64,
    pub offset: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespResult {
    pub err_code: i64,
    pub err_msg: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespDirEntry {
    pub ino: u64,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub dir_entry_type: u8,
    #[serde(rename(serialize = "file_name", deserialize = "file_name"))]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespReadDir{
    pub result: RespResult,
    pub offset: i64,
    pub files: Vec<RespDirEntry>,
}