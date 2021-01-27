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

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqDirFileAttr {
    pub region: String,
    pub bucket: String,
    #[serde(rename(serialize = "parent_ino", deserialize = "parent_ino"))]
    pub ino: u64,
    #[serde(rename(serialize = "file_name", deserialize = "file_name"))]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgFileAttr {
    /// Inode number
    pub ino: u64,
    /// genration when ino has collision
    pub generation: u64,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: i64,
    /// Time of last modification
    pub mtime: i64,
    /// Time of last change
    pub ctime: i64,
    /// Kind of file (directory, file, pipe, etc)
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub kind: u8,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    #[serde(default)]
    pub rdev: u32,
    /// Flags (macOS only, see chflags(2))
    #[serde(default)]
    pub flags: u32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespDirFileAttr{
    pub result: RespResult,
    #[serde(rename(serialize = "file", deserialize = "file"))]
    pub attr: MsgFileAttr,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqFileAttr{
    pub region: String,
    pub bucket: String,
    pub ino: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespFileAttr {
    pub result: RespResult,
    #[serde(rename(serialize = "file", deserialize = "file"))]
    pub attr: MsgFileAttr,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqMount{
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespMount{
    pub result: RespResult,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqFileLeader {
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
    pub ino: u64,
    pub flag: u8,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgLeaderInfo {
    pub zone: String,
    pub leader: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespFileLeader {
    pub result: RespResult,
    pub leader_info: MsgLeaderInfo,
}