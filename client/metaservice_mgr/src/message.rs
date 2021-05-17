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
    pub uid: u32,
    pub gid: u32,
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

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqFileCreate {
    pub zone: String,
    pub machine: String,
    pub region: String,
    pub bucket: String,
    #[serde(rename(serialize = "parent_ino", deserialize = "parent_ino"))]
    pub ino: u64,
    #[serde(rename(serialize = "file_name", deserialize = "file_name"))]
    pub name: String,
    pub uid: u32,
    pub gid: u32, 
    pub perm: u32,
}


#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespFileCreate {
    pub result: RespResult,
    pub leader_info: MsgLeaderInfo,
    #[serde(rename(serialize = "file", deserialize = "file"))]
    pub file_info: MsgFileAttr,
}


#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgSetFileAttr {
    /// ino of the file.
    pub ino: u64,
    /// Size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<u64>,
    /// Time of last access
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub atime: Option<i64>,
    /// Time of last modification
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub mtime: Option<i64>,
    /// Time of last change
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub ctime: Option<i64>,
    /// Permissions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub perm: Option<u16>,
    /// User id
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub uid: Option<u32>,
    /// Group id
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub gid: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqSetFileAttr {
    pub region: String,
    pub bucket: String,
    #[serde(rename(serialize = "file", deserialize = "file"))]
    pub attr: MsgSetFileAttr,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespSetFileAttr {
    pub result: RespResult,
    #[serde(rename(serialize = "file", deserialize = "file"))]
    pub attr: MsgFileAttr,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgSegment {
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub capacity: u64,
    pub size: u64,
    pub backend_size: u64,
    pub leader: String,
    pub blocks: Vec<MsgBlock>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgBlock {
    pub offset: u64,
    pub seg_start_addr: u64,
    pub seg_end_addr: u64,
    pub size: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqGetSegments {
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
    pub ino: u64,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespGetSegments {
    pub result: RespResult,
    pub segments: Vec<MsgSegment>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqUpdateSegments{
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub ino: u64,
    pub generation: u64,
    pub segments: Vec<MsgSegment>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespUpdateSegments {
    pub result: RespResult,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqAddBlock{
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
    pub ino: u64,
    pub generation: u64,
    pub segment: MsgSegment,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespAddBock{
    pub result: RespResult,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgSegmentOffset{
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub backend_size: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqUploadSegment{
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
    #[serde(rename(serialize = "segment", deserialize = "segment"))]
    pub segment: MsgSegmentOffset,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespUploadSegment{
    pub result: RespResult,
}


#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqHeartbeat{
    pub region: String,
    pub bucket: String,
    pub zone: String,
    pub machine: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgUploadSegment{
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub next_offset: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MsgRemoveSegment{
    pub seg_id0: u64,
    pub seg_id1: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespHeartbeat {
    pub result: RespResult,
    pub upload_segments: Vec<MsgUploadSegment>,
    pub remove_segments: Vec<MsgRemoveSegment>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ReqDeleteFile {
    pub region: String,
    pub bucket: String,
    pub ino: u64,
    pub zone: String,
    pub machine: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RespDeleteFile {
    pub result: RespResult,
}