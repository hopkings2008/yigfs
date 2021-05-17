use crate::types::{DirEntry, FileLeader, 
    NewFileInfo, SetFileAttr, 
    Segment, HeartbeatResult};
use crate::types::FileAttr;
use common::error::Errno;

pub trait MetaServiceMgr: Send + Sync {
    fn mount(&self, uid: u32, gid: u32) -> Result<(), Errno>;
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, Errno>;
    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<FileAttr, Errno>;
    fn read_file_attr(&self, ino: u64) -> Result<FileAttr, Errno>;
    fn set_file_attr(&self, attr: &SetFileAttr) -> Result<FileAttr, Errno>;
    fn new_ino_leader(&self, parent: u64, name: &String, uid: u32, gid: u32, perm: u32) -> Result<NewFileInfo, Errno>;
    fn get_file_leader(&self, ino: u64) -> Result<FileLeader, Errno>;
    fn get_file_segments(&self, ino: u64, offset: Option<u64>, size: Option<i64>) -> Result<Vec<Segment>, Errno>;
    fn get_machine_id(&self) -> String;
    fn add_file_block(&self, ino: u64, seg: &Segment) -> Errno;
    fn update_file_segments(&self, ino: u64, segs: &Vec<Segment>) -> Errno;
    fn upload_segment(&self, id0: u64, id1: u64, next_offset: u64) -> Errno;
    fn heartbeat(&self)-> Result<HeartbeatResult, Errno>;
    fn delete_file(&self, ino: u64) -> Errno;
}