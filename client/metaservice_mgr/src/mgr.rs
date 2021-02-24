use crate::types::{DirEntry, FileLeader, NewFileInfo, SetFileAttr, Segment};
use crate::types::FileAttr;
use common::error::Errno;

pub trait MetaServiceMgr {
    fn mount(&self, uid: u32, gid: u32) -> Result<(), Errno>;
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, Errno>;
    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<FileAttr, Errno>;
    fn read_file_attr(&self, ino: u64) -> Result<FileAttr, Errno>;
    fn set_file_attr(&self, attr: &SetFileAttr) -> Result<FileAttr, Errno>;
    fn new_ino_leader(&self, parent: u64, name: &String, uid: u32, gid: u32, perm: u32) -> Result<NewFileInfo, Errno>;
    fn get_file_leader(&self, ino: u64) -> Result<FileLeader, Errno>;
    fn get_file_segments(&self, ino: u64, offset: Option<u64>, size: Option<i64>) -> Result<Vec<Segment>, Errno>;
}