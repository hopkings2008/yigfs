use crate::types::{DirEntry, FileLeader};
use crate::types::FileAttr;
use crate::common::error::Errno;

pub trait MetaServiceMgr {
    fn mount(&self) -> Result<(), Errno>;
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, Errno>;
    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<FileAttr, Errno>;
    fn read_file_attr(&self, ino: u64) -> Result<FileAttr, Errno>;
    fn get_file_leader(&self, ino: u64, flag: u8) -> Result<FileLeader, Errno>;
}