use crate::types::DirEntry;

pub trait MetaServiceMgr {
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, String>;
}