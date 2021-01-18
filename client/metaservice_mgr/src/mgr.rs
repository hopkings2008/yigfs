#[path = "./types.rs"] 
pub mod types;

pub trait MetaServiceMgr {
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<types::DirEntry>, String>;
}