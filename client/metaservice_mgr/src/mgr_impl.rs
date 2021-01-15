use crate::mgr;
pub struct MetaServiceMgrImpl{
}

impl mgr::MetaServiceMgr for MetaServiceMgrImpl{
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<mgr::types::DirEntry>, Box<dyn std::error::Error>>{
        let entrys = Vec::new();
        return Ok(entrys);
    }
}