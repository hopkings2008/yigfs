
use common::error::Errno;
use std::collections::HashMap;

/*
* note that BackendStore must be implemented as threadsafe.
*/
pub trait BackendStore{
    // return: file size
    fn open(&self, id0: u64, id1: u64) -> Errno;
    fn write(&self, id0: u64, id1: u64, offset: u64, data: &[u8])->Errno;
    fn read(&self, id0: u64, id1: u64, offset: u64, size: u32)->Result<Vec<u8>, Errno>;
    fn close(&self, id0: u64, id1: u64) -> Errno;
}

pub trait BackendStoreFactory {
    // cfg is the configuration settings.
    fn new_backend_store(&self, cfg: &HashMap<String, String>) -> Result<Box<dyn BackendStore>, Errno>;
}