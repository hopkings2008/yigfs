use crate::types::MsgFileReadData;
use common::{error::Errno, runtime::Executor};
use crossbeam_channel::Sender;

#[derive(Debug, Default)]
pub struct CacheWriteResult {
    // from which for this write operation.
    pub offset: u64,
    // data length of the write for this write operation.
    pub nwrite: u32,
}

pub trait CacheStore{
    // return: file size
    fn open(&self, id0: u64, id1: u64, dir: &String) -> Errno;
    // capacity: the max size of one cache file.
    // add capacity in this api to avoid maintain it in cache implementation.
    fn write(&self, id0: u64, id1: u64, dir: &String, offset: u64, capacity: u64, data: &[u8])->Result<CacheWriteResult, Errno>;
    fn read(&self, id0: u64, id1: u64, dir: &String, offset: u64, size: u32)->Result<Option<Vec<u8>>, Errno>;
    // for backup to backend store.
    // read_resp is used for the cache thread to send read response throught it,
    // and can be used to implement the pipeline pattern.
    fn read_async(&self, id0: u64, id1: u64, dir: &String, offset: u64, size: u32, read_resp: Sender<MsgFileReadData>) -> Errno;
    fn close(&self, id0: u64, id1: u64) -> Errno;
}

pub struct CacheStoreConfig{
    pub thread_num: u32,
}

pub trait CacheStoreFactory {
    // cfg is the configuration settings.
    fn new_cache_store(&self, cfg: &CacheStoreConfig, exec: &Executor) -> Result<Box<dyn CacheStore>, Errno>;
}