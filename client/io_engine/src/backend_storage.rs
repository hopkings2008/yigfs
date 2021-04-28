
use common::error::Errno;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::sync::Arc;

use crate::types::{MsgFileOp, MsgFileOpResp, MsgFileReadData, MsgFileWriteResp};



/*
* note that BackendStore must be implemented as threadsafe.
*/
pub trait BackendStore{
    // return: file size
    fn open(&self, id0: u64, id1: u64) -> Errno;
    fn write(&self, id0: u64, id1: u64, offset: u64, data: &[u8])->MsgFileWriteResp;
    fn write_async(&self, id0: u64, id1: u64, offset: u64, data: &[u8], resp_sender: Sender<MsgFileOpResp>)->Errno;
    fn read(&self, id0: u64, id1: u64, offset: u64, size: u32)->Result<Option<Vec<u8>>, Errno>;
    fn read_async(&self, id0: u64, id1: u64, offset: u64, size: u32, resp_sender: Sender<MsgFileOpResp>) -> Errno;
    fn close(&self, id0: u64, id1: u64) -> Errno;
}

pub trait BackendStoreFactory {
    // cfg is the configuration settings.
    fn new_backend_store(&self, cfg: &HashMap<String, String>) -> Result<Arc<dyn BackendStore>, Errno>;
}