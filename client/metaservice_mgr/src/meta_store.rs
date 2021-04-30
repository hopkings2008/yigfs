
use crossbeam_channel::Sender;

use common::error::Errno;
use crate::{meta_op::MetaOpResp, meta_thread_pool::MetaThreadPool, mgr::MetaServiceMgr};
use std::sync::Arc;

pub struct MetaStore{
    meta_pool: MetaThreadPool,
}

impl MetaStore{
    pub fn new(thread_num: u32, mgr: Arc<dyn MetaServiceMgr>)->Self{
        MetaStore{
            meta_pool: MetaThreadPool::new(
                thread_num, 
                &String::from("MetaThread"), 
            mgr),
        }
    }

    pub fn stop(&self) {
        self.meta_pool.stop();
    }

    pub fn upload_segment_async(&self, id0: u64, id1: u64, offset: u64, resp_tx: Sender<MetaOpResp>) -> Errno {
        let thr = self.meta_pool.get_meta_thread_for_seg(id0, id1);
        let ret = thr.upload_segment(id0, id1, offset, resp_tx);
        if !ret.is_success() {
            println!("upload_segment_async: failed to upload segment for id0: {}, id1: {}, offset: {}, err: {:?}",
            id0, id1, offset, ret);
        }
        return ret;
    }
}