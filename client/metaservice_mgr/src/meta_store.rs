
use std::collections::HashMap;
use crossbeam_channel::Sender;

use common::error::Errno;
use crate::{meta_op::MetaOpResp, meta_thread_pool::MetaThreadPool, mgr::MetaServiceMgr};
use crate::types::Segment;
use std::sync::Arc;
use log::{error};

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

    pub fn upload_segment_async(&self, id0: u64, id1: u64, offset: u64, resp_tx: Sender<MetaOpResp>) -> Errno {
        let thr = self.meta_pool.get_meta_thread_for_seg(id0, id1);
        let ret = thr.upload_segment(id0, id1, offset, resp_tx);
        if !ret.is_success() {
            error!("upload_segment_async: failed to upload segment for id0: {}, id1: {}, offset: {}, err: {:?}",
            id0, id1, offset, ret);
        }
        return ret;
    }

    pub fn update_changed_segments(&self, ino: u64, segs: &HashMap<u128, Segment>, garbages: &HashMap<u128, Segment>) -> Errno {
        let mut vsegs: Vec<Segment> = Vec::new();
        let mut vgarbages: Vec<Segment> = Vec::new();

        for (_, s) in segs {
            vsegs.push(s.copy());
        }
        for (_, s) in garbages {
            vgarbages.push(s.copy());
        }

        //let thr = self.meta_pool.get_meta_thread_roundrobin();
        let thr = self.meta_pool.get_meta_thread_for_seg(ino, 0);
        let ret = thr.update_changed_segments(ino, vsegs, vgarbages);
        if !ret.is_success(){
            error!("update_changed_segments: failed to upload changed segments for ino: {}, err: {:?}", ino, ret);
        }
        return ret;
    }
}

impl Drop for MetaStore{
    fn drop(&mut self){
        self.meta_pool.stop();
    }
}