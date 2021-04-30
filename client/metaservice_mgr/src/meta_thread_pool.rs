use std::sync::Arc;
use common::numbers::NumberOp;

use crate::{meta_thread::MetaThread, mgr::MetaServiceMgr};



pub struct MetaThreadPool{
    pool: Vec<MetaThread>,
}

impl MetaThreadPool {
    pub fn new(num: u32, prefix: &String, mgr: Arc<dyn MetaServiceMgr>) -> Self {
        let mut mp = MetaThreadPool{
            pool: Vec::new(),
        };
        for i in 0..num {
            let name = format!("{}_{}", prefix, i);
            mp.pool.push(MetaThread::new(
                &name,
                mgr.clone(),
            ))
        }
        return mp;
    }

    pub fn stop(&self){
        for t in &self.pool {
            t.stop();
        }
    }

    pub fn num(&self) -> u32 {
        self.pool.len() as u32
    }

    pub fn get_meta_thread_for_seg(&self, id0: u64, id1: u64) -> &MetaThread{
        let id = NumberOp::to_u128(id0, id1);
        let size = self.pool.len() as u32;
        let idx = (id % size as u128) as usize;
        &self.pool[idx]
    }
}