use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use common::numbers::NumberOp;

use crate::{meta_thread::MetaThread, mgr::MetaServiceMgr};



pub struct MetaThreadPool{
    pool: Vec<MetaThread>,
    idx: AtomicUsize,
}

impl MetaThreadPool {
    pub fn new(num: u32, prefix: &String, mgr: Arc<dyn MetaServiceMgr>) -> Self {
        let mut mp = MetaThreadPool{
            pool: Vec::new(),
            idx: AtomicUsize::new(0),
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

    pub fn stop(&mut self){
        for t in &mut self.pool {
            t.stop();
        }
    }

    pub fn num(&self) -> u32 {
        self.pool.len() as u32
    }

    pub fn get_meta_thread_for_seg(&self, id0: u64, id1: u64) -> &MetaThread{
        let id = NumberOp::to_u128(id0, id1);
        let size = self.num();
        let idx = (id % size as u128) as usize;
        &self.pool[idx]
    }

    pub fn get_meta_thread_roundrobin(&self) -> &MetaThread{
        let total = self.pool.len();
        let mut id = self.idx.load(Ordering::Relaxed);
        let mut idx = (id + 1) % total;
        loop {
            let ret = self.idx.compare_exchange(id, idx, Ordering::Acquire, Ordering::Relaxed);
            match ret {
                Ok(ret) => {
                    return &self.pool[ret];
                }
                Err(ret) => {
                    id = ret;
                    idx = (id + 1) % total;
                }
            }
        }
    }
}