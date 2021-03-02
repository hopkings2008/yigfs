use crate::io_thread::IoThread;
use common::runtime::Executor;
use common::numbers::NumberOp;

pub struct IoThreadPool {
    pool: Vec<IoThread>,
}

impl IoThreadPool {
    pub fn new(num: u32, exec: &Executor)->Self {
        let mut pool = IoThreadPool{
            pool: Vec::new(),
        };
        for i in 0..num {
            let thr = IoThread::create(&format!("IoThread{}", i+1), exec);
            pool.pool.push(thr);
        }
        return pool;
    }

    pub fn size(&self) -> u32 {
        self.pool.len() as u32
    }

    pub fn stop(&mut self) {
        for i in &mut self.pool {
            i.stop();
        }
        self.pool.clear();
    }

    pub fn get_worker(&self, id0: u64, id1: u64) -> &IoThread {
        let size = self.pool.len() as u32;
        let id = NumberOp::to_u128(id0, id1);
        // enhance the hash algo here later.
        let idx = (id % size as u128) as u32;
        &self.pool[idx as usize]
    }
}