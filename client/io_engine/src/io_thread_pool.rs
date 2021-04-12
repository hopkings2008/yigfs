use common::{numbers::NumberOp, runtime::Executor};

use crate::io_thread::IoThread;
use crate::io_worker::IoWorkerFactory;
use common::error::Errno;

pub struct IoThreadPool {
    pool: Vec<IoThread>,
}

impl IoThreadPool {
    pub fn new(num: u32, name: &String, exec: &Executor, worker_factory: &Box<dyn IoWorkerFactory>) -> Self{
        let mut pool = IoThreadPool {
            pool: Vec::new(),
        };
        for i in 0..num {
            pool.pool.push(IoThread::create(&format!("{}IoThread{}", name, i+1), exec, worker_factory));
        }

        return pool;
    }

    pub fn size(&self) -> u32 {
        self.pool.len() as u32
    }

    pub fn stop(&mut self) -> Errno {
        for t in &mut self.pool {
            let ret = t.stop();
            if !ret.is_success() {
                return ret;
            }
        }
        return Errno::Esucc;
    }

    pub fn get_thread(&self, id0: u64, id1: u64) -> &IoThread {
        let size = self.pool.len() as u32;
        let id = NumberOp::to_u128(id0, id1);
        let idx = (id % size as u128) as usize;
        &self.pool[idx]
    }
}