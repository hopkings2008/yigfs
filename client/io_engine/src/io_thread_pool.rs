
use crate::io_thread::IoThread;
use common::runtime::Executor;

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
}