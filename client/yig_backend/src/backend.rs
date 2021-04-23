

use crate::yig_io_worker::YigIoWorkerFactory;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::backend_storage::{BackendStore, BackendStoreFactory};
use io_engine::types::{MsgFileOp, MsgFileOpenOp};
use io_engine::io_thread_pool::IoThreadPool;
use std::collections::HashMap;
use crossbeam_channel::bounded;

pub struct YigBackend{
    bucket: String,
    yig_pool: IoThreadPool,
}

impl BackendStore for YigBackend{
    fn open(&self, id0: u64, id1: u64) -> Errno {
        let thr = self.yig_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<Errno>(1);
        let msg = MsgFileOpenOp{
            id0: id0,
            id1: id1,
            dir: self.bucket.clone(),
            resp_sender: tx,
        };
        let ret = thr.do_io(MsgFileOp::OpOpen(msg));
        if !ret.is_success() {
            println!("YigBackend::open: failed to send io open req for id0: {}, id1: {}, err: {:?}",
            id0, id1, ret);
            return ret;
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                return ret;
            }
            Err(err) => {
                println!("YigBackend::open: failed to got result for open id0: {}, id1: {}, err: {}",
                id0, id1, err);
                return Errno::Eintr;
            }
        }
    }
    fn write(&self, id0: u64, id1: u64, offset: u64, data: &[u8])->Errno{
        Errno::Enotsupp
    }
    fn read(&self, id0: u64, id1: u64, offset: u64, size: u32)->Result<Vec<u8>, Errno>{
        Err(Errno::Enotsupp)
    }
    fn close(&self, id0: u64, id1: u64) -> Errno{
        Errno::Enotsupp
    }
}

impl YigBackend {
    pub fn new(region: &String, endpoint: &String, ak: &String, sk: &String, bucket: &String, num: u32, exec: &Executor) -> Self {
        YigBackend{
            bucket: bucket.clone(),
            yig_pool: IoThreadPool::new(num, 
                &format!("yig_io_thread_"), 
                exec, 
  &YigIoWorkerFactory::new(region, endpoint, ak, sk)),
        }
    }
}

pub struct YigBackendFactory{
    exec: Executor,
}

impl BackendStoreFactory for YigBackendFactory{
    fn new_backend_store(&self, cfg: &HashMap<String, String>) -> Result<Box<dyn BackendStore>, Errno>{
        let region: String;
        let endpoint: String;
        let ak: String;
        let sk: String;
        let bucket: String;
        let thread_num: u32;

        if let Some(r) = cfg.get("region"){
            region = r.clone();
        } else {
            println!("new_backend_store: missing region setting");
            return Err(Errno::Eintr);
        }

        if let Some(e) = cfg.get("endpoint"){
            endpoint = e.clone();
        } else {
            println!("new_backend_store: missing endpoint setting");
            return Err(Errno::Eintr);
        }

        if let Some(a) = cfg.get("ak") {
            ak = a.clone();
        } else {
            println!("new_backend_store: missing ak");
            return Err(Errno::Eintr);
        }

        if let Some(s) = cfg.get("sk") {
            sk = s.clone();
        } else {
            println!("new_backend_store: missing sk");
            return Err(Errno::Eintr);
        }

        if let Some(n) = cfg.get("thread_num") {
            thread_num = n.parse::<u32>().unwrap();
        } else {
            println!("new_backend_store: missing thread_num");
            return Err(Errno::Eintr);
        }

        if let Some(b) = cfg.get("bucket") {
            bucket = b.clone();
        } else {
            println!("new_backend_store: missing bucket");
            return Err(Errno::Eintr);
        }

        Ok(Box::new(YigBackend::new(&region, &endpoint, &ak, &sk, &bucket, thread_num, &self.exec)))
    }
}

impl YigBackendFactory {
    pub fn new(exec: &Executor) -> Box<dyn BackendStoreFactory> {
        Box::new(YigBackendFactory{
            exec: exec.clone(),
        })
    }
}