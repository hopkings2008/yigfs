

use crate::yig_io_worker::YigIoWorkerFactory;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::{backend_storage::{BackendStore, BackendStoreFactory}, types::{MsgFileOpResp, MsgFileOpenResp, MsgFileReadData, MsgFileReadOp, MsgFileWriteOp, MsgFileWriteResp}};
use io_engine::types::{MsgFileOp, MsgFileOpenOp};
use io_engine::io_thread_pool::IoThreadPool;
use std::collections::HashMap;
use crossbeam_channel::bounded;
use crossbeam_channel::Sender;
use std::sync::Arc;

pub struct YigBackend{
    bucket: String,
    yig_pool: IoThreadPool,
}

impl BackendStore for YigBackend{
    fn open(&self, id0: u64, id1: u64) -> Errno {
        let thr = self.yig_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
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
                match ret {
                    MsgFileOpResp::OpRespOpen(ret) => {
                        if !ret.err.is_success(){
                            println!("YigBackend::open: failed to open id0: {}, id1: {}, err: {:?}",
                            id0, id1, ret.err);
                        }
                        return ret.err;
                    }
                    _ => {
                        println!("YigBackend::open: got invalid open resp for id0: {}, id1: {}",
                        id0, id1);
                        return Errno::Eintr;
                    }
                }
            }
            Err(err) => {
                println!("YigBackend::open: failed to got result for open id0: {}, id1: {}, err: {}",
                id0, id1, err);
                return Errno::Eintr;
            }
        }
    }
    fn write(&self, id0: u64, id1: u64, offset: u64, data: &[u8])->MsgFileWriteResp{
        let mut result = MsgFileWriteResp{
            offset: offset,
            nwrite: 0,
            err: Errno::Eintr,
        };
        let thr = self.yig_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
        let msg = MsgFileWriteOp{
            id0: id0,
            id1: id1,
            dir: self.bucket.clone(),
            max_size: 0, // currently, not used.
            offset: offset,
            data: data.to_vec(),
            resp_sender: tx,
        };
        let ret = thr.do_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success(){
            println!("YigBackend::write: failed to send OpWrite for {}/id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
            self.bucket, id0, id1, offset, data.len(), ret);
            result.err = ret;
            return result;
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                match ret {
                    MsgFileOpResp::OpRespWrite(ret) => {
                        result.nwrite = ret.nwrite;
                        result.err = ret.err;
                    }
                    _ => {
                        println!("YigBackend::write: got invalid write resp for {}/id0: {}, id1: {}, offset: {}, size: {}",
                        self.bucket, id0, id1, offset, data.len());
                        result.err = Errno::Eintr;
                        return result;
                    }
                }
            }
            Err(err) => {
                println!("YigBackend::write: failed to recv write result for {}/id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
            self.bucket, id0, id1, offset, data.len(), err);
                result.err = Errno::Eintr;
            }
        }
        result
    }

    fn write_async(&self, id0: u64, id1: u64, offset: u64, data: &[u8], resp_sender: Sender<MsgFileOpResp>)->Errno{
        let thr = self.yig_pool.get_thread(id0, id1);
        let msg = MsgFileWriteOp{
            id0: id0,
            id1: id1,
            dir: self.bucket.clone(),
            max_size: 0, // currently, not used.
            offset: offset,
            data: data.to_vec(),
            resp_sender: resp_sender,
        };
        let ret = thr.do_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success(){
            println!("YigBackend::write_async: failed to send OpWrite for {}/id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
            self.bucket, id0, id1, offset, data.len(), ret);
            return ret;
        }
        return Errno::Esucc;
    }

    fn read(&self, id0: u64, id1: u64, offset: u64, size: u32)->Result<Option<Vec<u8>>, Errno>{
        let thr = self.yig_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
        let msg_read = MsgFileReadOp{
            id0: id0,
            id1: id1,
            dir: self.bucket.clone(),
            offset: offset,
            size: size,
            data_sender: tx,
        };
        let ret = thr.do_io(MsgFileOp::OpRead(msg_read));
        if !ret.is_success(){
            println!("YigBackend::read: failed to send OpRead for bucket: {}, id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
            self.bucket, id0, id1, offset, size, ret);
            return Err(ret);
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                match ret {
                    MsgFileOpResp::OpRespRead(ret) => {
                        if ret.err.is_success() {
                            return Ok(ret.data);
                        }
                        println!("YigBackend::read: failed to read bucket: {}, id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
                    self.bucket, id0, id1, offset, size, ret);
                        return Err(ret.err);
                    }
                    _ => {
                        println!("YigBackend::read: got invalid read resp for bucket: {}, id0: {}, id1: {}, offset: {}, size: {}",
                        self.bucket, id0, id1, offset, size);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                println!("YigBackend::read: failed to recv read resp for bucket: {}, id0: {}, id1: {}, offset: {}, size: {}, err: {}",
            self.bucket, id0, id1, offset, size, err);
                return Err(Errno::Eintr);
            }
        }
    }

    fn read_async(&self, id0: u64, id1: u64, offset: u64, size: u32, resp_sender: Sender<MsgFileOpResp>) -> Errno{
        let thr = self.yig_pool.get_thread(id0, id1);
        let msg_read = MsgFileReadOp{
            id0: id0,
            id1: id1,
            dir: self.bucket.clone(),
            offset: offset,
            size: size,
            data_sender: resp_sender,
        };
        let ret = thr.do_io(MsgFileOp::OpRead(msg_read));
        if !ret.is_success(){
            println!("YigBackend::read_async: failed to send OpRead for bucket: {}, id0: {}, id1: {}, offset: {}, size: {}, err: {:?}",
            self.bucket, id0, id1, offset, size, ret);
            return ret;
        }
        return Errno::Esucc;
    }
    
    fn close(&self, _id0: u64, _id1: u64) -> Errno{
        Errno::Esucc
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
    fn new_backend_store(&self, cfg: &HashMap<String, String>) -> Result<Arc<dyn BackendStore>, Errno>{
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

        Ok(Arc::new(YigBackend::new(&region, &endpoint, &ak, &sk, &bucket, thread_num, &self.exec)))
    }
}

impl YigBackendFactory {
    pub fn new(exec: &Executor) -> Box<dyn BackendStoreFactory> {
        Box::new(YigBackendFactory{
            exec: exec.clone(),
        })
    }
}