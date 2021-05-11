
use crate::{cache_store::CacheStatResult, io_thread_pool::IoThreadPool, types::{MsgFileStatOp, MsgFileStatResult}};
use crate::types::{MsgFileOp, MsgFileOpenOp, MsgFileWriteOp, MsgFileReadOp, MsgFileCloseOp, MsgFileOpResp};
use crate::cache_store::{CacheStore, CacheStoreFactory, CacheStoreConfig, CacheWriteResult};
use crate::disk_io_worker::DiskIoWorkerFactory;
use common::runtime::Executor;
use common::error::Errno;
use crossbeam_channel::{bounded, Sender};
use std::sync::Arc;

// must be implemented as thread safe.
pub struct DiskCache {
    disk_pool: IoThreadPool,
}

impl CacheStore for DiskCache {
    // return: file size
    fn open(&self, id0: u64, id1: u64, dir: &String) -> Errno{
        let worker = self.disk_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
        let msg = MsgFileOpenOp{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            resp_sender: tx,
        };
        let ret = worker.do_io(MsgFileOp::OpOpen(msg));
        if !ret.is_success() {
            println!("open(id0: {}, id1: {}, dir: {}): failed to send open msg, err: {:?}",
            id0, id1, dir, ret);
            return ret;
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                match ret {
                    MsgFileOpResp::OpRespOpen(e) => {
                        if !e.err.is_success() {
                            println!("open(id0: {}, id1: {}, dir: {}) failed with errno: {:?}", id0, id1, dir, e.err);
                            return e.err;
                        }
                        return Errno::Esucc;
                    }
                    _ => {
                        println!("open(id0: {}, id1: {}, dir: {}), got invalid resp", id0, id1, dir);
                        return Errno::Eintr;
                    }
                }
                
            }
            Err(err) => {
                println!("open: failed to get response for (id0: {}, id1: {}, dir: {}), err: {}", 
                id0, id1, dir, err);
                return Errno::Eintr;
            }
        }
    }

    fn open_async(&self, id0: u64, id1: u64, dir: &String, open_resp: Sender<MsgFileOpResp>) -> Errno{
        let worker = self.disk_pool.get_thread(id0, id1);
        let msg = MsgFileOpenOp{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            resp_sender: open_resp,
        };
        let ret = worker.do_io(MsgFileOp::OpOpen(msg));
        if !ret.is_success() {
            println!("open(id0: {}, id1: {}, dir: {}): failed to send open msg, err: {:?}",
            id0, id1, dir, ret);
            return ret;
        }
        return Errno::Esucc;
    }

    // capacity: the max size of one cache file.
    // add capacity in this api to avoid maintain it in cache implementation.
    fn write(&self, id0: u64, id1: u64, dir: &String, offset: u64, capacity: u64, data: &[u8])->Result<CacheWriteResult, Errno>{
        let worker = self.disk_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
        let msg = MsgFileWriteOp{
            id0: id0,
            id1: id1,
            max_size: capacity,
            dir: dir.clone(),
            offset: offset, // the file offset is not used currently.
            data: data.to_vec(),
            resp_sender: tx,
        };
        let ret = worker.do_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success() {
            println!("write: failed to send_disk_io for seg(id0: {}, id1: {}, dir: {}), err: {:?}",
            id0, id1, dir, ret);
            return Err(Errno::Eintr);
        }
        let ret = rx.recv();
        match ret{
            Ok(ret) => {
                match ret {
                    MsgFileOpResp::OpRespWrite(ret) => {
                        if ret.err.is_success() {
                            return Ok(CacheWriteResult{
                                offset: ret.offset,
                                nwrite: ret.nwrite,
                            });
                        }
                        return Err(ret.err);
                    }
                    _ => {
                        println!("write: failed to get resp for seg(id0: {}, id1: {}, dir: {})",
                        id0, id1, dir);
                        return Err(Errno::Eintr);
                    }
                }
                
            }
            Err(err) => {
                println!("disk_cache_store: write: failed to recv write result for seg(id0: {}, id1: {}, dir: {}), err: {}",
            id0, id1, dir, err);
            return Err(Errno::Eintr);
            }
        }
    }

    fn write_async(&self, id0: u64, id1: u64, dir: &String, offset: u64, capacity: u64, data: &[u8], write_resp: Sender<MsgFileOpResp>) -> Errno{
        let worker = self.disk_pool.get_thread(id0, id1);
        let msg = MsgFileWriteOp{
            id0: id0,
            id1: id1,
            max_size: capacity,
            dir: dir.clone(),
            offset: offset, // the file offset is not used currently.
            data: data.to_vec(),
            resp_sender: write_resp,
        };
        let ret = worker.do_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success() {
            println!("write: failed to send_disk_io for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}",
            id0, id1, dir, offset, ret);
            return ret;
        }
        return Errno::Esucc;
    }

    fn read(&self, id0: u64, id1: u64, dir: &String, offset: u64, size: u32)->Result<Option<Vec<u8>>, Errno>{
        let worker = self.disk_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileOpResp>(1);
        let msg = MsgFileReadOp{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            offset: offset,
            size: size,
            data_sender: tx,
        };

        let ret = worker.do_io(MsgFileOp::OpRead(msg));
        if !ret.is_success(){
            println!("disk_cache_store: read: failed to send read op for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}", 
            id0, id1, dir, offset, ret);
            return Err(ret);
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                match ret {
                    MsgFileOpResp::OpRespRead(ret) => {
                        if ret.err.is_success() {
                            return Ok(ret.data);
                        } else if ret.err.is_eof() {
                            println!("disk_cache_store: read: read eof for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}", 
                    id0, id1, dir, offset, ret.err);
                            return Err(Errno::Eeof);
                        } else {
                            println!("disk_cache_store: read: failed to read data for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}", 
                    id0, id1, dir, offset, ret.err);
                            return Err(ret.err);
                        }
                    }
                    _ => {
                        println!("disk_cache_store: read: failed to get invalid resp for seg(id0: {}, id1: {}, dir: {}",
                    id0, id1, dir);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                println!("disk_cache_store: read: failed to recv read resp for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}", 
            id0, id1, dir, offset, err);
                return Err(Errno::Eintr);
            }
        }
    }
    // for backup to backend store.
    // read_resp is used for the cache thread to send read response throught it,
    // and can be used to implement the pipeline pattern.
    fn read_async(&self, id0: u64, id1: u64, dir: &String, offset: u64, size: u32, read_resp: Sender<MsgFileOpResp>) -> Errno{
        let worker = self.disk_pool.get_thread(id0, id1);
        let msg = MsgFileReadOp{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            offset: offset,
            size: size,
            data_sender: read_resp,
        };

        let ret = worker.do_io(MsgFileOp::OpRead(msg));
        if !ret.is_success(){
            println!("disk_cache_store: read: failed to send read op for seg(id0: {}, id1: {}, dir: {}), offset: {}, err: {:?}", 
            id0, id1, dir, offset, ret);
            return ret;
        }
        return Errno::Esucc;
    }

    fn stat(&self, id0: u64, id1: u64) -> Result<CacheStatResult, Errno>{
        let worker = self.disk_pool.get_thread(id0, id1);
        let (tx, rx) = bounded::<MsgFileStatResult>(1);
        let msg = MsgFileStatOp{
            id0: id0,
            id1: id1,
            result_tx: tx,
        };
        let ret = worker.do_io(MsgFileOp::OpStat(msg));
        if !ret.is_success(){
            println!("disk_cache_store::stat: failed to perform stat for seg: id0: {}, id1: {}, err: {:?}",
            id0, id1, ret);
            return Err(ret);
        }

        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                return Ok(CacheStatResult{
                    id0: ret.id0,
                    id1: ret.id1,
                    size: ret.size,
                });
            }
            Err(err) => {
                println!("disk_cache_store::stat failed to receive stat result for seg: id0: {}, id1: {}, err: {}",
                id0, id1, err);
                return Err(Errno::Eintr);
            }
        }
    }

    fn close(&self, id0: u64, id1: u64) -> Errno{
        let worker = self.disk_pool.get_thread(id0, id1);
        let msg = MsgFileCloseOp{
            id0: id0,
            id1: id1,
        };
        let ret = worker.do_io(MsgFileOp::OpClose(msg));
        if !ret.is_success(){
            println!("disk_cache_store: close: failed to close seg: id0: {}, id1: {}, err: {:?}", 
                id0, id1, ret);
            return ret;
        }
        return Errno::Esucc;
    }
}

impl Drop for DiskCache {
    fn drop(&mut self){
        self.disk_pool.stop();
    }
}

impl DiskCache {
    pub fn new(thread_num: u32, exec: &Executor) -> Self{
        DiskCache{
            disk_pool: IoThreadPool::new(
                thread_num,
                &format!("disk_cache_"),
                exec,
                &DiskIoWorkerFactory::new(),
            ),
        }
    }
}

pub struct DiskCacheStoreFactory{
}

impl CacheStoreFactory for DiskCacheStoreFactory{
    fn new_cache_store(&self, cfg: &CacheStoreConfig, exec: &Executor) -> Result<Arc<dyn CacheStore>, Errno>{
        Ok(Arc::new(DiskCache::new(cfg.thread_num, exec)))
    }
}

impl DiskCacheStoreFactory{
    pub fn new()->Box<dyn CacheStoreFactory>{
        Box::new(DiskCacheStoreFactory{})
    }
}