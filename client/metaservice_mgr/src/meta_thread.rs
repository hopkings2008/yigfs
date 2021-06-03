use std::sync::Arc;
use common::thread::Thread;
use common::error::Errno;
use crossbeam_channel::Sender;

use crate::meta_op::{MetaOp, MetaOpResp, MetaOpUploadSeg};
use crate::mgr::MetaServiceMgr;
use crate::meta_worker::MetaWorker;
use log::error;


pub struct MetaThread{
    thr: Thread,
    op_tx: Sender<MetaOp>,
    stop_tx: Sender<u8>,
}

impl MetaThread {
    pub fn new(name: &String, mgr: Arc<dyn MetaServiceMgr>) -> Self{
        let (op_tx, op_rx) = crossbeam_channel::unbounded::<MetaOp>();
        let (stop_tx, stop_rx) = crossbeam_channel::bounded::<u8>(1);
        let meta_worker = MetaWorker::new(mgr, op_rx, stop_rx);
        let mut thr = MetaThread{
            thr: Thread::create(name),
            op_tx: op_tx,
            stop_tx: stop_tx,
        };
        thr.thr.run(move || {
            meta_worker.start();
        });
        return thr;
    }

    pub fn stop(&mut self) {
        let ret = self.stop_tx.send(1);
        match ret{
            Ok(_) =>{
                self.thr.join();
            }
            Err(err) => {
                error!("MetaThread::stop: failed to send the stop signal, err: {}", err);
            }
        }
    }

    pub fn upload_segment(&self, id0: u64, id1: u64, offset: u64, resp_tx: Sender<MetaOpResp>) -> Errno {
        let op = MetaOpUploadSeg{
            id0: id0,
            id1: id1,
            offset: offset,
            tx: resp_tx,
        };
        let ret = self.op_tx.send(MetaOp::OpUploadSeg(op));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("upload_segment: failed to send op for id0: {}, id1: {}, offset: {}, err: {}",
                id0, id1, offset, err);
                return Errno::Eintr;
            }
        }
    }
}