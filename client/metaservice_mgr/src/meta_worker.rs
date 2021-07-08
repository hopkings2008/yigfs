use std::sync::Arc;

use crossbeam_channel::{Receiver, select};

use crate::{meta_op::{MetaOpResp, MetaOpUpdateSegs, MetaOpUploadSeg, MetaOpUploadSegResp}, mgr::MetaServiceMgr};
use crate::meta_op::MetaOp;
use log::{warn, error};


pub struct MetaWorker{
    meta_mgr: Arc<dyn MetaServiceMgr>,
    op_rx: Receiver<MetaOp>,
    stop_rx: Receiver<u8>,
}

impl MetaWorker {
    pub fn new(mgr: Arc<dyn MetaServiceMgr>, op_rx: Receiver<MetaOp>, stop_rx: Receiver<u8>) -> Self {
        MetaWorker{
            meta_mgr: mgr,
            op_rx: op_rx,
            stop_rx: stop_rx,
        }
    }

    pub fn start(&self){
        loop {
            select! {
                recv(self.op_rx)->msg => {
                    match msg {
                        Ok(msg) => {
                            self.do_work(msg);
                        }
                        Err(err) => {
                            error!("MetaWorker::start: failed to recv op_rx, err: {}", err);
                        }
                    }
                }
                recv(self.stop_rx) -> msg => {
                    match msg{
                        Ok(msg) => {
                            warn!("MetaWorker::start: got stop signal: {}, stopping...", msg);
                            return;
                        }
                        Err(err) => {
                            error!("MetaWorker::start: failed to recv stop_rx, err: {}", err);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn do_work(&self, msg: MetaOp) {
        match msg {
            MetaOp::OpUploadSeg(msg) => {
                self.do_upload_seg(msg);
            }
            MetaOp::OpUpdateChangedSegs(msg) => {
                self.do_upload_changed_segs(msg);
            }
        }
    }

    fn do_upload_seg(&self, op: MetaOpUploadSeg){
        let ret = self.meta_mgr.upload_segment(op.id0, op.id1, op.offset);
        if !ret.is_success(){
            error!("do_upload_seg: failed to upload segment for id0: {}, id1: {}, offset: {}, err: {:?}",
            op.id0, op.id1, op.offset, ret);
        }
        let resp = MetaOpUploadSegResp{
            id0: op.id0,
            id1: op.id1,
            err: ret,
        };
        let ret = op.response(MetaOpResp::RespUploadSeg(resp));
        if !ret.is_success(){
            error!("do_upload_seg: failed to send resp for id0: {}, id1: {}, offset: {}, err: {:?}",
            op.id0, op.id1, op.offset, ret);
        }
    }

    fn do_upload_changed_segs(&self, op: MetaOpUpdateSegs){
        let ret = self.meta_mgr.update_file_segments(op.ino, &op.segs, &op.garbages);
        if !ret.is_success() {
            error!("do_update_changed_segs: failed to upload changes segs for ino: {}, err: {:?}", op.ino, ret);
        }

        if let Some(tx) = op.tx {
            let cret = tx.send(ret);
            match cret {
                Ok(_) => {}
                Err(err) => {
                    error!("do_update_changed_segs: failed to send response for ino: {}, err: {}", op.ino, err);
                }
            }
        }
    }
}