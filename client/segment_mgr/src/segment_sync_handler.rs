
use crate::{segment_state::SegStateMachine, types::SegSyncOp};
use crate::segment_state::SegState;
use common::numbers::NumberOp;
use io_engine::types::{MsgFileOpResp, MsgFileOpenResp};
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
use std::sync::Arc;
use std::collections::HashMap;
use crossbeam_channel::{Receiver, Sender, select, unbounded};


pub struct SegSyncHandler{
    cache_store: Arc<dyn CacheStore>,
    backend_store: Arc<dyn BackendStore>,
    cache_op_tx: Sender<MsgFileOpResp>,
    cache_op_rx: Receiver<MsgFileOpResp>,
    op_rx: Receiver<SegSyncOp>,
    stop_rx: Receiver<u8>,
    seg_state_machines: HashMap<u128, SegStateMachine>,
}

impl SegSyncHandler{
    pub fn new(cache_store: Arc<dyn CacheStore>, 
        backend_store: Arc<dyn BackendStore>, 
        op_rx: Receiver<SegSyncOp>,
        stop_rx: Receiver<u8>) -> Self{
        let (cache_op_tx, cache_op_rx) = unbounded::<MsgFileOpResp>();
        SegSyncHandler{
            cache_store: cache_store,
            backend_store: backend_store,
            cache_op_tx: cache_op_tx,
            cache_op_rx: cache_op_rx,
            op_rx: op_rx,
            stop_rx: stop_rx,
            seg_state_machines: HashMap::new(),
        }
    }

    pub fn start(&mut self){
        loop {
            select! {
                recv(self.op_rx) -> msg => {}
                recv(self.cache_op_rx) -> msg => {}
                recv(self.stop_rx) -> msg => {}
            }
        }
    }

    fn do_op(&mut self, op: SegSyncOp){
        match op{
            SegSyncOp::OpUpload(op) => {
                // we will skip the op if it is being processed.
                let seg_id = NumberOp::to_u128(op.id0, op.id1);
                if self.seg_state_machines.contains_key(&seg_id) {
                    println!("SegSyncHandler::do_op: seg(id0: {}, id1: {}) is being processed, skip upload op",
                op.id0, op.id1);
                    return;
                }
                let mut seg_state = SegStateMachine::new(
                    op.id0, op.id1, &op.dir
                );
                // set the init state
                seg_state.set_state(SegState::CacheOpen);
                seg_state.set_offset(op.offset);
                self.seg_state_machines.insert(seg_id, seg_state);
                // perform the cache read.
                let ret = self.cache_store.open_async(op.id0, op.id1, &op.dir, self.cache_op_tx.clone());
                if !ret.is_success(){
                    println!("SegSyncHandler::do_op: failed to open segment id0:{}, id1: {}, dir: {}, err: {:?}",
                    op.id0, op.id1, op.dir, ret);
                }
            }
            SegSyncOp::OpDownload(op) => {}
        }
    }

    fn handle_cache_op(&mut self, op: MsgFileOpResp){
        match op {
            MsgFileOpResp::OpRespOpen(open_op) => {}
            MsgFileOpResp::OpRespRead(read_op) => {}
            MsgFileOpResp::OpRespWrite(write_op) => {}
        }
    }

    fn handle_cache_open(&mut self, op: MsgFileOpenResp){
        let seg_id = NumberOp::to_u128(op.id0, op.id1);
        // we will perform 4MB read each time.
        if let Some(s) = self.seg_state_machines.get_mut(&seg_id) {
            s.set_state(SegState::CacheRead);
            let ret = self.cache_store.read_async(op.id0, op.id1, s.get_dir(), s.get_offset(), 4<<20, self.cache_op_tx.clone());
            if !ret.is_success(){
                println!("SegSyncHandler::handle_cache_open: failed to perform read for seg id0: {}, id1: {}
                dir: {}, offset: {}, err: {:?}", op.id0, op.id1, s.get_dir(), s.get_offset(), ret);
            }
            return;
        }
        // seg doesn't exists before...
        println!("SegSyncHandler::handle_cache_open: got unopend seg: id0: {}, id1: {}",
        op.id0, op.id1);
    }
}