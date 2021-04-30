
use crate::{segment_state::SegStateMachine, types::SegSyncOp};
use crate::segment_state::SegState;
use common::numbers::NumberOp;
use io_engine::types::{MsgFileOpResp, MsgFileOpenResp, MsgFileReadData, MsgFileWriteResp};
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
use metaservice_mgr::{meta_op::{MetaOpResp, MetaOpUploadSeg, MetaOpUploadSegResp}, meta_store::MetaStore};
use std::sync::Arc;
use std::collections::HashMap;
use crossbeam_channel::{Receiver, Sender, select, unbounded};


pub struct SegSyncHandler{
    cache_store: Arc<dyn CacheStore>,
    backend_store: Arc<dyn BackendStore>,
    meta_store: Arc<MetaStore>,
    cache_op_tx: Sender<MsgFileOpResp>,
    cache_op_rx: Receiver<MsgFileOpResp>,
    backend_op_tx: Sender<MsgFileOpResp>,
    backend_op_rx: Receiver<MsgFileOpResp>,
    meta_op_tx: Sender<MetaOpResp>,
    meta_op_rx: Receiver<MetaOpResp>,
    op_rx: Receiver<SegSyncOp>,
    stop_rx: Receiver<u8>,
    seg_state_machines: HashMap<u128, SegStateMachine>,
}

impl SegSyncHandler{
    pub fn new(cache_store: Arc<dyn CacheStore>, 
        backend_store: Arc<dyn BackendStore>, 
        meta_store: Arc<MetaStore>,
        op_rx: Receiver<SegSyncOp>,
        stop_rx: Receiver<u8>) -> Self{
        let (cache_op_tx, cache_op_rx) = unbounded::<MsgFileOpResp>();
        let (backend_op_tx, backend_op_rx) = unbounded::<MsgFileOpResp>();
        let (meta_op_tx, meta_op_rx) = unbounded::<MetaOpResp>();
        SegSyncHandler{
            cache_store: cache_store,
            backend_store: backend_store,
            meta_store: meta_store,
            cache_op_tx: cache_op_tx,
            cache_op_rx: cache_op_rx,
            backend_op_tx: backend_op_tx,
            backend_op_rx: backend_op_rx,
            meta_op_tx: meta_op_tx,
            meta_op_rx: meta_op_rx,
            op_rx: op_rx,
            stop_rx: stop_rx,
            seg_state_machines: HashMap::new(),
        }
    }

    pub fn start(&mut self){
        loop {
            select! {
                recv(self.op_rx) -> ret => {
                    match ret {
                        Ok(msg) => {
                            self.do_op(msg);
                        }
                        Err(err) => {
                            println!("SegSyncHandler::start: failed to recv op, err: {}", err);
                        }
                    }
                }
                recv(self.cache_op_rx) -> ret => {
                    match ret {
                        Ok(msg) => {
                            self.handle_cache_op(msg);
                        }
                        Err(err) => {
                            println!("SegSyncHandler::start: failed to recv cache_store message, err: {}", err);
                        }
                    }
                }
                recv(self.backend_op_rx) -> ret => {
                    match ret {
                        Ok(msg) => {
                            self.handle_backend_store_op(msg);
                        }
                        Err(err) => {
                            println!("SegSyncHandler::start: failed to recv backend_store message, err: {}", err);
                        }
                    }
                }
                recv(self.meta_op_rx) -> ret => {
                    match ret{
                        Ok(msg) => {
                            self.handle_meta_store_op(msg);
                        }
                        Err(err) => {
                            println!("SegSyncHandler::start: failed to recv meta_store message, err: {}", err);
                        }
                    }
                }
                recv(self.stop_rx) -> ret => {
                    match ret {
                        Ok(msg) => {
                            println!("SegSyncHandler::start: got stop signal: {}, stopping...", msg);
                            return;
                        }
                        Err(err) => {
                            println!("SegSyncHandler::start: failed to recv stop signal, err: {}", err);
                            return;
                        }
                    }
                }
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
                seg_state.prepare_for_upload();
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

    // handle cache io callback.
    fn handle_cache_op(&mut self, op: MsgFileOpResp){
        match op {
            MsgFileOpResp::OpRespOpen(open_op) => {
                self.handle_cache_open(open_op);
            }
            MsgFileOpResp::OpRespRead(read_op) => {
                self.handle_cache_read(read_op);
            }
            MsgFileOpResp::OpRespWrite(write_op) => {}
        }
    }

    fn handle_cache_open(&mut self, op: MsgFileOpenResp){
        let seg_id = NumberOp::to_u128(op.id0, op.id1);
        // we will perform 4MB read each time.
        if let Some(s) = self.seg_state_machines.get_mut(&seg_id) {
            // we need check current state firstly.
            if !s.is_state_match(&SegState::CacheOpen) {
                println!("SegSyncHandler::handle_cache_open: the state machine of id0: {}, id1: {} is not cache_open, it is: {:?}",
                op.id0, op.id1, s.get_current_state());
                // close the segment and remove the seg state machine.
                self.cache_store.close(op.id0, op.id1);
                self.seg_state_machines.remove(&seg_id);
                return;
            }
            // get next state to process.
            let next_state = s.get_next_state();
            match next_state {
                SegState::CacheRead => {
                    s.set_state(SegState::CacheRead);
                    let ret = self.cache_store.read_async(op.id0, op.id1, s.get_dir(), s.get_offset(), 4<<20, self.cache_op_tx.clone());
                    if !ret.is_success(){
                        println!("SegSyncHandler::handle_cache_open: failed to perform read for seg id0: {}, id1: {}
                        dir: {}, offset: {}, err: {:?}", op.id0, op.id1, s.get_dir(), s.get_offset(), ret);
                        // close the segment and remove the seg state machine.
                        self.cache_store.close(op.id0, op.id1);
                        self.seg_state_machines.remove(&seg_id);
                    }
                }
                _ => {
                    println!("SegSyncHandler::handle_cache_open: next_state{:?} is not supported.",
                    next_state);
                    // close the segment and remove the seg state machine.
                    self.cache_store.close(op.id0, op.id1);
                    self.seg_state_machines.remove(&seg_id);
                }
            }
            return;
        }
        // seg doesn't exists before...
        println!("SegSyncHandler::handle_cache_open: got unopend seg: id0: {}, id1: {}",
        op.id0, op.id1);
        // close the seg.
        self.cache_store.close(op.id0, op.id1);
    }

    fn handle_cache_read(&mut self, op: MsgFileReadData) {
        let seg_id = NumberOp::to_u128(op.id0, op.id1);

        if let Some(s) = self.seg_state_machines.get_mut(&seg_id) {
            // check state match.
            if !s.is_state_match(&SegState::CacheRead) {
                println!("SegSyncHandler::handle_cache_read: got unmatched state for seg: id0: {}, id1: {},
                 current state: {:?}, expected state: CacheRead", op.id0, op.id1, s.get_current_state());
                return;
            }
            // get next state to process.
            let next_state = s.get_next_state();
            match next_state {
                SegState::BackendWrite => {
                    if !op.err.is_success() {
                        if op.err.is_eof(){
                            println!("SegSyncHandler::handle_cache_read: got eof for seg: id0: {}, id1: {}, 
                            offset: {}.", op.id0, op.id1, s.get_offset());
                            s.set_state(SegState::CacheClose);                            
                        } else {
                            println!("SegSyncHandler::handle_cache_read: failed to read seg: id0: {}, id1: {},
                            offset: {}, err: {:?}", op.id0, op.id1, s.get_offset(), op.err);
                        }
                        // close & remove the seg state machine.
                        self.cache_store.close(op.id0, op.id1);
                        self.seg_state_machines.remove(&seg_id);
                        return;
                    }
                    if let Some(data) = op.data {
                        s.set_state(SegState::BackendWrite);
                        let ret = self.backend_store.write_async(op.id0, 
                        op.id1, s.get_offset(), data.as_slice(), self.backend_op_tx.clone());
                        if !ret.is_success() {
                            println!("SegSyncHandler::handle_cache_read: failed to perform write for seg id0: {}, id1: {},
                            offset: {}, err: {:?}", op.id0, op.id1, s.get_offset(), ret);
                            // close & remove the state machine.
                            self.cache_store.close(op.id0, op.id1);
                            self.seg_state_machines.remove(&seg_id);
                        }
                        return;
                    }
                }
                _ => {
                    println!("SegSyncHandler::handle_cache_read: got invalid state: {:?} for seg id0: {}, id1: {}",
                    next_state, op.id0, op.id1);
                    self.cache_store.close(op.id0, op.id1);
                    self.seg_state_machines.remove(&seg_id);
                    return;
                }
            }
        }
        println!("SegSyncHandler::handle_cache_read: got invalid op resp for seg id0: {}, id1: {}", op.id0, op.id1);
        // close the seg.
        self.cache_store.close(op.id0, op.id1);
    }

    // handle backend io callback.
    fn handle_backend_store_op(&mut self, op: MsgFileOpResp){
        match op{
            MsgFileOpResp::OpRespOpen(open_op) => {
                println!("handle_backend_store_op: skip open_op: seg: id0: {}, id1: {}",
                open_op.id0, open_op.id1);
            }
            MsgFileOpResp::OpRespRead(read_op) => {
                println!("handle_backend_store_op: skip read_op: seg: id0: {}, id1: {}",
                read_op.id0, read_op.id1);
            }
            MsgFileOpResp::OpRespWrite(write_op) => {
                self.handle_backend_store_write(write_op);
            }
        }
    }

    fn handle_backend_store_write(&mut self, op: MsgFileWriteResp) {
        let seg_id = NumberOp::to_u128(op.id0, op.id1);

        if let Some(s) = self.seg_state_machines.get_mut(&seg_id) {
            if !s.is_state_match(&SegState::BackendWrite){
                println!("handle_backend_store_write: got invalid seg state, expected BackendWrite, got: {:?}
                 for seg id0: {}, id1: {}", s.get_current_state(), op.id0, op.id1);
                 // close the cache & remove the state machines.
                 self.cache_store.close(op.id0, op.id1);
                 self.seg_state_machines.remove(&seg_id);
                 return;
            }
            // get next state to process.
            let next_state = s.get_next_state();
            match next_state {
                SegState::MetaUpload => {
                    let offset = op.offset+op.nwrite as u64;
                    s.set_state(SegState::MetaUpload);
                    // update the segment offset.
                    s.set_offset(offset);
                    let ret = self.meta_store.upload_segment_async(op.id0, op.id1, offset-1, self.meta_op_tx.clone());
                    if !ret.is_success() {
                        println!("handle_backend_store_write: failed to send update segment for id0: {}, id1: {}, offset: {}, err: {:?}",
                        op.id0, op.id1, offset, ret);
                        self.cache_store.close(op.id0, op.id1);
                        self.seg_state_machines.remove(&seg_id);
                    }
                    return;
                }
                _ => {
                    println!("handle_backend_store_write: got invalid state: {:?}, expected MetaUpload for id0: {}, id1: {}",
                    next_state, op.id0, op.id1);
                    self.cache_store.close(op.id0, op.id1);
                    self.seg_state_machines.remove(&seg_id);
                    return;
                }
            }
        }
        println!("handle_backend_store_write: got unmanged seg id0: {}, id1: {}", op.id0, op.id1);
        self.cache_store.close(op.id0, op.id1);
    }

    // handle meta io callback.
    fn handle_meta_store_op(&mut self, op: MetaOpResp){
        match op{
            MetaOpResp::RespUploadSeg(op) => {
                self.handle_meta_store_upload_seg(op);
            }
        }
    }

    fn handle_meta_store_upload_seg(&mut self, op: MetaOpUploadSegResp){
        let seg_id = NumberOp::to_u128(op.id0, op.id1);

        if let Some(s) = self.seg_state_machines.get_mut(&seg_id) {
            if !s.is_state_match(&SegState::MetaUpload){
                println!("handle_meta_store_upload_seg: got invalid seg state, expected MetaUpload, got: {:?}
                 for seg id0: {}, id1: {}", s.get_current_state(), op.id0, op.id1);
                 // close the cache & remove the state machines.
                 self.cache_store.close(op.id0, op.id1);
                 self.seg_state_machines.remove(&seg_id);
                 return;
            }
            // get next state to process.
            let next_state = s.get_next_state();
            match next_state {
                SegState::CacheRead => {
                    s.set_state(SegState::CacheRead);
                    let ret = self.cache_store.read_async(op.id0, op.id1, s.get_dir(), 
                    s.get_offset(), 4<<20, self.cache_op_tx.clone());
                    if !ret.is_success(){
                        println!("handle_meta_store_upload_seg: failed to preform cache read for id0: {}, id1: {},
                        offset: {}, err: {:?}", op.id0, op.id1, s.get_offset(), ret);
                        self.cache_store.close(op.id0, op.id1);
                        self.seg_state_machines.remove(&seg_id);
                    }
                    return;
                }
                _ => {
                    println!("handle_meta_store_upload_seg: got invalid seg next state {:?} for id0: {}, id1: {},
                        expected CacheRead", next_state, op.id0, op.id1);
                    self.cache_store.close(op.id0, op.id1);
                    self.seg_state_machines.remove(&seg_id);
                    return;
                }
            }
        }
    }
}