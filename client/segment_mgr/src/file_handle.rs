extern crate crossbeam_channel;

use std::collections::HashMap;
use std::thread;
use std::thread::JoinHandle;
use crossbeam_channel::{Sender, Receiver, bounded, select};
use common::error::Errno;
use common::defer;
use crate::types::{Block, FileHandle, MsgAddBlock, MsgAddSegment, MsgFileHandleOp, MsgGetLastSegment, MsgQueryHandle, Segment};

pub struct FileHandleMgr {
    //for update file handle.
    handle_op_tx: Sender<MsgFileHandleOp>,
    stop_tx: Sender<u32>,
    handle_mgr_th: Option<JoinHandle<()>>,
}

impl FileHandleMgr {
    pub fn create() -> FileHandleMgr {
        let (tx, rx) = bounded::<MsgFileHandleOp>(100);
        let (stop_tx, stop_rx) = bounded::<u32>(1);
        
        let mut handle_mgr = HandleMgr{
            handles: HashMap::<u64, FileHandle>::new(),
            handle_op_rx: rx,
            stop_rx: stop_rx,
        };

        let mgr = FileHandleMgr{
            handle_op_tx: tx,
            stop_tx: stop_tx,
            handle_mgr_th: Some(thread::spawn(move || handle_mgr.start())),
        };

        return mgr;
    }

    pub fn stop(&mut self){
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to stop file handle mgr, err: {}", err);
            }
        }
        // join the HandleMgr thread.
        if let Some(h) = self.handle_mgr_th.take() {
            let ret = h.join();
            match ret {
                Ok(_) => {
                    println!("FileHandleMgr has stopped.");
                }
                Err(_) => {
                    println!("FileHandleMgr failes to stop, join failed");
                }
            }
        }
        drop(self.handle_op_tx.clone());
        drop(self.stop_tx.clone());
    }

    pub fn add(&self, handle: &FileHandle) -> Errno {
        let msg = MsgFileHandleOp::Add(handle.copy());
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to add handle for ino: {}, err: {}", handle.ino, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn add_segment(&self, ino: u64, seg: &Segment) -> Errno {
        let msg_add_segment = MsgAddSegment{
            ino: ino,
            seg: seg.copy(),
        };
        let msg = MsgFileHandleOp::AddSegment(msg_add_segment);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to add segment(id0: {}, id1: {}) for ino: {}, err: {}",
                seg.seg_id0, seg.seg_id1, ino, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn add_block(&self, ino: u64, id0: u64, id1: u64, b: &Block) -> Errno {
        let msg_add_block = MsgAddBlock{
            ino: ino,
            id0: id0,
            id1: id1,
            block: b.copy(),
        };
        let msg = MsgFileHandleOp::AddBlock(msg_add_block);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to add_block for ino: {}, seg_id0: {}, seg_id1: {}, err: {}",
                ino, id0, id1, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn del(&self, ino: u64) -> Errno {
        let msg = MsgFileHandleOp::Del(ino);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to del handle for ino: {}, err: {}", ino, err);
                return Errno::Eintr;
            }
        }
    }

    // Vec[0]: id0; Vec[1]: id1; Vec[2]: max_size of segment.
    pub fn get_last_segment(&self, ino: u64) -> Result<Vec<u64>, Errno> {
        let (tx, rx) = bounded::<Vec<u64>>(1);
        let query = MsgGetLastSegment{
            ino: ino,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        };
        let ret = self.handle_op_tx.send(MsgFileHandleOp::GetLastSegment(query));
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("get_last_segment: failed to get last segment for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                return Ok(ret);
            }
            Err(err) => {
                println!("get_last_segment: failed to recv response for get last segment for ino: {}, err: {}",
                ino, err);
                return Err(Errno::Eintr);
            }
        }
    }

    pub fn get(&self, ino: u64) -> Result<FileHandle, Errno>{
        let (tx, rx) = bounded::<Option<FileHandle>>(1);
        let query = MsgQueryHandle{
            ino: ino,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        };
        let msg = MsgFileHandleOp::Get(query);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                let ret = rx.recv();
                match ret {
                    Ok(ret) => {
                        match ret {
                            Some(mut h) => {
                                //sort the segments.
                                let mut iter_seg = h.segments.iter_mut();
                                loop {
                                    let i = iter_seg.next();
                                    match i {
                                        Some(i) => {
                                            i.blocks.sort_by(|a, b| a.offset.cmp(&b.offset));
                                        }
                                        None => {
                                            break;
                                        }
                                    }
                                }
                                return Ok(h);
                            }
                            None => {
                                return Err(Errno::Enoent);
                            }
                        }
                    }
                    Err(err) => {
                        println!("get: failed to get handle for ino: {}, recv failed with err: {}", ino, err);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                println!("get: failed to get handle for ino: {}, failed to send query with err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
    }

    pub fn is_leader(&self, machine: &String, ino: u64) -> bool {
        let ret = self.get(ino);
        match ret {
            Ok(ret) => {
                if ret.leader == *machine {
                    return true;
                }
                return false;
            }
            Err(err) => {
                println!("failed to get file handle for ino: {}, err: {:?}", ino, err);
                return false;
            }
        }
    }
}

struct HandleMgr {
    // ino-->FileHandle
    handles: HashMap<u64, FileHandle>,
    handle_op_rx: Receiver<MsgFileHandleOp>,
    stop_rx: Receiver<u32>,
}

impl HandleMgr {
    pub fn start(&mut self) {
        loop {
            select!{
                recv(self.handle_op_rx) -> msg => {
                    let op : MsgFileHandleOp;
                    match msg {
                        Ok(msg) => {
                            op = msg;
                        }
                        Err(err) => {
                            println!("handle_op: failed to got handle_op msg, err: {}", err);
                            continue;
                        }
                    }
                    match op {
                        MsgFileHandleOp::Add(h) => {
                            self.add(h);
                        }
                        MsgFileHandleOp::AddBlock(m) => {
                            self.add_block(&m);
                        }
                        MsgFileHandleOp::AddSegment(m) => {
                            self.add_segment(&m);
                        }
                        MsgFileHandleOp::Del(ino) => {
                            self.del(ino);
                        }
                        MsgFileHandleOp::Get(m) => {
                            self.get(m);
                        }
                        MsgFileHandleOp::GetLastSegment(m) => {
                            self.get_last_segment(&m);
                        }                        
                    }
                },
                recv(self.stop_rx) -> msg => {
                    let rx = self.stop_rx.clone();
                    drop(rx);
                    let rx = self.handle_op_rx.clone();
                    drop(rx);
                    match msg {
                        Ok(_) => {
                            println!("got stop signal, stop the loop...");
                            break;
                        }
                        Err(err) => {
                            println!("recv invalid stop signal with err: {} and stop the loop...", err);
                            break;
                        }
                    }
                },
            }
        }
    }

    fn add(&mut self, handle: FileHandle) {
        self.handles.insert(handle.ino, handle);
    }

    fn add_segment(&mut self, msg: &MsgAddSegment) {
        if let Some(h) = self.handles.get_mut(&msg.ino) {
           h.segments.push(msg.seg.copy());
           return;
        }
    }

    fn add_block(&mut self, msg: &MsgAddBlock) {
        if let Some(h) = self.handles.get_mut(&msg.ino) {
            for s in &mut h.segments {
                if s.seg_id0 != msg.id0 || s.seg_id1 != msg.id1 {
                    continue;
                }
                s.add_block(msg.ino, msg.block.offset, msg.block.seg_start_addr, msg.block.size);
                return;
            }
        }
    }

    fn del(&mut self, ino: u64) {
        self.handles.remove(&ino);
    }
    
    fn get_last_segment(&self, msg: &MsgGetLastSegment) {
        let mut v : Vec<u64> = Vec::new();
        let mut found = false;
        let mut id0: u64 = 0;
        let mut id1: u64 = 0;
        let mut max_size: u64 = 0;
        let tx = msg.tx.clone();
        defer! {
            drop(tx);
        };
        if let Some(h) = self.handles.get(&msg.ino) {
            let mut offset: u64 = 0;
            for s in &h.segments {
                if s.blocks.is_empty() {
                    found = true;
                    id0 = s.seg_id0;
                    id1 = s.seg_id1;
                    max_size = s.max_size;
                    break;
                }
                for b in &s.blocks {
                    if b.offset >= offset {
                        found = true;
                        offset = b.offset;
                        id0 = s.seg_id0;
                        id1 = s.seg_id1;
                        max_size = s.max_size;
                    }
                }
            }
        }
        if found {
            v.push(id0);
            v.push(id1);
            v.push(max_size);
        }
        let ret = msg.tx.send(v);
        match ret {
            Ok(_) => {
                return;
            }
            Err(err) => {
                println!("get_last_segment: failed to send segment id0: {}, id1: {} for ino: {}, err: {}",
                id0, id1, msg.ino, err);
                return;
            }
        }
    }

    fn get(&mut self, msg: MsgQueryHandle){
        let mut handle: Option<FileHandle> = None;
        let tx = msg.tx.clone();
        defer!{
            drop(tx);
        };
        if let Some(h) = self.handles.get(&msg.ino) {
            handle = Some(h.copy());
        }
        let ret = msg.tx.send(handle);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to send handle for ino: {}, err: {}", msg.ino, err);
            }
        }
    }
}