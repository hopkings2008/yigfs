extern crate crossbeam_channel;

use std::collections::HashMap;
use std::thread;
use std::thread::JoinHandle;
use common::numbers::NumberOp;
use crossbeam_channel::{Sender, Receiver, bounded, select};
use common::error::Errno;
use common::defer;
use metaservice_mgr::types::{Segment, Block};
use crate::types::ChangedSegments;
use crate::types::MsgGetBlocks;
use crate::types::MsgGetFileSegments;
use crate::types::MsgOpenHandle;
use crate::types::MsgSetSegStatus;
use crate::types::SegStatus;
use crate::types::{FileHandle, MsgAddBlock, 
    MsgAddSegment, MsgFileHandleOp, MsgGetLastSegment, 
    MsgQueryHandle, MsgGetChangedBlocks, RespChangedBlocks};
use log::{warn, error};

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
            handle_mgr_th: Some(thread::spawn(move|| handle_mgr.start())),
        };

        return mgr;
    }

    pub fn stop(&mut self){
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to stop file handle mgr, err: {}", err);
            }
        }
        // join the HandleMgr thread.
        if let Some(h) = self.handle_mgr_th.take() {
            let ret = h.join();
            match ret {
                Ok(_) => {
                    warn!("FileHandleMgr has stopped.");
                }
                Err(_) => {
                    error!("FileHandleMgr failes to stop, join failed");
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
                error!("failed to add handle for ino: {}, err: {}", handle.ino, err);
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
                error!("failed to add segment(id0: {}, id1: {}) for ino: {}, err: {}",
                seg.seg_id0, seg.seg_id1, ino, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn add_block(&self, ino: u64, id0: u64, id1: u64, b: &Block, tx: Option<Sender<ChangedSegments>>) -> Errno {
        let msg_add_block = MsgAddBlock{
            ino: ino,
            id0: id0,
            id1: id1,
            block: b.copy(),
            tx: tx,
        };
        let msg = MsgFileHandleOp::AddBlock(msg_add_block);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("add_block: failed to add_block for ino: {}, seg_id0: {}, seg_id1: {}, err: {}",
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
                error!("failed to del handle for ino: {}, err: {}", ino, err);
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
                error!("get_last_segment: failed to get last segment for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                return Ok(ret);
            }
            Err(err) => {
                error!("get_last_segment: failed to recv response for get last segment for ino: {}, err: {}",
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
                            Some(h) => {
                                return Ok(h);
                            }
                            None => {
                                return Err(Errno::Enoent);
                            }
                        }
                    }
                    Err(err) => {
                        error!("get: failed to get handle for ino: {}, recv failed with err: {}", ino, err);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                error!("get: failed to get handle for ino: {}, failed to send query with err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
    }

    pub fn open_handle(&self, ino: u64) -> Result<String, Errno>{
        let (tx, rx) = bounded::<String>(1);
        let query = MsgOpenHandle{
            ino: ino,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        };
        let msg = MsgFileHandleOp::OpenHandle(query);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                let ret = rx.recv();
                match ret {
                    Ok(ret) => {
                        if !ret.is_empty(){
                            return Ok(ret);
                        }
                        return Err(Errno::Enoent);
                    }
                    Err(err) => {
                        error!("get: failed to get handle for ino: {}, recv failed with err: {}", ino, err);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                error!("get: failed to get handle for ino: {}, failed to send query with err: {}", ino, err);
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
                error!("failed to get file handle for ino: {}, err: {:?}", ino, err);
                return false;
            }
        }
    }

    pub fn get_blocks(&self, ino: u64, offset: u64, size: u64) -> Vec<Block> {
        let (tx, rx) = bounded::<Vec<Block>>(1);
        let msg = MsgGetBlocks{
            ino: ino,
            offset: offset,
            size: size,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        };
        let ret = self.handle_op_tx.send(MsgFileHandleOp::GetBlocks(msg));
        match ret {
            Ok(_) => {
            }
            Err(err) => {
                error!("get_blocks: failed to send GetBlocks msg for ino: {}, offset: {}, size: {}, err: {}",
            ino, offset, size, err);
                return Vec::new();
            }
        }
        let ret = rx.recv();
        match ret {
            Ok(blocks) => {
                return blocks;
            }
            Err(err) => {
                error!("get_blocks: failed to recv GetBlocks resp for ino: {}, offset: {}, size: {}, err: {}",
                ino, offset, size, err);
                return Vec::new();
            }
        }
    }

    pub fn set_seg_status(&self, ino: u64, id0: u64, id1: u64, need_sync: bool) -> Errno {
        let msg = MsgFileHandleOp::SetSegStatus(MsgSetSegStatus{
            ino: ino,
            id0: id0,
            id1: id1,
            need_sync: need_sync,
        });
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("failed to set_seg_status for ino: {}, id0: {}, id1: {}, need_sync: {}, err: {}", 
                ino, id0, id1, need_sync, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn get_file_segments(&self, ino: u64) -> Result<Vec<Segment>, Errno>{
        let (tx, rx) = bounded::<Vec<Segment>>(1);
        let msg = MsgFileHandleOp::GetFileSegments(MsgGetFileSegments{
            ino: ino,
            tx: tx,
        });
        let ret = self.handle_op_tx.send(msg);
        match ret{
            Ok(_) => {}
            Err(err) => {
                error!("get_file_segments: failed to send query for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                return Ok(ret);
            }
            Err(err) => {
                error!("get_file_segments: failed to get file segments for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
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

unsafe impl Send for HandleMgr {}

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
                            error!("handle_op: failed to got handle_op msg, err: {}", err);
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
                        MsgFileHandleOp::OpenHandle(m) => {
                            self.open_handle(m);
                        }
                        MsgFileHandleOp::GetBlocks(m) => {
                            // query the blocks
                            self.get_blocks(m);
                        }
                        MsgFileHandleOp::GetLastSegment(m) => {
                            self.get_last_segment(&m);
                        }
                        MsgFileHandleOp::SetSegStatus(m) => {
                            self.set_seg_status(m);
                        }
                        MsgFileHandleOp::GetFileSegments(m) => {
                            self.get_file_segments(m);
                        }
                        MsgFileHandleOp::GetChangedBlocks(m) => {
                            self.get_changed_blocks(m);
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
                            warn!("got stop signal, stop the loop...");
                            break;
                        }
                        Err(err) => {
                            error!("recv invalid stop signal with err: {} and stop the loop...", err);
                            break;
                        }
                    }
                },
            }
        }
    }

    fn add(&mut self, handle: FileHandle) {
        if let Some(h) = self.handles.get_mut(&handle.ino) {
            h.reference += 1;
            return;
        }
        self.handles.insert(handle.ino, handle);
    }

    fn add_segment(&mut self, msg: &MsgAddSegment) {
        if let Some(h) = self.handles.get_mut(&msg.ino) {
           h.segments.push(msg.seg.copy());
           let idx = h.segments.len() - 1;
           let id = NumberOp::to_u128(msg.seg.seg_id0, msg.seg.seg_id1);
           h.segments_index.insert(id, idx);
           h.mark_dirty();
           return;
        }
    }

    fn add_block(&mut self, msg: &MsgAddBlock) {
        let mut changed_segs = ChangedSegments{
            ret: Errno::Esucc,
            segments: HashMap::new(),
            garbages: HashMap::new(),
        };
        let mut need_track = true;
        if msg.tx.is_some() {
            need_track = false;
        }
        if let Some(h) = self.handles.get_mut(&msg.ino) {
            let start = msg.block.offset;
            let end = msg.block.offset + msg.block.size as u64;

            //check the sequence write
            let last_node = h.block_tree.get_largest_node();
            if last_node.borrow().is_nil() {
                // no block yet. just insert this new one.
                h.block_tree.insert_node(start, end, msg.block.clone());
                // add to changed blocks.
                let err = h.add_changed_block(&mut changed_segs.segments, &msg.block);
                if !err.is_success() {
                    error!("HandleMgr::add_block: failed to add changed block {:?}, err: {:?}", msg.block, err);
                    changed_segs.ret = err;
                }
                h.mark_dirty();
                msg.response(changed_segs);
                return;
            }
            // check whether it is the sequence write.
            let last_block = last_node.borrow().get_value();
            if last_block.seg_id0 == msg.block.seg_id0 && last_block.seg_id1 == msg.block.seg_id1 && 
            (last_block.offset + last_block.size as u64) == start && 
            (last_block.seg_start_addr + last_block.size as u64) == msg.block.seg_start_addr{
                //merge the last block with the new one.
                let mut new_block = last_block.clone();
                new_block.size += msg.block.size;
                h.block_tree.delete(&last_node);
                h.block_tree.insert_node(new_block.offset, new_block.offset+new_block.size as u64, new_block.clone());
                let ret = h.add_changed_block(&mut changed_segs.segments, &new_block);
                if !ret.is_success(){
                    error!("HandleMgr::add_block: failed to add changed block {:?}, err: {:?}", msg.block, ret);
                    changed_segs.ret = ret;
                }
                h.mark_dirty();
                msg.response(changed_segs);
                return;
            }

            // check overlap.
            let nodes = h.block_tree.get(start, end);
            if nodes.is_empty() {
                // no overlap, just insert the new nodes.
                h.block_tree.insert_node(start, end, msg.block.clone());
                let ret = h.add_changed_block(&mut changed_segs.segments, &msg.block);
                if !ret.is_success() {
                    error!("HandleMgr::add_block: failed to add changed block for {:?}, err: {:?}", msg.block, ret);
                    changed_segs.ret = ret;
                }
                h.mark_dirty();
                msg.response(changed_segs);
                return;
            }
            let mut blocks: Vec<Block> = Vec::new();
            for n in &nodes {
                let mut b = n.borrow().get_value();
                h.block_tree.delete(n);
                if b.offset == start {
                    blocks.push(msg.block.clone());
                    // [b.offset, b.offset+b.size) is the subset of [start, end)
                    if (b.offset + b.size as u64) <= end {
                        // just skip this block, add it to garbage
                        let ret = h.add_garbage_block(&mut changed_segs.garbages, b.clone());
                        if !ret.is_success() {
                            error!("HandleMgr::add_block: failed to add garbage block: {:?}, err: {:?}", b, ret);
                            changed_segs.ret = ret;
                            msg.response(changed_segs);
                            return;
                        }
                        if need_track {
                            let ret = h.track_garbage_block(b.clone());
                            if !ret.is_success() {
                                error!("HandleMgr::add_block: failed to track garbage block: {:?}, err: {:?}", b, ret);
                                changed_segs.ret = ret;
                                msg.response(changed_segs);
                                return;
                            }
                        }
                        continue;
                    }
                    let size = end - start;
                    b.offset += size;
                    b.size -= size as i64;
                    b.seg_start_addr += size;
                    blocks.push(b);
                    continue;                    
                }
                if b.offset < start {
                    let size = start - b.offset;
                    b.size = size as i64;
                    blocks.push(b);
                    blocks.push(msg.block.clone());
                    continue;
                }
                // [start, [b.offset, b.offset+b.size), end)
                // add to garbage
                if (b.offset + b.size as u64) <= end {
                    if need_track{
                        let ret = h.track_garbage_block(b.clone());
                        if !ret.is_success(){
                            error!("HandleMgr::add_block: failed to track garbage block: {:?}, err: {:?}", b, ret);
                            changed_segs.ret = ret;
                            msg.response(changed_segs);
                            return;
                        }
                    } 
                    let ret = h.add_garbage_block(&mut changed_segs.garbages, b.clone());
                    if !ret.is_success(){
                        error!("HandleMgr::add_block: failed to add garbage block: {:?}, err: {:?}", b, ret);
                        changed_segs.ret = ret;
                        msg.response(changed_segs);
                        return;
                    }
                    continue;
                }
                let size = end - b.offset;
                b.offset = end;
                b.size -= size as i64;
                b.seg_start_addr += size;
                blocks.push(b);
            }
            // insert the merged blocks.
            for b in &blocks {
                h.block_tree.insert_node(b.offset, b.offset+b.size as u64, b.clone());
                let ret = h.add_changed_block(&mut changed_segs.segments, b);
                if !ret.is_success() {
                    error!("HandleMgr::add_block: failed to add changed block: {:?}, err: {:?}", b, ret);
                    changed_segs.ret = ret;
                    msg.response(changed_segs);
                    return;
                }
            }
            h.mark_dirty();
            msg.response(changed_segs);
        }
    }

    fn del(&mut self, ino: u64) {
        // free the block_tree
        if let Some(h) = self.handles.get_mut(&ino) {
            h.reference -= 1;
            if h.reference <=0 {
                h.block_tree.free();
                self.handles.remove(&ino);
            }
        }
    }
    
    fn get_last_segment(&self, msg: &MsgGetLastSegment) {
        let mut v : Vec<u64> = Vec::new();
        let mut found = false;
        let mut id0: u64 = 0;
        let mut id1: u64 = 0;
        let mut max_size: u64 = 0;
        let mut size: u64 = 0;
        // store the status whether the local segment should be synced from yig.
        let mut need_sync = 0;
        let tx = msg.tx.clone();
        defer! {
            drop(tx);
        };
        if let Some(h) = self.handles.get(&msg.ino) {
            // get the last entry for the last segment.
            if let Some(l) = h.segments.last() {
                found = true;
                id0 = l.seg_id0;
                id1 = l.seg_id1;
                max_size = l.capacity;
                size = l.size;
                let id = NumberOp::to_u128(id0, id1);
                if let Some(status) = h.seg_status.get(&id){
                    if status.need_sync {
                        need_sync = 1;
                    }
                }
            }
        }
        if found {
            v.push(id0);
            v.push(id1);
            v.push(max_size);
            v.push(size);
            v.push(need_sync);
        }
        let ret = msg.tx.send(v);
        match ret {
            Ok(_) => {
                return;
            }
            Err(err) => {
                error!("get_last_segment: failed to send segment id0: {}, id1: {} for ino: {}, err: {}",
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
                error!("failed to send handle for ino: {}, err: {}", msg.ino, err);
            }
        }
    }

    fn open_handle(&mut self, msg: MsgOpenHandle){
        let leader: String;
        let tx = msg.tx.clone();
        defer!{
            drop(tx);
        };
        if let Some(h) = self.handles.get_mut(&msg.ino) {
            h.reference += 1;
            leader = h.leader.clone();
        } else {
            leader = String::from("");
        }
        let ret = msg.tx.send(leader);
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send handle for ino: {}, err: {}", msg.ino, err);
            }
        }
    }

    fn get_blocks(&self, msg: MsgGetBlocks) {
        let mut blocks: Vec<Block> = Vec::new();
        if let Some(h) = self.handles.get(&msg.ino) {
            let nodes = h.block_tree.get(msg.offset, msg.offset+msg.size);
            for n in nodes {
                if n.borrow().is_nil() {
                    continue;
                }
                blocks.push(n.borrow().get_value());
            }
        }
        let ret = msg.tx.send(blocks);
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("get_blocks: failed to send blocks for ino: {}, offset: {}, size: {}, err: {}",
                msg.ino, msg.offset, msg.size, err);
            }
        }
    }

    fn set_seg_status(&mut self, m: MsgSetSegStatus) {
        if let Some(h) = self.handles.get_mut(&m.ino) {
            let id = NumberOp::to_u128(m.id0, m.id1);
            if let Some(status) = h.seg_status.get_mut(&id) {
                status.need_sync = m.need_sync;
                return;
            }
            h.seg_status.insert(id, SegStatus{
                id0: m.id0,
                id1: m.id1,
                need_sync: m.need_sync,
            });
        }
    }

    fn get_file_segments(&self, m: MsgGetFileSegments){
        let segs: Vec<Segment>;
        if let Some(h) = self.handles.get(&m.ino) {
            segs = h.segments.clone();
        } else {
            segs = Vec::new();
        }
        let ret = m.tx.send(segs);
        match ret{
            Ok(_) => {}
            Err(err) => {
                error!("get_file_segments: failed to send segments resp for ino: {}, err: {}", m.ino, err);
            }
        }
    }

    fn get_changed_blocks(&mut self, m: MsgGetChangedBlocks)-> Errno{
        let segs: HashMap<u128, Segment>;
        let garbages: HashMap<u128, Segment>;
        let version: u64;
        if let Some(h) = self.handles.get_mut(&m.ino){
            let visit = |version: u64| -> bool {
                if version >= m.version {
                    return true;
                }
                return false;
            };
            let (ss, ver, ret) = h.visitor(visit);
            if !ret.is_success(){
                error!("get_changed_blocks: failed to get changed blocks for ino: {}, version: {}, err: {:?}",
                m.ino, m.version, ret);
                return ret;
            }
            segs = ss;
            version = ver;
            if let Some(g) = h.garbages.get(&m.version){
                garbages = g.clone();
                if m.version > 0 {
                    let remove_ver = m.version - 1;
                    h.garbages.remove(&remove_ver);
                }
            } else {
                garbages = HashMap::new();
            }
        } else {
            segs = HashMap::new();
            garbages = HashMap::new();
            version = 0;
        }

        let ret = m.tx.send(RespChangedBlocks{
            segs: segs,
            garbages: garbages,
            version: version,
        });
        match ret{
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("get_changed_blocks: failed to send changed segments for ino: {}, err: {}",
                m.ino, err);
                return Errno::Eintr;
            }
        }
    }
}