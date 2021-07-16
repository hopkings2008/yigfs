use std::collections::HashMap;
use crossbeam_channel::{Sender};
use common::numbers::NumberOp;
use common::error::Errno;
use log::{error};
use metaservice_mgr::types::{Segment, Block};
use interval_tree::tree::IntervalTree;


#[derive(Debug, Default)]
pub struct DataDir {
    pub dir: String,
    pub size: u64,
    pub num: u32,
}



// below structs are for Leader usage.
#[derive(Debug, Default)]
pub struct SegmentIo {
    pub id0: u64,
    pub id1: u64,
    // the folder which segment resides
    pub dir: String,
}

#[derive(Debug, Default)]
pub struct BlockIo {
    pub id0: u64,
    pub id1: u64,
    // note: this offset is the start addr in the segment file.
    pub offset: u64,
    // the size of this block.
    pub size: u32,
}

#[derive(Debug, Default, Clone)]
pub struct SegStatus {
    pub id0: u64,
    pub id1: u64,
    pub need_sync: bool,
}

#[derive(Debug)]
pub struct FileHandle {
    pub ino: u64,
    pub leader: String,
    // note: segments only contains meta, it doesn't contain blocks.
    // all the blocks are in block_tree.
    pub segments:  Vec<Segment>,
    // segments_index contains the index of the segments according to the segment ids.
    pub segments_index: HashMap<u128, usize>,
    pub garbages: HashMap<u64, HashMap<u128, Segment>>,
    pub block_tree: IntervalTree<Block>,
    pub seg_status: HashMap<u128, SegStatus>,
    pub is_dirty: u8,
    pub reference: i64,
}

impl FileHandle {
    pub fn create(ino: u64, leader: String, segments: Vec<Segment>) -> Self {
        let mut h = FileHandle{
            ino: ino,
            leader: leader,
            segments: Vec::new(),
            segments_index: HashMap::new(),
            garbages: HashMap::new(),
            block_tree: IntervalTree::new(Block::default()),
            seg_status: HashMap::new(),
            is_dirty: 0,
            reference: 1,
        };

        let mut idx = 0;
        for s in segments {
            h.segments.push(Segment{
                seg_id0: s.seg_id0,
                seg_id1: s.seg_id1,
                capacity: s.capacity,
                size: s.size,
                backend_size: s.backend_size,
                leader: s.leader.clone(),
                blocks: Vec::new(),
            });
            let id = NumberOp::to_u128(s.seg_id0, s.seg_id1);
            h.segments_index.insert(id, idx);
            idx += 1;
            for b in s.blocks {
                let mut block = b.clone();
                block.ino = ino;
                block.generation = 0;
                block.seg_id0 = s.seg_id0;
                block.seg_id1 = s.seg_id1;
                h.add_block(block);
            }
        }

        // future changed version starts from 1.
        h.block_tree.set_version(1);
        
        return h;
    }

    pub fn copy(&self)->Self {
        let mut handle = FileHandle{
            ino: self.ino,
            leader: self.leader.clone(),
            segments: Vec::new(),
            segments_index: HashMap::new(),
            garbages: self.garbages.clone(),
            block_tree: self.block_tree.clone(),
            seg_status: self.seg_status.clone(),
            is_dirty: self.is_dirty,
            reference: self.reference,
        };
        let mut idx = 0;
        for s in &self.segments {
            handle.segments.push(s.copy());
            handle.segments_index.insert(NumberOp::to_u128(s.seg_id0, s.seg_id1), idx);
            idx += 1;
        }
        return handle;
    }
    
    pub fn new(ino: u64)->Self{
        let mut h = FileHandle{
            ino: ino,
            leader: String::from(""),
            segments: Vec::new(),
            segments_index: HashMap::new(),
            garbages: HashMap::new(),
            block_tree: IntervalTree::new(Block::default()),
            seg_status: HashMap::new(),
            is_dirty: 0,
            reference: 1,
        };
        return h;
    }

    pub fn mark_dirty(&mut self){
        self.is_dirty = 1;
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty == 1
    }

    pub fn get_segments(&self) -> Vec<Segment> {
        let mut segments = Vec::<Segment>::new();
        let mut msegs = HashMap::<u128, usize>::new();
        let mut idx = 0;
        for s in &self.segments {
            segments.push(s.copy());
            let id = NumberOp::to_u128(s.seg_id0, s.seg_id1);
            msegs.insert(id, idx);
            idx += 1;
        }
        let blocks = self.block_tree.traverse();
        for b in blocks {
            let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
            let e = msegs.get(&id);
            match e {
                Some(idx) => {
                    segments[*idx].add_block(b.ino, b.offset, b.seg_start_addr, b.size);
                }
                None => {
                    panic!("got invalid block whose segment doesn't exists, id0: {}, id1: {}", 
                b.seg_id0, b.seg_id1);
                }
            }
        }

        return segments;
    }

    pub fn visitor<F>(&mut self, visitor: F) -> (HashMap<u128, Segment>, u64, Errno) where
    F: Fn(u64) -> bool {
        let mut segments: HashMap<u128, Segment> = HashMap::new();
        let version = self.block_tree.get_version();
        let blocks = self.block_tree.visitor(visitor);
        for b in blocks {
            let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
            if let Some(s) = segments.get_mut(&id){
                s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
                continue;
            }
            let e = self.segments_index.get(&id);
            match e {
                Some(idx) => {
                    let mut s = self.segments[*idx].clone();
                    s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
                    segments.insert(id, s);
                }
                None => {
                    error!("got invalid block whose segment doesn't exists, id0: {}, id1: {}", 
                b.seg_id0, b.seg_id1);
                    return (segments, version, Errno::Eintr);
                }
            }
        }
        self.block_tree.set_version(version+1);
        return (segments, version, Errno::Esucc);
    }

    pub fn add_block(&mut self, b: Block)->Errno{
        if b.ino != self.ino {
            error!("add_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
            return Errno::Eintr;
        }
        self.block_tree.insert_node(b.offset, b.offset + b.size as u64, b);
        return Errno::Esucc;
    }

    pub fn add_changed_block(&mut self, segs: &mut HashMap<u128, Segment>, b: &Block)->Errno{
        if b.ino != self.ino {
            error!("add_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
            return Errno::Eintr;
        }
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(s) = segs.get_mut(&id){
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            return Errno::Esucc;
        }
        let leader: String;
        let capacity: u64;
        let size: u64;
        let backend_size: u64;
        if let Some(idx) = self.segments_index.get(&id){
            leader = self.segments[*idx].leader.clone();
            capacity = self.segments[*idx].capacity;
            size = self.segments[*idx].size;
            backend_size = self.segments[*idx].backend_size;
        } else {
            error!("add_changed_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
            return Errno::Eintr;
        }
        let mut s = Segment{
            seg_id0: b.seg_id0,
            seg_id1: b.seg_id1,
            capacity: capacity,
            size: size,
            backend_size: backend_size,
            leader: leader,
            blocks: Vec::new(),
        };
        s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
        segs.insert(id, s);
        return Errno::Esucc;
    }

    pub fn track_garbage_block(&mut self, b: Block) -> Errno {
        if b.ino != self.ino {
            error!("track_garbage_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
            return Errno::Eintr;
        }
        let current_version = self.block_tree.get_version();
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(seg_map) = self.garbages.get_mut(&current_version) {
            if let Some(s) = seg_map.get_mut(&id){
                s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
                return Errno::Esucc;
            }
            if let Some(idx) = self.segments_index.get(&id) {
                let mut s = self.segments[*idx].clone();
                s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
                seg_map.insert(id, s);
                return Errno::Esucc;
            }
            error!("track_garbage_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
            return Errno::Eintr;
        }
        let mut seg_map: HashMap<u128, Segment> = HashMap::new();
        if let Some(idx) = self.segments_index.get(&id) {
            let mut s = self.segments[*idx].clone();
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            seg_map.insert(id, s);
            self.garbages.insert(current_version, seg_map);
            return Errno::Esucc;
        }
        error!("track_garbage_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
        return Errno::Eintr
    }

    pub fn add_garbage_block(&mut self, segs: &mut HashMap<u128, Segment>, b: Block)->Errno{
        if b.ino != self.ino {
            error!("add_garbage_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
            return Errno::Eintr;
        }
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(s) = segs.get_mut(&id) {
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            return Errno::Esucc;
        }
        let leader: String;
        let capacity: u64;
        let size: u64;
        let backend_size: u64;
        if let Some(idx) = self.segments_index.get(&id){
            leader = self.segments[*idx].leader.clone();
            capacity = self.segments[*idx].capacity;
            size = self.segments[*idx].size;
            backend_size = self.segments[*idx].backend_size;
        } else {
            error!("add_changed_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
            return Errno::Eintr;
        }
        let mut s = Segment{
            seg_id0: b.seg_id0,
            seg_id1: b.seg_id1,
            capacity: capacity,
            size: size,
            backend_size: backend_size,
            leader: leader,
            blocks: Vec::new(),
        };
        s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
        segs.insert(id, s);
        return Errno::Esucc;
    }
}

#[derive(Debug)]
pub enum MsgUpdateHandleType {
    // add
    MsgHandleAdd = 0,
    // delete
    MsgHandleDel = 1,
}

#[derive(Debug)]
pub struct MsgUpdateHandle{
    pub update_type: MsgUpdateHandleType,
    pub handle: FileHandle,
}

#[derive(Debug)]
pub struct MsgQueryHandle{
    pub ino: u64,
    pub tx: Sender<Option<FileHandle>>,
}

#[derive(Debug)]
pub struct ChangedSegments{
    pub ret: Errno,
    pub segments: HashMap<u128, Segment>,
    pub garbages: HashMap<u128, Segment>,
}

#[derive(Debug)]
pub struct MsgAddBlock{
    pub ino: u64,
    pub id0: u64,
    pub id1: u64,
    pub block: Block,
    pub tx: Option<Sender<ChangedSegments>>,
}

impl MsgAddBlock{
    pub fn response(&self, segs: ChangedSegments)->Errno{
        if let Some(s) = &self.tx {
            let ret = s.send(segs);
            match ret{
                Ok(_) => {
                    return Errno::Esucc;
                }
                Err(err) => {
                    return Errno::Eintr;
                }
            }
        }
        return Errno::Esucc;
    }
}

#[derive(Debug)]
pub struct MsgGetLastSegment{
    pub ino: u64,
    pub tx: Sender<Vec<u64>>,
}

#[derive(Debug)]
pub struct MsgAddSegment{
    pub ino: u64,
    pub seg: Segment,
}

#[derive(Debug)]
pub struct MsgGetBlocks{
    pub ino: u64,
    pub offset: u64,
    pub size: u64,
    pub tx: Sender<Vec<Block>>,
}

#[derive(Debug)]
pub struct MsgSetSegStatus{
    pub ino: u64,
    pub id0: u64,
    pub id1: u64,
    pub need_sync: bool,
}

#[derive(Debug)]
pub struct MsgOpenHandle{
    pub ino: u64,
    pub tx: Sender<String>,
}

#[derive(Debug)]
pub struct MsgGetFileSegments{
    pub ino: u64,
    pub tx: Sender<Vec<Segment>>,
}

#[derive(Debug)]
pub struct RespChangedBlocks{
    pub segs: HashMap<u128, Segment>,
    pub garbages: HashMap<u128, Segment>,
    pub version: u64,
}
#[derive(Debug)]
pub struct MsgGetChangedBlocks{
    pub ino: u64,
    // the current largest changed version in block tree.
    pub version: u64,
    pub tx: Sender<RespChangedBlocks>,
}

#[derive(Debug)]
pub struct RespIntervalChangedBlocks{
    pub err: Errno,
    pub segs: HashMap<u64, RespChangedBlocks>,
}
#[derive(Debug)]
pub struct MsgGetIntervalChangedBlocks{
    pub start: u64,
    pub end: u64,
    pub tx: Sender<RespIntervalChangedBlocks>,
}

impl MsgGetIntervalChangedBlocks{
    pub fn response(&self, resp: RespIntervalChangedBlocks){
        let ret = self.tx.send(resp);
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send response for interval changed blocks, err: {}", err);
            }
        }
    }
}

#[derive(Debug)]
pub enum MsgFileHandleOp{
    Add(FileHandle),
    AddBlock(MsgAddBlock),
    Del(u64),
    Get(MsgQueryHandle),
    OpenHandle(MsgOpenHandle),
    GetBlocks(MsgGetBlocks),
    GetLastSegment(MsgGetLastSegment),
    AddSegment(MsgAddSegment),
    SetSegStatus(MsgSetSegStatus),
    GetFileSegments(MsgGetFileSegments),
    GetChangedBlocks(MsgGetChangedBlocks),
    GetIntervalChangedBlocks(MsgGetIntervalChangedBlocks),
}

#[derive(Debug)]
pub struct SegUpload{
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub offset: u64, // from where to upload.
}

#[derive(Debug)]
pub struct SegDownload{
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub capacity: u64,
    pub offset: u64, // from where to download.
}

#[derive(Debug)]
pub struct ChangedSegsUpdate{
    pub ino: u64,
    pub segs: HashMap<u128, Segment>,
    pub garbages: HashMap<u128, Segment>,
}
#[derive(Debug)]
pub enum SegSyncOp{
    OpUpload(SegUpload),
    OpDownload(SegDownload),
}


#[derive(Debug)]
pub enum MetaSyncOp{
    OpUpdateChangedSegs(ChangedSegsUpdate),
}

#[derive(Debug, Clone)]
pub struct FileMetaTracker{
    // file ino
    pub ino: u64,
    // start time to track
    pub start: u64,
    // end time of the interval
    pub end: u64,
    // interval: interval = end-start, that is [start, end)
    pub interval: u64,
    // version for the changed blocks.
    pub version: u64,
}

impl FileMetaTracker {
    pub fn new(ino: u64, start: u64, interval: u64, ver: u64) -> Self {
        FileMetaTracker{
            ino: ino,
            start: start,
            end: start+interval,
            interval: interval,
            version: ver,
        }
    }

    pub fn is_in(&self, start: u64) -> bool {
        self.start <= start && self.end >= start
    }

    pub fn is_the_file(&self, ino: u64) -> bool {
        self.ino == ino
    }

    pub fn update_start(&mut self, start: u64) {
        // this will update start and end by using self.interval
        self.start = start;
        self.end = self.start + self.interval;
    }
}