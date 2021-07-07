use std::collections::HashMap;
use crossbeam_channel::{Sender};
use common::numbers::NumberOp;
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
    pub segments_index: HashMap<u128, usize>,
    pub changed_blocks: Vec<HashMap<u128, Segment>>,
    pub garbage_blocks: Vec<HashMap<u128, Segment>>,
    pub block_tree: IntervalTree<Block>,
    pub seg_status: HashMap<u128, SegStatus>,
    pub is_dirty: u8,
    pub reference: i64,
    pub change_version: usize,
}

impl FileHandle {
    pub fn create(ino: u64, leader: String, segments: Vec<Segment>) -> Self {
        let mut h = FileHandle{
            ino: ino,
            leader: leader,
            segments: Vec::new(),
            segments_index: HashMap::new(),
            changed_blocks: Vec::new(),
            garbage_blocks: Vec::new(),
            block_tree: IntervalTree::new(Block::default()),
            seg_status: HashMap::new(),
            is_dirty: 0,
            reference: 1,
            change_version: 0,
        };
        h.changed_blocks.push(HashMap::new());
        h.changed_blocks.push(HashMap::new());
        h.garbage_blocks.push(HashMap::new());
        h.garbage_blocks.push(HashMap::new());

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
        
        return h;
    }
    pub fn copy(&self)->Self {
        let mut handle = FileHandle{
            ino: self.ino,
            leader: self.leader.clone(),
            segments: Vec::new(),
            segments_index: HashMap::new(),
            changed_blocks: self.changed_blocks.clone(),
            garbage_blocks: self.garbage_blocks.clone(),
            block_tree: self.block_tree.clone(),
            seg_status: self.seg_status.clone(),
            is_dirty: self.is_dirty,
            reference: self.reference,
            change_version: self.change_version,
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
            changed_blocks: Vec::new(),
            garbage_blocks: Vec::new(),
            block_tree: IntervalTree::new(Block::default()),
            seg_status: HashMap::new(),
            is_dirty: 0,
            reference: 1,
            change_version: 0,
        };
        h.changed_blocks.push(HashMap::new());
        h.changed_blocks.push(HashMap::new());
        h.garbage_blocks.push(HashMap::new());
        h.garbage_blocks.push(HashMap::new());
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

    pub fn fresh_changed_blocks(&mut self) -> (usize, Vec<Segment>, Vec<Segment>) {
        let mut segs: Vec<Segment> = Vec::new();
        let mut garbages: Vec<Segment> = Vec::new();
        let current = self.change_version;
        let new_version = (self.change_version + 1) % 2;
        // if new_version's blocks is not empty, they must be former changed blocks
        // must return them firstly and don't switch the version.
        if !self.changed_blocks[new_version].is_empty() || !self.garbage_blocks[new_version].is_empty() {
            for (_, s) in &self.changed_blocks[new_version] {
                segs.push(s.clone());
            }
            for (_, s) in &self.garbage_blocks[new_version] {
                garbages.push(s.clone());
            }
            return (new_version, segs, garbages);
        }
        // first switch the version.
        self.change_version = new_version;
        // return the blocks in original version.
        for (_, s) in &self.changed_blocks[current] {
            segs.push(s.clone());
        }
        for (_, s) in &self.garbage_blocks[current] {
            garbages.push(s.clone());
        }
        (current, segs, garbages)
    }

    pub fn clear_changed_blocks(&mut self, version: usize) {
        if version >= self.changed_blocks.len() || version >= self.garbage_blocks.len(){
            // invalid version number.
            return;
        }
        self.changed_blocks[version].clear();
        self.garbage_blocks[version].clear();
    }

    pub fn add_block(&mut self, b: Block){
        if b.ino != self.ino {
            panic!("add_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
        }
        self.block_tree.insert_node(b.offset, b.offset + b.size as u64, b);
    }

    pub fn get_current_change_version(&self) -> usize {
        self.change_version
    }

    pub fn fresh_change_version(&mut self) {
        self.change_version = (self.change_version + 1) % 2;
    }

    pub fn add_changed_block(&mut self, b: &Block){
        if b.ino != self.ino {
            panic!("add_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
        }
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(s) = self.changed_blocks[self.change_version].get_mut(&id){
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            return;
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
            panic!("add_changed_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
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
        self.changed_blocks[self.change_version].insert(id, s);
    }

    pub fn add_garbage_block(&mut self, b: Block){
        if b.ino != self.ino {
            panic!("add_garbage_block: got invalid ino: {} for block: offset: {}, size: {}, expect: ino: {}",
            b.ino, b.offset, b.size, self.ino);
        }
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(s) = self.garbage_blocks[self.change_version].get_mut(&id) {
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            return;
        }
        let leader: String;
        if let Some(idx) = self.segments_index.get(&id){
            leader = self.segments[*idx].leader.clone();
        } else {
            panic!("add_changed_block: cannot find segment[{}, {}] for block: {:?}",
            b.seg_id0, b.seg_id1, b);
        }
        let mut s = Segment{
            seg_id0: b.seg_id0,
            seg_id1: b.seg_id1,
            capacity: 0,
            size: 0,
            backend_size: 0,
            leader: leader,
            blocks: Vec::new(),
        };
        s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
        self.garbage_blocks[self.change_version].insert(id, s);
    }

    pub fn has_garbage_blocks(&self) -> bool {
        self.garbage_blocks[0].is_empty() && self.garbage_blocks[1].is_empty()
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
pub struct MsgAddBlock{
    pub ino: u64,
    pub id0: u64,
    pub id1: u64,
    pub block: Block,
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
pub struct RespChangedSegments{
    pub version: usize,
    pub segs: Vec<Segment>,
    pub garbages: Vec<Segment>,
}
#[derive(Debug)]
pub struct MsgGetChangedSegments{
    pub ino: u64,
    pub tx: Sender<RespChangedSegments>,
}

#[derive(Debug)]
pub struct MsgClearChangedSegments{
    pub ino: u64,
    pub version: usize,
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
    GetChangedSegments(MsgGetChangedSegments),
    ClearChangedSegments(MsgClearChangedSegments),
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
pub enum SegSyncOp{
    OpUpload(SegUpload),
    OpDownload(SegDownload),
}