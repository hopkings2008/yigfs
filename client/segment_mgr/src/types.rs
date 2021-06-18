use std::collections::HashMap;
use crossbeam_channel::{Sender};
use common::uuid;
use common::numbers::NumberOp;
use metaservice_mgr::types::Block as MetaBlock;
use metaservice_mgr::types::Segment as MetaSegment;
use interval_tree::tree::IntervalTree;


#[derive(Debug, Default)]
pub struct DataDir {
    pub dir: String,
    pub size: u64,
    pub num: u32,
}

#[derive(Debug)]
pub struct Segment {
    // seg_id will be generated from UUID. And UUID is u128, so we need two i64s.
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub capacity: u64,
    pub size: u64,
    pub backend_size: u64,
    pub leader: String,
    pub blocks: Vec<Block>,
}

impl Default for Segment {
    fn default() -> Self {
        Segment{
            seg_id0: 0,
            seg_id1: 0,
            capacity: 0,
            size: 0,
            backend_size: 0,
            leader: Default::default(),
            blocks: Default::default(),
        }
    }
}

impl Segment {
    pub fn new(leader: &String) -> Self {
        let ids = uuid::uuid_u64_le();
        Segment{
            seg_id0: ids[0],
            seg_id1: ids[1],
            capacity: 0,
            size: 0,
            backend_size: 0,
            leader: leader.clone(),
            blocks: Vec::<Block>::new(),
        }
    }

    pub fn rich_new(id0: u64, id1: u64, capacity: u64, leader: String) -> Self{
        Segment{
            seg_id0: id0,
            seg_id1: id1,
            capacity: capacity,
            size: 0,
            backend_size: 0,
            leader: leader,
            blocks: Vec::<Block>::new(),
        }
    }

    pub fn copy(&self) -> Self{
        let mut s = Segment{
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            capacity: self.capacity,
            size: self.size,
            backend_size: self.backend_size,
            leader: self.leader.clone(),
            blocks: Vec::<Block>::new(),
        };
        for b in &self.blocks{
            s.blocks.push(b.copy());
        }
        return s;
    }

    pub fn add_block(&mut self, ino: u64, offset: u64, seg_start_offset: u64, nwrite: i64) {
        let b = Block{
            ino: ino,
            generation: 0,
            offset: offset,
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            seg_start_addr: seg_start_offset,
            size: nwrite,
        };
        // we cannot find the consecutive block.
        self.blocks.push(b);
        
    }

    pub fn usage(&self) -> u64 {
        let mut total : u64 = 0;
        for b in &self.blocks {
            total += b.size as u64;
        }
        total
    }

    pub fn is_empty(&self)->bool {
        if self.blocks.is_empty() {
            return true;
        }
        return false;
    }

    pub fn to_meta_segment(&self) -> MetaSegment {
        let mut meta_seg = MetaSegment {
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            capacity: self.capacity,
            size: self.size,
            backend_size: self.backend_size,
            leader: self.leader.clone(),
            blocks: Vec::new(),
        };
        for b in &self.blocks {
            meta_seg.blocks.push(b.to_meta_block());
        }
        return meta_seg;
    }
}

#[derive(Debug, Default, Clone)]
pub struct Block {
    // file ino
    pub ino: u64,
    pub generation: u64,
    // the offset in the file specified by ino & generation
    pub offset: u64,
    // segment ids
    pub seg_id0: u64,
    pub seg_id1: u64,
    // the offset in this segment
    // note: range in segment is: [seg_start_addr, seg_end_addr)
    pub seg_start_addr: u64,
    // the size of this block
    pub size: i64,
}

impl Block {
    pub fn default() -> Self {
        Block{
            ino: 0,
            generation: 0,
            offset: 0,
            seg_id0: 0,
            seg_id1: 0,
            seg_start_addr: 0,
            size: -1,
        }
    }
    pub fn copy(&self) -> Self{
        Block{
            ino: self.ino,
            generation: self.generation,
            offset: self.offset,
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            seg_start_addr: self.seg_start_addr,
            size: self.size,
        }
    }

    pub fn to_meta_block(&self) ->  MetaBlock{
        MetaBlock{
            offset: self.offset,
            seg_start_addr: self.seg_start_addr,
            size: self.size,
        }
    }
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

#[derive(Debug)]
pub struct FileHandle {
    pub ino: u64,
    pub leader: String,
    // note: segments only contains meta, it doesn't contain blocks.
    // all the blocks are in block_tree.
    pub segments:  Vec<Segment>,
    pub garbage_blocks: HashMap<u128, Segment>,
    pub block_tree: IntervalTree<Block>,
    pub is_dirty: u8,
}

impl FileHandle {
    pub fn create(ino: u64, leader: String, segments: Vec<Segment>) -> Self {
        let mut h = FileHandle{
            ino: ino,
            leader: leader,
            segments: Vec::new(),
            garbage_blocks: HashMap::new(),
            block_tree: IntervalTree::new(Block::default()),
            is_dirty: 0,
        };

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
            for b in s.blocks {
                h.add_block(b);
            }
        }
        
        return h;
    }
    pub fn copy(&self)->Self {
        let mut handle = FileHandle{
            ino: self.ino,
            leader: self.leader.clone(),
            segments: Vec::new(),
            garbage_blocks: HashMap::new(),
            block_tree: self.block_tree.clone(),
            is_dirty: self.is_dirty,
        };
        for s in &self.segments {
            handle.segments.push(s.copy());
        }
        return handle;
    }
    
    pub fn new(ino: u64)->Self{
        FileHandle{
            ino: ino,
            leader: String::from(""),
            segments: Vec::new(),
            garbage_blocks: HashMap::new(),
            block_tree: IntervalTree::new(Block::default()),
            is_dirty: 0,
        }
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

    pub fn add_block(&mut self, b: Block){
        self.block_tree.insert_node(b.offset, b.offset + b.size as u64, b);
    }

    pub fn add_garbage_block(&mut self, b: Block){
        let id = NumberOp::to_u128(b.seg_id0, b.seg_id1);
        if let Some(s) = self.garbage_blocks.get_mut(&id) {
            s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
            return;
        }
        let mut s = Segment{
            seg_id0: b.seg_id0,
            seg_id1: b.seg_id1,
            capacity: 0,
            size: 0,
            backend_size: 0,
            leader: String::from(""),
            blocks: Vec::new(),
        };
        s.add_block(b.ino, b.offset, b.seg_start_addr, b.size);
        self.garbage_blocks.insert(id, s);
    }

    pub fn has_garbage_blocks(&self) -> bool {
        self.garbage_blocks.is_empty()
    }

    pub fn get_garbage_blocks(&self) -> Vec<Segment> {
        let mut segs: Vec<Segment> = Vec::new();
        for s in self.garbage_blocks.values(){
            segs.push(s.copy());
        }

        return segs;
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
pub enum MsgFileHandleOp{
    Add(FileHandle),
    AddBlock(MsgAddBlock),
    Del(u64),
    Get(MsgQueryHandle),
    GetBlocks(MsgGetBlocks),
    GetLastSegment(MsgGetLastSegment),
    AddSegment(MsgAddSegment),
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