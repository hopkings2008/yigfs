use std::collections::HashMap;
use crossbeam_channel::{Sender};
use common::uuid;
use metaservice_mgr::types::Block as MetaBlock;
use metaservice_mgr::types::Segment as MetaSegment;


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
    pub max_size: u64,
    pub leader: String,
    pub blocks: Vec<Block>,
    // ino --> largest_offset
    file_largest_offsets: HashMap<u64, u64>,
}

impl Default for Segment {
    fn default() -> Self {
        Segment{
            seg_id0: 0,
            seg_id1: 0,
            max_size: 0,
            leader: Default::default(),
            blocks: Default::default(),
            file_largest_offsets: HashMap::new(),
        }
    }
}

impl Segment {
    pub fn new(leader: &String) -> Self {
        let ids = uuid::uuid_u64_le();
        Segment{
            seg_id0: ids[0],
            seg_id1: ids[1],
            max_size: 0,
            leader: leader.clone(),
            blocks: Vec::<Block>::new(),
            file_largest_offsets: HashMap::new(),
        }
    }

    pub fn rich_new(id0: u64, id1: u64, max_size: u64, leader: String) -> Self{
        Segment{
            seg_id0: id0,
            seg_id1: id1,
            max_size: max_size,
            leader: leader,
            blocks: Vec::<Block>::new(),
            file_largest_offsets: HashMap::new(),
        }
    }

    pub fn copy(&self) -> Self{
        let mut s = Segment{
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            max_size: self.max_size,
            leader: self.leader.clone(),
            blocks: Vec::<Block>::new(),
            file_largest_offsets: HashMap::new(),
        };
        for b in &self.blocks{
            s.blocks.push(b.copy());
        }
        for (k,v) in &self.file_largest_offsets {
            s.file_largest_offsets.insert(*k, *v);
        }
        return s;
    }

    pub fn add_block(&mut self, ino: u64, offset: u64, seg_start_offset: u64, nwrite: i64) {
        let b = Block{
            ino: ino,
            generation: 0,
            offset: offset,
            seg_start_addr: seg_start_offset,
            seg_end_addr: seg_start_offset+nwrite as u64,
            size: nwrite,
        };
        for b in &mut self.blocks {
            // original offset keeps the same, but we concatenate the two consecutive blocks.
            if b.seg_end_addr == seg_start_offset {
                b.seg_end_addr = seg_start_offset + nwrite as u64;
                b.size += nwrite;
                return;
            }
        }
        // we cannot find the consecutive block.
        self.blocks.push(b);
        // set the largest file offset.
        let o = self.file_largest_offsets.get(&ino);
        match o {
            Some(o) => {
                if *o < offset {
                    self.file_largest_offsets.insert(ino, offset);
                }
            }
            None => {
                self.file_largest_offsets.insert(ino, offset);
            }
        }
    }

    pub fn usage(&self) -> u64 {
        let mut total : u64 = 0;
        for b in &self.blocks {
            total += b.size as u64;
        }
        total
    }

    pub fn get_largest_offset(&self, ino: u64) -> u64 {
        let o = self.file_largest_offsets.get(&ino);
        match o {
            Some(o) => {
                *o
            }
            None => {
                0
            }
        }
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
            max_size: self.max_size,
            leader: self.leader.clone(),
            blocks: Vec::new(),
        };
        for b in &self.blocks {
            meta_seg.blocks.push(b.to_meta_block());
        }
        return meta_seg;
    }
}

#[derive(Debug, Default)]
pub struct Block {
    pub ino: u64,
    pub generation: u64,
    // the offset in the file specified by ino & generation
    pub offset: u64,
    // the offset in this segment
    // note: range in segment is: [seg_start_addr, seg_end_addr)
    pub seg_start_addr: u64,
    // the end in this segment
    pub seg_end_addr: u64,
    // the size of this block
    pub size: i64,
}

impl Block {
    pub fn copy(&self) -> Self{
        Block{
            ino: self.ino,
            generation: self.generation,
            offset: self.offset,
            seg_start_addr: self.seg_start_addr,
            seg_end_addr: self.seg_end_addr,
            size: self.size,
        }
    }

    pub fn to_meta_block(&self) ->  MetaBlock{
        MetaBlock{
            offset: self.offset,
            seg_start_addr: self.seg_start_addr,
            seg_end_addr: self.seg_end_addr,
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
    pub segments: Vec<Segment>,
}

impl FileHandle {
    pub fn copy(&self)->Self {
        let mut handle = FileHandle{
            ino: self.ino,
            leader: self.leader.clone(),
            segments: Vec::<Segment>::new(),
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
            segments: Vec::<Segment>::new(),
        }
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
    pub seg_max_size: u64,
    pub leader: String,
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
pub enum MsgFileHandleOp{
    Add(FileHandle),
    AddBlock(MsgAddBlock),
    Del(u64),
    Get(MsgQueryHandle),
    GetLastSegment(MsgGetLastSegment),
    AddSegment(MsgAddSegment),
}