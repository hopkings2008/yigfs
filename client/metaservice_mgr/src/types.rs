extern crate serde;

use serde::{Serialize, Deserialize};
use common::uuid;
#[derive (Debug, Copy, Clone)]
pub enum FileType {
    UNKNOWN = 0,
    FILE = 1,
    DIR = 2,
    LINK = 3,
}

impl From<u8> for FileType {
    fn from(u: u8) -> FileType {
        match u {
            1 => {
                FileType::FILE
            }
            2 => {
                FileType::DIR
            }
            3 => {
                FileType::LINK
            }
            _ => {
                FileType::UNKNOWN
            }
        }
    }
}

#[derive (Debug)]
pub struct DirEntry{
    pub ino: u64,
    pub file_type: FileType,
    pub name: String,
}

#[derive (Debug)]
pub struct FileAttr {
    /// Inode number
    pub ino: u64,
    /// genration when ino has collision
    pub generation: u64,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: i64,
    /// Time of last modification
    pub mtime: i64,
    /// Time of last change
    pub ctime: i64,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: FileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

#[derive(Debug)]
pub struct FileLeader {
    pub zone: String,
    pub leader: String,
    pub ino: u64,
}
#[derive(Debug)]
pub struct NewFileInfo {
    pub leader_info: FileLeader,
    pub attr: FileAttr,
}

#[derive(Debug)]
pub struct SetFileAttr {
    pub ino: u64,
    /// Size in bytes
    pub size: Option<u64>,
    /// Time of last access
    pub atime: Option<i64>,
    /// Time of last modification
    pub mtime: Option<i64>,
    /// Time of last change
    pub ctime: Option<i64>,
    /// Permissions
    pub perm: Option<u16>,
    /// User id
    pub uid: Option<u32>,
    /// Group id
    pub gid: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Block {
    // file ino
    #[serde(skip)]
    pub ino: u64,
    #[serde(skip)]
    pub generation: u64,
    // the offset in the file specified by ino & generation
    pub offset: u64,
    // segment ids
    #[serde(skip)]
    pub seg_id0: u64,
    #[serde(skip)]
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
}

#[derive(Debug, Default)]
pub struct HeartbeatUploadSeg{
    pub id0: u64,
    pub id1: u64,
    // next offset to upload
    pub offset: u64,
}

#[derive(Debug, Default)]
pub struct HeartbeatRemoveSeg{
    pub id0: u64,
    pub id1: u64,
}
#[derive(Debug, Default)]
pub struct HeartbeatResult{
    pub upload_segments: Vec<HeartbeatUploadSeg>,
    pub remove_segments: Vec<HeartbeatRemoveSeg>,
}

