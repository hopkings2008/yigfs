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

#[derive(Debug, Default)]
pub struct Segment {
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub leader: String,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Default)]
pub struct Block {
    pub offset: u64,
    pub seg_start_addr: u64,
    pub seg_end_addr: u64,
    pub size: i64,
}

