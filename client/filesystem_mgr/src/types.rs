use segment_mgr::types::{Segment};
use crossbeam_channel::{Sender};

#[derive(Debug)]
pub struct FileHandle {
    pub ino: u64,
    pub segments: Vec<Segment>,
}

impl FileHandle {
    pub fn copy(&self)->FileHandle {
        let mut handle = FileHandle{
            ino: self.ino,
            segments: Vec::<Segment>::new(),
        };
        for s in &self.segments {
            handle.segments.push(s.copy());
        }
        return handle;
    }
    
    pub fn new(ino: u64)->FileHandle{
        FileHandle{
            ino: ino,
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