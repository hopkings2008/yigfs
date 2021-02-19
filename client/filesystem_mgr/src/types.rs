use segment_mgr::types::{Segment};

pub struct FileHandle {
    pub ino: i64,
    pub leader: String,
    pub segments: Vec<Segment>,
}

