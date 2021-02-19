use segment_mgr::types::{Segment};

pub struct FileHandle {
    pub ino: i64,
    pub segments: Vec<Segment>,
}

