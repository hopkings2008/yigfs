

#[derive(Debug, Default)]
pub struct Segment {
    // seg_id will be generated from UUID. And UUID is u128, so we need two i64s.
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub leader: String,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Default)]
pub struct Block {
    pub ino: u64,
    pub generation: u64,
    // the offset in the file specified by ino & generation
    pub offset: u64,
    // the offset in this segment
    pub seg_start_addr: u64,
    // the end in this segment
    pub seg_end_addr: u64,
    // the size of this block
    pub size: i64,
}