

#[derive(Debug, Default)]
pub struct Segment {
    // seg_id will be generated from UUID. And UUID is u128, so we need two i64s.
    pub seg_id0: u64,
    pub seg_id1: u64,
    pub leader: String,
    pub blocks: Vec<Block>,
}

impl Segment {
    pub fn copy(&self) -> Self{
        let mut s = Segment{
            seg_id0: self.seg_id0,
            seg_id1: self.seg_id1,
            leader: self.leader.clone(),
            blocks: Vec::<Block>::new(),
        };
        for b in &self.blocks{
            s.blocks.push(b.copy());
        }
        return s;
    }

    pub fn size(&self) -> u64 {
        let mut total : u64 = 0;
        for b in &self.blocks {
            total += b.size as u64;
        }
        total
    }

    pub fn contains(&self, offset: u64) -> bool {
        if self.blocks.is_empty() {
            return false;
        }
        let len = self.blocks.len();
        if self.blocks[0].offset > offset {
            return false;
        }
        if (self.blocks[len -1 ].offset + self.blocks[len-1].size as u64) < offset {
            return false;
        }

        return true;
    }
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
}