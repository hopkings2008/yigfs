use common::error::Errno;
use crate::types::{SegmentIo, BlockIo};
pub trait Leader {
    // open the segment for io
    fn open(&self, seg: &SegmentIo) -> Errno;
    // write the block into the segment file.
    // all the blocks are appended to the segment file.
    fn write(&self, seg: &SegmentIo, data: &[u8]) -> Result<BlockIo, Errno>;
}