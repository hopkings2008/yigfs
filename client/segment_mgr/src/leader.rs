use common::error::Errno;
use crate::types::Segment;
pub trait Leader {
    fn open_segment(seg: &Segment) -> Errno;
    fn write_segment(seg: &mut Segment, ino: u64, offset: u64, data: &[u8]) -> Result<u32, Errno>;
}