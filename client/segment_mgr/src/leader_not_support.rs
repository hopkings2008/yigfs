use common::error::Errno;
use crate::leader::Leader;
use crate::types::{SegmentIo, BlockIo};

pub struct LeaderNotSupport {
}

impl Leader for LeaderNotSupport {
    fn open(&self, seg: &SegmentIo) -> Errno {
        Errno::Enotsupp
    }

    fn write(&self, seg: &SegmentIo, data: &[u8]) -> Result<BlockIo, Errno> {
        Err(Errno::Enotsupp)
    }
}

impl LeaderNotSupport{
    pub fn new()->Self{
        LeaderNotSupport{}
    }
}