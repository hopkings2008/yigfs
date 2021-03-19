use common::error::Errno;
use crate::leader::Leader;
use crate::types::{BlockIo};

pub struct LeaderNotSupport {
}

impl Leader for LeaderNotSupport {
    fn open(&self, _ino: u64) -> Errno {
        Errno::Enotsupp
    }

    fn write(&self, _ino: u64, _offset: u64, _data: &[u8]) -> Result<BlockIo, Errno> {
        Err(Errno::Enotsupp)
    }

    fn close(&self, _ino: u64)->Errno{
        Errno::Enotsupp
    }

    fn release(&mut self){}
}

impl LeaderNotSupport{
    pub fn new()->Self{
        LeaderNotSupport{}
    }
}