
use common::error::Errno;
use crossbeam_channel::Sender;
use log::error;

pub struct MetaOpUploadSegResp{
    pub id0: u64,
    pub id1: u64,
    pub err: Errno,
}
pub struct MetaOpUploadSeg{
    pub id0: u64,
    pub id1: u64,
    pub offset: u64,
    // response sender
    pub tx: Sender<MetaOpResp>,
}

impl MetaOpUploadSeg{
    pub fn new(id0: u64, id1: u64, offset: u64, tx: Sender<MetaOpResp>)->Self{
        MetaOpUploadSeg{
            id0: id0,
            id1: id1,
            offset: offset,
            tx: tx,
        }
    }

    pub fn response(&self, resp: MetaOpResp) -> Errno {
        let ret = self.tx.send(resp);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("MetaOpUploadSeg::response: failed to send response for id0: {}, id1: {}, offset: {},
                err: {}", self.id0, self.id1, self.offset, err);
                return Errno::Eintr;
            }
        }
    }
}

pub enum MetaOp{
    OpUploadSeg(MetaOpUploadSeg),
}

pub enum MetaOpResp{
    RespUploadSeg(MetaOpUploadSegResp),
}