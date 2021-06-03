extern crate crossbeam_channel;

use common::error::Errno;
use crossbeam_channel::Sender;
use log::error;

#[derive(Debug)]
pub struct MsgFileOpenOp {
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub resp_sender: Sender<MsgFileOpResp>,
}

impl MsgFileOpenOp{
    pub fn response(&self, err: Errno){
        let ret = self.resp_sender.send(MsgFileOpResp::OpRespOpen(MsgFileOpenResp{
            id0: self.id0,
            id1: self.id1,
            err: err,
        }));
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send response for open(id0: {}, id1: {}), err: {}",
                self.id0, self.id1, err);
            }
        }
    }
}

#[derive(Debug)]
pub struct MsgFileOpenResp{
    pub id0: u64,
    pub id1: u64,
    pub err: Errno,
}

#[derive(Debug)]
pub struct MsgFileDelOp {
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub resp_sender: Sender<Errno>,
}

#[derive(Debug)]
pub struct MsgFileCloseOp{
    pub id0: u64,
    pub id1: u64,
}

#[derive(Debug)]
pub struct MsgFileWriteResp {
    pub id0: u64,
    pub id1: u64,
    pub offset: u64,
    pub nwrite: u32,
    pub err: Errno,
}

#[derive(Debug)]
pub struct MsgFileWriteOp {
    // segment ids.
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    // maximum size of this segment
    pub max_size: u64,
    // file offset, not the offset in the segment.
    pub offset: u64,
    pub data: Vec<u8>,
    pub resp_sender: Sender<MsgFileOpResp>,
}

impl MsgFileWriteOp {
    pub fn response(&self, msg: MsgFileWriteResp){
        let ret = self.resp_sender.send(MsgFileOpResp::OpRespWrite(msg));
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send response for write(id0: {}, id1: {}, offset: {}), err: {}", 
                    self.id0, self.id1, self.offset, err);
            }
        }
    }
}

#[derive(Debug)]
pub struct MsgFileReadData {
    pub id0: u64,
    pub id1: u64,
    pub data: Option<Vec<u8>>,
    pub err: Errno,
}

#[derive(Debug)]
pub struct MsgFileReadOp {
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub offset: u64,
    pub size: u32,
    pub data_sender: Sender<MsgFileOpResp>,
}

impl MsgFileReadOp {
    pub fn response(&self, msg: MsgFileReadData) {
        let ret = self.data_sender.send(MsgFileOpResp::OpRespRead(msg));
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send response for read(id0: {}, id1: {}, offset: {}, size: {}), err: {}", 
                    self.id0, self.id1, self.offset, self.size, err);
            }
        }
    }
}

#[derive(Debug)]
pub struct MsgFileStatOp{
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub result_tx: Sender<MsgFileStatResult>,
}

impl MsgFileStatOp{
    pub fn response(&self, msg: MsgFileStatResult){
        let ret = self.result_tx.send(msg);
        match ret {
            Ok(_) => {}
            Err(err) => {
                error!("failed to send response for stat(id0: {}, id1: {}), err: {}",
                self.id0, self.id1, err);
            }
        }
    }
}

#[derive(Debug)]
pub struct MsgFileStatResult{
    pub id0: u64,
    pub id1: u64,
    pub size: u64,
    pub err: Errno,
}

#[derive(Debug)]
pub enum MsgFileOp {
    OpOpen(MsgFileOpenOp),
    OpWrite(MsgFileWriteOp),
    OpRead(MsgFileReadOp),
    OpClose(MsgFileCloseOp),
    OpStat(MsgFileStatOp),
    //OpDel(MsgFileDelOp),
}

#[derive(Debug)]
pub enum MsgFileOpResp{
    OpRespOpen(MsgFileOpenResp),
    OpRespRead(MsgFileReadData),
    OpRespWrite(MsgFileWriteResp),
    OpRespStat(MsgFileStatResult),
}