extern crate crossbeam_channel;

use common::error::Errno;
use crossbeam_channel::Sender;

#[derive(Debug)]
pub struct MsgFileOpenOp {
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub resp_sender: Sender<MsgFileOpenResp>,
}

impl MsgFileOpenOp{
    pub fn response(&self, err: Errno){
        let ret = self.resp_sender.send(MsgFileOpenResp{
            id0: self.id0,
            id1: self.id1,
            err: err,
        });
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to send response for open(id0: {}, id1: {}), err: {}",
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
    pub resp_sender: Sender<MsgFileWriteResp>,
}

impl MsgFileWriteOp {
    pub fn response(&self, msg: MsgFileWriteResp){
        let ret = self.resp_sender.send(msg);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to send response for write(id0: {}, id1: {}, offset: {}), err: {}", 
                    self.id0, self.id1, self.offset, err);
            }
        }
    }
}

#[derive(Debug)]
pub struct MsgFileReadData {
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
    pub data_sender: Sender<MsgFileReadData>,
}

impl MsgFileReadOp {
    pub fn response(&self, msg: MsgFileReadData) {
        let ret = self.data_sender.send(msg);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to send response for read(id0: {}, id1: {}, offset: {}, size: {}), err: {}", 
                    self.id0, self.id1, self.offset, self.size, err);
            }
        }
    }
}

#[derive(Debug)]
pub enum MsgFileOp {
    OpOpen(MsgFileOpenOp),
    OpWrite(MsgFileWriteOp),
    OpRead(MsgFileReadOp),
    OpClose(MsgFileCloseOp),
    //OpDel(MsgFileDelOp),
}