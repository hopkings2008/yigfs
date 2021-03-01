use tokio::sync::mpsc::{Sender};
use common::error::Errno;

#[derive(Debug)]
pub struct MsgFileOpenOp {
    pub id0: u64,
    pub id1: u64,
    pub dir: String,
    pub resp_sender: Sender<Errno>,
}

impl MsgFileOpenOp{
    pub async fn response(&self, err: Errno){
        let ret = self.resp_sender.send(err).await;
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
pub struct MsgFileDelOp {
    pub id0: u64,
    pub id1: u64,
    pub resp_sender: Sender<Errno>,
}

#[derive(Debug)]
pub struct MsgFileCloseOp{
    pub id0: u64,
    pub id1: u64,
}

#[derive(Debug)]
pub struct MsgFileWriteResp {
    pub nwrite: u32,
    pub err: Errno,
}

#[derive(Debug)]
pub struct MsgFileWriteOp {
    pub id0: u64,
    pub id1: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub resp_sender: Sender<MsgFileWriteResp>,
}

impl MsgFileWriteOp {
    pub async fn response(&self, msg: MsgFileWriteResp){
        let ret = self.resp_sender.send(msg).await;
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
    pub offset: u64,
    pub size: u32,
    pub data_sender: Sender<MsgFileReadData>,
}

impl MsgFileReadOp {
    pub async fn response(&self, msg: MsgFileReadData) {
        let ret = self.data_sender.send(msg).await;
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