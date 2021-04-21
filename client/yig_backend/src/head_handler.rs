use crate::handler::CompleteHandler;
use crate::event::{IoEventResult, RespHeadObject};
use common::error::Errno;
use crossbeam_channel::Sender;

pub struct HeadHandler {
    op_tx: Sender<Errno>,
}

impl CompleteHandler for HeadHandler{
    fn handle(&self, event: IoEventResult){
        match event {
            IoEventResult::IoHeadResult(e) => {
                let ret = self.op_tx.send(Errno::Esucc);
                match ret {
                    Ok(_) => {
                        return;
                    }
                    Err(err) => {
                        println!("HeadHandler: failed to send res, err: {}", err);
                    }
                }
            }
            _ => {
                println!("HeadHander: got non head event result: {:?}", event);
            }
        }
    }
}