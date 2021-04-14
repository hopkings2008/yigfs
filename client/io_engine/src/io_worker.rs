extern crate crossbeam_channel;

use crate::types::MsgFileOp;
use common::runtime::Executor;
use crossbeam_channel::Receiver;

pub trait IoWorker {
    fn start(&mut self);
}

pub trait IoWorkerFactory {
    fn new_worker(&self, exec: &Executor, op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>)->Box<dyn IoWorker + Send>;
}