extern crate crossbeam_channel;

use crate::types::MsgFileOp;
use crate::io_worker::IoWorkerFactory;
use common::{error::Errno, thread::Thread};
use common::runtime::Executor;
use crossbeam_channel::{unbounded, bounded, Sender};


pub struct IoThread{
    thr: Thread,
    op_tx: Sender<MsgFileOp>,
    stop_tx: Sender<u8>,
}


impl IoThread {
    pub fn create(name: &String, exec: &Executor, worker_factory: &Box<dyn IoWorkerFactory>) -> Self {
        let (op_tx, op_rx) = unbounded::<MsgFileOp>();
        let(stop_tx, stop_rx) = bounded::<u8>(1);
        let mut worker = worker_factory.new_worker(exec, op_rx,stop_rx);
        
        let mut thr = IoThread{
            thr: Thread::create(name),
            op_tx: op_tx,
            stop_tx: stop_tx,
        };
        thr.thr.run(move || {
            worker.start();
        });
        return thr;
    }

    pub fn stop(&mut self)->Errno{
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {
                self.thr.join();
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to stop thr {}, err: {}", self.thr.name(), err);
                return Errno::Eintr;
            }
        }
    }

    pub fn do_io(&self, msg: MsgFileOp) -> Errno {
        let ret = self.op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("do_io: failed to send io, err: {}", err);
                return Errno::Eintr;
            }
        }
    }
}