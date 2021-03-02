extern crate tokio;

use std::collections::HashMap;
use std::io::SeekFrom;
use common::thread::Thread;
use common::numbers::NumberOp;
use common::error::Errno;
use common::runtime::Executor;
use tokio::{fs::{File, OpenOptions}, io::AsyncWriteExt, io::AsyncSeekExt, io::AsyncReadExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};
use crate::types::{MsgFileCloseOp, MsgFileOp, MsgFileOpenOp, MsgFileReadData, MsgFileReadOp, MsgFileWriteOp, MsgFileWriteResp};

pub struct IoThread {
    thr: Thread,
    op_tx: Sender<MsgFileOp>,
    stop_tx: Sender<u8>,
    exec: Executor,
}

impl IoThread  {
    pub fn create(name: &String, exec: &Executor)->Self {
        let (tx, rx) = mpsc::channel::<MsgFileOp>(1000);
        let(stop_tx, stop_rx) = mpsc::channel::<u8>(0);
        let mut worker = IoThreadWorker{
            handles: HashMap::<u128, File>::new(),
            op_rx: rx,
            stop_rx: stop_rx,
        };
        let mut thr = IoThread {
            thr: Thread::create(name),
            op_tx: tx,
            stop_tx: stop_tx,
            exec: exec.clone(),
        };
        thr.thr.run(move ||{
            let runtime = Runtime::new().expect("create runtime for iothread");
            runtime.block_on(worker.start());
        });
        return thr;
    }

    pub fn stop(&mut self) {
        let ret = self.exec.get_runtime().block_on(self.stop_tx.send(1));
        match ret {
            Ok(_)=>{}
            Err(err) => {
                println!("failed to stop IoThreadWorker, err: {}", err);
                return;
            }
        }
        self.thr.join();
    }

    pub fn send_disk_io(&self, msg: MsgFileOp)->Errno{
        let ret = self.exec.get_runtime().block_on(self.op_tx.send(msg));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("send_disk_io: failed to send op, err: {}", err);
                return Errno::Eintr;
            }
        }
    }
}

struct IoThreadWorker {
    //id0&id1 -> File
    handles: HashMap<u128, File>,
    op_rx: Receiver<MsgFileOp>,
    stop_rx: Receiver<u8>,
}

impl IoThreadWorker {
    pub fn new(op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>) -> Self {
        IoThreadWorker{
            handles: HashMap::<u128, File>::new(),
            op_rx: op_rx,
            stop_rx: stop_rx,
        }
    }

    pub async fn start(&mut self) {
        loop{
            tokio::select! {
                Some(msg) = self.op_rx.recv() => {
                    self.do_work(&msg).await;
                }
                Some(msg) = self.stop_rx.recv() => {
                    println!("got stop signal {}, stopping...", msg);
                    break;
                }
            }
        }
    }

    async fn do_work(&mut self, msg: &MsgFileOp){
        match msg {
            MsgFileOp::OpOpen(msg) => {
                self.do_open(msg).await;
            }
            MsgFileOp::OpRead(msg) => {
                self.do_read(msg).await;
            }
            MsgFileOp::OpWrite(msg) => {
                self.do_write(msg).await;
            }
            MsgFileOp::OpClose(msg) => {
                self.do_close(msg).await;
            }
        }
    }

    async fn do_open(&mut self, msg: &MsgFileOpenOp){
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        let name = self.to_file_name(d, &msg.dir);
        let f: File;
        // check wether the handle already opened
        if self.handles.contains_key(&d) {
            msg.response(Errno::Esucc).await;
            return;
        }
        let ret = OpenOptions::new().read(true).append(true).open(&name).await;
        match ret {
            Ok(ret) => {
                f = ret;
            }
            Err(err) => {
                println!("failed to open({}), err: {}", name, err);
                msg.response(Errno::Eintr).await;
                return;
            }
        }
        self.handles.insert(d, f);
        msg.response(Errno::Esucc).await;
        return;
    }

    async fn do_write(&mut self, msg: &MsgFileWriteOp) {
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        let mut resp_msg = MsgFileWriteResp{
            nwrite: 0,
            err: Errno::Enotf,
        };
        if let Some(h) = self.handles.get_mut(&d) {
            // should we seek before write?
            let ret = h.write(msg.data.as_slice()).await;
            match ret {
                Ok(ret) => {
                    resp_msg.nwrite = ret as u32;
                    resp_msg.err = Errno::Esucc;
                    msg.response(resp_msg).await;
                    return;
                }
                Err(err) => {
                    println!("failed to write(id0: {}, id1: {}) with offset: {}, err: {}", msg.id0, msg.id1, msg.offset, err);
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg).await;
                    return;
                }
            }
        }
        println!("no file handle for id0: {}, id1: {}", msg.id0, msg.id1);
        resp_msg.err = Errno::Enotf;
        msg.response(resp_msg).await;
    }

    async fn do_read(&mut self, msg: &MsgFileReadOp) {
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        if let Some(h) = self.handles.get_mut(&d) {
            let ret = h.seek(SeekFrom::Start(msg.offset)).await;
            match ret {
                Ok(ret) => {
                    println!("do_read: succeed to seek to {} for {:?}", ret, msg);
                }
                Err(err) => {
                    println!("do_read: fail to seek to {} for {:?}, err: {}", msg.offset, msg, err);
                    let resp_msg = MsgFileReadData{
                        data: None,
                        err: Errno::Eintr,
                    };
                    msg.response(resp_msg).await;
                    return;
                }
            }
            let mut total = 0;
            let mut errno = Errno::Esucc;
            let mut resp_data: Vec<u8> = Vec::new();
            loop {
                let mut data: [u8; 4<<10] = [0; 4<<10];
                //On a successful read, the number of read bytes is returned. 
                //If the supplied buffer is not empty and the function returns Ok(0),
                //then the source has reached an "end-of-file" event.
                let ret = h.read(&mut data[..]).await;
                match ret {
                    Ok(ret) => {
                        if ret == 0 {
                            // eof happens.
                            errno = Errno::Eeof;
                            break;
                        }
                        total += ret as u32;
                        let vdata = data[..ret].to_vec();
                        resp_data.extend(vdata);
                        if total >= msg.size {
                            println!("do_read: finish to read {} data for {:?}.", total, msg);
                            break;
                        }
                    }
                    Err(err) => {
                        println!("do_read: failed to read{:?} with err: {}", msg, err);
                        errno = Errno::Eintr;
                        break;
                    }
                }
            } // loop
            if errno.is_eof() || errno.is_success() {
                let resp_msg = MsgFileReadData{
                    data: Some(resp_data),
                    err: errno,
                };
                msg.response(resp_msg).await;
                return;
            }
            let resp_msg = MsgFileReadData{
                data: None,
                err: errno,
            };
            msg.response(resp_msg).await;
        } // if
        // file handle not found.
        println!("do_read: cannot find file handle for id0: {}, id1: {}", msg.id0, msg.id1);
        let resp_msg = MsgFileReadData{
            data: None,
            err: Errno::Enotf,
        };
        msg.response(resp_msg).await;
    }

    async fn do_close(&mut self, msg: &MsgFileCloseOp){
        let id = NumberOp::to_u128(msg.id0, msg.id1);
        if let Some(f) = self.handles.get_mut(&id) {
            let ret = f.flush().await;
            match ret {
                Ok(_) => {
                    self.handles.remove(&id);
                }
                Err(err) => {
                    println!("failed to flush File(id0: {}, id1: {}), err: {}", msg.id0, msg.id1, err);
                }
            }
        }
    }

    fn to_file_name(&self, id: u128, dir: &String) -> String {
        format!("{}/{}.seg", dir, id)
    }
}