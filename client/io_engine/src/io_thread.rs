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
        let(stop_tx, stop_rx) = mpsc::channel::<u8>(1);
        let mut worker = IoThreadWorker::new(rx, stop_rx);
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
                msg = self.op_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            self.do_work(&msg).await;
                        }
                        None => {
                            println!("IoThreadWorker: op_tx has dropped.");
                            break;
                        }
                    }
                }
                msg = self.stop_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            println!("IoThreadWorker: got stop signal {}, stopping...", msg);
                        }
                        None => {
                            println!("IoThreadWorker: stop_tx has dropped.");
                        }
                    }
                    self.exists().await;
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
        let ret = OpenOptions::new().create(true).read(true).append(true).open(&name).await;
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
            offset: 0,
            nwrite: 0,
            err: Errno::Enotf,
        };
        // open the file first.
        if !self.handles.contains_key(&d) {
            let d = NumberOp::to_u128(msg.id0, msg.id1);
            let name = self.to_file_name(d, &msg.dir);
            let ret = OpenOptions::new().create(true).read(true).append(true).open(&name).await;
            match ret {
                Ok(f) => {
                    self.handles.insert(d, f);
                }
                Err(err) => {
                    println!("do_write: failed to open({}), err: {}", name, err);
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg).await;
                    return;
                }
            }
        }

        if let Some(h) = self.handles.get_mut(&d) {
            // should we seek to end before write?
            let ret = h.seek(SeekFrom::End(0)).await;
            match ret {
                Ok(ret) => {
                    resp_msg.offset = ret;
                }
                Err(err) => {
                    println!("do_write: failed to seek to end for msg({:?}, err: {}", msg, err);
                    resp_msg.err = Errno::Eseek;
                    msg.response(resp_msg).await;
                    return;
                }
            }
            // check whether segment has enough space for this write.
            if msg.max_size > resp_msg.offset {
                let left_space = msg.max_size - resp_msg.offset;
                if left_space < msg.data.len() as u64 {
                    println!("do_write: there is no space left of the segment: id0: {}, id1: {}, current offset: {}, left: {}",
                    msg.id0, msg.id1, resp_msg.offset, left_space);
                    resp_msg.err = Errno::Enospc;
                    msg.response(resp_msg).await;
                    return;
                }
            } else {
                println!("do_write: there is no space left of the segment: id0: {}, id1: {}, current offset: {}, seg_max_size:{}",
                    msg.id0, msg.id1, resp_msg.offset, msg.max_size);
                resp_msg.err = Errno::Enospc;
                msg.response(resp_msg).await;
                return;
            }
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
       
        // below line cannot be executed since if file is failed to open, we will return before.
        println!("no file handle for id0: {}, id1: {}", msg.id0, msg.id1);
        resp_msg.err = Errno::Enotf;
        msg.response(resp_msg).await;
    }

    async fn do_read(&mut self, msg: &MsgFileReadOp) {
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        // open the file first.
        if !self.handles.contains_key(&d) {
            let d = NumberOp::to_u128(msg.id0, msg.id1);
            let name = self.to_file_name(d, &msg.dir);
            let ret = OpenOptions::new().create(true).read(true).append(true).open(&name).await;
            match ret {
                Ok(f) => {
                    self.handles.insert(d, f);
                }
                Err(err) => {
                    println!("do_write: failed to open({}), err: {}", name, err);
                    let mut resp_msg = MsgFileReadData{
                        data: None,
                        err: Errno::Eintr,
                    };
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg).await;
                    return;
                }
            }
        }
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

    async fn exists(&mut self) {
        for (k,v) in &mut self.handles {
            let ret = v.flush().await;
            match ret {
                Ok(_) =>{}
                Err(err) => {
                    let ids = NumberOp::from_u128(*k);
                    println!("failed to flush file(id0: {}, id1: {}), err: {}", ids[0], ids[1], err);
                }
            }
        }
        self.handles.clear();
    }

    fn to_file_name(&self, id: u128, dir: &String) -> String {
        format!("{}/{}.seg", dir, id)
    }
}