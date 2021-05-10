extern crate crossbeam_channel;

use crate::io_worker::{IoWorker, IoWorkerFactory};
use std::{collections::HashMap, io::{Read, Seek, Write}};
use std::io::SeekFrom;
use std::fs::{File, OpenOptions};
use common::numbers::NumberOp;
use common::error::Errno;
use common::runtime::Executor;
use crossbeam_channel::{Receiver, select};

use crate::types::{MsgFileCloseOp, MsgFileOp, MsgFileOpenOp, 
    MsgFileReadData, MsgFileReadOp, MsgFileWriteOp, MsgFileWriteResp};
use crate::file_handle_ref::FileHandleRef;

struct DiskIoWorker {
    //id0&id1 -> File
    handles: HashMap<u128, FileHandleRef>,
    op_rx: Receiver<MsgFileOp>,
    stop_rx: Receiver<u8>,
}

impl IoWorker for DiskIoWorker{
    fn start(&mut self) {
        loop {
            select!{
                recv(self.op_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            self.do_work(&msg);
                        }
                        Err(err) => {
                            println!("DiskIoWorker: failed to recv op_tx, err: {}", err);
                            break;
                        }
                    }
                }
                recv(self.stop_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            println!("DiskIoWorker: got stop signal {}, stopping...", msg);
                        }
                        Err(err) => {
                            println!("DiskIoWorker: stop_rx recved err: {}, stopping...", err);
                        }
                    }
                    self.exits();
                    break;
                }
            }
        }
    }
}

impl DiskIoWorker {
    pub fn new(op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>) -> Self {
        DiskIoWorker{
            handles: HashMap::<u128, FileHandleRef>::new(),
            op_rx: op_rx,
            stop_rx: stop_rx,
        }
    }

    fn do_work(&mut self, msg: &MsgFileOp){
        match msg {
            MsgFileOp::OpOpen(msg) => {
                self.do_open(msg);
            }
            MsgFileOp::OpRead(msg) => {
                self.do_read(msg);
            }
            MsgFileOp::OpWrite(msg) => {
                self.do_write(msg);
            }
            MsgFileOp::OpClose(msg) => {
                self.do_close(msg);
            }
        }
    }

    fn do_open(&mut self, msg: &MsgFileOpenOp){
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        let name = self.to_file_name(msg.id0, msg.id1, &msg.dir);
        let f: File;
        // check wether the handle already opened
        if let Some(rh) = self.handles.get_mut(&d) {
            rh.get();
            msg.response(Errno::Esucc);
            return;
        }
        let ret = OpenOptions::new().create(true).read(true).append(true).open(&name);
        match ret {
            Ok(ret) => {
                f = ret;
            }
            Err(err) => {
                println!("failed to open({}), err: {}", name, err);
                msg.response(Errno::Eintr);
                return;
            }
        }
        let file_size: u64;
        let ret = f.metadata();
        match ret {
            Ok(ret) => {
                file_size = ret.len();
            }
            Err(err) => {
                println!("faild to get file size for {}, err: {}", name, err);
                msg.response(Errno::Eintr);
                return;
            }
        }
        self.handles.insert(d, FileHandleRef::new(f, file_size));
        msg.response(Errno::Esucc);
        return;
    }

    fn do_write(&mut self, msg: &MsgFileWriteOp) {
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        let mut resp_msg = MsgFileWriteResp{
            id0: msg.id0,
            id1: msg.id1,
            offset: 0,
            nwrite: 0,
            err: Errno::Enotf,
        };
        // open the file first.
        if !self.handles.contains_key(&d) {
            let name = self.to_file_name(msg.id0, msg.id1, &msg.dir);
            let ret = OpenOptions::new().create(true).read(true).append(true).open(&name);
            match ret {
                Ok(f) => {
                    let file_size: u64;
                    let ret = f.metadata();
                    match ret {
                        Ok(ret) => {
                            file_size = ret.len();
                        }
                        Err(err) => {
                            println!("do_write: failed to get file size for {}, err: {}", name, err);
                            resp_msg.err = Errno::Eintr;
                            msg.response(resp_msg);
                            return;
                        }
                    }
                    self.handles.insert(d, FileHandleRef::new(f, file_size));
                }
                Err(err) => {
                    println!("do_write: failed to open({}), err: {}", name, err);
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg);
                    return;
                }
            }
        }

        if let Some(rf) = self.handles.get_mut(&d) {
            // should we seek to end before write?
            let ret = rf.file.seek(SeekFrom::End(0));
            match ret {
                Ok(ret) => {
                    resp_msg.offset = ret;
                }
                Err(err) => {
                    println!("do_write: failed to seek to end for msg({:?}, err: {}", msg, err);
                    resp_msg.err = Errno::Eseek;
                    msg.response(resp_msg);
                    return;
                }
            }
            // check whether segment has enough space for this write.
            if msg.max_size > resp_msg.offset {
                let left_space = msg.max_size - resp_msg.offset;
                if left_space < msg.data.len() as u64 {
                    println!("do_write: there is no space left of the segment: id0: {}, id1: {}, id: {}, current offset: {}, left: {}",
                    msg.id0, msg.id1, d, resp_msg.offset, left_space);
                    resp_msg.err = Errno::Enospc;
                    msg.response(resp_msg);
                    return;
                }
            } else {
                println!("do_write: there is no space left of the segment: id0: {}, id1: {}, id: {}, current offset: {}, seg_max_size:{}",
                    msg.id0, msg.id1, d, resp_msg.offset, msg.max_size);
                resp_msg.err = Errno::Enospc;
                msg.response(resp_msg);
                return;
            }
            let ret = rf.file.write(msg.data.as_slice());
            match ret {
                Ok(ret) => {
                    resp_msg.nwrite = ret as u32;
                    resp_msg.err = Errno::Esucc;
                    rf.size += ret as u64;
                    msg.response(resp_msg);
                    return;
                }
                Err(err) => {
                    println!("failed to write(id0: {}, id1: {}) with offset: {}, err: {}", msg.id0, msg.id1, msg.offset, err);
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg);
                    return;
                }
            }
        }
       
        // below line cannot be executed since if file is failed to open, we will return before.
        println!("no file handle for id0: {}, id1: {}", msg.id0, msg.id1);
        resp_msg.err = Errno::Enotf;
        msg.response(resp_msg);
    }

    fn do_read(&mut self, msg: &MsgFileReadOp) {
        let d = NumberOp::to_u128(msg.id0, msg.id1);
        // open the file first.
        if !self.handles.contains_key(&d) {
            let name = self.to_file_name(msg.id0, msg.id1, &msg.dir);
            let ret = OpenOptions::new().create(true).read(true).append(true).open(&name);
            match ret {
                Ok(f) => {
                    let file_size: u64;
                    let ret = f.metadata();
                    match ret{
                        Ok(ret) => {
                            file_size = ret.len();
                        }
                        Err(err) => {
                            println!("do_read: failed to get file size for {}, err: {}", name, err);
                            let resp_msg = MsgFileReadData{
                                id0: msg.id0,
                                id1: msg.id1,
                                data: None,
                                err: Errno::Eintr,
                            };
                            msg.response(resp_msg);
                            return;
                        }
                    }
                    self.handles.insert(d, FileHandleRef::new(f, file_size));
                }
                Err(err) => {
                    println!("do_read: failed to open({}), err: {}", name, err);
                    let mut resp_msg = MsgFileReadData{
                        id0: msg.id0,
                        id1: msg.id1,
                        data: None,
                        err: Errno::Eintr,
                    };
                    resp_msg.err = Errno::Eintr;
                    msg.response(resp_msg);
                    return;
                }
            }
        }
        if let Some(h) = self.handles.get_mut(&d) {
            let ret = h.file.seek(SeekFrom::Start(msg.offset));
            match ret {
                Ok(_ret) => {
                    //println!("do_read: succeed to seek to {} for {:?}", ret, msg);
                }
                Err(err) => {
                    println!("do_read: fail to seek to {} for {:?}, err: {}", msg.offset, msg, err);
                    let resp_msg = MsgFileReadData{
                        id0: msg.id0,
                        id1: msg.id1,
                        data: None,
                        err: Errno::Eintr,
                    };
                    msg.response(resp_msg);
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
                let ret = h.file.read(&mut data[..]);
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
                            //println!("do_read: finish to read {} data for {:?}.", total, msg);
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
            if errno.is_success() {
                let resp_msg = MsgFileReadData{
                    id0: msg.id0,
                    id1: msg.id1,
                    data: Some(resp_data),
                    err: errno,
                };
                msg.response(resp_msg);
                return;
            } else if errno.is_eof() {
                let mut resp_msg = MsgFileReadData{
                    id0: msg.id0,
                    id1: msg.id1,
                    data: None,
                    err: errno,
                };
                if resp_data.len() > 0 {
                    resp_msg.data = Some(resp_data);
                    resp_msg.err = Errno::Esucc;
                }
                msg.response(resp_msg);
                return;
            }
            let resp_msg = MsgFileReadData{
                id0: msg.id0,
                id1: msg.id1,
                data: None,
                err: errno,
            };
            msg.response(resp_msg);
            return;
        } // if
        // file handle not found.
        println!("do_read: cannot find file handle for id0: {}, id1: {}", msg.id0, msg.id1);
        let resp_msg = MsgFileReadData{
            id0: msg.id0,
            id1: msg.id1,
            data: None,
            err: Errno::Enotf,
        };
        msg.response(resp_msg);
    }

    fn do_close(&mut self, msg: &MsgFileCloseOp){
        let id = NumberOp::to_u128(msg.id0, msg.id1);
        if let Some(f) = self.handles.get_mut(&id) {
            let ret = f.file.sync_all();
            match ret {
                Ok(_) => {
                    if f.put() {
                        self.handles.remove(&id);
                    }
                }
                Err(err) => {
                    println!("failed to flush File(id0: {}, id1: {}), err: {}", msg.id0, msg.id1, err);
                }
            }
        }
    }

    fn exits(&mut self) {
        for (k,v) in &mut self.handles {
            let ret = v.file.sync_all();
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

    fn to_file_name(&self, id0: u64, id1: u64, dir: &String) -> String {
        format!("{}/{}.{}.seg", dir, id0, id1)
    }
}

pub struct DiskIoWorkerFactory {
}

impl IoWorkerFactory for DiskIoWorkerFactory {
    fn new_worker(&self, _exec: &Executor, op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>)->Box<dyn IoWorker + Send>{
        Box::new(DiskIoWorker::new(op_rx, stop_rx))
    }
}

impl DiskIoWorkerFactory {
    pub fn new() -> Box<dyn IoWorkerFactory> {
        Box::new(DiskIoWorkerFactory{})
    }
}