extern crate crossbeam_channel;

use std::collections::HashMap;
use std::thread;
use std::thread::JoinHandle;
use crate::types::{FileHandle, MsgQueryHandle, MsgFileHandleOp};
use crossbeam_channel::{Sender, Receiver, bounded, select};
use common::error::Errno;
use common::defer;

pub struct FileHandleMgr {
    //for update file handle.
    handle_op_tx: Sender<MsgFileHandleOp>,
    stop_tx: Sender<u32>,
    handle_mgr_th: Option<JoinHandle<()>>,
}

impl FileHandleMgr {
    pub fn create() -> FileHandleMgr {
        let (tx, rx) = bounded::<MsgFileHandleOp>(100);
        let (stop_tx, stop_rx) = bounded::<u32>(1);
        
        let mut handle_mgr = HandleMgr{
            handles: HashMap::<u64, FileHandle>::new(),
            handle_op_rx: rx,
            stop_rx: stop_rx,
        };

        let mgr = FileHandleMgr{
            handle_op_tx: tx,
            stop_tx: stop_tx,
            handle_mgr_th: Some(thread::spawn(move || handle_mgr.start())),
        };

        return mgr;
    }

    pub fn stop(&mut self){
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to stop file handle mgr, err: {}", err);
            }
        }
        // join the HandleMgr thread.
        if let Some(h) = self.handle_mgr_th.take() {
            let ret = h.join();
            match ret {
                Ok(_) => {
                    println!("FileHandleMgr has stopped.");
                }
                Err(_) => {
                    println!("FileHandleMgr failes to stop, join failed");
                }
            }
        }
        drop(self.handle_op_tx.clone());
        drop(self.stop_tx.clone());
    }

    pub fn add(&self, handle: &FileHandle) -> Errno {
        let msg = MsgFileHandleOp::Add(handle.copy());
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to add handle for ino: {}, err: {}", handle.ino, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn del(&self, ino: u64) -> Errno {
        let msg = MsgFileHandleOp::Del(ino);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("failed to del handle for ino: {}, err: {}", ino, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn get(&self, ino: u64) -> Result<FileHandle, Errno>{
        let (tx, rx) = bounded::<Option<FileHandle>>(1);
        let query = MsgQueryHandle{
            ino: ino,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        };
        let msg = MsgFileHandleOp::Get(query);
        let ret = self.handle_op_tx.send(msg);
        match ret {
            Ok(_) => {
                let ret = rx.recv();
                match ret {
                    Ok(ret) => {
                        match ret {
                            Some(mut h) => {
                                //sort the segments.
                                let mut iter_seg = h.segments.iter_mut();
                                loop {
                                    let i = iter_seg.next();
                                    match i {
                                        Some(i) => {
                                            i.blocks.sort_by(|a, b| a.offset.cmp(&b.offset));
                                        }
                                        None => {
                                            break;
                                        }
                                    }
                                }
                                return Ok(h);
                            }
                            None => {
                                return Err(Errno::Enoent);
                            }
                        }
                    }
                    Err(err) => {
                        println!("get: failed to get handle for ino: {}, recv failed with err: {}", ino, err);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                println!("get: failed to get handle for ino: {}, failed to send query with err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
    }

    pub fn is_leader(&self, machine: &String, ino: u64) -> bool {
        let ret = self.get(ino);
        match ret {
            Ok(ret) => {
                if ret.leader == *machine {
                    return true;
                }
                return false;
            }
            Err(err) => {
                println!("failed to get file handle for ino: {}, err: {:?}", ino, err);
                return false;
            }
        }
    }
}

struct HandleMgr {
    // ino-->FileHandle
    handles: HashMap<u64, FileHandle>,
    handle_op_rx: Receiver<MsgFileHandleOp>,
    stop_rx: Receiver<u32>,
}

impl HandleMgr {
    pub fn start(&mut self) {
        loop {
            select!{
                recv(self.handle_op_rx) -> msg => {
                    let op : MsgFileHandleOp;
                    match msg {
                        Ok(msg) => {
                            op = msg;
                        }
                        Err(err) => {
                            println!("handle_op: failed to got handle_op msg, err: {}", err);
                            continue;
                        }
                    }
                    match op {
                        MsgFileHandleOp::Add(h) => {
                            self.add(h);
                        }
                        MsgFileHandleOp::Del(ino) => {
                            self.del(ino);
                        }
                        MsgFileHandleOp::Get(m) => {
                            self.get(m);
                        }
                    }
                },
                recv(self.stop_rx) -> msg => {
                    let rx = self.stop_rx.clone();
                    drop(rx);
                    let rx = self.handle_op_rx.clone();
                    drop(rx);
                    match msg {
                        Ok(_) => {
                            println!("got stop signal, stop the loop...");
                            break;
                        }
                        Err(err) => {
                            println!("recv invalid stop signal with err: {} and stop the loop...", err);
                            break;
                        }
                    }
                },
            }
        }
    }

    fn add(&mut self, handle: FileHandle) {
        self.handles.insert(handle.ino, handle);
    }

    fn del(&mut self, ino: u64) {
        self.handles.remove(&ino);
    }

    fn get(&mut self, msg: MsgQueryHandle){
        let mut handle: Option<FileHandle> = None;
        let tx = msg.tx.clone();
        defer!{
            drop(tx);
        };
        if let Some(h) = self.handles.get(&msg.ino) {
            handle = Some(h.copy());
        }
        let ret = msg.tx.send(handle);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to send handle for ino: {}, err: {}", msg.ino, err);
            }
        }
    }
}