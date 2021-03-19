
extern crate crossbeam_channel;

use std::collections::HashMap;
use std::thread;
use std::thread::JoinHandle;
use crossbeam_channel::{Sender, Receiver, bounded, select};
use common::error::Errno;
use common::defer;

pub struct MsgGetHandleInfo{
    pub ino: u64,
    pub tx: Sender<Option<FileHandleInfo>>,
}
pub enum FileHandleInfoOp {
    AddHandleInfo(FileHandleInfo),
    DelHandleInfo(u64),
    GetHandleInfo(MsgGetHandleInfo),
}


#[derive(Debug)]
pub struct FileHandleInfo {
    pub ino: u64,
    pub leader: String,
}

pub struct FileHandleInfoMgr {
    op_tx: Sender<FileHandleInfoOp>,
    stop_tx: Sender<u32>,
    impl_join_handle: Option<JoinHandle<()>>,
}

impl FileHandleInfoMgr {
    pub fn new() -> Self {
        let (tx, rx) = bounded::<FileHandleInfoOp>(100);
        let (stop_tx, stop_rx) = bounded::<u32>(1);
        
        let mut cacher = HandleCacher::new(rx, stop_rx);

        FileHandleInfoMgr{
            op_tx: tx,
            stop_tx: stop_tx,
            impl_join_handle: Some(thread::spawn(move || cacher.start())),
        }
    }

    pub fn stop(&mut self){
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to stop file handle info mgr, err: {}", err);
            }
        }
        // join the HandleMgr thread.
        if let Some(h) = self.impl_join_handle.take() {
            let ret = h.join();
            match ret {
                Ok(_) => {
                    println!("FileHandleInfoMgr has stopped.");
                }
                Err(_) => {
                    println!("FileHandleInfoMgr failes to stop, join failed");
                }
            }
        }
        drop(self.op_tx.clone());
        drop(self.stop_tx.clone());
    }

    pub fn add_handle_info(&self, info: FileHandleInfo)->Errno {
        let ino = info.ino;
        let leader = info.leader.clone();
        let ret = self.op_tx.send(FileHandleInfoOp::AddHandleInfo(info));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("add_handle_info: failed to send handle(ino: {}, leader: {}), err: {}", ino, leader, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn get_handle_info(&self, ino: u64) -> Result<FileHandleInfo, Errno> {
        let (tx, rx) = bounded::<Option<FileHandleInfo>>(1);
        let msg = MsgGetHandleInfo{
            ino: ino,
            tx: tx,
        };
        defer!{
            let rxc = rx.clone();
            drop(rxc);
        }
        let ret = self.op_tx.send(FileHandleInfoOp::GetHandleInfo(msg));
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("get_handle_info: failed to send ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
        let ret = rx.recv();
        match ret {
            Ok(ret) => {
                if let Some(h) = ret {
                    return Ok(h);
                }
                return Err(Errno::Enoent);
            }
            Err(err) => {
                println!("get_handle_info: failed to get handle for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
    }

    pub fn del_handle_info(&self, ino: u64) -> Errno {
        let ret = self.op_tx.send(FileHandleInfoOp::DelHandleInfo(ino));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                println!("del_handle_info: failed to send ino: {}, err: {}", ino, err);
                return Errno::Eintr;
            }
        }
    }
}

struct HandleCacher{
    op_rx: Receiver<FileHandleInfoOp>,
    stop_rx: Receiver<u32>,
    handles: HashMap<u64, FileHandleInfo>,
}

impl HandleCacher {
    pub fn new(op_rx: Receiver<FileHandleInfoOp>, stop_rx: Receiver<u32>) -> Self {
        HandleCacher{
            op_rx: op_rx,
            stop_rx: stop_rx,
            handles: HashMap::new(),
        }
    }

    pub fn start(&mut self) {
        loop {
            select!{
                recv(self.op_rx) -> msg => {
                    let op : FileHandleInfoOp;
                    match msg {
                        Ok(msg) => {
                            op = msg;
                        }
                        Err(err) => {
                            println!("start: failed to got handle_op msg, err: {}", err);
                            continue;
                        }
                    }
                    match op {
                        FileHandleInfoOp::AddHandleInfo(handle) => {
                            self.add_handle_info(handle);
                        }
                        FileHandleInfoOp::GetHandleInfo(msg) => {
                            self.get_handle_info(&msg);
                        }
                        FileHandleInfoOp::DelHandleInfo(ino) => {
                            self.del_handle_info(ino);
                        }
                    }
                },
                recv(self.stop_rx) -> msg => {
                    let rx = self.stop_rx.clone();
                    drop(rx);
                    let rx = self.op_rx.clone();
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

    fn add_handle_info(&mut self, h: FileHandleInfo) {
        self.handles.insert(h.ino, h);
    }

    fn get_handle_info(&mut self, msg: &MsgGetHandleInfo){
        let mut handle = None;
        defer!{
            let txc = msg.tx.clone();
            drop(txc);
        }
        if let Some(h) = self.handles.get(&msg.ino){
            handle = Some(FileHandleInfo{
                ino: h.ino,
                leader: h.leader.clone(),
            });
        }
        let ret = msg.tx.send(handle);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("get_handle_info: failed to send handle, err: {}", err);
            }
        }
    }

    fn del_handle_info(&mut self, ino: u64) {
        self.handles.remove(&ino);
    }

}