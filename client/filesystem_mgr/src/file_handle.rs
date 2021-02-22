extern crate crossbeam_channel;

use crate::types::{FileHandle, MsgQueryHandle, MsgUpdateHandle, MsgUpdateHandleType};
use std::collections::HashMap;
use crossbeam_channel::{Sender, Receiver, bounded, select};
use common::error::Errno;

pub struct FileHandleMgr {
    // ino-->FileHandle
    handles: HashMap<u64, FileHandle>,
    //for update file handle.
    handle_update_tx: Sender<MsgUpdateHandle>,
    handle_update_rx: Receiver<MsgUpdateHandle>,
    handle_query_tx: Sender<MsgQueryHandle>,
    handle_query_rx: Receiver<MsgQueryHandle>,
    stop_tx: Sender<u32>,
    stop_rx: Receiver<u32>,
}

impl FileHandleMgr {
    pub fn create() -> FileHandleMgr {
        let (tx, rx) = bounded::<MsgUpdateHandle>(100);
        let (tx_query, rx_query) = bounded::<MsgQueryHandle>(1);
        let (stop_tx, stop_rx) = bounded::<u32>(1);
        let mut mgr = FileHandleMgr{
            handles: HashMap::<u64, FileHandle>::new(),
            handle_update_tx: tx,
            handle_update_rx: rx,
            handle_query_tx: tx_query,
            handle_query_rx: rx_query,
            stop_tx: stop_tx,
            stop_rx: stop_rx,
        };
        return mgr;
    }

    pub fn start(&mut self) {
        loop {
            select!{
                recv(self.handle_update_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            match msg.update_type {
                                MsgUpdateHandleType::MsgHandleAdd => {
                                    self.handles.insert(msg.handle.ino, msg.handle);
                                }
                                MsgUpdateHandleType::MsgHandleDel => {
                                    self.handles.remove(&msg.handle.ino);
                                }
                            }
                        }
                        Err(err) => {
                            println!("got invalid handle update msg, err: {}", err);
                        }
                    }
                },
                recv(self.handle_query_rx) -> msg => {
                    match msg {
                        Ok(msg)=> {
                            if let Some(h) = self.handles.get(&msg.ino){
                                let resp_handle = h.clone();
                                let ret = msg.tx.send(Some(resp_handle.copy()));
                                match ret {
                                    Ok(_) => {}
                                    Err(err) => {
                                        println!("failed to send handle for ino: {}, err: {}",
                                        msg.ino, err);
                                    }
                                }
                                // drop the sender of msg.
                                drop(msg.tx);
                            }
                        }
                        Err(err) => {
                            println!("got invalid handle query msg, err: {}", err);
                        }
                    }
                },
                recv(self.stop_rx) -> msg => {
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

    pub fn stop(&self){
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {}
            Err(err) => {
                println!("failed to stop file handle mgr, err: {}", err);
            }
        }
    }
    pub fn add(&mut self, handle: &FileHandle) -> Errno {
        let msg = MsgUpdateHandle{
            update_type: MsgUpdateHandleType::MsgHandleAdd,
            handle: handle.copy(),
        };
        let ret = self.handle_update_tx.send(msg);
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

    pub fn del(&mut self, ino: u64) -> Errno {
        let msg = MsgUpdateHandle{
            update_type: MsgUpdateHandleType::MsgHandleDel,
            handle: FileHandle::new(ino),
        };
        let ret = self.handle_update_tx.send(msg);
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

    pub fn get(&mut self, ino: u64) -> Result<FileHandle, Errno>{
        let (tx, rx) = bounded::<Option<FileHandle>>(1);
        let msg = MsgQueryHandle{
            ino: ino,
            tx: tx,
        };
        let ret = self.handle_query_tx.send(msg);
        match ret {
            Ok(_) => {
                let ret = rx.recv();
                match ret {
                    Ok(ret) => {
                        match ret {
                            Some(h) => {
                                return Ok(h);
                            }
                            None => {
                                return Err(Errno::Enoent);
                            }
                        }
                    }
                    Err(err) => {
                        println!("failed to get handle for ino: {}, recv failed with err: {}", ino, err);
                        return Err(Errno::Eintr);
                    }
                }
            }
            Err(err) => {
                println!("failed to get handle for ino: {}, err: {}", ino, err);
                return Err(Errno::Eintr);
            }
        }
    }
}