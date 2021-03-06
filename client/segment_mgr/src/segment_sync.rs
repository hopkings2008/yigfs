
use std::sync::Arc;
use std::collections::HashMap;
use common::thread::Thread;
use common::error::Errno;
use crossbeam_channel::{Sender, unbounded, bounded};
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
use metaservice_mgr::meta_store::MetaStore;
use metaservice_mgr::types::Segment;

use crate::types::ChangedSegsUpdate;
use crate::types::MetaSyncOp;
use crate::{segment_sync_handler::SegSyncHandler, types::{SegDownload, SegSyncOp, SegUpload}};
use log::error;

pub struct SegSyncer{
    op_tx: Sender<SegSyncOp>,
    meta_sync_tx: Sender<MetaSyncOp>,
    stop_tx: Sender<u8>,
    thr: Thread,
}

impl SegSyncer {
    pub fn new(cache_store: Arc<dyn CacheStore>, backend_store: Arc<dyn BackendStore>, meta_store: Arc<MetaStore>) -> Self{
        let (op_tx, op_rx) = unbounded::<SegSyncOp>();
        let (meta_sync_tx, meta_sync_rx) = unbounded::<MetaSyncOp>();
        let (stop_tx, stop_rx) = bounded::<u8>(1);
        let mut seg_sync_handler = SegSyncHandler::new(cache_store.clone(),
        backend_store.clone(),
        meta_store.clone(),
        op_rx,
        meta_sync_rx,
        stop_rx);
        let mut syncer = SegSyncer{
            thr: Thread::create(&format!("seg_syncer")),
            op_tx: op_tx,
            meta_sync_tx: meta_sync_tx,
            stop_tx: stop_tx,
        };
        syncer.thr.run(move || {
            seg_sync_handler.start();
        });
        syncer
    }

    pub fn upload_segment(&self, dir: &String, id0: u64, id1: u64, offset: u64) -> Errno {
        let op = SegUpload{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            offset: offset,
        };
        let ret = self.op_tx.send(SegSyncOp::OpUpload(op));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("sync_segment: failed to send upload op for id0: {}, id1: {}, offset: {}, err: {}",
                id0, id1, offset, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn download_segment(&self, dir: &String, id0: u64, id1: u64, offset: u64, capacity: u64) -> Errno {
        let op = SegDownload{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            capacity: capacity,
            offset: offset,
        };
        let ret = self.op_tx.send(SegSyncOp::OpDownload(op));
        match ret {
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("sync_segment: failed to perform download segment for id0: {}, id1: {}, offset: {}, err: {}",
            id0, id1, offset, err);
                return Errno::Eintr;
            }
        }
    }

    pub fn update_changed_segments(&self, ino: u64, segs: HashMap<u128, Segment>, garbages: HashMap<u128, Segment>) -> Errno{
        let op = ChangedSegsUpdate{
            ino: ino,
            segs: segs,
            garbages: garbages,
        };
        let ret = self.meta_sync_tx.send(MetaSyncOp::OpUpdateChangedSegs(op));
        match ret{
            Ok(_) => {
                return Errno::Esucc;
            }
            Err(err) => {
                error!("update_changed_segments: failed to send req of changed segments for ino: {}, err: {}",
            ino, err);
                return Errno::Eintr;
            }
        }
    }
}

impl Drop for SegSyncer{
    fn drop(&mut self) {
        let ret = self.stop_tx.send(1);
        match ret {
            Ok(_) => {
                self.thr.join();
            }
            Err(err) => {
                error!("SegSyncer: failed to perform stop, err: {}", err);
            }
        }
    }
}