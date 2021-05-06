use std::time::Duration;
use common::thread::Thread;
use crossbeam_channel::{Receiver, Sender, bounded, select};
use metaservice_mgr::mgr::MetaServiceMgr;
use std::sync::Arc;
use crate::{segment_mgr::SegmentMgr, segment_sync::SegSyncer};
use metaservice_mgr::types::HeartbeatResult;


pub struct HeartbeatMgr{
    // timeout to send heartbeat, by default, it is 5s
    stop_tx: Sender<u8>,
    thr: Thread,
}

impl HeartbeatMgr {
    pub fn new(timeout: u64, syncer: Arc<SegSyncer>, meta_mgr: Arc<dyn MetaServiceMgr>, segment_mgr: Arc<SegmentMgr>) -> Self {
        let (stop_tx, stop_rx) = bounded::<u8>(1);
        let mut hm = HeartbeatMgr{
            stop_tx: stop_tx,
            thr: Thread::create(&format!("HeartbeatMgr")),
        };
        let hi = HeartbeatImpl::new(timeout,
            stop_rx,
            syncer,
            meta_mgr,
            segment_mgr);
        hm.thr.run(move || {
            hi.start();
        });
        return hm;
    }

    pub fn stop(&mut self){
        let ret = self.stop_tx.send(1);
        match ret{
            Ok(_) => {
                self.thr.join();
            }
            Err(err) => {
                println!("HeartbeatMgr: failed to perform stop, err: {}", err);
            }
        }
    }
}

struct HeartbeatImpl {
    timeout: u64,
    stop_rx: Receiver<u8>,
    segment_syncer: Arc<SegSyncer>,
    meta_mgr: Arc<dyn MetaServiceMgr>,
    segment_mgr: Arc<SegmentMgr>,
}

impl HeartbeatImpl {
    pub fn new(timeout: u64, 
        stop_rx: Receiver<u8>, 
        syncer: Arc<SegSyncer>, 
        meta_mgr: Arc<dyn MetaServiceMgr>,
        segment_mgr: Arc<SegmentMgr>) -> Self{
        HeartbeatImpl{
            timeout: timeout,
            stop_rx: stop_rx,
            segment_syncer: syncer,
            meta_mgr: meta_mgr,
            segment_mgr: segment_mgr,
        }
    }

    pub fn start(&self){
        loop {
            select! {
                recv(self.stop_rx)->msg => {
                    match msg{
                        Ok(msg) => {
                            println!("HeartbeatImpl: got stop signal: {}, stopping...", msg);
                        }
                        Err(err) => {
                            println!("HeartbeatImpl: receive error from stop_rx, err: {}", err);
                        }
                    }
                    return;
                }
                default(Duration::from_secs(self.timeout)) => {
                    // perform heartbeat
                    let result: HeartbeatResult;
                    let ret = self.meta_mgr.heartbeat();
                    match ret {
                        Ok(ret) => {
                            result = ret;
                        }
                        Err(err) => {
                            println!("HeartbeatImpl: failed to perform heartbeat, err: {:?}", err);
                            continue;
                        }
                    }
                    // upload the segments.
                    for u in &result.upload_segments {
                        let seg_dir = self.segment_mgr.get_segment_dir(u.id0, u.id1);
                        let err = self.segment_syncer.upload_segment(&seg_dir, u.id0, u.id1, u.offset);
                        if !err.is_success() {
                            println!("HeartbeatImpl: failed to upload segment: id0: {}, id1: {}, dir: {}, err: {:?}",
                            u.id0, u.id1, seg_dir, err);
                            continue;
                        }
                    }
                    // TODO remove the segments.
                }
            }
        }
    }
}