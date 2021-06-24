extern crate tokio;
extern crate hash_ring;

use std::sync::Arc;
use crate::types::DataDir;
use common::{error::Errno, numbers::NumberOp};
use common::config::Config;
use metaservice_mgr::mgr::MetaServiceMgr;
use metaservice_mgr::types::{Segment, Block};
use hash_ring::HashRing;
use log::error;

pub struct SegmentMgr {
    meta_service_mgr: Arc<dyn MetaServiceMgr>,
    // initialized during new() and later readonly.
    data_dirs: Vec<DataDir>,
    // key: u128 stands for segmentid; nodes usize stands for the index of data_dirs.
    // initialized during new() and later readonly.
    dirs_sharder: HashRing<usize>,
}

impl SegmentMgr {
    // make sure this function is threadsafe.
    pub fn get_file_segments(&self, ino: u64, leader: &String)-> Result<Vec<Segment>, Errno> {
        let mut segs : Vec<Segment>;
        let ret = self.meta_service_mgr.get_file_segments(ino, None, None);
        match ret {
            Ok(ret) => {
                segs = ret;
            }
            Err(err) => {
                error!("failed to get_file_segments for ino {}, err: {:?}", ino, err);
                return Err(err);
            }
        }
        if segs.is_empty() {
            // create new segment and set it's max_size.
            let seg = self.new_segment(leader);
            segs.push(seg);
        }
        
        //println!("the segments of ino: {} are: {:?}", ino, segments);
        Ok(segs)
    }

    pub fn new_segment(&self, leader: &String) -> Segment {
        let l: String;
        if leader == "" {
            l = self.meta_service_mgr.get_machine_id();
        } else {
            l = leader.clone();
        }
        let mut seg = Segment::new(&l);
        let idx = self.get_segment_dir_idx(seg.seg_id0, seg.seg_id1);
        seg.capacity = self.data_dirs[idx].size;
        seg
    }

    pub fn create(cfg: &Config, mgr: Arc<dyn MetaServiceMgr>) -> Self {
        let mut dirs: Vec<DataDir> = Vec::new();
        let mut dir_idxs: Vec<usize> = Vec::new();
        let mut idx : usize = 0;
        for d in &cfg.segment_configs {
            let dir = DataDir{
                dir: d.dir.clone(),
                size: d.size,
                num: d.num,
            };
            dir_idxs.push(idx);
            idx += 1;
            dirs.push(dir);
        }
        
        let ring: HashRing<usize> = HashRing::new(dir_idxs, 10);
        SegmentMgr{
            meta_service_mgr: mgr,
            data_dirs: dirs,
            dirs_sharder: ring,
        }
    }

    pub fn upload_block(&self, ino: u64, seg_id0: u64, seg_id1: u64, b: &Block)->Errno {
        let idx = self.get_segment_dir_idx(seg_id0, seg_id1);
        let data_dir = &self.data_dirs[idx];
        let mut seg = Segment::rich_new(seg_id0, seg_id1, data_dir.size, self.meta_service_mgr.get_machine_id());
        seg.add_block(ino, b.offset,b.seg_start_addr, b.size);
        let ret = self.meta_service_mgr.add_file_block(ino, &seg);
        return ret;
    }

    pub fn get_segment_dir(&self, id0: u64, id1: u64) -> String {
        let idx = self.get_segment_dir_idx(id0, id1);
        self.data_dirs[idx].dir.clone()
    }

    pub fn update_segments(&self, ino: u64, segs: &Vec<Segment>) -> Errno {
        let ret = self.meta_service_mgr.update_file_segments(ino, &segs);
        if !ret.is_success() {
            error!("update_segments: failed to update segments for ino: {}, err: {:?}", ino, ret);
            return ret;
        }
        return ret;
    }

    // private member functions.
    fn get_segment_dir_idx(&self, id0: u64, id1: u64) -> usize {
        let id = NumberOp::to_u128(id0, id1);
        let idx = self.dirs_sharder.get_node(id.to_string());
        // will return an element because none will not occur.
        match idx {
            Some(idx) => {*idx}
            None => {0}
        }
    }
}