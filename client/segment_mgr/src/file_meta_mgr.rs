use std::collections::HashMap;
use crate::types::FileMetaTracker;
use interval_tree::tree::IntervalTree;

/*
* FileMetaMgr is not threadsafe
*
*/
#[derive(Debug)]
pub struct FileMetaMgr{
    file_meta_tree: IntervalTree<FileMetaTracker>,
    // ino-->start, can use this to find the FileMetaTracker quickly
    file_ino_map: HashMap<u64, u64>,
    interval: u64,
}

impl FileMetaMgr {
    pub fn new(interval: u64)-> Self {
        FileMetaMgr{
            // create the 'nil' FileMetaTracker for nil node of IntervalTree
            file_meta_tree: IntervalTree::new(FileMetaTracker::new(0, 0, 0)),
            file_ino_map: HashMap::new(),
            interval: interval,
        }
    }

    // insert the ino of the file to tracker.
    pub fn insert(&mut self, ino: u64, start: u64){
        let end = start + self.interval;
        self.file_meta_tree.insert_node(start, end, 
            FileMetaTracker::new(ino, start, self.interval));
        self.file_ino_map.insert(ino, start);
    }

    pub fn update(&mut self, ino: u64, new_start: u64){
        let new_end = new_start + self.interval;
        // if we cannot find the ino, just insert it.
        if let Some(old_start) = self.file_ino_map.get(&ino){
            let old_end = *old_start + self.interval;
            let nodes = self.file_meta_tree.get(*old_start, old_end);
            for n in nodes {
                let meta_tracker = n.borrow().get_value();
                if meta_tracker.is_the_file(ino){
                    self.file_meta_tree.delete(&n);
                }
            }
        }
        
        self.file_meta_tree.insert_node(new_start, new_end, 
        FileMetaTracker::new(ino, new_start, self.interval));
        // insert or update the file_ino_map.
        self.file_ino_map.insert(ino, new_start);
    }

    pub fn get_range(&self, start: u64) -> Vec<FileMetaTracker> {
        let mut metas: Vec<FileMetaTracker> = Vec::new();
        // [start, end) should be larger than the interval for retrieving inos.
        let end = start + self.interval;
        let nodes = self.file_meta_tree.get(start, end);
        for n in nodes {
            let meta_tracker = n.borrow().get_value();
            metas.push(meta_tracker);
        }

        return metas;
    }

    pub fn remove(&mut self, ino: u64){
        if let Some(start) = self.file_ino_map.get(&ino){
            let end = *start + self.interval;
            let nodes = self.file_meta_tree.get(*start, end);
            for n in nodes {
                let meta_tracker = n.borrow().get_value();
                if meta_tracker.ino == ino {
                    self.file_meta_tree.delete(&n);
                    self.file_ino_map.remove(&ino);
                }
            }
        }
    }
}