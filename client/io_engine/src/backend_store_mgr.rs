use common::error::Errno;

use crate::backend_storage::{BackendStore, BackendStoreFactory};
use std::collections::HashMap;
use std::sync::Arc;

pub struct BackendStoreMgr {
    backend_stores: HashMap<u32, Box<dyn BackendStoreFactory>>,
}

impl BackendStoreMgr {
    pub fn new() -> Self {
        BackendStoreMgr{
            backend_stores: HashMap::new(),
        }
    }

    pub fn register(&mut self, backend_store_type: u32, factory: Box<dyn BackendStoreFactory>) {
        self.backend_stores.insert(backend_store_type, factory);
    }

    pub fn get_backend_store(&self, backend_store_type: u32, settings: &HashMap<String, String>) -> Result<Arc<dyn BackendStore>, Errno> {
        if let Some(f) = self.backend_stores.get(&backend_store_type) {
            let ret = f.new_backend_store(settings);
            return ret;
        }

        return Err(Errno::Enoent);
    }

}