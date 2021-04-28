
use std::sync::Arc;
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
pub struct SegSyncer{
}

impl SegSyncer {
    pub fn new(cache_store: Arc<dyn CacheStore>, backend_store: Arc<dyn BackendStore>) -> Self{
        SegSyncer{
        }
    }
}