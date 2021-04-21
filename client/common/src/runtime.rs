extern crate tokio;

use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
pub struct Executor {
    runtime: Arc<Runtime>,
}

impl Executor{
    pub fn create() -> Self {
        Executor {
            runtime: Arc::new(Runtime::new().expect("runtime new successfully.")),
        }
    }

    pub fn create_single_threaded()-> Self {
        Executor{
            runtime: Arc::new(tokio::runtime::Builder::new_current_thread().
            build().expect("runtime curent thread new successfully.")),
        }
    }

    pub fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }
}