extern crate tokio;

use std::rc::Rc;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
pub struct Executor {
    runtime: Rc<Runtime>,
}

impl Executor{
    pub fn create() -> Self {
        Executor {
            runtime: Rc::new(Runtime::new().expect("runtime new successfully.")),
        }
    }

    pub fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }
}