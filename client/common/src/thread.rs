use std::thread;
use std::thread::{JoinHandle, Builder};

pub struct Thread {
    builder: Builder,
    handle: Option<JoinHandle<()>>,
    name: String,
}

impl Thread {
    pub fn create(name: &String)->Self {
        let builder = thread::Builder::new().name(name.to_string());
        Thread{
            builder: builder,
            handle: None,
            name: name.clone(),
        }
    }

    pub fn run<F>(mut self, f: F)
    where F: FnOnce(),
    F: Send + 'static {
        let ret = self.builder.spawn(f);
        match ret {
            Ok(ret) => {
                self.handle = Some(ret);
            }
            Err(err) => {
                println!("failed to spawn thread {} with err: {}", self.name, err);
            }
        }
    }

    pub fn join(self) {
        if let Some(h) = self.handle {
            h.join();
        }
    }
}