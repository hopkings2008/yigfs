use tokio::fs::File;

pub struct FileHandleRef{
    pub file: File,
    pub handle_ref: Ref,
}

impl FileHandleRef {
    pub fn new(f: File) -> Self{
        FileHandleRef{
            file: f,
            handle_ref: Ref::new(),
        }
    }

    pub fn get(&mut self) {
        self.handle_ref.get();
    }

    pub fn put(&mut self) -> bool {
        self.handle_ref.put()
    }
}

pub struct Ref {
    pub ref_count: u32,
}

impl Ref {
    pub fn new() -> Self {
        Ref{
            ref_count: 1,
        }
    }

    pub fn get(&mut self) {
        self.ref_count += 1;
    }

    pub fn put(&mut self) -> bool {
        self.ref_count -= 1;
        self.ref_count <= 0
    }
}

