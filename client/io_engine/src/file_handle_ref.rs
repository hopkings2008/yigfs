use std::fs::File;

// such as segment file handle.
pub struct FileHandleRef{
    pub file: File,
    pub size: u64,
    pub handle_ref: Ref,
}

impl FileHandleRef {
    pub fn new(f: File, size: u64) -> Self{
        FileHandleRef{
            file: f,
            size: size,
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

