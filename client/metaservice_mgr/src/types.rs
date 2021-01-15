pub enum FileType {
    FILE = 1,
    DIR = 2,
    LINK = 3,
}

pub struct DirEntry{
    pub ino: u64,
    pub file_type: FileType,
    pub name: String,
}