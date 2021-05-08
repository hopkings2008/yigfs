use common::error::Errno;

#[derive(Debug, Default)]
pub struct S3ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub size: u64,
}

#[derive(Debug)]
pub struct AppendS3ObjectResp {
    pub bucket: String,
    pub name: String,
    pub next_append_position: u64,
    pub err: Errno,
}