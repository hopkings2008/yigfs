use serde_derive::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config{
    pub mount_config: MountConfig,
    pub s3_config: S3Config,
    pub metaserver_config: MetaServerConfig,
}

#[derive(Deserialize, Debug)]
pub struct MountConfig {
    pub mnt: String,
}
#[derive(Deserialize, Debug)]
pub struct S3Config {
    pub region: String,
    pub server: String,
    pub bucket: String,
    pub ak: String,
    pub sk: String,
}
#[derive(Deserialize, Debug)]
pub struct MetaServerConfig {
    pub meta_server: String,
}