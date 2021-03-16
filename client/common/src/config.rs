use serde_derive::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Config{
    pub mount_config: MountConfig,
    pub s3_config: S3Config,
    pub metaserver_config: MetaServerConfig,
    pub zone_config: ZoneConfig,
    pub segment_configs: Vec<SegmentConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MountConfig {
    pub mnt: String,
}
#[derive(Deserialize, Debug, Clone)]
pub struct S3Config {
    pub region: String,
    pub server: String,
    pub bucket: String,
    pub ak: String,
    pub sk: String,
}
#[derive(Deserialize, Debug, Clone)]
pub struct MetaServerConfig {
    pub meta_server: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ZoneConfig {
    pub zone: String,
    pub machine: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SegmentConfig {
    pub dir: String,
    pub size: u64,
    pub num: u32, // by default is 0.
}