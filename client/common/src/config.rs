use serde_derive::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Clone)]
pub struct Config{
    pub mount_config: MountConfig,
    pub s3_config: S3Config,
    pub metaserver_config: MetaServerConfig,
    pub zone_config: ZoneConfig,
    pub segment_configs: Vec<SegmentConfig>,
    pub disk_cache_config: DiskCacheConfig,
    pub backend_store_config: BackendStoreConfig,
    pub heartbeat_config: HeartbeatConfig,
    pub log_path_config: LogPathConfig,
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
    pub thread_num: u32,
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

#[derive(Deserialize, Debug, Clone)]
pub struct DiskCacheConfig{
    pub thread_num: u32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BackendStoreConfig {
    pub backend_type: u32,
    pub settings: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HeartbeatConfig{
    pub timeout: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LogPathConfig{
    pub log_path: String,
}