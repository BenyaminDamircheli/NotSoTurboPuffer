use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer};
use std::time::Duration;
use tokio::{fs, sync::OnceCell};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub limits: LimitsConfig,
    pub batching: BatchingConfig,
    pub indexing: IndexingConfig,
    pub storage: StorageConfig,
    pub compactor: CompactorConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub max_request_body_size: usize,
    pub request_timeout_ms: u64,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    3000
}

#[derive(Debug, Deserialize)]
pub struct LimitsConfig {
    pub max_unindexed_wal_bytes: u64,
    pub max_top_k: usize,
    pub max_vector_dimensions: usize,
    pub max_attribute_value_size: usize,
    pub max_document_size: usize,
    pub max_rows_affected_by_patch_by_filter: usize,
    pub max_rows_affected_by_delete_by_filter: usize,
    pub max_concurrent_queries_per_namespace: usize,
}

#[derive(Debug, Deserialize)]
pub struct BatchingConfig {
    #[serde(rename = "max_batch_time_ms", deserialize_with = "duration_from_ms")]
    pub max_batch_time: Duration,
    pub max_batch_size: usize,
    pub max_batch_bytes: usize,
}

#[derive(Debug, Deserialize)]
pub struct IndexingConfig {
    pub reindex_threshold_wal_count: usize,
    pub reindex_threshold_row_count: usize,
    pub reindex_threshold_bytes: u64,
    pub compaction_batch_size_bytes: u64,
    pub cache_fill_concurrency: usize,
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub max_s3_connections: usize,
    pub max_wal_write_retries: usize,
    pub write_collision_retry_delay_ms: u64,
    #[serde(default = "default_s3_endpoint")]
    pub s3_endpoint: String,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    #[serde(default = "default_local_cache_path")]
    pub local_cache_path: String,
    #[serde(default)]
    pub s3_bucket: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompactorConfig {
    pub sweeper_interval_secs: u64,
    pub max_pending_requests: usize,
    pub max_files_per_cycle: usize,
}

fn duration_from_ms<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    Ok(Duration::from_millis(u64::deserialize(deserializer)?))
}

fn default_s3_endpoint() -> String {
    "https://t3.storage.dev".to_string()
}

fn default_s3_region() -> String {
    "sin".to_string()
}

fn default_local_cache_path() -> String {
    "./data/cache".to_string()
}

static CONFIG: OnceCell<Config> = OnceCell::const_new();

pub async fn get_config() -> Result<&'static Config> {
    CONFIG
        .get_or_try_init(|| async {
            dotenvy::dotenv().ok();

            let content = fs::read_to_string("config.toml")
                .await
                .context("failed to read config.toml")?;

            let mut config: Config =
                toml::from_str(&content).context("failed to parse config.toml")?;

            // Environment overrides for deployment-specific storage settings.
            if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
                config.storage.s3_endpoint = endpoint;
            }
            if let Ok(region) = std::env::var("S3_REGION") {
                config.storage.s3_region = region;
            }
            if let Ok(path) = std::env::var("LOCAL_CACHE_PATH") {
                config.storage.local_cache_path = path;
            }
            if let Ok(bucket) = std::env::var("S3_BUCKET") {
                config.storage.s3_bucket = Some(bucket);
            }
            // Deploy platforms (for example, Railway) inject the listen port.
            if let Ok(port) = std::env::var("PORT")
                && let Ok(port) = port.parse()
            {
                config.server.port = port;
            }

            Ok(config)
        })
        .await
}
