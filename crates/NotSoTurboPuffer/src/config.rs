use anyhow::{Context, Result, anyhow};
use std::time::Duration;
use tokio::{fs, sync::OnceCell};
use toml::Table;


#[derive(Debug)]
pub struct Config {
    pub server: ServerConfig,
    pub limits: LimitsConfig,
    pub batching: BatchingConfig,
    pub indexing: IndexingConfig,
    pub storage: StorageConfig,
    pub compactor: CompactorConfig,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub max_request_body_size: usize,
    pub request_timeout_ms: u64,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct BatchingConfig {
    pub max_batch_time: Duration,
    pub max_batch_size: usize,
    pub max_batch_bytes: usize,
}

#[derive(Debug)]
pub struct IndexingConfig {
    pub reindex_threshold_wal_count: usize,
    pub reindex_threshold_row_count: usize,
    pub reindex_threshold_bytes: u64,
    pub compaction_batch_size_bytes: u64,
    pub cache_fill_concurrency: usize,
}

#[derive(Debug)]
pub struct StorageConfig {
    pub max_s3_connections: usize,
    pub max_wal_write_retries: usize,
    pub write_collision_retry_delay_ms: u64,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub local_cache_path: String,
}

#[derive(Debug)]
pub struct CompactorConfig {
    pub sweeper_interval_secs: u64,
    pub max_pending_requests: usize,
    pub max_files_per_cycle: usize,
}

static CONFIG: OnceCell<Config> = OnceCell::const_new();

pub async fn get_config() -> Result<&'static Config> {
    CONFIG
        .get_or_try_init(|| async {
            dotenvy::dotenv().ok();

            let content = fs::read_to_string("config.toml")
                .await
                .context("failed to read config.toml")?;

            let root = content
                .parse::<Table>()
                .map_err(|e| anyhow!("failed to parse toml: {e}"))?;

            let server = root.get("server").ok_or(anyhow!("missing [server]"))?;
            let limits = root.get("limits").ok_or(anyhow!("missing [limits]"))?;
            let batching = root.get("batching").ok_or(anyhow!("missing [batching]"))?;
            let indexing = root.get("indexing").ok_or(anyhow!("missing [indexing]"))?;
            let storage = root.get("storage").ok_or(anyhow!("missing [storage]"))?;
            let compactor = root
                .get("compactor")
                .ok_or(anyhow!("missing [compactor]"))?;

            Ok(Config {
                server: ServerConfig {
                    max_request_body_size: server
                        .get("max_request_body_size")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_request_body_size"))?
                        .try_into()?,
                    request_timeout_ms: server
                        .get("request_timeout_ms")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad request_timeout_ms"))?
                        .try_into()?,
                },
                limits: LimitsConfig {
                    max_unindexed_wal_bytes: limits
                        .get("max_unindexed_wal_bytes")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_unindexed_wal_bytes"))?
                        .try_into()?,
                    max_top_k: limits
                        .get("max_top_k")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_top_k"))?
                        .try_into()?,
                    max_vector_dimensions: limits
                        .get("max_vector_dimensions")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_vector_dimensions"))?
                        .try_into()?,
                    max_attribute_value_size: limits
                        .get("max_attribute_value_size")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_attribute_value_size"))?
                        .try_into()?,
                    max_document_size: limits
                        .get("max_document_size")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_document_size"))?
                        .try_into()?,
                    max_rows_affected_by_patch_by_filter: limits
                        .get("max_rows_affected_by_patch_by_filter")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_rows_affected_by_patch_by_filter"))?
                        .try_into()?,
                    max_rows_affected_by_delete_by_filter: limits
                        .get("max_rows_affected_by_delete_by_filter")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_rows_affected_by_delete_by_filter"))?
                        .try_into()?,
                    max_concurrent_queries_per_namespace: limits
                        .get("max_concurrent_queries_per_namespace")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_concurrent_queries_per_namespace"))?
                        .try_into()?,
                },
                batching: BatchingConfig {
                    max_batch_time: Duration::from_millis(
                        batching
                            .get("max_batch_time_ms")
                            .and_then(toml::Value::as_integer)
                            .ok_or(anyhow!("bad max_batch_time_ms"))?
                            .try_into()?,
                    ),
                    max_batch_size: batching
                        .get("max_batch_size")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_batch_size"))?
                        .try_into()?,
                    max_batch_bytes: batching
                        .get("max_batch_bytes")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_batch_bytes"))?
                        .try_into()?,
                },
                indexing: IndexingConfig {
                    reindex_threshold_wal_count: indexing
                        .get("reindex_threshold_wal_count")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad reindex_threshold_wal_count"))?
                        .try_into()?,
                    reindex_threshold_row_count: indexing
                        .get("reindex_threshold_row_count")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad reindex_threshold_row_count"))?
                        .try_into()?,
                    reindex_threshold_bytes: indexing
                        .get("reindex_threshold_bytes")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad reindex_threshold_bytes"))?
                        .try_into()?,
                    compaction_batch_size_bytes: indexing
                        .get("compaction_batch_size_bytes")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad compaction_batch_size_bytes"))?
                        .try_into()?,
                    cache_fill_concurrency: indexing
                        .get("cache_fill_concurrency")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad cache_fill_concurrency"))?
                        .try_into()?,
                },
                storage: StorageConfig {
                    max_s3_connections: storage
                        .get("max_s3_connections")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_s3_connections"))?
                        .try_into()?,
                    max_wal_write_retries: storage
                        .get("max_wal_write_retries")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_wal_write_retries"))?
                        .try_into()?,
                    write_collision_retry_delay_ms: storage
                        .get("write_collision_retry_delay_ms")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad write_collision_retry_delay_ms"))?
                        .try_into()?,
                    s3_endpoint: std::env::var("S3_ENDPOINT").unwrap_or_else(|_| {
                        storage
                            .get("s3_endpoint")
                            .and_then(toml::Value::as_str)
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "https://t3.storage.dev".to_string())
                    }),
                    s3_region: std::env::var("S3_REGION").unwrap_or_else(|_| {
                        storage
                            .get("s3_region")
                            .and_then(toml::Value::as_str)
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "sin".to_string())
                    }),
                    local_cache_path: std::env::var("LOCAL_CACHE_PATH").unwrap_or_else(|_| {
                        storage
                            .get("local_cache_path")
                            .and_then(toml::Value::as_str)
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "./data/cache".to_string())
                    }),
                },
                compactor: CompactorConfig {
                    sweeper_interval_secs: compactor
                        .get("sweeper_interval_secs")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad sweeper_interval_secs"))?
                        .try_into()?,
                    max_pending_requests: compactor
                        .get("max_pending_requests")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_pending_requests"))?
                        .try_into()?,
                    max_files_per_cycle: compactor
                        .get("max_files_per_cycle")
                        .and_then(toml::Value::as_integer)
                        .ok_or(anyhow!("bad max_files_per_cycle"))?
                        .try_into()?,
                },
            })
        })
        .await
}