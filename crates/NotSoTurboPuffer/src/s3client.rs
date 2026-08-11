use std::{path::PathBuf, time::Duration};

use crate::config::get_config;
use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::create_bucket::CreateBucketError,
    primitives::ByteStream,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, MutexGuard, OnceCell},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    pub id: String,
}

#[derive(Debug, Default)]
pub struct RoundTripCounter {
    count: u64,
    keys: Vec<String>,
}

impl RoundTripCounter {
    const fn new() -> Self {
        Self {
            count: 0,
            keys: Vec::new(),
        }
    }
}

static S3_ROUND_TRIPS: Mutex<RoundTripCounter> = Mutex::const_new(RoundTripCounter::new());

async fn counter() -> MutexGuard<'static, RoundTripCounter> {
    S3_ROUND_TRIPS.lock().await
}

pub async fn record_s3_round_trip(key: &str) {
    let mut c = counter().await;
    c.count += 1;
    c.keys.push(key.to_string());
}

pub async fn get_s3_round_trips() -> (u64, Vec<String>) {
    let c = counter().await;
    (c.count, c.keys.clone())
}

pub async fn reset_s3_round_trips() {
    let mut c = counter().await;
    c.count = 0;
    c.keys.clear();
}

static S3: OnceCell<aws_sdk_s3::Client> = OnceCell::const_new();

async fn get_local_path(namespace: &str, key: &str) -> Result<PathBuf> {
    let config = get_config().await?;
    let mut path = PathBuf::from(&config.storage.local_cache_path);
    path.push(namespace);
    path.push(key);
    Ok(path)
}

async fn read_cache(namespace: &str, key: &str) -> Result<Vec<u8>> {
    let path = get_local_path(namespace, key).await?;
    match tokio::fs::read(&path).await {
        Ok(data) => {
            tracing::info!("Cache hit for key: {}", key);
            Ok(data)
        }
        Err(e) => {
            tracing::info!("Cache miss for key: {}", key);
            Err(anyhow::anyhow!("Failed to read cache: {}", e))
        }
    }
}

async fn write_cache(namespace: &str, key: &str, data: &[u8]) -> Result<()> {
    let path = get_local_path(namespace, key).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    file.write_all(data).await?;
    file.flush().await?;
    tracing::info!("Cached data for key: {}", key);
    Ok(())
}

async fn delete_cache(namespace: &str, key: &str) -> Result<()> {
    let path = get_local_path(namespace, key).await?;
    if path.exists() {
        tokio::fs::remove_file(&path).await?;
        tracing::info!("Deleted cache for key: {}", key);
    }
    Ok(())
}

async fn get_s3_client() -> Result<&'static aws_sdk_s3::Client> {
    S3.get_or_try_init(|| async {
        let config = get_config().await?;
        tracing::info!(
            "Initialized S3 client region: {}",
            config.storage.s3_region
        );

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        config_loader = config_loader.endpoint_url(&config.storage.s3_endpoint);
        config_loader = config_loader.region(aws_config::Region::new(
            config.storage.s3_region.clone(),
        ));

        config_loader = config_loader.timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(10))
                .operation_attempt_timeout(Duration::from_secs(5))
                .build(),
        );
        config_loader = config_loader
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(2));

        let aws_config = config_loader.load().await;
        let client = aws_sdk_s3::Client::new(&aws_config);

        tracing::debug!("S3 client initialized successfully");
        Ok(client)
    })
    .await
}

// ...
async fn create_metadata(namespace: &str) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let metadata = crate::engine::Metadata {
        approx_row_count: 0,
        index: crate::engine::IndexState {
            status: crate::engine::IndexStatus::UpToDate,
            unindexed_bytes: None,
            ann_index_file: None,
            inverted_index_file: None,
            indexed_row_count: 0,
        },
        created_at: now,
        updated_at: now,
        wal_files: Vec::new(),
        deleted_files: Vec::new(),
        unindexed_bytes: 0,
        distance_metric: crate::engine::DistanceMetric::default(),
        schema: crate::engine::Schema::default(),
        vector_dimensions: None,
    };

    let json = serde_json::to_vec(&metadata)?;
    let client = get_s3_client().await?;
    let key = "metadata/metadata.json";

    match client
        .put_object()
        .bucket(namespace)
        .key(key)
        .body(ByteStream::from(json))
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!(
                "Successfully created metadata file for namespace: {}",
                namespace
            );
        }
        Err(err) => {
            tracing::error!(
                "Failed to create metadata file for namespace {}: {:?}",
                namespace,
                err
            );
            return Err(err.into());
        }
    }

    Ok(())
}

pub async fn create_namespace_resources(raw_namespace: &str) -> Result<()> {
    let namespace = sanitize_namespace(raw_namespace)?;
    let client = get_s3_client().await?;

    match client.create_bucket().bucket(&namespace).send().await {
        Ok(_) => {
            tracing::info!("Successfully created bucket: {}", namespace);
        }
        Err(err) => {
            let service_err = err.into_service_error();
            match &service_err {
                CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                    tracing::debug!("Bucket {} already exists and is owned by us", namespace);
                }
                CreateBucketError::BucketAlreadyExists(_) => {
                    tracing::warn!("Bucket {} already exists. Proceeding...", namespace);
                }
                _ => {
                    tracing::error!("Failed to create bucket {}: {:?}", namespace, service_err);
                    return Err(service_err.into());
                }
            }
        }
    }

    // Create metadata file (initializing or re-initializing)
    create_metadata(&namespace).await?;
    Ok(())
}

pub async fn put_object_if_not_exists(namespace: &str, key: &str, data: &[u8]) -> Result<()> {
    record_s3_round_trip(&format!("s3:put:{key}")).await;
    let client = get_s3_client().await?;

    let res = client
        .put_object()
        .bucket(namespace)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await;
    tracing::info!(
        "S3 put_object_if_not_exists for {} ({} bytes)",
        key,
        data.len()
    );

    match res {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            let error_code = err.err().code().unwrap_or("Unknown");
            let error_msg = err.err().message().unwrap_or("No message");

            tracing::error!(
                "S3 put_object_if_not_exists failed for {}/{}: HTTP {}, Error: {} - {}",
                namespace,
                key,
                status,
                error_code,
                error_msg
            );

            if status == 412 {
                bail!("Object already exists: collision detected for {namespace}/{key}");
            }
            // if bucket is missing (404) or Forbidden (403), create it and retry
            if status == 404 || status == 403 {
                tracing::warn!(
                    "Bucket {} not found/accessible (status {}), creating...",
                    namespace,
                    status
                );
                create_namespace_resources(namespace).await?;

                // Retry the write
                let res_retry = client
                    .put_object()
                    .bucket(namespace)
                    .key(key)
                    .if_none_match("*")
                    .body(ByteStream::from(data.to_vec()))
                    .send()
                    .await;

                match res_retry {
                    Ok(_) => {
                        tracing::info!(
                            "S3 put_object_if_not_exists retry succeeded for {}/{}",
                            namespace,
                            key
                        );
                        return Ok(());
                    }
                    Err(SdkError::ServiceError(retry_err)) => {
                        let retry_status = retry_err.raw().status().as_u16();
                        let retry_error_code = retry_err.err().code().unwrap_or("Unknown");
                        let retry_error_msg = retry_err.err().message().unwrap_or("No message");

                        tracing::error!(
                            "S3 put_object_if_not_exists retry failed for {}/{}: HTTP {}, Error: {} - {}",
                            namespace,
                            key,
                            retry_status,
                            retry_error_code,
                            retry_error_msg
                        );

                        if retry_status == 412 {
                            bail!(
                                "Object already exists: collision detected for {namespace}/{key}"
                            );
                        }
                        bail!(
                            "S3 put operation failed after retry for {namespace}/{key}: HTTP {retry_status}, {retry_error_code} - {retry_error_msg}"
                        );
                    }
                    Err(other) => {
                        tracing::error!(
                            "S3 put_object_if_not_exists retry failed with non-service error for {}/{}: {:?}",
                            namespace,
                            key,
                            other
                        );
                        bail!(
                            "S3 put operation failed after retry for {namespace}/{key}: {other:?}"
                        );
                    }
                }
            }
            bail!(
                "S3 put operation failed for {namespace}/{key}: HTTP {status}, {error_code} - {error_msg}"
            )
        }
        Err(other_err) => {
            tracing::error!(
                "S3 put_object_if_not_exists failed with non-service error for {}/{}: {:?}",
                namespace,
                key,
                other_err
            );
            bail!("S3 put operation failed for {namespace}/{key}: {other_err:?}");
        }
    }
}

pub async fn put_object(namespace: &str, key: &str, data: &[u8]) -> Result<()> {
    // Write-through cache: Update local first
    if let Err(e) = write_cache(namespace, key, data).await {
        tracing::warn!(
            "Failed to write to cache during put_object for {}/{}: {:?}",
            namespace,
            key,
            e
        );
    }

    record_s3_round_trip(&format!("s3:put:{key}")).await;
    let client = get_s3_client().await?;

    match client
        .put_object()
        .bucket(namespace)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!(
                "S3 put_object succeeded for {}/{} ({} bytes)",
                namespace,
                key,
                data.len()
            );
            Ok(())
        }
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            let error_code = err.err().code().unwrap_or("Unknown");
            let error_msg = err.err().message().unwrap_or("No message");

            tracing::error!(
                "S3 put_object failed for {}/{}: HTTP {}, Error: {} - {}",
                namespace,
                key,
                status,
                error_code,
                error_msg
            );

            bail!("S3 put failed for {namespace}/{key}: HTTP {status}, {error_code} - {error_msg}")
        }
        Err(other_err) => {
            tracing::error!(
                "S3 put_object failed with non-service error for {}/{}: {:?}",
                namespace,
                key,
                other_err
            );
            bail!("S3 put failed for {namespace}/{key}: {other_err:?}");
        }
    }
}

pub async fn get_namespaces() -> Result<Vec<Namespace>> {
    let client = get_s3_client().await?;
    match client.list_buckets().send().await {
        Ok(output) => {
            let namespaces = output
                .buckets()
                .iter()
                .filter_map(|b| {
                    b.name().map(|name| Namespace {
                        id: name.to_string(),
                    })
                })
                .collect();
            Ok(namespaces)
        }
        Err(_) => {
            bail!("failed to get the list of namespaces");
        }
    }
}

pub async fn get_file(namespace: &str, key: &str) -> Result<Option<Vec<u8>>> {
    // 1. Try Cache
    if let Ok(data) = read_cache(namespace, key).await {
        record_s3_round_trip(&format!("cache:read:{key}")).await;
        return Ok(Some(data));
    }

    // 2. Try S3
    record_s3_round_trip(&format!("s3:read:{key}")).await;
    let client = get_s3_client().await?;

    match client.get_object().bucket(namespace).key(key).send().await {
        Ok(output) => {
            let data = output.body.collect().await?.into_bytes().to_vec();
            // 3. Populate Cache
            if let Err(e) = write_cache(namespace, key, &data).await {
                tracing::warn!(
                    "Failed to write to cache for {}/{}: {:?}",
                    namespace,
                    key,
                    e
                );
            }
            Ok(Some(data))
        }
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            if status == 404 || status == 403 {
                Ok(None)
            } else {
                let error_code = err.err().code().unwrap_or("Unknown");
                let error_msg = err.err().message().unwrap_or("No message");

                tracing::error!(
                    "S3 get_object failed for {}/{}: HTTP {}, Error: {} - {}",
                    namespace,
                    key,
                    status,
                    error_code,
                    error_msg
                );

                bail!(
                    "S3 get failed for {namespace}/{key}: HTTP {status}, {error_code} - {error_msg}"
                )
            }
        }
        Err(other_err) => {
            tracing::error!(
                "S3 get_object failed with non-service error for {}/{}: {:?}",
                namespace,
                key,
                other_err
            );
            bail!("S3 get failed for {namespace}/{key}: {other_err:?}");
        }
    }
}

// ETag-aware version for metadata race condition protection
pub async fn get_file_with_etag(namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
    record_s3_round_trip(&format!("s3:read:{key}")).await;
    let client = get_s3_client().await?;

    match client.get_object().bucket(namespace).key(key).send().await {
        Ok(output) => {
            let etag = output.e_tag().unwrap_or("").to_string();
            let data = output.body.collect().await?.into_bytes().to_vec();
            tracing::info!("S3 get_object for {} ({} bytes)", key, data.len());
            Ok(Some((data, etag)))
        }
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            if status == 404 || status == 403 {
                Ok(None)
            } else {
                let error_code = err.err().code().unwrap_or("Unknown");
                let error_msg = err.err().message().unwrap_or("No message");

                tracing::error!(
                    "S3 get_object_with_etag failed for {}/{}: HTTP {}, Error: {} - {}",
                    namespace,
                    key,
                    status,
                    error_code,
                    error_msg
                );

                bail!(
                    "S3 get with etag failed for {namespace}/{key}: HTTP {status}, {error_code} - {error_msg}"
                )
            }
        }
        Err(other_err) => {
            tracing::error!(
                "S3 get_object_with_etag failed with non-service error for {}/{}: {:?}",
                namespace,
                key,
                other_err
            );
            bail!("S3 get with etag failed for {namespace}/{key}: {other_err:?}");
        }
    }
}

// Conditional put with ETag matching for atomic metadata updates
pub async fn put_object_if_match(
    namespace: &str,
    key: &str,
    data: &[u8],
    etag: &str,
) -> Result<String> {
    record_s3_round_trip(&format!("s3:put:{key}")).await;
    let client = get_s3_client().await?;

    let res = client
        .put_object()
        .bucket(namespace)
        .key(key)
        .if_match(etag)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await;
    tracing::info!("S3 put_object_if_match for {} ({} bytes)", key, data.len());

    match res {
        Ok(output) => {
            let new_etag = output.e_tag().unwrap_or("").to_string();
            Ok(new_etag)
        }
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            let error_code = err.err().code().unwrap_or("Unknown");
            let error_msg = err.err().message().unwrap_or("No message");

            tracing::error!(
                "S3 put_object_if_match failed for {}/{}: HTTP {}, Error: {} - {}",
                namespace,
                key,
                status,
                error_code,
                error_msg
            );

            if status == 412 {
                bail!("Metadata version conflict: ETag mismatch detected for {namespace}/{key}");
            }
            bail!(
                "S3 conditional put failed for {namespace}/{key}: HTTP {status}, {error_code} - {error_msg}"
            )
        }
        Err(other_err) => {
            tracing::error!(
                "S3 put_object_if_match failed with non-service error for {}/{}: {:?}",
                namespace,
                key,
                other_err
            );
            bail!("S3 conditional put failed for {namespace}/{key}: {other_err:?}");
        }
    }
}

pub async fn delete_object(namespace: &str, key: &str) -> Result<()> {
    // Delete from cache first
    if let Err(e) = delete_cache(namespace, key).await {
        tracing::warn!(
            "Failed to delete from cache for {}/{}: {:?}",
            namespace,
            key,
            e
        );
    }

    record_s3_round_trip(&format!("s3:delete:{key}")).await;
    let client = get_s3_client().await?;

    match client
        .delete_object()
        .bucket(namespace)
        .key(key)
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!("S3 delete_object succeeded for {}/{}", namespace, key);
            Ok(())
        }
        Err(SdkError::ServiceError(err)) => {
            let status = err.raw().status().as_u16();
            let error_code = err.err().code().unwrap_or("Unknown");
            let error_msg = err.err().message().unwrap_or("No message");

            tracing::error!(
                "S3 delete_object failed for {}/{}: HTTP {}, Error: {} - {}",
                namespace,
                key,
                status,
                error_code,
                error_msg
            );
            bail!("S3 delete failed: {error_code} - {error_msg}")
        }
        Err(other) => {
            tracing::error!(
                "S3 delete_object failed with non-service error: {:?}",
                other
            );
            bail!("S3 delete failed: {other:?}")
        }
    }
}

// Helper to list files with a specific prefix
async fn list_prefix(
    client: &aws_sdk_s3::Client,
    namespace: &str,
    prefix: &str,
) -> Result<Vec<String>> {
    record_s3_round_trip(&format!("s3:list:{prefix}")).await; // At least one list call
    let mut files = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut req = client.list_objects_v2().bucket(namespace).prefix(prefix);

        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }

        let output = req
            .send()
            .await
            .with_context(|| format!("Failed to list files in {prefix}"))?;

        if let Some(objects) = output.contents {
            for obj in objects {
                if let Some(key) = obj.key {
                    files.push(key);
                }
            }
        }

        if output.is_truncated == Some(true) {
            continuation_token = output.next_continuation_token;
        } else {
            break;
        }
    }
    Ok(files)
}

pub async fn list_wal_files(namespace: &str) -> Result<Vec<String>> {
    let client = get_s3_client().await?;
    let mut files = Vec::new();

    // List both WALs and Index segments
    let wal_files = list_prefix(client, namespace, "wal/").await?;
    let index_files = list_prefix(client, namespace, "index/").await?;

    files.extend(wal_files);
    files.extend(index_files);

    // Sort to ensure time ordering (assuming ULIDs or timestamps in filenames)
    files.sort();

    Ok(files)
}
pub fn sanitize_namespace(raw: &str) -> Result<String> {
    if raw.is_empty() {
        bail!("namespace cannot be empty!");
    }

    if !(3..=63).contains(&raw.len()) {
        bail!("namespace must be between 3 and 63 characters");
    }

    if raw.chars().any(|c| c.is_ascii_uppercase()) {
        bail!("namespace must be lowercase");
    }

    if raw.starts_with('-') || raw.ends_with('-') {
        bail!("namespace cannot start or end with a hyphen");
    }

    if raw.as_bytes().windows(2).any(|window| window == b"--") {
        bail!("namespace cannot contain consecutive hyphens");
    }

    if raw
        .chars()
        .any(|c| !matches!(c, 'a'..='z' | '0'..='9' | '-'))
    {
        bail!("namespace may only contain lowercase letters, digits, or hyphens");
    }

    Ok(raw.to_owned())
}

#[cfg(test)]
mod tests {
    use super::sanitize_namespace;

    #[test]
    fn accepts_valid_names() {
        assert_eq!(sanitize_namespace("valid-name").unwrap(), "valid-name");
    }

    #[test]
    fn rejects_uppercase() {
        assert!(sanitize_namespace("Invalid").is_err());
    }

    #[test]
    fn rejects_repeated_hyphen() {
        assert!(sanitize_namespace("bad--name").is_err());
    }

    #[test]
    fn rejects_invalid_chars() {
        assert!(sanitize_namespace("bad!name").is_err());
    }

    #[test]
    fn rejects_out_of_range_lengths() {
        assert!(sanitize_namespace("aa").is_err());
        assert!(sanitize_namespace(&"a".repeat(64)).is_err());
    }

    #[test]
    fn rejects_leading_or_trailing_hyphen() {
        assert!(sanitize_namespace("-bad").is_err());
        assert!(sanitize_namespace("bad-").is_err());
    }
}
