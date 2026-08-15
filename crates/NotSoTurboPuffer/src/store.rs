use std::{fmt, path::PathBuf, time::Duration};

use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::create_bucket::CreateBucketError,
    primitives::ByteStream,
};
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;

use crate::config::get_config;

/// Typed marker attached to errors caused by a missing or inaccessible bucket
/// (HTTP 404/403). Callers detect it with `err.downcast_ref::<BucketMissing>()`.
#[derive(Debug)]
pub struct BucketMissing;

impl fmt::Display for BucketMissing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bucket is missing or not accessible")
    }
}

impl std::error::Error for BucketMissing {}

/// Typed marker attached to errors caused by a failed S3 precondition
/// (HTTP 412): an `if-none-match` collision or an `if-match` ETag conflict.
#[derive(Debug)]
pub struct PreconditionFailed;

impl fmt::Display for PreconditionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3 precondition failed (412)")
    }
}

impl std::error::Error for PreconditionFailed {}

#[allow(async_fn_in_trait)]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn get_with_etag(&self, namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>>;
    async fn put(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()>;
    async fn put_if_not_exists(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()>;
    async fn put_if_match(
        &self,
        namespace: &str,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> Result<String>;
    async fn delete(&self, namespace: &str, key: &str) -> Result<()>;
    async fn list_page(
        &self,
        namespace: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)>;
    async fn list_namespaces(&self) -> Result<Vec<String>>;
    async fn create_bucket(&self, namespace: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// S3Store: the raw backend. No caching, no counting.
// ---------------------------------------------------------------------------

pub struct S3Store {
    client: aws_sdk_s3::Client,
}

impl S3Store {
    pub async fn from_config() -> Result<Self> {
        let config = get_config().await?;
        tracing::info!(
            "Initializing S3 client, region: {}",
            config.storage.s3_region
        );

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(&config.storage.s3_endpoint)
            .region(aws_config::Region::new(config.storage.s3_region.clone()))
            .timeout_config(
                aws_config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .operation_attempt_timeout(Duration::from_secs(5))
                    .build(),
            )
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(2))
            .load()
            .await;

        Ok(Self {
            client: aws_sdk_s3::Client::new(&aws_config),
        })
    }
}

fn status_of<E>(err: &SdkError<E>) -> Option<u16> {
    match err {
        SdkError::ServiceError(service_err) => Some(service_err.raw().status().as_u16()),
        _ => None,
    }
}

fn s3_error<E: ProvideErrorMetadata + fmt::Debug>(
    op: &str,
    namespace: &str,
    key: &str,
    err: &SdkError<E>,
) -> anyhow::Error {
    let base = match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            let code = service_err.err().code().unwrap_or("Unknown");
            let message = service_err.err().message().unwrap_or("No message");
            anyhow!("S3 {op} failed for {namespace}/{key}: HTTP {status}, {code} - {message}")
        }
        other => anyhow!("S3 {op} failed for {namespace}/{key}: {other:?}"),
    };
    tracing::error!("{base}");

    match status_of(err) {
        Some(404) | Some(403) => base.context(BucketMissing),
        Some(412) => base.context(PreconditionFailed),
        _ => base,
    }
}

impl ObjectStore for S3Store {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>> {
        match self
            .client
            .get_object()
            .bucket(namespace)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(Some(output.body.collect().await?.into_bytes().to_vec())),
            Err(err) if matches!(status_of(&err), Some(404) | Some(403)) => Ok(None),
            Err(err) => Err(s3_error("get", namespace, key, &err)),
        }
    }

    async fn get_with_etag(&self, namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self
            .client
            .get_object()
            .bucket(namespace)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let etag = output.e_tag().unwrap_or("").to_string();
                let data = output.body.collect().await?.into_bytes().to_vec();
                Ok(Some((data, etag)))
            }
            Err(err) if matches!(status_of(&err), Some(404) | Some(403)) => Ok(None),
            Err(err) => Err(s3_error("get", namespace, key, &err)),
        }
    }

    async fn put(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(namespace)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|err| s3_error("put", namespace, key, &err))?;
        Ok(())
    }

    async fn put_if_not_exists(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(namespace)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|err| s3_error("put-if-not-exists", namespace, key, &err))?;
        Ok(())
    }

    async fn put_if_match(
        &self,
        namespace: &str,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> Result<String> {
        let output = self
            .client
            .put_object()
            .bucket(namespace)
            .key(key)
            .if_match(etag)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|err| s3_error("put-if-match", namespace, key, &err))?;
        Ok(output.e_tag().unwrap_or("").to_string())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(namespace)
            .key(key)
            .send()
            .await
            .map_err(|err| s3_error("delete", namespace, key, &err))?;
        Ok(())
    }

    async fn list_page(
        &self,
        namespace: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(namespace)
            .prefix(prefix);
        if let Some(token) = token {
            req = req.continuation_token(token);
        }

        let output = req
            .send()
            .await
            .map_err(|err| s3_error("list", namespace, prefix, &err))?;

        let keys = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .collect();
        let next_token = if output.is_truncated == Some(true) {
            output.next_continuation_token
        } else {
            None
        };
        Ok((keys, next_token))
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let output = self
            .client
            .list_buckets()
            .send()
            .await
            .map_err(|err| anyhow!("failed to list namespaces: {err:?}"))?;
        Ok(output
            .buckets()
            .iter()
            .filter_map(|b| b.name().map(ToString::to_string))
            .collect())
    }

    async fn create_bucket(&self, namespace: &str) -> Result<()> {
        match self.client.create_bucket().bucket(namespace).send().await {
            Ok(_) => {
                tracing::info!("Created bucket: {}", namespace);
                Ok(())
            }
            Err(err) => {
                let service_err = err.into_service_error();
                match &service_err {
                    CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                        tracing::debug!("Bucket {} already exists and is owned by us", namespace);
                        Ok(())
                    }
                    CreateBucketError::BucketAlreadyExists(_) => {
                        tracing::warn!("Bucket {} already exists. Proceeding...", namespace);
                        Ok(())
                    }
                    _ => {
                        tracing::error!("Failed to create bucket {}: {:?}", namespace, service_err);
                        Err(service_err.into())
                    }
                }
            }
        }
    }
}


#[derive(Debug, Default)]
struct RoundTripCounter {
    count: u64,
    keys: Vec<String>,
}

static S3_ROUND_TRIPS: Mutex<RoundTripCounter> = Mutex::new(RoundTripCounter {
    count: 0,
    keys: Vec::new(),
});

fn record_round_trip(label: String) {
    let mut counter = S3_ROUND_TRIPS.lock().expect("round-trip counter poisoned");
    counter.count += 1;
    counter.keys.push(label);
}

pub fn get_s3_round_trips() -> (u64, Vec<String>) {
    let counter = S3_ROUND_TRIPS.lock().expect("round-trip counter poisoned");
    (counter.count, counter.keys.clone())
}

pub fn reset_s3_round_trips() {
    let mut counter = S3_ROUND_TRIPS.lock().expect("round-trip counter poisoned");
    counter.count = 0;
    counter.keys.clear();
}

pub struct CountingStore<S> {
    inner: S,
}

impl<S> CountingStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: ObjectStore> ObjectStore for CountingStore<S> {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>> {
        record_round_trip(format!("s3:read:{key}"));
        self.inner.get(namespace, key).await
    }

    async fn get_with_etag(&self, namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        record_round_trip(format!("s3:read:{key}"));
        self.inner.get_with_etag(namespace, key).await
    }

    async fn put(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        record_round_trip(format!("s3:put:{key}"));
        self.inner.put(namespace, key, data).await
    }

    async fn put_if_not_exists(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        record_round_trip(format!("s3:put:{key}"));
        self.inner.put_if_not_exists(namespace, key, data).await
    }

    async fn put_if_match(
        &self,
        namespace: &str,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> Result<String> {
        record_round_trip(format!("s3:put:{key}"));
        self.inner.put_if_match(namespace, key, data, etag).await
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        record_round_trip(format!("s3:delete:{key}"));
        self.inner.delete(namespace, key).await
    }

    async fn list_page(
        &self,
        namespace: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        record_round_trip(format!("s3:list:{prefix}"));
        self.inner.list_page(namespace, prefix, token).await
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        record_round_trip("s3:list:buckets".to_string());
        self.inner.list_namespaces().await
    }

    async fn create_bucket(&self, namespace: &str) -> Result<()> {
        record_round_trip(format!("s3:create-bucket:{namespace}"));
        self.inner.create_bucket(namespace).await
    }
}

// ---------------------------------------------------------------------------
// CachingStore: write-through local-disk cache.
//
// Policy, in one place:
// - get: serve from disk when present; otherwise fetch and populate.
// - get_with_etag: always fetch (the caller needs a fresh ETag); populate.
// - put / successful conditional put: write through to disk.
// - delete: invalidate disk, then delete.
// - Cache I/O failures degrade to the inner store with a warning; they never
//   fail the operation.
// ---------------------------------------------------------------------------

pub struct CachingStore<S> {
    inner: S,
}

impl<S> CachingStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    async fn cache_path(namespace: &str, key: &str) -> Result<PathBuf> {
        let config = get_config().await?;
        let mut path = PathBuf::from(&config.storage.local_cache_path);
        path.push(namespace);
        path.push(key);
        Ok(path)
    }

    async fn read_cache(namespace: &str, key: &str) -> Option<Vec<u8>> {
        let path = Self::cache_path(namespace, key).await.ok()?;
        tokio::fs::read(&path).await.ok()
    }

    async fn write_cache(namespace: &str, key: &str, data: &[u8]) {
        let result: Result<()> = async {
            let path = Self::cache_path(namespace, key).await?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut file = tokio::fs::File::create(path).await?;
            file.write_all(data).await?;
            file.flush().await?;
            Ok(())
        }
        .await;

        if let Err(e) = result {
            tracing::warn!("Failed to write cache for {}/{}: {:?}", namespace, key, e);
        }
    }

    async fn delete_cache(namespace: &str, key: &str) {
        let result: Result<()> = async {
            let path = Self::cache_path(namespace, key).await?;
            if path.exists() {
                tokio::fs::remove_file(&path).await?;
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            tracing::warn!("Failed to delete cache for {}/{}: {:?}", namespace, key, e);
        }
    }
}

impl<S: ObjectStore> ObjectStore for CachingStore<S> {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(data) = Self::read_cache(namespace, key).await {
            tracing::debug!("Cache hit for key: {}", key);
            return Ok(Some(data));
        }

        let fetched = self.inner.get(namespace, key).await?;
        if let Some(data) = &fetched {
            Self::write_cache(namespace, key, data).await;
        }
        Ok(fetched)
    }

    async fn get_with_etag(&self, namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let fetched = self.inner.get_with_etag(namespace, key).await?;
        if let Some((data, _)) = &fetched {
            Self::write_cache(namespace, key, data).await;
        }
        Ok(fetched)
    }

    async fn put(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        Self::write_cache(namespace, key, data).await;
        self.inner.put(namespace, key, data).await
    }

    async fn put_if_not_exists(&self, namespace: &str, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put_if_not_exists(namespace, key, data).await?;
        Self::write_cache(namespace, key, data).await;
        Ok(())
    }

    async fn put_if_match(
        &self,
        namespace: &str,
        key: &str,
        data: &[u8],
        etag: &str,
    ) -> Result<String> {
        let new_etag = self.inner.put_if_match(namespace, key, data, etag).await?;
        Self::write_cache(namespace, key, data).await;
        Ok(new_etag)
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        Self::delete_cache(namespace, key).await;
        self.inner.delete(namespace, key).await
    }

    async fn list_page(
        &self,
        namespace: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        self.inner.list_page(namespace, prefix, token).await
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn create_bucket(&self, namespace: &str) -> Result<()> {
        self.inner.create_bucket(namespace).await
    }
}
