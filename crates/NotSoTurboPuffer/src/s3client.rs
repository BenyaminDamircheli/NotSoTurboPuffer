//! Public storage API for the engine.
//!
//! Thin facade over the store stack in `crate::store`. The stack order is
//! load-bearing (see `store.rs`): cache above counter above latency above S3.

use anyhow::{Result, bail};
use tokio::sync::OnceCell;

use crate::engine::Namespace;
use crate::store::{
    BucketMissing, CachingStore, CountingStore, ObjectStore, PreconditionFailed, S3Store,
};

pub use crate::store::{get_s3_round_trips, reset_s3_round_trips};

type EngineStore = CachingStore<CountingStore<S3Store>>;

static STORE: OnceCell<EngineStore> = OnceCell::const_new();

async fn store() -> Result<&'static EngineStore> {
    STORE
        .get_or_try_init(|| async {
            let s3 = S3Store::from_config().await?;
            Ok(CachingStore::new(CountingStore::new(s3)))
        })
        .await
}

pub async fn get_file(namespace: &str, key: &str) -> Result<Option<Vec<u8>>> {
    store().await?.get(namespace, key).await
}

// ETag-aware version for metadata race condition protection
pub async fn get_file_with_etag(namespace: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
    store().await?.get_with_etag(namespace, key).await
}

pub async fn put_object(namespace: &str, key: &str, data: &[u8]) -> Result<()> {
    store().await?.put(namespace, key, data).await
}

// Conditional put with ETag matching for atomic metadata updates
pub async fn put_object_if_match(
    namespace: &str,
    key: &str,
    data: &[u8],
    etag: &str,
) -> Result<String> {
    store().await?.put_if_match(namespace, key, data, etag).await
}

/// Writes an object only if the key does not exist yet. When the bucket itself
/// is missing, provisions the namespace once and retries the write.
pub async fn put_object_if_not_exists(namespace: &str, key: &str, data: &[u8]) -> Result<()> {
    let store = store().await?;
    match store.put_if_not_exists(namespace, key, data).await {
        Err(err) if err.downcast_ref::<BucketMissing>().is_some() => {
            tracing::warn!("Bucket {} not found/accessible, creating...", namespace);
            create_namespace_resources(namespace).await?;
            store.put_if_not_exists(namespace, key, data).await
        }
        result => result,
    }
}

pub async fn delete_object(namespace: &str, key: &str) -> Result<()> {
    store().await?.delete(namespace, key).await
}

pub async fn get_namespaces() -> Result<Vec<Namespace>> {
    let names = store().await?.list_namespaces().await?;
    Ok(names.into_iter().map(|id| Namespace { id }).collect())
}

async fn list_prefix(store: &EngineStore, namespace: &str, prefix: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut token = None;

    loop {
        let (keys, next_token) = store.list_page(namespace, prefix, token).await?;
        files.extend(keys);
        match next_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    Ok(files)
}

pub async fn list_wal_files(namespace: &str) -> Result<Vec<String>> {
    let store = store().await?;

    // List both WALs and Index segments
    let mut files = list_prefix(store, namespace, "wal/").await?;
    files.extend(list_prefix(store, namespace, "index/").await?);

    // Sort to ensure time ordering (assuming ULIDs or timestamps in filenames)
    files.sort();

    Ok(files)
}

/// Creates the namespace's initial metadata file. Never overwrites: when
/// metadata already exists, the existing file is left untouched.
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
        distance_metric: crate::vectors::DistanceMetric::default(),
        schema: crate::engine::Schema::default(),
        vector_dimensions: None,
    };
    let json = serde_json::to_vec(&metadata)?;

    let store = store().await?;
    match store
        .put_if_not_exists(namespace, "metadata/metadata.json", &json)
        .await
    {
        Ok(()) => {
            tracing::info!("Created metadata file for namespace: {}", namespace);
            Ok(())
        }
        Err(err) if err.downcast_ref::<PreconditionFailed>().is_some() => {
            tracing::debug!(
                "Metadata for namespace {} already exists; leaving it untouched",
                namespace
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub async fn create_namespace_resources(raw_namespace: &str) -> Result<()> {
    let namespace = sanitize_namespace(raw_namespace)?;
    let store = store().await?;

    store.create_bucket(&namespace).await?;
    create_metadata(&namespace).await
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
