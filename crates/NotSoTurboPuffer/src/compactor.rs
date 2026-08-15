use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use tokio::{
    sync::{OnceCell, mpsc},
    time::{interval, sleep},
};

use crate::{
    ann::spfresh,
    config::get_config,
    engine::{self, Metadata, Row},
    inverted_index::InvertedIndex,
    not_so_turbo_puffer::{
        Client, METADATA_KEY, invalidate_deleted_files, invalidate_metadata_cache,
        invalidate_spfresh_cache,
    },
    s3client,
    store::PreconditionFailed,
    vectors,
};

#[derive(Debug, Clone)]
pub struct CompactionRequest {
    pub namespace: String,
    pub forced: bool,
}

static COMPACTION_QUEUE: OnceCell<mpsc::Sender<CompactionRequest>> = OnceCell::const_new();

fn enqueue(request: CompactionRequest) {
    let Some(tx) = COMPACTION_QUEUE.get() else {
        tracing::error!(
            "Compaction queue not initialized, dropping request for namespace: {}",
            request.namespace
        );
        return;
    };

    match tx.try_send(request) {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!("Failed to queue compaction request: {:?}", e);
        }
    }
}

pub fn trigger_compaction(namespace: &str) {
    tracing::debug!("Compaction requested for namespace: {}", namespace);
    enqueue(CompactionRequest {
        namespace: namespace.to_string(),
        forced: false,
    });
}

pub fn trigger_forced_compaction(namespace: &str) {
    tracing::debug!("Forced compaction requested for namespace: {}", namespace);
    enqueue(CompactionRequest {
        namespace: namespace.to_string(),
        forced: true,
    });
}

struct ProcessingGuard<'a> {
    processing: &'a Mutex<HashSet<String>>,
    namespace: String,
}

impl Drop for ProcessingGuard<'_> {
    fn drop(&mut self) {
        self.processing
            .lock()
            .expect("processing set poisoned")
            .remove(&self.namespace);
    }
}

pub struct Compactor {
    processing: Mutex<HashSet<String>>,
}

impl Default for Compactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compactor {
    pub fn new() -> Self {
        Self {
            processing: Mutex::new(HashSet::new()),
        }
    }

    pub async fn start(self) -> Result<()> {
        let config = get_config().await?;
        let (tx, rx) = mpsc::channel(config.compactor.max_pending_requests);
        COMPACTION_QUEUE
            .set(tx)
            .map_err(|_| anyhow!("Compactor already started"))?;

        let compactor = Arc::new(self);

        let worker = compactor.clone();
        tokio::spawn(async move { worker.worker_loop(rx).await });

        let sweeper = compactor;
        tokio::spawn(async move { sweeper.sweeper_loop().await });

        tracing::info!("Compactor started");
        Ok(())
    }

    async fn worker_loop(&self, mut rx: mpsc::Receiver<CompactionRequest>) {
        tracing::info!("Compactor worker listening for requests");
        while let Some(request) = rx.recv().await {
            match self
                .compact_namespace(&request.namespace, request.forced)
                .await
            {
                Ok(true) => {
                    tracing::info!(
                        "More pending compaction work for {}, re-queueing",
                        request.namespace
                    );
                    enqueue(request);
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        "Compaction failed for namespace {}: {:?}",
                        request.namespace,
                        e
                    );
                }
            }
        }
        tracing::warn!("Compactor worker loop exited");
    }

    async fn sweeper_loop(&self) {
        let config = match get_config().await {
            Ok(config) => config,
            Err(e) => {
                tracing::error!("Failed to load config in sweeper loop: {:?}", e);
                return;
            }
        };

        let mut timer = interval(Duration::from_secs(config.compactor.sweeper_interval_secs));
        tracing::info!(
            "Compactor sweeper scheduled every {}s",
            config.compactor.sweeper_interval_secs
        );

        loop {
            timer.tick().await;
            tracing::info!("Compactor sweeper starting scan");

            match s3client::get_namespaces().await {
                Ok(namespaces) => {
                    for ns in namespaces {
                        if let Err(e) = self.check_and_trigger(&ns.id).await {
                            tracing::warn!("Sweeper failed to check {}: {:?}", ns.id, e);
                        }
                        sleep(Duration::from_millis(100)).await;
                    }
                }
                Err(e) => tracing::error!("Sweeper failed to list namespaces: {:?}", e),
            }
            tracing::info!("Compactor sweeper finished scan");
        }
    }

    fn is_processing(&self, namespace: &str) -> bool {
        self.processing
            .lock()
            .expect("processing set poisoned")
            .contains(namespace)
    }

    /// Claims the namespace for compaction. Returns `None` if a compaction for
    /// it is already running.
    fn try_begin(&self, namespace: &str) -> Option<ProcessingGuard<'_>> {
        let mut processing = self.processing.lock().expect("processing set poisoned");
        if processing.insert(namespace.to_string()) {
            Some(ProcessingGuard {
                processing: &self.processing,
                namespace: namespace.to_string(),
            })
        } else {
            None
        }
    }

    /// Sweeper check for one namespace: clean up soft-deleted files, then
    /// queue a compaction when the reindex thresholds are exceeded.
    async fn check_and_trigger(&self, namespace: &str) -> Result<()> {
        if self.is_processing(namespace) {
            return Ok(());
        }

        let client = Client::new().await?;
        let (metadata, _) = client.get_metadata(namespace).await?;

        // Deleting on the next sweep, not at swap time, gives in-flight
        // queries that still hold the old metadata time to finish.
        if !metadata.deleted_files.is_empty() {
            tracing::info!(
                "Sweeper found {} pending deletions for {}",
                metadata.deleted_files.len(),
                namespace
            );
            if let Err(e) = self.cleanup_deleted_files(namespace).await {
                tracing::warn!(
                    "Sweeper failed to clean up deleted files for {}: {:?}",
                    namespace,
                    e
                );
            }
        }

        let config = get_config().await?;
        if metadata.wal_files.len() >= config.indexing.reindex_threshold_wal_count
            || metadata.unindexed_bytes >= config.indexing.reindex_threshold_bytes
        {
            tracing::info!(
                "Sweeper triggering compaction for {} ({} WAL files)",
                namespace,
                metadata.wal_files.len()
            );
            trigger_compaction(namespace);
        }

        Ok(())
    }

    /// Compacts one batch of WAL files for the namespace. Returns `true` when
    /// enough WAL files remain to justify another cycle.
    async fn compact_namespace(&self, namespace: &str, force: bool) -> Result<bool> {
        let Some(_guard) = self.try_begin(namespace) else {
            tracing::info!("Namespace {} already being compacted, skipping", namespace);
            return Ok(false);
        };

        tracing::info!("Starting compaction for {} (force: {})", namespace, force);
        let client = Client::new().await?;
        let config = get_config().await?;

        for attempt in 0..config.storage.max_wal_write_retries {
            let (metadata, etag) = match client.get_metadata(namespace).await {
                Ok(found) => found,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch metadata for {} (attempt {}): {:?}; reconciling...",
                        namespace,
                        attempt,
                        e
                    );
                    self.reconcile_metadata(namespace)
                        .await
                        .context("Metadata reconciliation failed")?
                }
            };

            if !force && metadata.wal_files.len() <= 1 {
                tracing::info!(
                    "Compaction skipped for {}: only {} WAL file(s)",
                    namespace,
                    metadata.wal_files.len()
                );
                return Ok(false);
            }

            let (chunks, compacted_keys, compacted_bytes) =
                download_wal_batch(namespace, &metadata, force).await?;
            if compacted_keys.is_empty() {
                return Ok(false);
            }

            let (state, _tombstones) = engine::replay_wal_with_tombstones(&chunks)?;
            tracing::info!(
                "Replayed {} WAL files into {} unique documents for {}",
                compacted_keys.len(),
                state.len(),
                namespace
            );

            let rows: Vec<Row> = state.values().cloned().collect();
            let (ann_index_key, inverted_index_key) = if rows.is_empty() {
                (None, None)
            } else {
                let inverted = upload_inverted_index(namespace, &rows, &metadata).await?;
                let ann = update_spfresh_index(namespace, rows, &metadata, force).await?;
                (ann, Some(inverted))
            };

            let mut updated = metadata;

            let compacted_set: HashSet<&String> = compacted_keys.iter().collect();
            updated.wal_files.retain(|f| !compacted_set.contains(f));
            updated.deleted_files.extend(compacted_keys.iter().cloned());

            updated.unindexed_bytes = updated.unindexed_bytes.saturating_sub(compacted_bytes);
            if updated.wal_files.is_empty() {
                updated.unindexed_bytes = 0;
            }

            if let Some(key) = ann_index_key {
                if let Some(old) = updated.index.ann_index_file.replace(key) {
                    updated.deleted_files.push(old);
                }
                updated.index.indexed_row_count = state.len() as u64;
            }
            if let Some(key) = inverted_index_key
                && let Some(old) = updated.index.inverted_index_file.replace(key)
            {
                updated.deleted_files.push(old);
            }

            updated.index.status = engine::IndexStatus::UpToDate;
            updated.updated_at = chrono::Utc::now().timestamp();

            let more_pending =
                updated.wal_files.len() >= config.indexing.reindex_threshold_wal_count;

            let bytes = serde_json::to_vec(&updated)?;
            match s3client::put_object_if_match(namespace, METADATA_KEY, &bytes, &etag).await {
                Ok(_) => {
                    tracing::info!("Compaction metadata swap committed for {}", namespace);
                    invalidate_metadata_cache(namespace).await;
                    invalidate_deleted_files(namespace, &updated.deleted_files).await;
                    return Ok(more_pending);
                }
                Err(err) if err.downcast_ref::<PreconditionFailed>().is_some() => {
                    tracing::warn!(
                        "Metadata CAS conflict during compaction swap for {} (attempt {}), retrying",
                        namespace,
                        attempt + 1
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(anyhow!(
            "Max retries exceeded for compaction metadata swap for {namespace}"
        ))
    }

    async fn cleanup_deleted_files(&self, namespace: &str) -> Result<()> {
        let client = Client::new().await?;
        let config = get_config().await?;

        for _ in 0..config.storage.max_wal_write_retries {
            let (mut metadata, etag) = client.get_metadata(namespace).await?;
            if metadata.deleted_files.is_empty() {
                return Ok(());
            }

            let files_to_delete = std::mem::take(&mut metadata.deleted_files);
            let attempted = files_to_delete.len();
            let mut failed = Vec::new();
            for key in files_to_delete {
                if let Err(e) = s3client::delete_object(namespace, &key).await {
                    tracing::warn!(
                        "Failed to delete soft-deleted file {}/{}: {:?}",
                        namespace,
                        key,
                        e
                    );
                    failed.push(key);
                }
            }
            metadata.deleted_files = failed;
            metadata.updated_at = chrono::Utc::now().timestamp();

            let bytes = serde_json::to_vec(&metadata)?;
            match s3client::put_object_if_match(namespace, METADATA_KEY, &bytes, &etag).await {
                Ok(_) => {
                    tracing::info!(
                        "Cleaned up {} of {} deleted files for {}",
                        attempted - metadata.deleted_files.len(),
                        attempted,
                        namespace
                    );
                    invalidate_metadata_cache(namespace).await;
                    return Ok(());
                }
                Err(err) if err.downcast_ref::<PreconditionFailed>().is_some() => {
                    tracing::warn!(
                        "Metadata CAS conflict during cleanup for {}, retrying...",
                        namespace
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(anyhow!(
            "Max retries exceeded for deleted-file cleanup for {namespace}"
        ))
    }

    async fn reconcile_metadata(&self, namespace: &str) -> Result<(Metadata, String)> {
        tracing::info!("Reconciling metadata for namespace: {}", namespace);

        let wal_files: Vec<String> = s3client::list_wal_files(namespace)
            .await?
            .into_iter()
            .filter(|f| f.starts_with("wal/"))
            .collect();

        match s3client::get_file_with_etag(namespace, METADATA_KEY).await? {
            Some((data, etag)) => match serde_json::from_slice::<Metadata>(&data) {
                Ok(mut existing) => {
                    existing.wal_files = wal_files;
                    existing.updated_at = chrono::Utc::now().timestamp();
                    let bytes = serde_json::to_vec(&existing)?;
                    // A CAS conflict here means a concurrent writer holds newer
                    // state; propagate and let the next cycle retry.
                    s3client::put_object_if_match(namespace, METADATA_KEY, &bytes, &etag).await?;
                }
                Err(_) => {
                    // Corrupt metadata: overwrite with a fresh reconstruction.
                    let fresh = fresh_metadata(wal_files);
                    s3client::put_object(namespace, METADATA_KEY, &serde_json::to_vec(&fresh)?)
                        .await?;
                }
            },
            None => {
                let fresh = fresh_metadata(wal_files);
                s3client::put_object(namespace, METADATA_KEY, &serde_json::to_vec(&fresh)?).await?;
            }
        }

        invalidate_metadata_cache(namespace).await;

        let (data, etag) = s3client::get_file_with_etag(namespace, METADATA_KEY)
            .await?
            .context("Failed to retrieve reconciled metadata")?;
        let metadata =
            serde_json::from_slice(&data).context("Reconciled metadata failed to parse")?;
        Ok((metadata, etag))
    }
}


async fn download_wal_batch(
    namespace: &str,
    metadata: &Metadata,
    force: bool,
) -> Result<(Vec<Vec<u8>>, Vec<String>, u64)> {
    let config = get_config().await?;
    let max_files = config.compactor.max_files_per_cycle;
    let max_bytes = config.indexing.compaction_batch_size_bytes;

    let mut chunks = Vec::new();
    let mut compacted_keys = Vec::new();
    let mut total_bytes = 0u64;

    for key in metadata.wal_files.iter().take(max_files) {
        match s3client::get_file(namespace, key).await? {
            Some(data) => {
                let over_limit = total_bytes + data.len() as u64 > max_bytes;
                if !force && over_limit && !chunks.is_empty() {
                    tracing::info!(
                        "Compaction batch limit reached at {} bytes for {}",
                        total_bytes,
                        namespace
                    );
                    break;
                }
                total_bytes += data.len() as u64;
                chunks.push(data);
                compacted_keys.push(key.clone());
            }
            None => {
                tracing::warn!("WAL file missing during compaction download: {}", key);
                compacted_keys.push(key.clone());
            }
        }
    }

    Ok((chunks, compacted_keys, total_bytes))
}

/// Folds the replayed rows into the SPFresh index: incremental insert when a
/// compatible index exists, full build otherwise. Returns the new index key,
/// or `None` when the data is still too small to justify an index.
async fn update_spfresh_index(
    namespace: &str,
    rows: Vec<Row>,
    metadata: &Metadata,
    force: bool,
) -> Result<Option<String>> {
    let existing = match &metadata.index.ann_index_file {
        Some(file) => match s3client::get_file(namespace, file).await? {
            Some(data) => spfresh::SPFreshIndex::from_rkyv_bytes(&data, namespace.to_string()).ok(),
            None => None,
        },
        None => None,
    };

    let dimensions = rows[0].vector.len();
    let mut index = match existing {
        Some(mut index) if index.dimensions == dimensions => {
            if index.doc_to_posting.is_empty() && !index.posting_files.is_empty() {
                index.rebuild_doc_mapping().await?;
            }
            index.insert_vectors(rows).await?;
            index
        }
        Some(index) => {
            tracing::warn!(
                "Dimension mismatch in SPFresh index for {}: existing={}, new={}. Rebuilding.",
                namespace,
                index.dimensions,
                dimensions
            );
            spfresh::SPFreshIndex::build_from_rows(
                rows,
                spfresh::SPFreshConfig::default(),
                metadata.distance_metric,
                namespace.to_string(),
            )
            .await?
        }
        None => {
            if rows.len() < 10 && !force {
                return Ok(None); // Too few rows to justify an index yet
            }
            spfresh::SPFreshIndex::build_from_rows(
                rows,
                spfresh::SPFreshConfig::default(),
                metadata.distance_metric,
                namespace.to_string(),
            )
            .await?
        }
    };
    index.flush_deltas().await?;

    let bytes = index.to_rkyv_bytes()?;
    let key = format!("index/spfresh_metadata_{}", ulid::Ulid::generate());
    s3client::put_object(namespace, &key, &bytes).await?;
    invalidate_spfresh_cache(namespace).await;

    tracing::info!(
        "Updated SPFresh index for {}: {} postings, {} documents",
        namespace,
        index.posting_files.len(),
        index.vector_count()
    );
    Ok(Some(key))
}

async fn upload_inverted_index(
    namespace: &str,
    rows: &[Row],
    metadata: &Metadata,
) -> Result<String> {
    let index = InvertedIndex::build_from_rows(rows, &metadata.schema.index_attributes);
    let bytes = serde_json::to_vec(&index)?;
    let key = format!("index/inverted_{}", ulid::Ulid::generate());
    s3client::put_object(namespace, &key, &bytes).await?;

    tracing::info!("Uploaded inverted index {} ({} bytes)", key, bytes.len());
    Ok(key)
}

fn fresh_metadata(wal_files: Vec<String>) -> Metadata {
    let now = chrono::Utc::now().timestamp();
    Metadata {
        approx_row_count: 0,
        index: engine::IndexState {
            status: engine::IndexStatus::Updating,
            unindexed_bytes: None,
            ann_index_file: None,
            inverted_index_file: None,
            indexed_row_count: 0,
        },
        created_at: now,
        updated_at: now,
        wal_files,
        deleted_files: Vec::new(),
        unindexed_bytes: 0,
        distance_metric: vectors::DistanceMetric::default(),
        schema: engine::Schema::default(),
        vector_dimensions: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rkyv::to_bytes;

    use crate::engine::{self, DocumentId, Row, WalRecord};

    #[test]
    fn test_wal_replay_ordering_with_conflicts() {
        // Test that WAL replay correctly handles timestamp-based conflicts
        let doc_id = DocumentId::from("test_doc");

        // Create WAL records with different timestamps - later should win
        let early_row = Row {
            id: doc_id.clone(),
            vector: vec![1.0, 2.0, 3.0],
            attributes: HashMap::new(),
            timestamp: 1000,
        };

        let late_row = Row {
            id: doc_id.clone(),
            vector: vec![4.0, 5.0, 6.0],
            attributes: HashMap::new(),
            timestamp: 2000,
        };

        let records1 = vec![WalRecord::Upsert(early_row)];
        let records2 = vec![WalRecord::Upsert(late_row)];

        let wal1_bytes = to_bytes::<rkyv::rancor::Error>(&records1).unwrap().to_vec();
        let wal2_bytes = to_bytes::<rkyv::rancor::Error>(&records2).unwrap().to_vec();

        // Test both orders - later timestamp should always win regardless of WAL order
        let result1 = engine::replay_wal(&[wal1_bytes.clone(), wal2_bytes.clone()]).unwrap();
        let result2 = engine::replay_wal(&[wal2_bytes, wal1_bytes]).unwrap();

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);

        let final_row1 = &result1[&doc_id];
        let final_row2 = &result2[&doc_id];

        // Both should have the later timestamp data
        assert_eq!(final_row1.timestamp, 2000);
        assert_eq!(final_row2.timestamp, 2000);
        assert_eq!(final_row1.vector, vec![4.0, 5.0, 6.0]);
        assert_eq!(final_row2.vector, vec![4.0, 5.0, 6.0]);
    }

    #[test]
    fn test_wal_replay_with_deletions() {
        // Test that deletions are properly handled during WAL replay
        let doc_id = DocumentId::from("test_doc");

        let row = Row {
            id: doc_id.clone(),
            vector: vec![1.0, 2.0, 3.0],
            attributes: HashMap::new(),
            timestamp: 1000,
        };

        let records1 = vec![WalRecord::Upsert(row)];
        let records2 = vec![WalRecord::Delete {
            id: doc_id.clone(),
            timestamp: 2000,
        }];

        let wal1_bytes = to_bytes::<rkyv::rancor::Error>(&records1).unwrap().to_vec();
        let wal2_bytes = to_bytes::<rkyv::rancor::Error>(&records2).unwrap().to_vec();

        // Document should be deleted (not present in final state)
        let result = engine::replay_wal(&[wal1_bytes, wal2_bytes]).unwrap();
        assert_eq!(result.len(), 0);
        assert!(!result.contains_key(&doc_id));

        // Test reverse order - delete timestamp is later so should still win
        let records1 = vec![WalRecord::Upsert(Row {
            id: doc_id.clone(),
            vector: vec![1.0, 2.0, 3.0],
            attributes: HashMap::new(),
            timestamp: 1000,
        })];
        let records2 = vec![WalRecord::Delete {
            id: doc_id,
            timestamp: 2000,
        }];

        let wal1_bytes = to_bytes::<rkyv::rancor::Error>(&records1).unwrap().to_vec();
        let wal2_bytes = to_bytes::<rkyv::rancor::Error>(&records2).unwrap().to_vec();

        let result = engine::replay_wal(&[wal2_bytes, wal1_bytes]).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_wal_replay_delete_before_insert() {
        // Test edge case: delete timestamp before insert should keep the document
        let doc_id = DocumentId::from("test_doc");

        let records1 = vec![WalRecord::Delete {
            id: doc_id.clone(),
            timestamp: 1000,
        }];
        let records2 = vec![WalRecord::Upsert(Row {
            id: doc_id.clone(),
            vector: vec![1.0, 2.0, 3.0],
            attributes: HashMap::new(),
            timestamp: 2000,
        })];

        let wal1_bytes = to_bytes::<rkyv::rancor::Error>(&records1).unwrap().to_vec();
        let wal2_bytes = to_bytes::<rkyv::rancor::Error>(&records2).unwrap().to_vec();

        let result = engine::replay_wal(&[wal1_bytes, wal2_bytes]).unwrap();
        assert_eq!(result.len(), 1);

        let final_row = &result[&doc_id];
        assert_eq!(final_row.timestamp, 2000);
        assert_eq!(final_row.vector, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_wal_replay_multiple_documents() {
        // Test compaction of multiple documents across WAL files
        let doc1 = DocumentId::from("doc1");
        let doc2 = DocumentId::from("doc2");
        let doc3 = DocumentId::from("doc3");

        let records1 = vec![
            WalRecord::Upsert(Row {
                id: doc1.clone(),
                vector: vec![1.0],
                attributes: HashMap::new(),
                timestamp: 1000,
            }),
            WalRecord::Upsert(Row {
                id: doc2.clone(),
                vector: vec![2.0],
                attributes: HashMap::new(),
                timestamp: 1001,
            }),
        ];

        let records2 = vec![
            WalRecord::Upsert(Row {
                id: doc1.clone(),
                vector: vec![1.1], // Update doc1
                attributes: HashMap::new(),
                timestamp: 2000,
            }),
            WalRecord::Upsert(Row {
                id: doc3.clone(),
                vector: vec![3.0], // New doc3
                attributes: HashMap::new(),
                timestamp: 2001,
            }),
            WalRecord::Delete {
                id: doc2.clone(),
                timestamp: 2002,
            }, // Delete doc2
        ];

        let wal1_bytes = to_bytes::<rkyv::rancor::Error>(&records1).unwrap().to_vec();
        let wal2_bytes = to_bytes::<rkyv::rancor::Error>(&records2).unwrap().to_vec();

        let result = engine::replay_wal(&[wal1_bytes, wal2_bytes]).unwrap();

        // Should have doc1 (updated) and doc3 (new), but not doc2 (deleted)
        assert_eq!(result.len(), 2);

        let doc1_final = &result[&doc1];
        assert_eq!(doc1_final.vector, vec![1.1]); // Updated version
        assert_eq!(doc1_final.timestamp, 2000);

        let doc3_final = &result[&doc3];
        assert_eq!(doc3_final.vector, vec![3.0]);
        assert_eq!(doc3_final.timestamp, 2001);

        assert!(!result.contains_key(&doc2)); // Deleted
    }

    #[test]
    fn test_wal_replay_empty_files() {
        // Test that empty WAL files don't break replay
        let empty_records: Vec<WalRecord> = vec![];
        let empty_wal = to_bytes::<rkyv::rancor::Error>(&empty_records)
            .unwrap()
            .to_vec();

        let result = engine::replay_wal(std::slice::from_ref(&empty_wal)).unwrap();
        assert_eq!(result.len(), 0);

        // Test mix of empty and non-empty
        let doc_id = DocumentId::from("test_doc");
        let records = vec![WalRecord::Upsert(Row {
            id: doc_id.clone(),
            vector: vec![1.0],
            attributes: HashMap::new(),
            timestamp: 1000,
        })];
        let non_empty_wal = to_bytes::<rkyv::rancor::Error>(&records).unwrap().to_vec();

        let result = engine::replay_wal(&[empty_wal, non_empty_wal]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&doc_id));
    }

    #[test]
    fn test_compaction_output_deterministic() {
        // Test that compaction output is deterministic regardless of input order
        let doc1 = DocumentId::from("doc1");
        let doc2 = DocumentId::from("doc2");

        let records = vec![
            WalRecord::Upsert(Row {
                id: doc1,
                vector: vec![1.0],
                attributes: HashMap::new(),
                timestamp: 1000,
            }),
            WalRecord::Upsert(Row {
                id: doc2,
                vector: vec![2.0],
                attributes: HashMap::new(),
                timestamp: 1001,
            }),
        ];

        let wal_bytes = to_bytes::<rkyv::rancor::Error>(&records).unwrap().to_vec();

        // Replay same WAL multiple times - should be identical
        let result1 = engine::replay_wal(std::slice::from_ref(&wal_bytes)).unwrap();
        let result2 = engine::replay_wal(std::slice::from_ref(&wal_bytes)).unwrap();
        let result3 = engine::replay_wal(std::slice::from_ref(&wal_bytes)).unwrap();

        assert_eq!(result1.len(), result2.len());
        assert_eq!(result1.len(), result3.len());

        for (id, row1) in &result1 {
            let row2 = &result2[id];
            let row3 = &result3[id];
            assert_eq!(row1.timestamp, row2.timestamp);
            assert_eq!(row1.timestamp, row3.timestamp);
            assert_eq!(row1.vector, row2.vector);
            assert_eq!(row1.vector, row3.vector);
        }
    }

    #[test]
    fn test_compacted_output_serialization() {
        // Test that compacted output can be serialized back to WAL format
        let doc_id = DocumentId::from("test_doc");

        let original_row = Row {
            id: doc_id.clone(),
            vector: vec![1.0, 2.0, 3.0],
            attributes: {
                let mut m = HashMap::new();
                m.insert(
                    "key".to_string(),
                    crate::engine::AttributeValue::String("value".to_string()),
                );
                m
            },
            timestamp: 1000,
        };

        let records = vec![WalRecord::Upsert(original_row.clone())];
        let wal_bytes = to_bytes::<rkyv::rancor::Error>(&records).unwrap().to_vec();

        // Replay and convert back to WAL records
        let replay_result = engine::replay_wal(&[wal_bytes]).unwrap();
        let compacted_records: Vec<WalRecord> =
            replay_result.into_values().map(WalRecord::Upsert).collect();

        assert_eq!(compacted_records.len(), 1);
        if let WalRecord::Upsert(compacted_row) = &compacted_records[0] {
            assert_eq!(compacted_row.id, original_row.id);
            assert_eq!(compacted_row.vector, original_row.vector);
            assert_eq!(compacted_row.timestamp, original_row.timestamp);
        } else {
            panic!("Expected Upsert record");
        }

        // Test that re-serialized data can be deserialized
        let recompacted_bytes = to_bytes::<rkyv::rancor::Error>(&compacted_records)
            .unwrap()
            .to_vec();
        let final_result = engine::replay_wal(&[recompacted_bytes]).unwrap();

        assert_eq!(final_result.len(), 1);
        let final_row = &final_result[&doc_id];
        assert_eq!(final_row.vector, original_row.vector);
        assert_eq!(final_row.timestamp, original_row.timestamp);
    }
}
