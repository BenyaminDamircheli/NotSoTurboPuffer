pub mod ann;
pub mod compactor;
pub mod config;
pub mod engine;
pub mod inverted_index;
pub mod s3client;
pub mod store;
pub mod vectors;
pub mod wal_batcher;

pub mod not_so_turbo_puffer {
    //! Public client API: write, query, and namespace management.
    //!
    //! In-memory caches (WAL chunks, metadata, ANN indexes) sit above the
    //! disk/S3 store stack. Writes invalidate; TTLs bound staleness across
    //! processes.

    use std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result, anyhow};
    use moka::future::Cache;
    use tokio::sync::OnceCell;

    pub use crate::engine::{DocumentId, Metadata, Namespace, Row};
    use crate::{
        ann::spfresh,
        compactor, config,
        engine::{self, WalRecord},
        inverted_index::InvertedIndex,
        s3client,
        store::PreconditionFailed,
        vectors::{self, DistanceMetric},
        wal_batcher,
    };

    pub const METADATA_KEY: &str = "metadata/metadata.json";

    // -----------------------------------------------------------------------
    // Caches
    // -----------------------------------------------------------------------

    static WAL_CACHE: OnceCell<Cache<String, Vec<u8>>> = OnceCell::const_new();
    static METADATA_CACHE: OnceCell<Cache<String, (Metadata, String)>> = OnceCell::const_new();
    static SPFRESH_CACHE: OnceCell<Cache<String, spfresh::SPFreshIndex>> = OnceCell::const_new();

    async fn get_wal_cache() -> &'static Cache<String, Vec<u8>> {
        WAL_CACHE
            .get_or_init(|| async {
                Cache::builder()
                    .max_capacity(1000)
                    .time_to_live(Duration::from_secs(600))
                    .build()
            })
            .await
    }

    async fn get_metadata_cache() -> &'static Cache<String, (Metadata, String)> {
        METADATA_CACHE
            .get_or_init(|| async {
                Cache::builder()
                    .max_capacity(100)
                    // Short TTL: metadata is the freshness-critical object.
                    .time_to_live(Duration::from_secs(30))
                    .build()
            })
            .await
    }

    async fn get_spfresh_cache() -> &'static Cache<String, spfresh::SPFreshIndex> {
        SPFRESH_CACHE
            .get_or_init(|| async {
                Cache::builder()
                    .max_capacity(10) // Indexes are large, keep fewer
                    .time_to_live(Duration::from_secs(300))
                    // Required for invalidate_entries_if in invalidate_spfresh_cache.
                    .support_invalidation_closures()
                    .build()
            })
            .await
    }

    fn metadata_cache_key(namespace: &str) -> String {
        format!("metadata:{namespace}")
    }

    pub async fn invalidate_deleted_files(namespace: &str, deleted_files: &[String]) {
        let cache = get_wal_cache().await;
        for file in deleted_files {
            cache.invalidate(&format!("{namespace}/{file}")).await;
        }
    }

    pub async fn invalidate_metadata_cache(namespace: &str) {
        let cache = get_metadata_cache().await;
        cache.invalidate(&metadata_cache_key(namespace)).await;
    }

    pub async fn invalidate_spfresh_cache(namespace: &str) {
        let cache = get_spfresh_cache().await;
        let prefix = format!("spfresh:{namespace}:");
        let _ = cache.invalidate_entries_if(move |key, _| key.starts_with(&prefix));
    }

    // -----------------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------------

    #[derive(Debug)]
    pub struct DistanceMetricConflict {
        pub existing: DistanceMetric,
        pub requested: DistanceMetric,
    }

    impl std::fmt::Display for DistanceMetricConflict {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "namespace already uses {:?}, cannot change to {:?}",
                self.existing, self.requested
            )
        }
    }

    impl std::error::Error for DistanceMetricConflict {}

    #[derive(Debug)]
    pub struct DimensionMismatch {
        pub namespace: String,
        pub expected: usize,
        pub actual: usize,
    }

    impl std::fmt::Display for DimensionMismatch {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "Vector dimension mismatch: namespace '{}' expects {} dimensions, got {}",
                self.namespace, self.expected, self.actual
            )
        }
    }

    impl std::error::Error for DimensionMismatch {}

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Checks every upsert against the namespace's vector dimensions.
    /// The first vectors in an empty namespace establish the dimensions.
    fn validate_dimensions(
        namespace: &str,
        metadata: &mut Metadata,
        records: &[WalRecord],
    ) -> Result<()> {
        let mut dimensions = metadata.vector_dimensions;

        for record in records {
            if let WalRecord::Upsert(row) = record {
                match dimensions {
                    Some(expected) if row.vector.len() != expected => {
                        return Err(DimensionMismatch {
                            namespace: namespace.to_string(),
                            expected,
                            actual: row.vector.len(),
                        }
                        .into());
                    }
                    Some(_) => {}
                    None => {
                        dimensions = Some(row.vector.len());
                        tracing::info!(
                            "Establishing vector dimensions for namespace '{}': {}",
                            namespace,
                            row.vector.len()
                        );
                    }
                }
            }
        }

        metadata.vector_dimensions = dimensions;
        Ok(())
    }

    /// Merges a conflicting local metadata update on top of the remote state:
    /// union of WAL files, maximum of the counters.
    fn merge_metadata(remote: Metadata, local: &Metadata) -> Metadata {
        let mut merged = remote;

        let mut files: HashSet<String> = merged.wal_files.drain(..).collect();
        files.extend(local.wal_files.iter().cloned());
        merged.wal_files = files.into_iter().collect();
        // WAL keys are ULIDs, so a lexicographic sort restores time order.
        merged.wal_files.sort();

        merged.approx_row_count = merged.approx_row_count.max(local.approx_row_count);
        merged.unindexed_bytes = merged.unindexed_bytes.max(local.unindexed_bytes);
        merged.updated_at = chrono::Utc::now().timestamp();
        merged
    }

    /// Merges index candidates and WAL candidates by document id.
    /// When both sides hold the same document, the newer row wins;
    /// the WAL row wins timestamp ties because the WAL is more recent.
    fn merge_candidates(
        mut index_candidates: Vec<(f32, Row)>,
        mut wal_candidates: Vec<(f32, Row)>,
    ) -> Vec<(f32, Row)> {
        index_candidates.sort_by(|a, b| a.1.id.cmp(&b.1.id));
        wal_candidates.sort_by(|a, b| a.1.id.cmp(&b.1.id));

        let mut merged = Vec::with_capacity(index_candidates.len() + wal_candidates.len());
        let mut index_iter = index_candidates.into_iter().peekable();
        let mut wal_iter = wal_candidates.into_iter().peekable();

        while let (Some(index_next), Some(wal_next)) = (index_iter.peek(), wal_iter.peek()) {
            match index_next.1.id.cmp(&wal_next.1.id) {
                std::cmp::Ordering::Less => merged.push(index_iter.next().unwrap()),
                std::cmp::Ordering::Greater => merged.push(wal_iter.next().unwrap()),
                std::cmp::Ordering::Equal => {
                    let index_item = index_iter.next().unwrap();
                    let wal_item = wal_iter.next().unwrap();
                    if wal_item.1.timestamp >= index_item.1.timestamp {
                        merged.push(wal_item);
                    } else {
                        merged.push(index_item);
                    }
                }
            }
        }

        merged.extend(index_iter);
        merged.extend(wal_iter);
        merged
    }

    pub struct Client;

    impl Client {
        pub async fn new() -> Result<Self> {
            s3client::reset_s3_round_trips();
            Ok(Self)
        }

        pub async fn upsert(
            &self,
            namespace: &str,
            rows: impl IntoIterator<Item = Row>,
        ) -> Result<usize> {
            self.upsert_with_metric(namespace, None, rows).await
        }

        pub async fn upsert_with_metric(
            &self,
            namespace: &str,
            distance_metric: Option<DistanceMetric>,
            rows: impl IntoIterator<Item = Row>,
        ) -> Result<usize> {
            let records: Vec<WalRecord> = rows.into_iter().map(WalRecord::Upsert).collect();
            self.write(namespace, distance_metric, records).await
        }

        pub async fn write(
            &self,
            namespace: &str,
            distance_metric: Option<DistanceMetric>,
            mut records: Vec<WalRecord>,
        ) -> Result<usize> {
            let start = Instant::now();
            let (trips_before, _) = s3client::get_s3_round_trips();
            let count = records.len();
            if count == 0 {
                return Ok(0);
            }

            tracing::info!(
                "Starting write: {} records for namespace: {}",
                count,
                namespace
            );

            let (mut metadata, current_etag) = self
                .get_metadata(namespace)
                .await
                .with_context(|| format!("Failed to get metadata for namespace: {namespace}"))?;

            if let Some(metric) = distance_metric
                && metadata.distance_metric != metric
            {
                let has_data = metadata.approx_row_count > 0 || !metadata.wal_files.is_empty();
                if has_data {
                    return Err(DistanceMetricConflict {
                        existing: metadata.distance_metric,
                        requested: metric,
                    }
                    .into());
                }
                metadata.distance_metric = metric;
            }

            validate_dimensions(namespace, &mut metadata, &records)?;

            // Normalize vectors for cosine namespaces to match the distance definition.
            if metadata.distance_metric == DistanceMetric::CosineDistance {
                for record in &mut records {
                    if let WalRecord::Upsert(row) = record {
                        row.vector = vectors::normalize_vector(&row.vector);
                    }
                }
            }

            let wal_key = wal_batcher::submit_batched_write(namespace, records)
                .await
                .with_context(|| {
                    format!("Failed to write batched WAL for namespace: {namespace}")
                })?;

            metadata.wal_files.push(wal_key.clone());
            metadata.approx_row_count += count as u64;
            metadata.updated_at = chrono::Utc::now().timestamp();
            metadata.index.status = engine::IndexStatus::Updating;

            invalidate_metadata_cache(namespace).await;
            Self::persist_metadata(namespace, metadata.clone(), current_etag).await?;

            let cfg = config::get_config().await?;
            let needs_compaction = metadata.wal_files.len()
                >= cfg.indexing.reindex_threshold_wal_count
                || metadata.unindexed_bytes >= cfg.indexing.reindex_threshold_bytes;
            if needs_compaction {
                tracing::info!("Triggering compaction for {namespace}");
                compactor::trigger_compaction(namespace);
            }

            let (trips_after, trip_keys) = s3client::get_s3_round_trips();
            tracing::info!(
                "Write completed: {} records in {:?} (WAL: {}), S3 round trips: {} [{}]",
                count,
                start.elapsed(),
                wal_key,
                trips_after - trips_before,
                trip_keys[trips_before as usize..].join(", ")
            );

            Ok(count)
        }

        pub async fn query(
            &self,
            namespace: &str,
            query_vector: &[f32],
            top_k: usize,
            filters: Option<&HashMap<String, String>>,
        ) -> Result<Vec<Row>> {
            let start = Instant::now();
            let (trips_before, _) = s3client::get_s3_round_trips();

            // 1. Metadata
            let metadata_start = Instant::now();
            let (metadata, _etag) = self.get_metadata(namespace).await?;
            let metadata_duration = metadata_start.elapsed();

            let metric = metadata.distance_metric;
            let query_vector = match metric {
                DistanceMetric::CosineDistance => vectors::normalize_vector(query_vector),
                DistanceMetric::EuclideanSquared => query_vector.to_vec(),
            };

            // 2. Filters
            let filter_start = Instant::now();
            let mut allowed_ids: Option<HashSet<DocumentId>> = None;
            if let Some(f) = filters
                && !f.is_empty()
            {
                let inv_file = metadata.index.inverted_index_file.as_ref().ok_or_else(|| {
                    anyhow!("Filtering requested but inverted index not yet built")
                })?;
                let inv_index = self
                    .load_inverted_index(namespace, inv_file)
                    .await?
                    .ok_or_else(|| anyhow!("Failed to load inverted index"))?;

                if let Some(ids) = inv_index.filter(f) {
                    if ids.is_empty() {
                        return Ok(Vec::new());
                    }
                    allowed_ids = Some(ids);
                }
            }
            let filter_duration = filter_start.elapsed();

            // 3. WAL fetch and replay
            let wal_fetch_start = Instant::now();
            let wal_chunks = self.get_wal_chunks(namespace, &metadata.wal_files).await?;
            let wal_fetch_duration = wal_fetch_start.elapsed();

            let wal_replay_start = Instant::now();
            let (wal_state, deleted_ids) = engine::replay_wal_with_tombstones(&wal_chunks)?;
            let wal_replay_duration = wal_replay_start.elapsed();

            // 4. ANN search over the indexed data
            let ann_start = Instant::now();
            let mut index_candidates: Vec<(f32, Row)> = Vec::new();
            if let Some(ann_file) = &metadata.index.ann_index_file
                && let Some(mut ann_index) = self.load_ann_index(namespace, ann_file).await?
            {
                let ann_results = ann_index
                    .query_ann(&query_vector, top_k * 2, allowed_ids.as_ref())
                    .await?;

                for (distance, row) in ann_results {
                    // Drop rows deleted by a newer WAL tombstone.
                    let deleted = deleted_ids
                        .get(&row.id)
                        .is_some_and(|del_ts| *del_ts > row.timestamp);
                    if !deleted {
                        index_candidates.push((distance, row));
                    }
                }
            }
            let ann_duration = ann_start.elapsed();

            // 5. Score unindexed WAL rows and merge with index results
            let merge_start = Instant::now();
            let wal_doc_count = wal_state.len();
            let mut wal_candidates: Vec<(f32, Row)> = Vec::with_capacity(wal_state.len());
            for (doc_id, row) in wal_state {
                if let Some(ids) = &allowed_ids
                    && !ids.contains(&doc_id)
                {
                    continue;
                }
                let distance = vectors::distance(&row.vector, &query_vector, metric);
                wal_candidates.push((distance, row));
            }

            let mut candidates = merge_candidates(index_candidates, wal_candidates);
            let candidate_count = candidates.len();
            let merge_duration = merge_start.elapsed();

            // 6. Top-K by distance
            candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let results: Vec<Row> = candidates
                .into_iter()
                .take(top_k)
                .map(|(_, row)| row)
                .collect();

            let (trips_after, trip_keys) = s3client::get_s3_round_trips();
            tracing::info!(
                "Query for namespace={}: {} results (top_k={}) in {:?} | Metadata={:?} | Filter={:?} | WAL fetch={:?} replay={:?} ({} files, {} docs) | ANN={:?} | Merge={:?} ({} candidates)",
                namespace,
                results.len(),
                top_k,
                start.elapsed(),
                metadata_duration,
                filter_duration,
                wal_fetch_duration,
                wal_replay_duration,
                metadata.wal_files.len(),
                wal_doc_count,
                ann_duration,
                merge_duration,
                candidate_count
            );
            tracing::info!(
                "Query S3 round trips: {} [{}]",
                trips_after - trips_before,
                trip_keys[trips_before as usize..].join(", ")
            );

            Ok(results)
        }

        pub async fn get_metadata(&self, namespace: &str) -> Result<(Metadata, String)> {
            let _ = s3client::sanitize_namespace(namespace)?;

            let cache = get_metadata_cache().await;
            let cache_key = metadata_cache_key(namespace);

            if let Some((metadata, etag)) = cache.get(&cache_key).await {
                return Ok((metadata, etag));
            }

            let fetched = s3client::get_file_with_etag(namespace, METADATA_KEY)
                .await
                .with_context(|| {
                    format!("Failed to fetch metadata from S3 for namespace: {namespace}")
                })?;

            let (data, etag) = match fetched {
                Some(found) => found,
                None => {
                    tracing::info!(
                        "Metadata not found for namespace: {}, creating...",
                        namespace
                    );
                    s3client::create_namespace_resources(namespace)
                        .await
                        .with_context(|| {
                            format!("Failed to create namespace resources for: {namespace}")
                        })?;

                    s3client::get_file_with_etag(namespace, METADATA_KEY)
                        .await?
                        .context("Failed to retrieve metadata after creation")?
                }
            };

            let metadata: Metadata =
                serde_json::from_slice(&data).context("Failed to deserialize metadata")?;
            cache
                .insert(cache_key, (metadata.clone(), etag.clone()))
                .await;

            tracing::debug!(
                "Metadata fetched for namespace: {}, WAL files: {}, unindexed_bytes: {}",
                namespace,
                metadata.wal_files.len(),
                metadata.unindexed_bytes
            );

            Ok((metadata, etag))
        }

        /// Writes metadata with compare-and-swap. On an ETag conflict, merges
        /// the local update on top of the remote state and retries.
        async fn persist_metadata(
            namespace: &str,
            mut metadata: Metadata,
            mut etag: String,
        ) -> Result<()> {
            let cfg = config::get_config().await?;

            for _ in 0..cfg.storage.max_wal_write_retries {
                let bytes =
                    serde_json::to_vec(&metadata).context("Failed to serialize metadata")?;

                match s3client::put_object_if_match(namespace, METADATA_KEY, &bytes, &etag).await {
                    Ok(_new_etag) => {
                        invalidate_metadata_cache(namespace).await;
                        return Ok(());
                    }
                    Err(err) if err.downcast_ref::<PreconditionFailed>().is_some() => {
                        tracing::warn!("Metadata CAS conflict for {}, merging...", namespace);

                        let (remote_bytes, remote_etag) =
                            s3client::get_file_with_etag(namespace, METADATA_KEY)
                                .await?
                                .context("Metadata disappeared during conflict resolution")?;
                        let remote: Metadata = serde_json::from_slice(&remote_bytes)
                            .context("Failed to deserialize remote metadata during merge")?;

                        metadata = merge_metadata(remote, &metadata);
                        etag = remote_etag;
                    }
                    Err(err) => return Err(err),
                }
            }

            Err(anyhow!("Max retries exceeded for metadata persistence"))
        }

        /// Fetches WAL chunks, serving from cache where possible. Cache misses
        /// download concurrently. Chunk order always matches `wal_files` order,
        /// because replay depends on log order for equal timestamps.
        async fn get_wal_chunks(
            &self,
            namespace: &str,
            wal_files: &[String],
        ) -> Result<Vec<Vec<u8>>> {
            if wal_files.is_empty() {
                return Ok(Vec::new());
            }

            let cache = get_wal_cache().await;
            let mut slots: Vec<Option<Vec<u8>>> = Vec::with_capacity(wal_files.len());
            let mut fetches = Vec::new();

            for (slot, key) in wal_files.iter().enumerate() {
                let cache_key = format!("{namespace}/{key}");
                match cache.get(&cache_key).await {
                    Some(data) => slots.push(Some(data)),
                    None => {
                        slots.push(None);
                        let namespace = namespace.to_string();
                        let key = key.clone();
                        let task =
                            tokio::spawn(async move { s3client::get_file(&namespace, &key).await });
                        fetches.push((slot, cache_key, task));
                    }
                }
            }

            for (slot, cache_key, task) in fetches {
                match task.await? {
                    Ok(Some(data)) => {
                        cache.insert(cache_key, data.clone()).await;
                        slots[slot] = Some(data);
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e).context("Failed to fetch WAL file"),
                }
            }

            Ok(slots.into_iter().flatten().collect())
        }

        async fn load_ann_index(
            &self,
            namespace: &str,
            index_file: &str,
        ) -> Result<Option<spfresh::SPFreshIndex>> {
            let cache = get_spfresh_cache().await;
            let cache_key = format!("spfresh:{namespace}:{index_file}");

            if let Some(mut cached_index) = cache.get(&cache_key).await {
                cached_index.namespace = namespace.to_string();
                return Ok(Some(cached_index));
            }

            match s3client::get_file(namespace, index_file).await? {
                Some(data) => {
                    let index =
                        spfresh::SPFreshIndex::from_rkyv_bytes(&data, namespace.to_string())?;
                    cache.insert(cache_key, index.clone()).await;
                    Ok(Some(index))
                }
                None => Ok(None),
            }
        }

        async fn load_inverted_index(
            &self,
            namespace: &str,
            index_file: &str,
        ) -> Result<Option<InvertedIndex>> {
            match s3client::get_file(namespace, index_file).await? {
                Some(data) => {
                    let index: InvertedIndex = serde_json::from_slice(&data)
                        .map_err(|e| anyhow!("Failed to deserialize InvertedIndex: {e}"))?;
                    Ok(Some(index))
                }
                None => Ok(None),
            }
        }
    }

    pub async fn namespaces() -> Result<Vec<Namespace>> {
        s3client::get_namespaces().await
    }
}
