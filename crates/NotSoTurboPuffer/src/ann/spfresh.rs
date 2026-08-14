use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{
    ann::AnnIndex,
    engine::Row,
    vectors::{DistanceMetric, distance},
};

#[derive(Debug, Clone)]
pub struct SPFreshIndex {
    pub posting_files: HashMap<u32, String>,
    pub centroids: HashMap<u32, Vec<f32>>,
    pub distance_metric: DistanceMetric,
    pub dimensions: usize,
    pub config: SPFreshConfig,
    pub next_posting_id: u32,
    pub namespace: String,
    pub posting_cache: HashMap<u32, Posting>,
    pub doc_to_posting: HashMap<String, u32>,
    pub pending_deltas: HashMap<u32, PostingDelta>,
    pub index_version: u64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct SPFreshIndexPersistent {
    pub posting_files: HashMap<u32, String>,
    pub centroids: HashMap<u32, Vec<f32>>,
    pub distance_metric: DistanceMetric,
    pub dimensions: usize,
    pub config: SPFreshConfig,
    pub next_posting_id: u32,
    pub doc_to_posting: HashMap<String, u32>,
    pub index_version: u64,
}

impl From<&SPFreshIndex> for SPFreshIndexPersistent {
    fn from(index: &SPFreshIndex) -> Self {
        Self {
            posting_files: index.posting_files.clone(),
            centroids: index.centroids.clone(),
            distance_metric: index.distance_metric,
            dimensions: index.dimensions,
            config: index.config.clone(),
            next_posting_id: index.next_posting_id,
            doc_to_posting: index.doc_to_posting.clone(),
            index_version: index.index_version,
        }
    }
}

impl From<SPFreshIndexPersistent> for SPFreshIndex {
    fn from(p: SPFreshIndexPersistent) -> Self {
        Self {
            posting_files: p.posting_files,
            centroids: p.centroids,
            distance_metric: p.distance_metric,
            dimensions: p.dimensions,
            config: p.config,
            next_posting_id: p.next_posting_id,
            namespace: String::new(), // Must be set by caller
            posting_cache: HashMap::new(),
            doc_to_posting: p.doc_to_posting,
            pending_deltas: HashMap::new(),
            index_version: p.index_version,
        }
    }
}

impl SPFreshIndex {
    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>> {
        let persistent = SPFreshIndexPersistent::from(self);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&persistent)?;
        Ok(bytes.into_vec())
    }

    pub fn from_rkyv_bytes(data: &[u8], namespace: String) -> Result<Self> {
        let archived =
            rkyv::access::<rkyv::Archived<SPFreshIndexPersistent>, rkyv::rancor::Error>(data)?;

        let persistent: SPFreshIndexPersistent =
            rkyv::deserialize::<SPFreshIndexPersistent, rkyv::rancor::Error>(archived)?;

        let mut index = Self::from(persistent);
        index.namespace = namespace;
        Ok(index)
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct Posting {
    pub id: u32,
    pub rows: Vec<Row>,
    pub centroid: Vec<f32>,
    pub version: u64,
    pub created_at: i64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct PostingDelta {
    pub posting_id: u32,
    pub operations: Vec<DeltaOperation>,
    pub version: u64,
    pub created_at: i64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub enum DeltaOperation {
    Insert { row: Row },
    Delete { doc_id: String },
    UpdateCentroid { new_centroid: Vec<f32> },
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
pub struct SPFreshConfig {
    pub max_posting_size: usize,
    pub min_posting_size: usize,
    pub neighbor_fanout: usize,
    pub search_fanout: usize,
    pub random_seed: u64,
}

impl Default for SPFreshConfig {
    fn default() -> Self {
        Self {
            max_posting_size: 1000,
            min_posting_size: 100,
            neighbor_fanout: 32,
            search_fanout: 4,
            random_seed: 42,
        }
    }
}

impl SPFreshIndex {
    pub fn new(
        distance_metric: DistanceMetric,
        dimensions: usize,
        config: SPFreshConfig,
        namespace: String,
    ) -> Self {
        Self {
            posting_files: HashMap::new(),
            centroids: HashMap::new(),
            distance_metric,
            dimensions,
            config,
            next_posting_id: 0,
            namespace,
            posting_cache: HashMap::new(),
            doc_to_posting: HashMap::new(),
            pending_deltas: HashMap::new(),
            index_version: 0,
        }
    }

    async fn load_posting(&mut self, posting_id: u32) -> Result<Option<Posting>> {
        // Check cache first to avoid S3 calls
        if let Some(posting) = self.posting_cache.get(&posting_id) {
            return Ok(Some(posting.clone()));
        }

        if let Some(s3_key) = self.posting_files.get(&posting_id) {
            match crate::s3client::get_file(&self.namespace, s3_key).await? {
                Some(data) => {
                    let archived =
                        rkyv::access::<rkyv::Archived<Posting>, rkyv::rancor::Error>(&data)?;
                    let posting: Posting =
                        rkyv::deserialize::<Posting, rkyv::rancor::Error>(archived)?;
                    self.posting_cache.insert(posting_id, posting.clone());
                    Ok(Some(posting))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn save_posting(&mut self, posting: &Posting) -> Result<()> {
        let posting_key = if let Some(existing_key) = self.posting_files.get(&posting.id) {
            existing_key.clone()
        } else {
            format!("postings/posting_{}", ulid::Ulid::generate())
        };

        let posting_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(posting)?.into_vec();
        crate::s3client::put_object(&self.namespace, &posting_key, &posting_bytes).await?;

        self.posting_files.insert(posting.id, posting_key);
        self.posting_cache.insert(posting.id, posting.clone());

        Ok(())
    }

    fn add_delta_operation(&mut self, posting_id: u32, operation: DeltaOperation) {
        let delta = self
            .pending_deltas
            .entry(posting_id)
            .or_insert_with(|| PostingDelta {
                posting_id,
                operations: Vec::new(),
                version: self.index_version,
                created_at: chrono::Utc::now().timestamp(),
            });
        delta.operations.push(operation);
        self.index_version += 1;
    }

    pub async fn flush_deltas(&mut self) -> Result<()> {
        if self.pending_deltas.is_empty() {
            return Ok(());
        }

        for (posting_id, delta) in std::mem::take(&mut self.pending_deltas) {
            self.apply_delta_to_posting(posting_id, delta).await?;
        }

        Ok(())
    }

    async fn apply_delta_to_posting(&mut self, posting_id: u32, delta: PostingDelta) -> Result<()> {
        let mut posting = if let Some(cached) = self.posting_cache.get(&posting_id) {
            cached.clone()
        } else {
            self.load_posting(posting_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Posting {posting_id} not found"))?
        };

        for operation in delta.operations {
            match operation {
                DeltaOperation::Insert { row } => {
                    let doc_id = row.id.to_string();
                    posting.rows.retain(|r| r.id.to_string() != doc_id);
                    posting.rows.push(row);
                    self.doc_to_posting.insert(doc_id, posting_id);
                }
                DeltaOperation::Delete { doc_id } => {
                    posting.rows.retain(|r| r.id.to_string() != doc_id);
                    self.doc_to_posting.remove(&doc_id);
                }
                DeltaOperation::UpdateCentroid { new_centroid } => {
                    posting.centroid = new_centroid;
                }
            }
        }

        posting.version += 1;
        self.update_centroid_from_posting(&posting);

        if posting.rows.is_empty() {
            self.delete_posting_from_s3(posting_id).await?;
            return Ok(());
        } else if posting.rows.len() > self.config.max_posting_size {
            self.split_posting_async(posting_id).await?;
            return Ok(());
        }

        self.save_posting(&posting).await?;
        Ok(())
    }

    async fn maybe_flush_deltas(&mut self) -> Result<()> {
        let total_pending: usize = self
            .pending_deltas
            .values()
            .map(|d| d.operations.len())
            .sum();
        if total_pending >= 100 {
            self.flush_deltas().await?;
        }
        Ok(())
    }

    pub async fn rebuild_doc_mapping(&mut self) -> Result<()> {
        self.doc_to_posting.clear();

        let posting_ids: Vec<u32> = self.posting_files.keys().copied().collect();
        for posting_id in posting_ids {
            if let Some(posting) = self.load_posting(posting_id).await? {
                for row in &posting.rows {
                    self.doc_to_posting.insert(row.id.to_string(), posting_id);
                }
            }
        }

        Ok(())
    }

    async fn delete_posting_from_s3(&mut self, posting_id: u32) -> Result<()> {
        if let Some(s3_key) = self.posting_files.remove(&posting_id) {
            crate::s3client::delete_object(&self.namespace, &s3_key).await?;
        }
        self.posting_cache.remove(&posting_id);
        self.centroids.remove(&posting_id);
        Ok(())
    }

    pub async fn build_from_rows_async(
        rows: Vec<Row>,
        config: SPFreshConfig,
        distance_metric: DistanceMetric,
        namespace: String,
    ) -> Result<Self> {
        if rows.is_empty() {
            return Ok(Self::new(distance_metric, 0, config, namespace));
        }

        let dimensions = rows[0].vector.len();
        let mut index = Self::new(distance_metric, dimensions, config, namespace);

        index.insert_vectors_async(rows).await?;

        // Ensure all deltas are flushed for initial build
        index.flush_deltas().await?;

        Ok(index)
    }

    pub fn build_from_rows(
        rows: Vec<Row>,
        config: SPFreshConfig,
        distance_metric: DistanceMetric,
    ) -> Result<Self> {
        if rows.is_empty() {
            return Ok(Self::new(distance_metric, 0, config, String::new()));
        }

        let dimensions = rows[0].vector.len();
        let mut index = Self::new(distance_metric, dimensions, config, String::new());

        for row in rows {
            index.insert_row_sync(row)?;
        }

        Ok(index)
    }

    pub async fn insert_vectors_async(&mut self, rows: Vec<Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let chunk_size = 1000;
        let mut start_idx = 0;

        if self.posting_files.is_empty() {
            if let Some(first_row) = rows.first() {
                self.create_initial_posting_async(first_row.clone()).await?;
                start_idx = 1;
            }
        }

        while start_idx < rows.len() {
            let end_idx = (start_idx + chunk_size).min(rows.len());
            let chunk = &rows[start_idx..end_idx];

            let mut posting_groups: HashMap<u32, Vec<Row>> = HashMap::new();

            for row in chunk {
                let doc_id = row.id.to_string();
                if let Some(&existing_posting_id) = self.doc_to_posting.get(&doc_id) {
                    self.add_delta_operation(
                        existing_posting_id,
                        DeltaOperation::Delete {
                            doc_id: doc_id.clone(),
                        },
                    );
                    // Remove from tracking
                    self.doc_to_posting.remove(&doc_id);
                }

                let nearest_posting_id = self.find_nearest_posting(&row.vector);
                posting_groups
                    .entry(nearest_posting_id)
                    .or_default()
                    .push(row.clone());
            }

            // Create delta operations instead of loading full postings
            for (posting_id, group_rows) in posting_groups {
                for row in group_rows {
                    let doc_id = row.id.to_string();
                    // Add to delta operations
                    self.add_delta_operation(posting_id, DeltaOperation::Insert { row });

                    // Track document location
                    self.doc_to_posting.insert(doc_id, posting_id);
                }
            }

            // Batch flush deltas to S3 periodically or when threshold reached
            self.maybe_flush_deltas().await?;

            start_idx = end_idx;
        }

        // Ensure final flush
        self.flush_deltas().await?;

        Ok(())
    }

    pub async fn insert_row_async(&mut self, row: Row) -> Result<()> {
        if self.posting_files.is_empty() {
            self.create_initial_posting_async(row).await?;
            return Ok(());
        }

        let doc_id = row.id.to_string();

        // Check if this is an update to existing document
        if let Some(&existing_posting_id) = self.doc_to_posting.get(&doc_id) {
            // Delete old version first
            self.add_delta_operation(
                existing_posting_id,
                DeltaOperation::Delete {
                    doc_id: doc_id.clone(),
                },
            );
            self.doc_to_posting.remove(&doc_id);
        }

        let nearest_posting_id = self.find_nearest_posting(&row.vector);

        // Create insert delta instead of loading full posting
        self.add_delta_operation(nearest_posting_id, DeltaOperation::Insert { row });

        // Track document location
        self.doc_to_posting.insert(doc_id, nearest_posting_id);

        // Check if we need to flush deltas
        self.maybe_flush_deltas().await?;

        Ok(())
    }

    pub fn insert_row_sync(&mut self, row: Row) -> Result<()> {
        if self.posting_cache.is_empty() && self.posting_files.is_empty() {
            self.create_initial_posting_sync(row);
            return Ok(());
        }

        let doc_id = row.id.to_string();

        // Check if document already exists - handle updates
        if let Some(&existing_posting_id) = self.doc_to_posting.get(&doc_id) {
            // Remove old version
            if let Some(posting) = self.posting_cache.get_mut(&existing_posting_id) {
                posting.rows.retain(|r| r.id.to_string() != doc_id);
                posting.version += 1;
                let posting_clone = posting.clone();
                self.update_centroid_from_posting(&posting_clone);
            }
            self.doc_to_posting.remove(&doc_id);
        }

        let nearest_posting_id = self.find_nearest_posting(&row.vector);

        // For sync version, we work with cached postings only
        if let Some(posting) = self.posting_cache.get_mut(&nearest_posting_id) {
            posting.rows.push(row);
            posting.version += 1;

            // Track document mapping
            self.doc_to_posting.insert(doc_id, nearest_posting_id);

            let needs_splitting = posting.rows.len() > self.config.max_posting_size;
            let posting_clone = posting.clone();
            self.update_centroid_from_posting(&posting_clone);

            if needs_splitting {
                self.split_posting_sync(nearest_posting_id)?;
            }
        }

        Ok(())
    }

    pub fn insert_row(&mut self, row: Row) -> Result<()> {
        self.insert_row_sync(row)
    }

    async fn create_initial_posting_async(&mut self, row: Row) -> Result<()> {
        let posting_id = self.next_posting_id;
        self.next_posting_id += 1;

        let doc_id = row.id.to_string();
        let vector = row.vector.clone();

        let posting = Posting {
            id: posting_id,
            rows: vec![row],
            centroid: vector.clone(),
            version: 1,
            created_at: chrono::Utc::now().timestamp(),
        };

        self.save_posting(&posting).await?;
        self.centroids.insert(posting_id, vector);

        // Track document mapping
        self.doc_to_posting.insert(doc_id, posting_id);

        Ok(())
    }

    fn create_initial_posting_sync(&mut self, row: Row) {
        let posting_id = self.next_posting_id;
        self.next_posting_id += 1;

        let doc_id = row.id.to_string();
        let vector = row.vector.clone();

        let posting = Posting {
            id: posting_id,
            rows: vec![row],
            centroid: vector.clone(),
            version: 1,
            created_at: chrono::Utc::now().timestamp(),
        };

        self.posting_cache.insert(posting_id, posting);
        self.centroids.insert(posting_id, vector);

        // Track document mapping
        self.doc_to_posting.insert(doc_id, posting_id);
    }

    fn update_centroid_from_posting(&mut self, posting: &Posting) {
        if posting.rows.is_empty() {
            self.centroids.remove(&posting.id);
            return;
        }

        let mut centroid = vec![0.0; self.dimensions];
        for row in &posting.rows {
            for (i, &val) in row.vector.iter().enumerate() {
                centroid[i] += val;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let count = posting.rows.len() as f32;
        for val in &mut centroid {
            *val /= count;
        }

        self.centroids.insert(posting.id, centroid);
    }

    async fn split_posting_async(&mut self, posting_id: u32) -> Result<()> {
        let posting = self
            .load_posting(posting_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Posting {posting_id} not found for splitting"))?;

        if posting.rows.len() <= self.config.max_posting_size {
            return Ok(());
        }

        let (posting1, posting2) = self.perform_split(&posting)?;
        let new_posting_id = self.next_posting_id;
        self.next_posting_id += 1;

        let mut posting2 = posting2;
        posting2.id = new_posting_id;

        self.save_posting(&posting1).await?;
        self.save_posting(&posting2).await?;

        self.update_centroid_from_posting(&posting1);
        self.update_centroid_from_posting(&posting2);

        // Apply LIRE reassignment to improve graph quality after split
        if let Err(e) = self
            .apply_lire_reassignment_async(posting_id, new_posting_id)
            .await
        {
            tracing::warn!(
                "LIRE reassignment failed for postings {} and {}: {:?}",
                posting_id,
                new_posting_id,
                e
            );
            // Continue even if reassignment fails - split is still valid
        }

        Ok(())
    }

    fn split_posting_sync(&mut self, posting_id: u32) -> Result<()> {
        let posting = self
            .posting_cache
            .get(&posting_id)
            .ok_or_else(|| anyhow::anyhow!("Posting {posting_id} not found in cache"))?
            .clone();

        if posting.rows.len() <= self.config.max_posting_size {
            return Ok(());
        }

        let (posting1, posting2) = self.perform_split(&posting)?;
        let new_posting_id = self.next_posting_id;
        self.next_posting_id += 1;

        let mut posting2 = posting2;
        posting2.id = new_posting_id;

        self.posting_cache.insert(posting_id, posting1.clone());
        self.posting_cache.insert(new_posting_id, posting2.clone());

        // Update doc mappings for the new posting
        for row in &posting2.rows {
            self.doc_to_posting
                .insert(row.id.to_string(), new_posting_id);
        }

        self.update_centroid_from_posting(&posting1);
        self.update_centroid_from_posting(&posting2);

        Ok(())
    }

    /// Apply LIRE reassignment to improve index quality after posting splits
    /// This examines neighbor postings and reassigns vectors to better postings
    async fn apply_lire_reassignment_async(
        &mut self,
        split_posting_id: u32,
        new_posting_id: u32,
    ) -> Result<()> {
        // Find neighbor postings within reassignment scope
        let neighbor_postings =
            self.find_neighbor_postings_for_reassignment(split_posting_id, new_posting_id);

        if neighbor_postings.is_empty() {
            return Ok(());
        }

        // Collect all vectors from neighbor postings for potential reassignment
        let mut reassignment_candidates = Vec::new();

        for neighbor_id in &neighbor_postings {
            if let Some(posting) = self.load_posting(*neighbor_id).await? {
                for row in &posting.rows {
                    reassignment_candidates.push((row.clone(), *neighbor_id));
                }
            }
        }

        // Apply reassignment algorithm
        let reassignments = self.compute_lire_reassignments(
            &reassignment_candidates,
            split_posting_id,
            new_posting_id,
            &neighbor_postings,
        );

        // Execute reassignments using delta operations for efficiency
        for (row, old_posting_id, new_posting_id) in reassignments {
            let doc_id = row.id.to_string();
            if old_posting_id != new_posting_id {
                // Move vector from old posting to new posting

                // Delete from old posting
                self.add_delta_operation(
                    old_posting_id,
                    DeltaOperation::Delete {
                        doc_id: doc_id.clone(),
                    },
                );

                // Insert to new posting
                self.add_delta_operation(new_posting_id, DeltaOperation::Insert { row });

                // Update document mapping
                self.doc_to_posting.insert(doc_id, new_posting_id);
            }
        }

        // Note: Don't flush deltas here to avoid recursion
        // Deltas will be flushed later when threshold is reached

        Ok(())
    }

    /// Find neighbor postings for LIRE reassignment within the fanout scope
    fn find_neighbor_postings_for_reassignment(
        &self,
        split_posting_id: u32,
        new_posting_id: u32,
    ) -> Vec<u32> {
        let split_centroid = self.centroids.get(&split_posting_id);
        let new_centroid = self.centroids.get(&new_posting_id);

        let (Some(split_centroid), Some(new_centroid)) = (split_centroid, new_centroid) else {
            return Vec::new(); // Missing centroids, skip reassignment
        };

        // Find neighbor postings within neighbor_fanout distance
        let mut neighbor_distances: Vec<(u32, f32)> = Vec::new();

        for (&posting_id, centroid) in &self.centroids {
            if posting_id == split_posting_id || posting_id == new_posting_id {
                continue; // Skip the split postings themselves
            }

            // Calculate minimum distance to either split posting
            let dist_to_split = distance(centroid, split_centroid, self.distance_metric);
            let dist_to_new = distance(centroid, new_centroid, self.distance_metric);
            let min_distance = dist_to_split.min(dist_to_new);

            neighbor_distances.push((posting_id, min_distance));
        }

        // Sort by distance and take top neighbor_fanout postings
        neighbor_distances
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        neighbor_distances.truncate(self.config.neighbor_fanout);

        neighbor_distances.into_iter().map(|(id, _)| id).collect()
    }

    /// Compute LIRE reassignments for vectors in neighbor postings
    /// Returns (`doc_id`, `old_posting_id`, `new_posting_id`) tuples for vectors that should move
    fn compute_lire_reassignments(
        &self,
        candidates: &[(Row, u32)], // (row, current_posting_id)
        split_posting_id: u32,
        new_posting_id: u32,
        neighbor_posting_ids: &[u32],
    ) -> Vec<(Row, u32, u32)> {
        let mut reassignments = Vec::new();

        // Create list of all postings to consider for reassignment
        let mut target_postings = vec![split_posting_id, new_posting_id];
        target_postings.extend(neighbor_posting_ids.iter().copied());

        // For each vector, find its optimal posting
        for (row, current_posting_id) in candidates {
            let mut best_posting_id = *current_posting_id;
            let mut best_distance = f32::INFINITY;

            // Calculate distance to all candidate postings
            for &target_posting_id in &target_postings {
                if let Some(target_centroid) = self.centroids.get(&target_posting_id) {
                    let distance = distance(&row.vector, target_centroid, self.distance_metric);

                    if distance < best_distance {
                        best_distance = distance;
                        best_posting_id = target_posting_id;
                    }
                }
            }

            // Only reassign if the optimal posting is different and reassignment is beneficial
            if best_posting_id != *current_posting_id
                && self.should_reassign_vector(
                    &row.vector,
                    *current_posting_id,
                    best_posting_id,
                    best_distance,
                )
            {
                reassignments.push((row.clone(), *current_posting_id, best_posting_id));
            }
        }

        reassignments
    }

    /// Determine if a vector should be reassigned based on distance improvement and posting balance
    fn should_reassign_vector(
        &self,
        vector: &[f32],
        current_posting_id: u32,
        target_posting_id: u32,
        target_distance: f32,
    ) -> bool {
        // Calculate current distance to existing posting
        let current_distance =
            if let Some(current_centroid) = self.centroids.get(&current_posting_id) {
                distance(vector, current_centroid, self.distance_metric)
            } else {
                return true; // If current posting missing, definitely reassign
            };

        // Only reassign if there's a meaningful improvement
        let improvement_threshold = 0.1; // 10% improvement required
        let improvement = (current_distance - target_distance) / current_distance;

        if improvement < improvement_threshold {
            return false;
        }

        // Check posting size balance - avoid creating overly large postings
        let current_posting_size = self.get_posting_size_estimate(current_posting_id);
        let target_posting_size = self.get_posting_size_estimate(target_posting_id);

        // Don't reassign if target posting is already near capacity
        let max_size = self.config.max_posting_size;
        if target_posting_size >= (max_size * 8) / 10 {
            return false;
        }

        // Don't reassign if it would make current posting too small
        let min_size = self.config.min_posting_size;
        if current_posting_size <= min_size + 1 {
            return false;
        }

        true
    }

    /// Get posting size estimate without loading full posting
    fn get_posting_size_estimate(&self, posting_id: u32) -> usize {
        // Check if we have it in cache first
        if let Some(posting) = self.posting_cache.get(&posting_id) {
            return posting.rows.len();
        }

        // Check pending deltas for size changes
        if let Some(delta) = self.pending_deltas.get(&posting_id) {
            // Rough estimation based on delta operations
            let inserts = delta
                .operations
                .iter()
                .filter(|op| matches!(op, DeltaOperation::Insert { .. }))
                .count();
            let deletes = delta
                .operations
                .iter()
                .filter(|op| matches!(op, DeltaOperation::Delete { .. }))
                .count();

            // Very rough estimate - assume posting starts with average size
            let avg_posting_size =
                usize::midpoint(self.config.max_posting_size, self.config.min_posting_size);
            return avg_posting_size
                .saturating_add(inserts)
                .saturating_sub(deletes);
        }

        // Default estimate if no information available
        self.config.max_posting_size / 2
    }

    pub fn delete_vector(&mut self, doc_id: &str) -> Result<bool> {
        // Use doc mapping for O(1) lookup in sync version too
        if let Some(&posting_id) = self.doc_to_posting.get(doc_id) {
            // Remove from posting
            if let Some(posting) = self.posting_cache.get_mut(&posting_id) {
                posting.rows.retain(|r| r.id.to_string() != doc_id);
                posting.version += 1;
            }

            // Remove from mapping
            self.doc_to_posting.remove(doc_id);

            // Handle empty posting
            let posting_size = self
                .posting_cache
                .get(&posting_id)
                .map(|p| p.rows.len())
                .unwrap_or(0);

            if posting_size == 0 {
                self.remove_posting_sync(posting_id);
            } else if posting_size < self.config.min_posting_size {
                self.attempt_merge_posting_sync(posting_id);
            } else {
                if let Some(posting) = self.posting_cache.get(&posting_id).cloned() {
                    self.update_centroid_from_posting(&posting);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Fast incremental deletion using doc mapping - no posting search required
    pub async fn delete_vector_async(&mut self, doc_id: &str) -> Result<bool> {
        // Use doc_to_posting mapping for O(1) lookup instead of scanning all postings
        if let Some(&posting_id) = self.doc_to_posting.get(doc_id) {
            // Create delete delta operation
            self.add_delta_operation(
                posting_id,
                DeltaOperation::Delete {
                    doc_id: doc_id.to_string(),
                },
            );

            // Remove from tracking
            self.doc_to_posting.remove(doc_id);

            // Maybe flush if we have enough pending operations
            self.maybe_flush_deltas().await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn remove_posting_sync(&mut self, posting_id: u32) {
        // Remove doc mappings for all documents in this posting
        if let Some(posting) = self.posting_cache.get(&posting_id) {
            for row in &posting.rows {
                self.doc_to_posting.remove(&row.id.to_string());
            }
        }

        self.posting_cache.remove(&posting_id);
        self.centroids.remove(&posting_id);
    }

    fn attempt_merge_posting_sync(&mut self, small_posting_id: u32) {
        let small_posting = match self.posting_cache.get(&small_posting_id) {
            Some(p) => p.clone(),
            None => return,
        };

        let nearest_posting_id = self.find_nearest_posting_for_merge_sync(small_posting_id);

        if let Some(target_id) = nearest_posting_id {
            let target_size = self
                .posting_cache
                .get(&target_id)
                .map(|p| p.rows.len())
                .unwrap_or(0);
            let merged_size = target_size + small_posting.rows.len();

            if merged_size <= self.config.max_posting_size {
                if let Some(target_posting) = self.posting_cache.get_mut(&target_id) {
                    target_posting
                        .rows
                        .extend(small_posting.rows.iter().cloned());
                    target_posting.version += 1;
                }

                self.remove_posting_sync(small_posting_id);

                if let Some(target_posting) = self.posting_cache.get(&target_id).cloned() {
                    self.update_centroid_from_posting(&target_posting);
                }
            } else {
                if let Some(posting) = self.posting_cache.get(&small_posting_id).cloned() {
                    self.update_centroid_from_posting(&posting);
                }
            }
        } else {
            if let Some(posting) = self.posting_cache.get(&small_posting_id).cloned() {
                self.update_centroid_from_posting(&posting);
            }
        }
    }

    fn find_nearest_posting_for_merge_sync(&self, posting_id: u32) -> std::option::Option<u32> {
        #[allow(clippy::question_mark)]
        let Some(small_posting) = self.posting_cache.get(&posting_id) else {
            return None;
        };

        #[allow(clippy::question_mark)]
        let Some(small_centroid) = self.centroids.get(&posting_id) else {
            return None;
        };

        let mut best_candidate = None;
        let mut best_distance = f32::INFINITY;

        for (&other_id, other_centroid) in &self.centroids {
            if other_id == posting_id {
                continue;
            }

            let Some(other_posting) = self.posting_cache.get(&other_id) else {
                continue;
            };

            let merged_size = small_posting.rows.len() + other_posting.rows.len();
            if merged_size > self.config.max_posting_size {
                continue;
            }

            let distance = distance(small_centroid, other_centroid, self.distance_metric);
            if distance < best_distance {
                best_distance = distance;
                best_candidate = Some(other_id);
            }
        }

        best_candidate
    }

    fn find_nearest_posting(&self, vector: &[f32]) -> u32 {
        let mut best_posting = 0;
        let mut best_distance = f32::INFINITY;

        for (&posting_id, centroid) in &self.centroids {
            let distance = distance(vector, centroid, self.distance_metric);
            if distance < best_distance {
                best_distance = distance;
                best_posting = posting_id;
            }
        }

        best_posting
    }

    fn perform_split(&self, posting: &Posting) -> Result<(Posting, Posting)> {
        use rand::{RngExt, SeedableRng, rngs::StdRng};

        if posting.rows.len() < 2 {
            return Err(anyhow::anyhow!(
                "Cannot split posting with less than 2 vectors"
            ));
        }

        // Use simple k-means with k=2 to split
        let vectors: Vec<&Vec<f32>> = posting.rows.iter().map(|r| &r.vector).collect();

        // Initialize two centroids randomly
        let mut rng = StdRng::seed_from_u64(self.config.random_seed);

        let idx1 = rng.random_range(0..vectors.len());
        let mut idx2 = rng.random_range(0..vectors.len());
        while idx2 == idx1 {
            idx2 = rng.random_range(0..vectors.len());
        }

        let mut centroid1 = vectors[idx1].clone();
        let mut centroid2 = vectors[idx2].clone();

        // Run k-means iterations
        for _ in 0..10 {
            let mut cluster1 = Vec::new();
            let mut cluster2 = Vec::new();

            // Assign vectors to nearest centroid
            for (i, vector) in vectors.iter().enumerate() {
                let dist1 = distance(vector, &centroid1, self.distance_metric);
                let dist2 = distance(vector, &centroid2, self.distance_metric);

                if dist1 <= dist2 {
                    cluster1.push(i);
                } else {
                    cluster2.push(i);
                }
            }

            // Update centroids
            if !cluster1.is_empty() {
                centroid1 = self
                    .compute_centroid(&cluster1.iter().map(|&i| vectors[i]).collect::<Vec<_>>())?;
            }
            if !cluster2.is_empty() {
                centroid2 = self
                    .compute_centroid(&cluster2.iter().map(|&i| vectors[i]).collect::<Vec<_>>())?;
            }
        }

        // Final assignment
        let mut rows1 = Vec::new();
        let mut rows2 = Vec::new();

        for (i, vector) in vectors.iter().enumerate() {
            let dist1 = distance(vector, &centroid1, self.distance_metric);
            let dist2 = distance(vector, &centroid2, self.distance_metric);

            if dist1 <= dist2 {
                rows1.push(posting.rows[i].clone());
            } else {
                rows2.push(posting.rows[i].clone());
            }
        }

        // Ensure both postings have at least one vector
        if rows1.is_empty() {
            rows1.push(rows2.pop().unwrap());
        } else if rows2.is_empty() {
            rows2.push(rows1.pop().unwrap());
        }

        let posting1 = Posting {
            id: posting.id,
            rows: rows1,
            centroid: centroid1.to_vec(),
            version: posting.version + 1,
            created_at: chrono::Utc::now().timestamp(),
        };

        let posting2 = Posting {
            id: 0, // Will be set by caller
            rows: rows2,
            centroid: centroid2.to_vec(),
            version: 1,
            created_at: chrono::Utc::now().timestamp(),
        };

        Ok((posting1, posting2))
    }

    fn compute_centroid(&self, vectors: &[&Vec<f32>]) -> Result<Vec<f32>> {
        if vectors.is_empty() {
            return Err(anyhow::anyhow!(
                "Cannot compute centroid of empty vector set"
            ));
        }

        let mut centroid = vec![0.0; self.dimensions];
        for vector in vectors {
            for (i, &val) in vector.iter().enumerate() {
                centroid[i] += val;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let count = vectors.len() as f32;
        for val in &mut centroid {
            *val /= count;
        }

        Ok(centroid)
    }

    pub async fn query_ann_async(
        &mut self,
        query_vector: &[f32],
        top_k: usize,
        allowed_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<(f32, Row)>> {
        let start = std::time::Instant::now();

        if self.posting_files.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Find nearest centroids
        let centroid_start = std::time::Instant::now();
        let mut posting_distances: Vec<(u32, f32)> = self
            .centroids
            .iter()
            .map(|(&posting_id, centroid)| {
                let distance = distance(query_vector, centroid, self.distance_metric);
                (posting_id, distance)
            })
            .collect();

        posting_distances
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let centroid_duration = centroid_start.elapsed();

        // 2. Load and search postings
        let search_count = self.config.search_fanout.min(posting_distances.len());
        let mut candidates = Vec::new();
        let posting_load_start = std::time::Instant::now();
        let mut total_docs_searched = 0;

        for &(posting_id, _) in &posting_distances[..search_count] {
            if let Some(posting) = self.load_posting(posting_id).await? {
                total_docs_searched += posting.rows.len();
                for row in &posting.rows {
                    // Filter by allowed_ids if provided
                    if let Some(allowed) = allowed_ids {
                        if !allowed.contains(&row.id.to_string()) {
                            continue;
                        }
                    }

                    let distance = distance(query_vector, &row.vector, self.distance_metric);
                    candidates.push((distance, row.clone()));
                }
            }
        }
        let posting_load_duration = posting_load_start.elapsed();

        // 3. Sort and truncate
        let sort_start = std::time::Instant::now();
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(top_k);
        let sort_duration = sort_start.elapsed();

        let total_duration = start.elapsed();
        tracing::debug!(
            "SPFresh internal timing: Total={:?}, Centroids={:?}, PostingLoad={:?}, Sort={:?} | Searched {} docs in {} postings",
            total_duration,
            centroid_duration,
            posting_load_duration,
            sort_duration,
            total_docs_searched,
            search_count
        );

        Ok(candidates)
    }

    pub fn query_ann(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(f32, Row)>> {
        if self.posting_cache.is_empty() {
            return Ok(Vec::new());
        }

        let mut posting_distances: Vec<(u32, f32)> = self
            .centroids
            .iter()
            .map(|(&posting_id, centroid)| {
                let distance = distance(query_vector, centroid, self.distance_metric);
                (posting_id, distance)
            })
            .collect();

        posting_distances
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let search_count = self.config.search_fanout.min(posting_distances.len());
        let mut candidates = Vec::new();

        for &(posting_id, _) in &posting_distances[..search_count] {
            if let Some(posting) = self.posting_cache.get(&posting_id) {
                for row in &posting.rows {
                    let distance = distance(query_vector, &row.vector, self.distance_metric);
                    candidates.push((distance, row.clone()));
                }
            }
        }

        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(top_k);

        Ok(candidates)
    }

    pub fn contains_document(&self, doc_id: &str) -> bool {
        for posting in self.posting_cache.values() {
            if posting.rows.iter().any(|r| r.id.to_string() == doc_id) {
                return true;
            }
        }
        false
    }

    pub fn get_stats(&self) -> SPFreshStats {
        let total_vectors: usize = self.posting_cache.values().map(|p| p.rows.len()).sum();
        let posting_sizes: Vec<usize> = self.posting_cache.values().map(|p| p.rows.len()).collect();

        #[allow(clippy::cast_precision_loss)]
        let avg_posting_size = if posting_sizes.is_empty() {
            0.0
        } else {
            posting_sizes.iter().sum::<usize>() as f32 / posting_sizes.len() as f32
        };

        let min_posting_size = posting_sizes.iter().min().copied().unwrap_or(0);
        let max_posting_size = posting_sizes.iter().max().copied().unwrap_or(0);

        SPFreshStats {
            num_vectors: total_vectors,
            num_postings: self.posting_cache.len(),
            dimensions: self.dimensions,
            avg_posting_size,
            min_posting_size,
            max_posting_size,
            distance_metric: self.distance_metric,
            max_configured_posting_size: self.config.max_posting_size,
            search_fanout: self.config.search_fanout,
        }
    }
}

impl AnnIndex for SPFreshIndex {
    fn build_from_rows(rows: Vec<Row>, distance_metric: DistanceMetric) -> Result<Self> {
        Self::build_from_rows(rows, SPFreshConfig::default(), distance_metric)
    }

    fn insert_row(&mut self, row: Row) -> Result<()> {
        self.insert_row(row)
    }

    fn delete_vector(&mut self, doc_id: &str) -> Result<bool> {
        self.delete_vector(doc_id)
    }

    fn query_ann(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(f32, Row)>> {
        self.query_ann(query_vector, top_k)
    }

    fn contains_document(&self, doc_id: &str) -> bool {
        self.contains_document(doc_id)
    }

    fn get_vector_count(&self) -> usize {
        self.posting_cache.values().map(|p| p.rows.len()).sum()
    }
}

#[derive(Debug)]
pub struct SPFreshStats {
    pub num_vectors: usize,
    pub num_postings: usize,
    pub dimensions: usize,
    pub avg_posting_size: f32,
    pub min_posting_size: usize,
    pub max_posting_size: usize,
    pub distance_metric: DistanceMetric,
    pub max_configured_posting_size: usize,
    pub search_fanout: usize,
}

impl std::fmt::Display for SPFreshStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SPFresh Index Stats:\n\u{2022} Vectors: {}\n\u{2022} Postings: {}\n\u{2022} Dimensions: {}\n\u{2022} Avg posting size: {:.1}\n\u{2022} Posting size range: {} - {}\n\u{2022} Max configured posting size: {}\n\u{2022} Search fanout: {}\n\u{2022} Distance metric: {:?}",
            self.num_vectors,
            self.num_postings,
            self.dimensions,
            self.avg_posting_size,
            self.min_posting_size,
            self.max_posting_size,
            self.max_configured_posting_size,
            self.search_fanout,
            self.distance_metric
        )
    }
}