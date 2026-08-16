use not_so_turbo_puffer::{
    engine::IndexStatus, not_so_turbo_puffer::Metadata, vectors::DistanceMetric,
};
use serde::Serialize;

/// Client-facing view of namespace metadata. Internal bookkeeping (WAL file
/// keys, soft-deleted files, index file keys) stays out of the API.
#[derive(Serialize)]
pub struct MetadataResponse {
    pub namespace: String,
    pub approx_row_count: u64,
    pub vector_dimensions: Option<usize>,
    pub distance_metric: DistanceMetric,
    pub index_status: IndexStatus,
    pub indexed_row_count: u64,
    pub pending_wal_files: usize,
    pub unindexed_bytes: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

impl MetadataResponse {
    pub fn new(namespace: String, metadata: &Metadata) -> Self {
        Self {
            namespace,
            approx_row_count: metadata.approx_row_count,
            vector_dimensions: metadata.vector_dimensions,
            distance_metric: metadata.distance_metric,
            index_status: metadata.index.status.clone(),
            indexed_row_count: metadata.index.indexed_row_count,
            pending_wal_files: metadata.wal_files.len(),
            unindexed_bytes: metadata.unindexed_bytes,
            created_at: metadata.created_at,
            updated_at: metadata.updated_at,
        }
    }
}
