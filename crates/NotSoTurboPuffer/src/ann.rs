use anyhow::Result;

use crate::engine::Row;
use crate::vectors::DistanceMetric;

pub trait AnnIndex {
    fn build_from_rows(rows: Vec<Row>, distance_metric: DistanceMetric) -> Result<Self>
    where
        Self: Sized;

    fn insert_row(&mut self, row: Row) -> Result<()>;

    fn delete_vector(&mut self, doc_id: &str) -> Result<bool>;

    fn query_ann(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(f32, Row)>>;

    fn contains_document(&self, doc_id: &str) -> bool;

    fn get_vector_count(&self) -> usize;
}

pub mod spfresh;
