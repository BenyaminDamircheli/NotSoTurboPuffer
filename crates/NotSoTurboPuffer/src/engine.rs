use core::fmt;
use std::collections::HashMap;

use anyhow::{Result, anyhow};
use rkyv::{Archive, Deserialize, Serialize, access, deserialize, rancor::Failure};

use crate::vectors::DistanceMetric;

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
enum IdType {
    Uuid,
    String,
    U64,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentId {
    bytes: Vec<u8>,
    id_type: IdType,
}

impl From<String> for DocumentId {
    fn from(value: String) -> Self {
        Self {
            bytes: value.into_bytes(),
            id_type: IdType::String,
        }
    }
}

impl From<&str> for DocumentId {
    fn from(value: &str) -> Self {
        Self {
            bytes: value.as_bytes().to_vec(),
            id_type: IdType::String,
        }
    }
}

impl From<u64> for DocumentId {
    fn from(value: u64) -> Self {
        Self {
            bytes: value.to_le_bytes().to_vec(),
            id_type: IdType::U64,
        }
    }
}

impl From<uuid::Uuid> for DocumentId {
    fn from(value: uuid::Uuid) -> Self {
        Self {
            bytes: value.as_bytes().to_vec(),
            id_type: IdType::Uuid,
        }
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.id_type {
            IdType::Uuid => {
                let u = uuid::Uuid::from_slice(&self.bytes).unwrap();
                write!(f, "{}", u)
            }
            IdType::String => {
                let s = String::from_utf8_lossy(&self.bytes);
                write!(f, "{}", s)
            }
            IdType::U64 => {
                let n = u64::from_le_bytes(self.bytes.as_slice().try_into().unwrap());
                write!(f, "{}", n)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
pub struct Namespace {
    pub id: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
    Uuid(uuid::Uuid),
    DateTime(i64),
    Null,
    EmptyArray,
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
    UintArray(Vec<u64>),
    FloatArray(Vec<f64>),
    BoolArray(Vec<bool>),
    UuidArray(Vec<uuid::Uuid>),
    DateTimeArray(Vec<i64>),
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct Row {
    pub id: DocumentId,
    pub vector: Vec<f32>,
    pub attributes: HashMap<String, AttributeValue>,
    pub timestamp: i64,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub enum WalRecord {
    Upsert(Row),
    Delete {
        id: DocumentId,
        timestamp: i64,
    },
    Patch {
        id: DocumentId,
        timestamp: i64,
        attributes: HashMap<String, AttributeValue>,
    },
}

impl Row {
    pub fn new(
        id: DocumentId,
        vector: Vec<f32>,
        attributes: HashMap<String, AttributeValue>,
        timestamp: i64,
    ) -> Self {
        Self {
            id,
            vector,
            attributes,
            timestamp,
        }
    }
}

#[derive(Default, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Schema {
    pub index_attributes: std::collections::HashSet<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Metadata {
    pub approx_row_count: u64,
    pub index: IndexState,
    pub created_at: i64, // Unix timestamp
    pub updated_at: i64,
    pub wal_files: Vec<String>,
    #[serde(default)]
    pub deleted_files: Vec<String>,
    pub unindexed_bytes: u64,
    #[serde(default)]
    pub distance_metric: DistanceMetric,
    #[serde(default)]
    pub schema: Schema,
    #[serde(default)]
    pub vector_dimensions: Option<usize>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct IndexState {
    pub status: IndexStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unindexed_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_index_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inverted_index_file: Option<String>,
    #[serde(default)]
    pub indexed_row_count: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum IndexStatus {
    Updating,
    UpToDate,
}

pub fn replay_wal_with_tombstones(
    wal_chunks: &[Vec<u8>],
) -> Result<(HashMap<DocumentId, Row>, HashMap<DocumentId, i64>)> {
    let mut state: HashMap<DocumentId, (Row, i64)> = HashMap::new();
    let mut deleted_ids: HashMap<DocumentId, i64> = HashMap::new();

    for chunk in wal_chunks {
        let archived_records = access::<rkyv::Archived<Vec<WalRecord>>, Failure>(chunk)
            .map_err(|e| anyhow::anyhow!("Failed to access WAL chunk: {}", e))?;

        for record in archived_records.iter() {
            match record {
                rkyv::Archived::<WalRecord>::Upsert(row) => {
                    let deserialized_row: Row = deserialize::<Row, rkyv::rancor::Error>(row)
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize row: {}", e))?;

                    let id = deserialized_row.id.clone();
                    let ts = deserialized_row.timestamp;

                    let is_deleted = deleted_ids
                        .get(&id)
                        .map(|del_ts| del_ts > &ts)
                        .unwrap_or(false);

                    if !is_deleted {
                        if let Some((_, existing_ts)) = state.get(&id) {
                            if ts >= *existing_ts {
                                state.insert(id, (deserialized_row, ts));
                            }
                        } else {
                            state.insert(id, (deserialized_row, ts));
                        }
                    }
                }
                rkyv::Archived::<WalRecord>::Delete { id, timestamp } => {
                    let id: DocumentId = deserialize::<DocumentId, rkyv::rancor::Error>(id)
                        .map_err(|e| anyhow!("ID deserialization failed: {e}"))?;
                    let ts = timestamp.to_native();

                    if let Some(existing_del_ts) = deleted_ids.get(&id) {
                        if ts >= *existing_del_ts {
                            deleted_ids.insert(id.clone(), ts);
                        }
                    } else {
                        deleted_ids.insert(id.clone(), ts);
                    }

                    if let Some((_, row_ts)) = state.get(&id) {
                        if ts >= *row_ts {
                            state.remove(&id);
                        }
                    }
                }
                rkyv::Archived::<WalRecord>::Patch {
                    id,
                    timestamp,
                    attributes,
                } => {
                    let document_id: DocumentId =
                        deserialize::<DocumentId, rkyv::rancor::Error>(id)
                            .map_err(|e| anyhow!("ID deserialization failed: {e}"))?;
                    let ts = timestamp.to_native();
                    let patch_attrs: HashMap<String, AttributeValue> =
                        deserialize::<HashMap<String, AttributeValue>, rkyv::rancor::Error>(
                            attributes,
                        )
                        .map_err(|e| anyhow!("Attributes deserialization failed: {e}"))?;

                    let is_deleted = deleted_ids
                        .get(&document_id)
                        .map(|&del_ts| del_ts > ts)
                        .unwrap_or(false);

                    if !is_deleted {
                        if let Some((existing_row, existing_ts)) = state.get_mut(&document_id) {
                            if ts > *existing_ts {
                                for (k, v) in patch_attrs {
                                    existing_row.attributes.insert(k, v);
                                }
                                existing_row.timestamp = ts;
                                *existing_ts = ts;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok((
        state.into_iter().map(|(id, (row, _))| (id, row)).collect(),
        deleted_ids,
    ))
}
