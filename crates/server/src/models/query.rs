//! Data-plane request/response models and the JSON <-> engine conversions.

use std::collections::HashMap;

use not_so_turbo_puffer::{
    engine::{AttributeValue, WalRecord},
    not_so_turbo_puffer::{DocumentId, Row},
    vectors::DistanceMetric,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub filters: Option<HashMap<String, String>>,
}

fn default_top_k() -> usize {
    10
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub results: Vec<DocumentResponse>,
    pub count: usize,
}

impl QueryResponse {
    pub fn from_rows(rows: Vec<Row>) -> Self {
        let results: Vec<DocumentResponse> = rows.into_iter().map(DocumentResponse::from).collect();
        Self {
            count: results.len(),
            results,
        }
    }
}

#[derive(Serialize)]
pub struct DocumentResponse {
    pub id: String,
    pub vector: Vec<f32>,
    pub attributes: HashMap<String, Value>,
    pub timestamp: i64,
}

impl From<Row> for DocumentResponse {
    fn from(row: Row) -> Self {
        Self {
            id: row.id.to_string(),
            vector: row.vector,
            attributes: row
                .attributes
                .iter()
                .map(|(key, value)| (key.clone(), attribute_to_json(value)))
                .collect(),
            timestamp: row.timestamp,
        }
    }
}

// ---------------------------------------------------------------------------
// Upsert
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct UpsertRequest {
    pub rows: Vec<UpsertRow>,
    #[serde(default)]
    pub distance_metric: Option<DistanceMetric>,
}

#[derive(Deserialize)]
pub struct UpsertRow {
    pub id: DocumentIdInput,
    pub vector: Vec<f32>,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

impl UpsertRow {
    /// Converts the API row into an engine row. Fails with a client-safe
    /// message when an attribute value has an unsupported shape.
    pub fn into_row(self, timestamp: i64) -> Result<Row, String> {
        let attributes = convert_attributes(self.attributes)?;
        Ok(Row::new(self.id.into(), self.vector, attributes, timestamp))
    }
}

/// Document ids arrive as JSON strings or unsigned integers.
#[derive(Deserialize)]
#[serde(untagged)]
pub enum DocumentIdInput {
    String(String),
    Number(u64),
}

impl From<DocumentIdInput> for DocumentId {
    fn from(input: DocumentIdInput) -> Self {
        match input {
            DocumentIdInput::String(s) => s.into(),
            DocumentIdInput::Number(n) => n.into(),
        }
    }
}

#[derive(Serialize)]
pub struct UpsertResponse {
    pub upserted: usize,
}

// ---------------------------------------------------------------------------
// Delete and patch
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct DeleteRequest {
    pub ids: Vec<DocumentIdInput>,
}

#[derive(Serialize)]
pub struct DeleteResponse {
    /// Number of delete tombstones written (not a count of matched documents).
    pub deleted: usize,
}

#[derive(Deserialize)]
pub struct PatchRequest {
    pub rows: Vec<PatchRow>,
}

#[derive(Deserialize)]
pub struct PatchRow {
    pub id: DocumentIdInput,
    pub attributes: HashMap<String, Value>,
}

impl PatchRow {
    pub fn into_record(self, timestamp: i64) -> Result<WalRecord, String> {
        let attributes = convert_attributes(self.attributes)?;
        Ok(WalRecord::Patch {
            id: self.id.into(),
            timestamp,
            attributes,
        })
    }
}

#[derive(Serialize)]
pub struct PatchResponse {
    /// Number of patch records written (not a count of matched documents).
    pub patched: usize,
}

// ---------------------------------------------------------------------------
// Attribute conversions
// ---------------------------------------------------------------------------

/// Converts an API attribute map into engine attributes. Fails with a
/// client-safe message naming the first invalid attribute.
fn convert_attributes(
    attributes: HashMap<String, Value>,
) -> Result<HashMap<String, AttributeValue>, String> {
    let mut converted = HashMap::with_capacity(attributes.len());
    for (key, value) in attributes {
        let attribute = json_to_attribute(value)
            .map_err(|e| format!("invalid value for attribute '{key}': {e}"))?;
        converted.insert(key, attribute);
    }
    Ok(converted)
}

pub fn attribute_to_json(value: &AttributeValue) -> Value {
    match value {
        AttributeValue::String(s) => Value::from(s.clone()),
        AttributeValue::Int(i) => Value::from(*i),
        AttributeValue::Uint(u) => Value::from(*u),
        AttributeValue::Float(f) => Value::from(*f),
        AttributeValue::Bool(b) => Value::from(*b),
        AttributeValue::Uuid(u) => Value::from(u.to_string()),
        AttributeValue::DateTime(t) => Value::from(*t),
        AttributeValue::Null => Value::Null,
        AttributeValue::EmptyArray => Value::Array(Vec::new()),
        AttributeValue::StringArray(v) => Value::from(v.clone()),
        AttributeValue::IntArray(v) => Value::from(v.clone()),
        AttributeValue::UintArray(v) => Value::from(v.clone()),
        AttributeValue::FloatArray(v) => Value::from(v.clone()),
        AttributeValue::BoolArray(v) => Value::from(v.clone()),
        AttributeValue::UuidArray(v) => {
            Value::Array(v.iter().map(|u| Value::from(u.to_string())).collect())
        }
        AttributeValue::DateTimeArray(v) => Value::from(v.clone()),
    }
}

pub fn json_to_attribute(value: Value) -> Result<AttributeValue, String> {
    match value {
        Value::String(s) => Ok(AttributeValue::String(s)),
        Value::Bool(b) => Ok(AttributeValue::Bool(b)),
        Value::Null => Ok(AttributeValue::Null),
        Value::Number(n) => Ok(number_to_attribute(&n)),
        Value::Array(items) => array_to_attribute(items),
        Value::Object(_) => Err("nested objects are not supported".to_string()),
    }
}

fn number_to_attribute(n: &serde_json::Number) -> AttributeValue {
    if let Some(i) = n.as_i64() {
        AttributeValue::Int(i)
    } else if let Some(u) = n.as_u64() {
        AttributeValue::Uint(u)
    } else {
        // A JSON number is always i64, u64, or f64.
        AttributeValue::Float(n.as_f64().unwrap_or_default())
    }
}

/// Arrays must be homogeneous: all strings, all booleans, or all numbers.
fn array_to_attribute(items: Vec<Value>) -> Result<AttributeValue, String> {
    let Some(first) = items.first() else {
        return Ok(AttributeValue::EmptyArray);
    };

    match first {
        Value::String(_) => items
            .into_iter()
            .map(|v| match v {
                Value::String(s) => Ok(s),
                _ => Err("mixed-type arrays are not supported".to_string()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(AttributeValue::StringArray),
        Value::Bool(_) => items
            .into_iter()
            .map(|v| match v {
                Value::Bool(b) => Ok(b),
                _ => Err("mixed-type arrays are not supported".to_string()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(AttributeValue::BoolArray),
        Value::Number(_) => {
            let numbers: Vec<serde_json::Number> = items
                .into_iter()
                .map(|v| match v {
                    Value::Number(n) => Ok(n),
                    _ => Err("mixed-type arrays are not supported".to_string()),
                })
                .collect::<Result<_, _>>()?;

            if numbers.iter().all(serde_json::Number::is_i64) {
                Ok(AttributeValue::IntArray(
                    numbers.iter().filter_map(serde_json::Number::as_i64).collect(),
                ))
            } else {
                Ok(AttributeValue::FloatArray(
                    numbers.iter().filter_map(serde_json::Number::as_f64).collect(),
                ))
            }
        }
        _ => Err("array elements must be strings, booleans, or numbers".to_string()),
    }
}
