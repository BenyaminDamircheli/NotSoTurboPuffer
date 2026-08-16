use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use not_so_turbo_puffer::not_so_turbo_puffer::{Client, Schema};

use crate::models::{error::ApiResult, namespace::validated_namespace};

pub async fn get_schema(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
) -> ApiResult<Json<Schema>> {
    let namespace = validated_namespace(&namespace)?;
    let (metadata, _etag) = client.get_metadata(&namespace).await?;
    Ok(Json(metadata.schema))
}

/// Replaces the namespace schema. Attributes in the schema are indexed by the
/// next compaction cycle.
pub async fn update_schema(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
    Json(schema): Json<Schema>,
) -> ApiResult<Json<Schema>> {
    let namespace = validated_namespace(&namespace)?;
    let metadata = client.update_schema(&namespace, schema).await?;
    Ok(Json(metadata.schema))
}
