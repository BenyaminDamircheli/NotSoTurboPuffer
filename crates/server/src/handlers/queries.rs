use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use not_so_turbo_puffer::{
    config,
    engine::WalRecord,
    not_so_turbo_puffer::Client,
};

use crate::models::{
    error::{ApiError, ApiResult},
    namespace::validated_namespace,
    query::{
        DeleteRequest, DeleteResponse, PatchRequest, PatchResponse, QueryRequest, QueryResponse,
        UpsertRequest, UpsertResponse,
    },
};

pub async fn query(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
    Json(request): Json<QueryRequest>,
) -> ApiResult<Json<QueryResponse>> {
    let namespace = validated_namespace(&namespace)?;

    if request.vector.is_empty() {
        return Err(ApiError::bad_request("query vector cannot be empty"));
    }
    let max_top_k = config::get_config().await?.limits.max_top_k;
    if request.top_k == 0 || request.top_k > max_top_k {
        return Err(ApiError::bad_request(format!(
            "top_k must be between 1 and {max_top_k}"
        )));
    }

    let rows = client
        .query(
            &namespace,
            &request.vector,
            request.top_k,
            request.filters.as_ref(),
        )
        .await?;

    Ok(Json(QueryResponse::from_rows(rows)))
}

pub async fn upsert(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
    Json(request): Json<UpsertRequest>,
) -> ApiResult<Json<UpsertResponse>> {
    let namespace = validated_namespace(&namespace)?;

    if request.rows.is_empty() {
        return Err(ApiError::bad_request("rows cannot be empty"));
    }

    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut rows = Vec::with_capacity(request.rows.len());
    for row in request.rows {
        rows.push(row.into_row(timestamp).map_err(ApiError::bad_request)?);
    }

    let upserted = client
        .upsert_with_metric(&namespace, request.distance_metric, rows)
        .await?;

    Ok(Json(UpsertResponse { upserted }))
}

pub async fn delete(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
    Json(request): Json<DeleteRequest>,
) -> ApiResult<Json<DeleteResponse>> {
    let namespace = validated_namespace(&namespace)?;

    if request.ids.is_empty() {
        return Err(ApiError::bad_request("ids cannot be empty"));
    }

    let timestamp = chrono::Utc::now().timestamp_millis();
    let records: Vec<WalRecord> = request
        .ids
        .into_iter()
        .map(|id| WalRecord::Delete {
            id: id.into(),
            timestamp,
        })
        .collect();

    let deleted = client.write(&namespace, None, records).await?;
    Ok(Json(DeleteResponse { deleted }))
}

pub async fn patch(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
    Json(request): Json<PatchRequest>,
) -> ApiResult<Json<PatchResponse>> {
    let namespace = validated_namespace(&namespace)?;

    if request.rows.is_empty() {
        return Err(ApiError::bad_request("rows cannot be empty"));
    }

    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut records = Vec::with_capacity(request.rows.len());
    for row in request.rows {
        records.push(row.into_record(timestamp).map_err(ApiError::bad_request)?);
    }

    let patched = client.write(&namespace, None, records).await?;
    Ok(Json(PatchResponse { patched }))
}
