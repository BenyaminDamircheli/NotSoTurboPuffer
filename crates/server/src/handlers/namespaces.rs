use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use not_so_turbo_puffer::not_so_turbo_puffer::{self as puffer, Client};

use crate::models::{
    error::ApiResult, metadata::MetadataResponse, namespace::NamespaceList,
    namespace::validated_namespace,
};

pub async fn list(State(_client): State<Arc<Client>>) -> ApiResult<Json<NamespaceList>> {
    let namespaces = puffer::namespaces().await?;
    Ok(Json(NamespaceList {
        count: namespaces.len(),
        namespaces: namespaces.into_iter().map(|n| n.id).collect(),
    }))
}

/// Creates the namespace when it does not exist yet; fetching the metadata
/// provisions the bucket and the initial metadata file.
pub async fn create(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
) -> ApiResult<(StatusCode, Json<MetadataResponse>)> {
    let namespace = validated_namespace(&namespace)?;
    let (metadata, _etag) = client.get_metadata(&namespace).await?;
    Ok((
        StatusCode::CREATED,
        Json(MetadataResponse::new(namespace, &metadata)),
    ))
}

pub async fn metadata(
    State(client): State<Arc<Client>>,
    Path(namespace): Path<String>,
) -> ApiResult<Json<MetadataResponse>> {
    let namespace = validated_namespace(&namespace)?;
    let (metadata, _etag) = client.get_metadata(&namespace).await?;
    Ok(Json(MetadataResponse::new(namespace, &metadata)))
}
