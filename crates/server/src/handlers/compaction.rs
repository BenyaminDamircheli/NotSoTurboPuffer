use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
};
use not_so_turbo_puffer::compactor;
use serde::{Deserialize, Serialize};

use crate::models::{error::ApiResult, namespace::validated_namespace};

#[derive(Deserialize, Default)]
pub struct CompactionParams {
    #[serde(default)]
    pub force: bool,
}

#[derive(Serialize)]
pub struct CompactionQueued {
    pub namespace: String,
    pub forced: bool,
}

/// Queues a compaction for the namespace. Fire-and-forget: 202 means queued,
/// not completed. A dropped request is re-detected by the sweeper.
pub async fn trigger(
    Path(namespace): Path<String>,
    Query(params): Query<CompactionParams>,
) -> ApiResult<(StatusCode, Json<CompactionQueued>)> {
    let namespace = validated_namespace(&namespace)?;

    if params.force {
        compactor::trigger_forced_compaction(&namespace);
    } else {
        compactor::trigger_compaction(&namespace);
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CompactionQueued {
            namespace,
            forced: params.force,
        }),
    ))
}
