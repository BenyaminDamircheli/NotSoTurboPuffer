pub mod compaction;
pub mod namespaces;
pub mod queries;
pub mod schemas;

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post, put},
};
use not_so_turbo_puffer::not_so_turbo_puffer::Client;

pub fn router(client: Arc<Client>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/namespaces", get(namespaces::list))
        .route(
            "/v1/namespaces/{namespace}",
            put(namespaces::create).get(namespaces::metadata),
        )
        .route("/v1/namespaces/{namespace}/upsert", post(queries::upsert))
        .route("/v1/namespaces/{namespace}/delete", post(queries::delete))
        .route("/v1/namespaces/{namespace}/patch", post(queries::patch))
        .route("/v1/namespaces/{namespace}/query", post(queries::query))
        .route(
            "/v1/namespaces/{namespace}/schema",
            get(schemas::get_schema).put(schemas::update_schema),
        )
        .route(
            "/v1/namespaces/{namespace}/compact",
            post(compaction::trigger),
        )
        .with_state(client)
}

async fn health() -> &'static str {
    "ok"
}
