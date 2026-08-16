//! HTTP server for NotSoTurboPuffer.
//!
//! Thin layer over the `not_so_turbo_puffer::Client`: handlers translate
//! HTTP to client calls, models translate engine types to API types.
//! No storage or index logic lives here.

mod handlers;
mod models;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::{extract::DefaultBodyLimit, http::StatusCode};
use not_so_turbo_puffer::{compactor::Compactor, config, not_so_turbo_puffer::Client};
use tower_http::timeout::TimeoutLayer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = config::get_config().await?;
    let client = Arc::new(Client::new().await?);
    Compactor::new().start().await?;

    let app = handlers::router(client)
        .layer(DefaultBodyLimit::max(config.server.max_request_body_size))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(config.server.request_timeout_ms),
        ));

    let addr = format!("0.0.0.0:{}", config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Server listening on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
