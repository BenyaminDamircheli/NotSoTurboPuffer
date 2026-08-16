use serde::Serialize;

use crate::models::error::ApiError;

#[derive(Serialize)]
pub struct NamespaceList {
    pub namespaces: Vec<String>,
    pub count: usize,
}

/// Validates a namespace from the request path. Rejections map to 400.
pub fn validated_namespace(raw: &str) -> Result<String, ApiError> {
    not_so_turbo_puffer::s3client::sanitize_namespace(raw)
        .map_err(|e| ApiError::bad_request(e.to_string()))
}
