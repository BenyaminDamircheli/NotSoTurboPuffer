use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use not_so_turbo_puffer::not_so_turbo_puffer::{DimensionMismatch, DistanceMetricConflict};
use serde::Serialize;

pub type ApiResult<T> = Result<T, ApiError>;

/// An HTTP-mappable API error: a status code plus a client-safe message.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// Maps engine errors onto HTTP statuses. Typed engine errors get specific
/// codes; everything else is a 500.
impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        if let Some(mismatch) = err.downcast_ref::<DimensionMismatch>() {
            return Self::bad_request(mismatch.to_string());
        }
        if let Some(conflict) = err.downcast_ref::<DistanceMetricConflict>() {
            return Self::conflict(conflict.to_string());
        }

        tracing::error!("Internal error: {:?}", err);
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}
