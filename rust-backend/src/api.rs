use axum::{Json, Router, routing::get};
use serde::Serialize;

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    runtime: &'static str,
    trading_enabled: bool,
    contract_version: u8,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        runtime: "rust",
        trading_enabled: false,
        contract_version: 1,
    })
}

pub fn router() -> Router {
    Router::new().route("/healthz", get(health))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn migration_server_is_explicitly_non_trading() {
        let response = super::super::app()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["runtime"], "rust");
        assert_eq!(payload["trading_enabled"], false);
        assert_eq!(payload["contract_version"], 1);
    }

    #[tokio::test]
    async fn mutating_routes_are_absent_until_the_engine_is_compatible() {
        let response = super::super::app()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/grid/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
    }
}
