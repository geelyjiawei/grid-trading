pub mod api;
pub mod domain;

use axum::Router;
use tower_http::trace::TraceLayer;

pub fn app() -> Router {
    Router::new()
        .merge(api::router())
        .layer(TraceLayer::new_for_http())
}
