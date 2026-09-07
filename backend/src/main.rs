use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use sqlx::postgres::PgPoolOptions;

use backend::common::fallback::not_found;
use backend::common::middleware::request_id_middleware;
use backend::state::AppState;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,tower_http=debug".into()),
        )
        .init();

    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL 환경변수가 설정되어 있지 않습니다");

    let postgres = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("PostgreSQL 연결 실패");

    let state = Arc::new(AppState { postgres });

    let app = Router::new()
        .route("/health", get(health_check))
        .fallback(not_found)
        .layer(axum::middleware::from_fn(request_id_middleware))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    tracing::info!("listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> &'static str {
    "ok"
}
