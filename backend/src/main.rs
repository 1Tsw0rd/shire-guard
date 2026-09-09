use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use sqlx::postgres::PgPoolOptions;

use backend::common::fallback::not_found;
use backend::common::middleware::request_id_middleware;
use backend::config::AppConfig;
use backend::core::consumer::kafka;
use backend::state::AppState;

#[tokio::main]
async fn main() {
    // docker/.env: Docker 및 외부 시스템 접속 정보
    // Kafka/RedPanda, OpenSearch, ClickHouse 등의 인프라 설정을 로드
    dotenvy::from_filename("../docker/.env").ok();

    // backend/.env: Rust 애플리케이션 전용 설정
    // DATABASE_URL, RUST_LOG 및 이후 Rust에서만 사용하는 Secret을 로드
    dotenvy::dotenv().ok();

    let config = AppConfig::from_env().expect("설정 초기화 실패");

    tracing_subscriber::fmt()
        .pretty() // 가독성 있게 해줌
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()) // 주입된 RUST_LOG 환경변수 값을 실제로 읽어서 모듈별 로그 레벨을 적용, 설정하지 않으면 tracing 기본 필터(ERROR 레벨)만 출력
        .with_file(true) // 로그에 파일명 출력 활성화
        .with_line_number(true) // 로그에 에러 발생 위치 출력 활성화
        .init();

    // PostgreSQL 연결
    tracing::info!("[DB CONNECTING] PostgreSQL 서버에 연결 시도합니다...");
    let postgres = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .expect("PostgreSQL 연결 실패");
    tracing::info!("[DB CONNECTED] PostgreSQL 서버 연결 성공");

    // Kafka/RedPanda Consumer를 백그라운드 태스크로 실행 (HTTP 서버와 별개로 계속 동작)
    let consumer = kafka::build_consumer(&config.broker).expect("Kafka Consumer 생성 실패");
    let topic = config.broker.topic.clone();
    tokio::spawn(kafka::run(consumer, topic));

    // Axum HTTP 서버 구성 및 실행
    let state = Arc::new(AppState { postgres });

    let app = Router::new()
        .route("/health", get(health_check))
        .fallback(not_found)
        .layer(axum::middleware::from_fn(request_id_middleware))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("[SERVER RUNNING] http://{}", addr);

    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> &'static str {
    "ok"
}