use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State; // Axum 익스트랙터: .with_state(state)로 넘긴 앱 전역 상태를 핸들러 인자로 꺼내 쓸 수 있게 해줌
use axum::routing::get;
use sqlx::postgres::PgPoolOptions;

use backend::common::clients::redis::RedisClient;
use backend::common::error::AppError;
use backend::common::fallback::not_found;
use backend::common::metrics::Metrics;
use backend::common::middleware::request_id_middleware;
use backend::config::AppConfig;
use backend::core::consumer::kafka;
use backend::core::enrichment::providers::abuseipdb::AbuseIpDbProvider;
use backend::core::enrichment::providers::dns::DnsProvider;
use backend::core::enrichment::providers::virustotal::VirusTotalProvider;
use backend::core::enrichment::service::EnrichmentService;
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

    // Redis/Dragonfly 연결 (Enrichment 캐시/분산 락 용도)
    tracing::info!(
        "[CACHE CONNECTING] {:?} 서버에 연결 시도합니다...",
        config.cache.backend
    );
    let redis_raw =
        redis::Client::open(config.cache.url.as_str()).expect("Redis/Dragonfly Client 생성 실패");
    let redis = RedisClient::new(redis_raw)
        .await
        .expect("Redis/Dragonfly 연결 실패");
    tracing::info!("[CACHE CONNECTED] {:?} 연결 성공", config.cache.backend);

    // Redis 연결 직후, Metrics용으로 clone
    let redis_for_metrics = redis.clone();

    // Enrichment Provider 3개 생성
    let dns_provider = DnsProvider::new(&config.dns_resolver_host, config.dns_resolver_port)
        .expect("DNS Provider 초기화 실패");
    let abuseipdb_provider = AbuseIpDbProvider::new(config.abuseipdb_api_key.clone());
    let virustotal_provider = VirusTotalProvider::new(config.virustotal_api_key.clone());

    let enrichment = Arc::new(EnrichmentService::new(
        redis,
        dns_provider,
        abuseipdb_provider,
        virustotal_provider,
    ));

    // Prometheus metrics 초기화 (config.broker 기준으로 active_broker 게이지 세팅)
    let metrics =
        Arc::new(Metrics::new(&config.broker, &config.cache).expect("metrics 초기화 실패"));

    // Kafka/RedPanda Consumer를 백그라운드 태스크로 실행 (HTTP 서버와 별개로 계속 동작)
    let consumer =
        kafka::build_consumer(&config.broker, metrics.clone()).expect("Kafka Consumer 생성 실패");
    let topic = config.broker.topic.clone();
    tokio::spawn(kafka::run(
        consumer,
        topic,
        metrics.clone(),
        enrichment.clone(),
    ));

    // Redis/Dragonfly Key 개수 반환 매트릭(15초 주기)
    {
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
            loop {
                interval.tick().await;
                match redis_for_metrics.dbsize().await {
                    Ok(count) => metrics.cache_key_count.set(count),
                    Err(err) => tracing::warn!(error = ?err, "캐시 키 개수 조회 실패"),
                }
            }
        });
    }

    // Axum HTTP 서버 구성 및 실행
    let state = Arc::new(AppState {
        postgres,
        metrics,
        enrichment,
    });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_handler))
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

async fn metrics_handler(State(state): State<Arc<AppState>>) -> Result<String, AppError> {
    state.metrics.encode()
}
