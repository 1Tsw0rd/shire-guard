use axum::{
    body::Body,
    http::{HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use tracing::{Instrument, info};
use uuid::Uuid;

// main.rs에 .layer(axum::middleware::from_fn(request_id_middleware)); 로 적용함
// Request ID Middleware
// - 모든 api 요청마다 request-id를 저장하여 디버깅 용이하도록 함
// 예시:
//
// Client
//   |
//   | GET /products/1
//   |
//   v
// Middleware
//   request_id = "63e63954-dd7d-487f-9268-e24fcfd6a5e2"
//   |
//   v
// Controller / Service / Repository
//   |
//   v
// Logs
//   request_id: 동일한 UUID로 추적 가능
// 2026-08-06T16:22:18.592404Z  INFO my_app::common::middleware: request completed
//     at src\common\middleware.rs:31
//     in my_app::common::middleware::request with request_id: 63e63954-dd7d-487f-9268-e24fcfd6a5e2, method: GET, uri: /products/1
//
//   2026-08-06T16:22:18.592542Z DEBUG tower_http::trace::on_eos: end of stream, stream_duration: 0 ms
//     at C:\products\ohhoj\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\tower-http-0.7.0\src\trace\on_eos.rs:103
//     in tower_http::trace::make_span::request with method: GET, uri: /products/1, version: HTTP/1.1
//     in my_app::common::middleware::request with request_id: 63e63954-dd7d-487f-9268-e24fcfd6a5e2, method: GET, uri: /products/1
//
// Response Header
//   x-request-id: 63e63954-dd7d-487f-9268-e24fcfd6a5e2
pub async fn request_id_middleware(
    req: Request<Body>, // HTTP Request 객체 (method, uri, header, body 포함)
    next: Next,         // 다음 middleware 또는 handler 실행 (NestJS의 next()와 유사)
) -> Response {
    // Prometheus가 15초마다 스크레이프하는 /metrics 요청인지 미리 판별해둠
    let is_metrics_scrape = req.uri().path() == "/metrics";

    // 요청마다 고유한 Request ID 생성
    let request_id = Uuid::new_v4().to_string();

    // 현재 요청을 표현하는 tracing span 생성하여 request_id, method, uri 정보 공유
    let span = tracing::info_span!(
        "request",
        request_id = %request_id,
        method = %req.method(),
        uri = %req.uri()
    );

    // response header를 수정하기 위해 mut로 선언
    // next.run()으로 다음 middleware 또는 handler 실행
    // instrument()를 사용하여 요청 처리 Future 전체에 span 적용
    // 같은 request_id로 controller/service/repository 로그 추적 가능
    let mut response = next.run(req).instrument(span.clone()).await;


     // /metrics 스크레이프는 완료 로그에서 제외(이렇게 안하면 계속 찍힘)
    if !is_metrics_scrape {
        // response 완료 로그 기록
        // parent: &span 지정하면 해당 request 완료 로그가 request 로그 트리에 포함됨
        info!(
            parent: &span,
            "request completed"
        );
    }

    // response header에 request id 추가
    // 장애 분석 시 클라이언트가 받은 request id로 서버 로그 검색 가능
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());

    // 반환
    response
}
