// Enrichment Provider들이 지켜야 할 공통 규칙을 정의하는 파일

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "detail")]
pub enum EnrichmentStatus {
    Success,
    NotFound,
    RateLimited,
    Timeout,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentResult<T> {
    pub status: EnrichmentStatus,
    pub data: Option<T>,

    #[serde(skip)] // 직렬화 또는 역직렬화 시 필수 필드 아님
    pub cached: bool,
}

// 캐시/락/서킷브레이커는 공용 service.rs가 처리하고,
// 이 trait은 provider마다 다른 "실제 외부 API 호출"만 책임짐
#[async_trait]
pub trait EnrichmentProvider: Send + Sync {
    // 각 Provider가 반환하는 결과 타입은 Provider마다 다르지만, 반드시 이 조건을 만족해야 함
    // Serialize/DeserializeOwned: Redis 캐시 저장 및 조회를 위해 직렬화/역직렬화 가능해야 함
    // Clone: 결과를 복제할 수 있어야 함
    // Send + Sync: Tokio 비동기 환경에서 여러 task가 안전하게 공유할 수 있어야 함
    type Output: Serialize + DeserializeOwned + Clone + Send + Sync;

    // Redis 키(enrichment:{name}:{ioc})와 로그에 쓰이는 식별자
    // 예: name="abuseipdb", ioc="185.220.101.45" → enrichment:abuseipdb:185.220.101.45
    //     name="dns", ioc="cdn-update-service.net" → enrichment:dns:cdn-update-service.net
    //     name="virustotal", ioc="e3b0c442..." → enrichment:virustotal:e3b0c442...
    fn name(&self) -> &'static str;

    fn ttl_seconds(&self) -> u64;

    // IOC는 Indicator of Compromise, 침해 지표(침해가 발생했음을 나타내는 흔적/단서)를 의미
    async fn call(&self, ioc: &str) -> Result<Self::Output, EnrichmentStatus>;
}
