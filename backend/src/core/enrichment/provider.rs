/*
Enrichment Provider들이 지켜야 할 공통 규칙을 정의하는 파일

이 파일에는 다음 3가지가 정의되어 있음
- EnrichmentStatus: 외부 API 조회 결과의 상태(Success, NotFound, Timeout 등)
- EnrichmentResult<T>: 조회 상태와 실제 결과 데이터(data)를 함께 담는 공통 결과 타입
- EnrichmentProvider: AbuseIPDB, DNS, VirusTotal 같은 각 Provider가 따라야 하는 공통 규칙

각 Provider의 실제 외부 API 호출 로직은 개별 파일에서 구현하고,
이 파일에서는 Provider들이 공통으로 사용할 상태, 결과 타입, trait만 정의함
*/

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "detail")]
// 직렬화할 때 enum의 variant 이름을 status 필드에,
// variant가 값을 가지고 있다면 그 값을 detail 필드에 넣음
pub enum EnrichmentStatus {
    Success,
    NotFound,
    RateLimited,
    Timeout,
    Error(String),
    CacheUnavailable,
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
    // Serialize/DeserializeOwned: Redis/Dragonfly 캐시 저장 및 조회를 위해 직렬화/역직렬화 가능해야 함
    // Clone: 결과를 복제할 수 있어야 함
    // Send + Sync: Tokio 비동기 환경에서 여러 task가 안전하게 공유할 수 있어야 함
    type Output: Serialize + DeserializeOwned + Clone + Send + Sync;

    // Redis 키(enrichment:{name}:{ioc})와 로그에 쓰이는 식별자
    // 예: name="abuseipdb", ioc="185.220.101.45" → enrichment:abuseipdb:185.220.101.45
    //     name="dns", ioc="cdn-update-service.net" → enrichment:dns:cdn-update-service.net
    //     name="virustotal", ioc="e3b0c442..." → enrichment:virustotal:e3b0c442...
    fn name(&self) -> &'static str;

    fn ttl_seconds(&self) -> u64;

    // IOC(Indicator of Compromise)는 침해 지표(침해가 발생했음을 나타내는 흔적/단서)를 의미
    async fn call(&self, ioc: &str) -> Result<Self::Output, EnrichmentStatus>;
}

// cargo test --lib enrichment::provider
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: 모든 상태가 enrichment_*.status.status 경로에 문자열로 직렬화된다
    #[test]
    fn status_serializes_under_status_status_path() {
        let cases = [
            (EnrichmentStatus::Success, "Success"),
            (EnrichmentStatus::NotFound, "NotFound"),
            (EnrichmentStatus::RateLimited, "RateLimited"),
            (EnrichmentStatus::Timeout, "Timeout"),
            (EnrichmentStatus::Error("HTTP 500".into()), "Error"),
            (EnrichmentStatus::CacheUnavailable, "CacheUnavailable"),
        ];

        for (status, expected) in cases {
            let result: EnrichmentResult<()> = EnrichmentResult {
                status,
                data: None,
                cached: false,
            };
            let value = serde_json::to_value(&result).unwrap();
            assert_eq!(value["status"]["status"], expected);
        }
    }
}
