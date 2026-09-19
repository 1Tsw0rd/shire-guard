use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::core::enrichment::provider::{EnrichmentProvider, EnrichmentStatus};

// Redis에 저장되고 Evidence/EnrichmentResult<T> 안에 담기는 "우리 시스템 내부용" 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbuseIpDbData {
    pub abuse_confidence_score: u8,
    pub total_reports: u32,
}

// AbuseIPDB API 원본 응답 구조체(역직렬화 전용)
#[derive(Debug, Deserialize)]
struct AbuseIpDbResponse {
    data: AbuseIpDbResponseData,
}

// AbuseIPDB API가 보내주는 JSON 그대로를 받는 "입력 전용" 구조체
#[derive(Debug, Deserialize)]
struct AbuseIpDbResponseData {
    #[serde(rename = "abuseConfidenceScore")]
    abuse_confidence_score: u8,
    #[serde(rename = "totalReports")]
    total_reports: u32,
}

// AbuseIpDbProvider 인스턴스를 생성하기 위한 구조체와 생성자
pub struct AbuseIpDbProvider {
    client: Client,
    api_key: String,
}

impl AbuseIpDbProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
        }
    }
}

#[async_trait]
impl EnrichmentProvider for AbuseIpDbProvider {
    type Output = AbuseIpDbData;

    fn name(&self) -> &'static str {
        "abuseipdb"
    }

    fn ttl_seconds(&self) -> u64 {
        86400 // 24시간
    }

    async fn call(&self, ioc: &str) -> Result<Self::Output, EnrichmentStatus> {
        let response = self
            .client
            .get("https://api.abuseipdb.com/api/v2/check")
            .query(&[("ipAddress", ioc), ("maxAgeInDays", "90")]) // 최근 90일 이내에 발생한 AbuseIPDB 신고(report)만 조회
            .header("Key", &self.api_key)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() {
                    EnrichmentStatus::Timeout
                } else {
                    EnrichmentStatus::Error(err.to_string())
                }
            })?;

        match response.status().as_u16() {
            200 => {
                let parsed: AbuseIpDbResponse = response
                    .json()
                    .await
                    .map_err(|err| EnrichmentStatus::Error(format!("응답 파싱 실패: {err}")))?;

                Ok(AbuseIpDbData {
                    abuse_confidence_score: parsed.data.abuse_confidence_score,
                    total_reports: parsed.data.total_reports,
                })
            }
            429 => Err(EnrichmentStatus::RateLimited),
            other => Err(EnrichmentStatus::Error(format!("HTTP {other}"))),
        }
    }
}

// cargo test --lib enrichment::providers::abuseipdb -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    fn test_api_key() -> String {
        dotenvy::dotenv().ok(); // backend/.env 파일을 여기서 직접 읽어옴
        std::env::var("ABUSEIPDB_API_KEY")
            .expect("테스트 실행 전 ABUSEIPDB_API_KEY 환경변수를 설정해야 합니다")
    }

    // 시나리오 1: 평판이 깨끗한 IP는 낮은 score로 응답한다
    // 네트워크 + 실제 API 쿼터 사용 — 기본 cargo test에서는 제외
    #[tokio::test]
    #[ignore]
    async fn returns_low_score_for_clean_ip() {
        let provider = AbuseIpDbProvider::new(test_api_key());
        let result = provider.call("118.25.6.39").await.unwrap(); // (참고사항) 개발 당시 해당 IP abuseipdb에서 조회한 결과 score 0 이었음
        println!("AbuseIPDB result for clean IP: {result:?}");
        assert_eq!(result.abuse_confidence_score, 0);
    }

    // 시나리오 2: 평판이 나쁜 IP는 높은 score로 응답한다
    #[tokio::test]
    #[ignore]
    async fn returns_high_score_for_malicious_ip() {
        let provider = AbuseIpDbProvider::new(test_api_key());
        let result = provider.call("144.48.243.18").await.unwrap(); // (참고사항) 개발 당시 해당 IP abuseipdb에서 조회한 결과 score 100 이었음
        println!("AbuseIPDB result for malicious IP: {result:?}");
        assert!(result.abuse_confidence_score >= 80);
    }
}
