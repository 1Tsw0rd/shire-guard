use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::core::enrichment::provider::{EnrichmentProvider, EnrichmentStatus};

// Redis에 저장되고 Evidence/EnrichmentResult<T> 안에 담기는 "우리 시스템 내부용" 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirusTotalData {
    pub malicious: u32,  // 보안 엔진이 악성(malicious)으로 판정한 엔진 수
    pub suspicious: u32, // 보안 엔진이 의심스러움(suspicious)으로 판정한 엔진 수
    pub harmless: u32,   // 보안 엔진이 정상(harmless)으로 판정한 엔진 수
    pub undetected: u32, // 보안 엔진이 탐지하지 못했거나 판정을 내리지 않은 엔진 수
    pub timeout: u32,    // 분석 시간이 초과되어 판정을 완료하지 못한 엔진 수

    #[serde(rename = "confirmed-timeout")]
    pub confirmed_timeout: u32, // VirusTotal이 최종적으로 timeout으로 확정한 엔진 수

    pub failure: u32, // 분석 과정에서 오류가 발생해 정상적으로 판정하지 못한 엔진 수

    #[serde(rename = "type-unsupported")]
    pub type_unsupported: u32, // 해당 파일 형식을 지원하지 않아 분석하지 못한 엔진 수

    // VirusTotal 샌드박스 실행 중 생성된 Sysmon 로그에 Sigma 규칙(행위 기반 탐지)을 적용한 행위 분석 결과의 매치 건수
    // Sigma: 보안 로그에서 공격 또는 의심스러운 행위 패턴을 탐지하기 위한 규칙 형식(비슷한 예로 snort: 네트워크 트래픽 대상으로 탐지 규칙 적용)
    pub sigma_high: u32,
    pub sigma_medium: u32,
    pub sigma_critical: u32,
    pub sigma_low: u32,
}

// VirusTotal API 원본 응답 구조체(역직렬화 전용)
// 실제 JSON: { "data": { "attributes": { "last_analysis_stats": {...} } } }
// 3중 중첩이라 구조체도 3단계로 나눔 (AbuseIPDB의 2단계 중첩과 같은 원리)
#[derive(Debug, Deserialize)]
struct VirusTotalResponse {
    data: VirusTotalResponseData,
}

#[derive(Debug, Deserialize)]
struct VirusTotalResponseData {
    attributes: VirusTotalAttributes,
}

#[derive(Debug, Deserialize)]
struct VirusTotalAttributes {
    last_analysis_stats: VirusTotalStats,

    // 스캔 이력이 없는 파일엔 이 필드 자체가 응답에 없을 수 있어 #[serde(default)]로 처리
    // #[serde(default)]: 해당 필드가 없어도 역직렬화 실패하지 말라는 설정
    #[serde(default)]
    sigma_analysis_stats: SigmaStats,
}

// AV 엔진 판정 결과 원본 (last_analysis_stats)
#[derive(Debug, Deserialize)]
struct VirusTotalStats {
    malicious: u32,
    suspicious: u32,
    harmless: u32,
    undetected: u32,
    timeout: u32,
    #[serde(rename = "confirmed-timeout")]
    confirmed_timeout: u32,
    failure: u32,
    #[serde(rename = "type-unsupported")]
    type_unsupported: u32,
}

// Sigma 룰 매치 집계 원본 (sigma_analysis_stats)
// VirusTotal이 sigma_analysis_summary(룰셋별 상세)를 이미 합산해서 제공하는 필드
#[derive(Debug, Deserialize, Default)]
struct SigmaStats {
    #[serde(default)]
    high: u32,
    #[serde(default)]
    medium: u32,
    #[serde(default)]
    critical: u32,
    #[serde(default)]
    low: u32,
}

pub struct VirusTotalProvider {
    client: Client,
    api_key: String,
}

impl VirusTotalProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
        }
    }
}

#[async_trait]
impl EnrichmentProvider for VirusTotalProvider {
    type Output = VirusTotalData;

    fn name(&self) -> &'static str {
        "virustotal"
    }

    fn ttl_seconds(&self) -> u64 {
        604800 // 7일
    }

    async fn call(&self, ioc: &str) -> Result<Self::Output, EnrichmentStatus> {
        let url = format!("https://www.virustotal.com/api/v3/files/{ioc}");

        let response = self
            .client
            .get(&url)
            .header("x-apikey", &self.api_key)
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
                let parsed: VirusTotalResponse = response
                    .json()
                    .await
                    .map_err(|err| EnrichmentStatus::Error(format!("응답 파싱 실패: {err}")))?;

                let stats = parsed.data.attributes.last_analysis_stats;
                let sigma = parsed.data.attributes.sigma_analysis_stats;

                Ok(VirusTotalData {
                    malicious: stats.malicious,
                    suspicious: stats.suspicious,
                    harmless: stats.harmless,
                    undetected: stats.undetected,
                    timeout: stats.timeout,
                    confirmed_timeout: stats.confirmed_timeout,
                    failure: stats.failure,
                    type_unsupported: stats.type_unsupported,
                    sigma_high: sigma.high,
                    sigma_medium: sigma.medium,
                    sigma_critical: sigma.critical,
                    sigma_low: sigma.low,
                })
            }
            404 => Err(EnrichmentStatus::NotFound), // 이 해시를 VT가 한 번도 스캔한 적 없음
            429 => Err(EnrichmentStatus::RateLimited),
            other => Err(EnrichmentStatus::Error(format!("HTTP {other}"))),
        }
    }
}

// cargo test --lib enrichment::providers::virustotal -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    fn test_api_key() -> String {
        dotenvy::dotenv().ok();
        std::env::var("VIRUSTOTAL_API_KEY")
            .expect("테스트 실행 전 VIRUSTOTAL_API_KEY 환경변수를 설정해야 합니다")
    }

    // 시나리오 1: 알려진 악성 해시(EICAR)는 다수 엔진에서 malicious로 탐지된다
    // 네트워크 + 실제 API 쿼터 사용 — 기본 cargo test에서는 제외
    #[tokio::test]
    #[ignore]
    async fn returns_high_detection_for_known_malware() {
        let provider = VirusTotalProvider::new(test_api_key());
        let result = provider
            .call("275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f")
            .await
            .unwrap();
        println!("VirusTotal result for EICAR: {result:?}"); // VirusTotal result for EICAR: VirusTotalData { malicious: 66, suspicious: 0, harmless: 0, undetected: 2, timeout: 0, confirmed_timeout: 0, failure: 0, type_unsupported: 6, sigma_high: 0, sigma_medium: 0, sigma_critical: 0, sigma_low: 1 }
        assert!(result.malicious > 0);
    }

    // 시나리오 2: 한 번도 스캔된 적 없는 해시는 NotFound를 반환한다
    #[tokio::test]
    #[ignore]
    async fn returns_not_found_for_unknown_hash() {
        let provider = VirusTotalProvider::new(test_api_key());
        let result = provider
            .call("0000000000000000000000000000000000000000000000000000000000000001")
            .await;
        println!("VirusTotal result for unknown hash: {result:?}"); // VirusTotal result for unknown hash: Err(NotFound)
        assert!(matches!(result, Err(EnrichmentStatus::NotFound)));
    }
}
