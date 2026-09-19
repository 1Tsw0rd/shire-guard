/*
Event
    ↓
IOC Field Extraction (event.rs의 enrichment_iocs())
    ↓
┌───────────────────────┐
│ src_ip 존재?          │ → AbuseIPDB
│ dst_domain 존재?      │ → DNS
│ file_sha256 존재?     │ → VirusTotal
└───────────────────────┘
    ↓
(tokio::join!로 위 3갈래 동시 실행 — 없는 필드는 즉시 None 반환, 실제 호출 없음)
    ↓
Enriched Evidence
    ↓
Playbook

서킷 브레이커 장치 작동 설명은 circuit_breaker.rs 참고
*/

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned; // Redis JSON을 Rust 타입으로 역직렬화하되, 원본 데이터 참조 없이 독립된 소유값(owned value) 만들어냄
use tokio::time::sleep; // 현재 async task를 일정 시간만큼 비동기 대기시키고, 대기 중에는 Tokio executor가 다른 task를 실행할 수 있음
use uuid::Uuid;

use crate::common::clients::redis::RedisClient;
use crate::core::enrichment::circuit_breaker::CircuitBreaker;
use crate::core::enrichment::provider::{EnrichmentProvider, EnrichmentResult, EnrichmentStatus};
use crate::core::enrichment::providers::abuseipdb::{AbuseIpDbData, AbuseIpDbProvider};
use crate::core::enrichment::providers::dns::{DnsData, DnsProvider};
use crate::core::enrichment::providers::virustotal::{VirusTotalData, VirusTotalProvider};

use crate::core::consumer::event::RawEvent;
use crate::core::enrichment::evidence::Evidence;

const LOCK_TTL_SECONDS: u64 = 10; // Lock 생존시간(10초)
const LOCK_POLL_INTERVAL_MS: u64 = 100; // 다른 요청이 처리한 enrichment 결과가 cache에 저장됐는지 확인하는 polling 간격(100ms)
const LOCK_MAX_WAIT_MS: u64 = 3000; // 최대 대기시간(3000ms)

const BREAKER_FAILURE_THRESHOLD: u32 = 3; // 이 횟수만큼 연속 실패하면 서킷브레이커가 Open으로 전환됨
const BREAKER_COOLDOWN: Duration = Duration::from_secs(30); // 서킷브레이커 OPEN 상태에서 HALF-OPEN 상태로 전환하기까지 대기하는 시간

// provider마다 서킷브레이커를 독립적으로 가짐
// 예를 들어 DNS 장애가 AbuseIPDB 호출을 막으면 안 되므로 각각 별도의 breaker를 사용
pub struct EnrichmentService {
    redis: RedisClient,

    dns: DnsProvider,
    dns_breaker: CircuitBreaker,

    abuseipdb: AbuseIpDbProvider,
    abuseipdb_breaker: CircuitBreaker,

    virustotal: VirusTotalProvider,
    virustotal_breaker: CircuitBreaker,
}

impl EnrichmentService {
    pub fn new(
        redis: RedisClient,
        dns: DnsProvider,
        abuseipdb: AbuseIpDbProvider,
        virustotal: VirusTotalProvider,
    ) -> Self {
        // 임계치 3회, 쿨다운 30초
        Self {
            redis,

            dns,
            dns_breaker: CircuitBreaker::new(BREAKER_FAILURE_THRESHOLD, BREAKER_COOLDOWN),

            abuseipdb,
            abuseipdb_breaker: CircuitBreaker::new(BREAKER_FAILURE_THRESHOLD, BREAKER_COOLDOWN),

            virustotal,
            virustotal_breaker: CircuitBreaker::new(BREAKER_FAILURE_THRESHOLD, BREAKER_COOLDOWN),
        }
    }

    // Consumer가 개별 IOC enrichment를 직접 요청할 때 사용하는 공개 진입점
    pub async fn enrich_src_ip(&self, ioc: &str) -> EnrichmentResult<AbuseIpDbData> {
        self.enrich(&self.abuseipdb, &self.abuseipdb_breaker, ioc)
            .await
    }

    pub async fn enrich_dst_domain(&self, ioc: &str) -> EnrichmentResult<DnsData> {
        self.enrich(&self.dns, &self.dns_breaker, ioc).await
    }

    pub async fn enrich_file_sha256(&self, ioc: &str) -> EnrichmentResult<VirusTotalData> {
        self.enrich(&self.virustotal, &self.virustotal_breaker, ioc)
            .await
    }

    // 공용 enrichment orchestration.
    async fn enrich<P: EnrichmentProvider>(
        // P 자리엔 아무 타입이나 올 수 있는데, 단 EnrichmentProvider trait을 구현한 타입이어야 함
        &self,
        provider: &P,
        breaker: &CircuitBreaker,
        ioc: &str,
    ) -> EnrichmentResult<P::Output>
    where
        P::Output: Serialize + DeserializeOwned, // Serialize: Redis에 저장할 때 필요, DeserializeOwned: Redis에서 꺼낼 때 필요
    {
        // Redis 키 예시) enrichment:dns:test.com / enrichment:abuseipdb:123.123.123.123 / enrichment:virustotal:<sha256>
        let cache_key = format!("enrichment:{}:{}", provider.name(), ioc);

        // 1. 캐시 확인: 캐시가 있으면 외부 Provider와 관계없이 바로 반환
        if let Some(mut cached) = self.try_read_cache::<P::Output>(&cache_key).await {
            cached.cached = true;
            return cached;
        }

        // 2. 서킷브레이커 상태 확인:
        if !breaker.allow_request() {
            return self.breaker_open_result(provider); // 서킷브레이커가 현재 Provider 호출을 허용하지 않으면 호출을 건너뜀
        }

        // 3. 동일 IOC에 대한 중복 외부 호출 방지를 위해 분산 락 획득
        let lock_key = format!("enrichment:lock:{}:{}", provider.name(), ioc);
        let owner = Uuid::new_v4().to_string();

        match self
            .redis
            .try_lock(&lock_key, &owner, LOCK_TTL_SECONDS)
            .await
        {
            // 3-1. Lock을 획득한 경우
            Ok(true) => {
                // Lock을 획득한 사이 다른 요청이 캐시를 채웠을 가능성을 한 번 더 확인
                if let Some(mut cached) = self.try_read_cache::<P::Output>(&cache_key).await {
                    cached.cached = true;

                    if let Err(err) = self.redis.unlock(&lock_key, &owner).await {
                        tracing::warn!(
                            provider = provider.name(),
                            %ioc,
                            error = ?err,
                            "Lock 해제 실패"
                        );
                    }

                    return cached;
                }

                // Lock 획득 성공 + 여전히 cache miss → 실제 API 호출
                let result = self
                    .call_and_store(provider, breaker, ioc, &cache_key)
                    .await;

                if let Err(err) = self.redis.unlock(&lock_key, &owner).await {
                    tracing::warn!(
                        provider = provider.name(),
                        %ioc,
                        error = ?err,
                        "Lock 해제 실패"
                    );
                }

                result
            }

            // 3-2. Lock 획득 실패한 경우
            Ok(false) => {
                // 다른 요청이 정상적으로 Lock을 보유 중이므로 해당 요청이 결과를 cache에 저장하기를 기다림
                self.wait_for_result(&cache_key, provider, breaker, ioc)
                    .await
            }

            Err(err) => {
                // Redis 자체 장애
                // cache/lock은 최적화 계층이므로 외부 Provider 호출 자체는 계속함
                tracing::warn!(
                    provider = provider.name(),
                    %ioc,
                    error = ?err,
                    "Redis Lock 시도 실패, 직접 호출로 대체"
                );

                // Redis 호출에 시간이 걸리는 동안 breaker 상태가 변경됐을 수 있으므로 실제 Provider 호출 직전에 한 번 더 확인
                if !breaker.allow_request() {
                    return self.breaker_open_result(provider);
                }

                self.call_and_store(provider, breaker, ioc, &cache_key)
                    .await
            }
        }
    }

    async fn try_read_cache<T: DeserializeOwned>(
        &self,
        cache_key: &str,
    ) -> Option<EnrichmentResult<T>> {
        match self.redis.get(cache_key).await {
            Ok(None) => None, // redis에 없는 경우 None 반환

            Ok(Some(json)) => match serde_json::from_str(&json) {
                Ok(value) => Some(value),

                Err(err) => {
                    tracing::warn!(
                        %cache_key,
                        error = %err, // Display(사람이 읽기 좋은 문자열 형태)
                        "Cache 값 파싱 실패"
                    );
                    None
                }
            },

            Err(err) => {
                tracing::warn!(
                    %cache_key,
                    error = ?err, // {:?} 형태, 구조체 내부까지 다 보여줌
                    "Cache 조회 실패"
                );
                None
            }
        }
    }

    // 락을 얻지 못한 경우 다른 요청이 cache를 채우기를 잠시 기다림
    //
    // polling 중 cache가 채워지면 해당 결과를 반환하고,
    // 제한 시간까지 결과가 없으면 breaker 상태를 다시 확인한 뒤 직접 Provider를 호출함
    async fn wait_for_result<P: EnrichmentProvider>(
        &self,
        cache_key: &str,
        provider: &P,
        breaker: &CircuitBreaker,
        ioc: &str,
    ) -> EnrichmentResult<P::Output>
    where
        P::Output: Serialize + DeserializeOwned,
    {
        let attempts = LOCK_MAX_WAIT_MS / LOCK_POLL_INTERVAL_MS;

        for _ in 0..attempts {
            sleep(Duration::from_millis(LOCK_POLL_INTERVAL_MS)).await;

            if let Some(mut cached) = self.try_read_cache::<P::Output>(cache_key).await {
                cached.cached = true;
                return cached;
            }
        }

        // 기다리는 동안 다른 요청의 실패로 breaker가 Open 상태가 되었을 수 있음
        // 직접 호출하기 전에 반드시 다시 확인
        if !breaker.allow_request() {
            return self.breaker_open_result(provider);
        }

        // 제한 시간 초과
        // 이 경우 중복 호출이 발생할 수 있지만,
        // 결과를 무한정 기다리는 것보다 직접 조회하는 것을 선택
        self.call_and_store(provider, breaker, ioc, cache_key).await
    }

    async fn call_and_store<P: EnrichmentProvider>(
        &self,
        provider: &P,
        breaker: &CircuitBreaker,
        ioc: &str,
        cache_key: &str,
    ) -> EnrichmentResult<P::Output>
    where
        P::Output: Serialize + DeserializeOwned,
    {
        match provider.call(ioc).await {
            Ok(data) => {
                breaker.record_success();

                let result = EnrichmentResult {
                    status: EnrichmentStatus::Success,
                    data: Some(data),
                    cached: false,
                };

                self.write_cache(cache_key, &result, provider.ttl_seconds())
                    .await;

                result
            }

            Err(EnrichmentStatus::NotFound) => {
                // Provider가 정상적으로 응답했지만 조회 결과가 없는 경우
                // 외부 시스템 장애가 아니므로 breaker failure로 취급하지 않는다
                breaker.record_success();

                let result = EnrichmentResult {
                    status: EnrichmentStatus::NotFound,
                    data: None,
                    cached: false,
                };

                // NotFound도 negative cache로 저장
                self.write_cache(cache_key, &result, provider.ttl_seconds())
                    .await;

                result
            }

            Err(status) => {
                // RateLimited / Timeout / Error
                // 현재 정책에서는 Provider 호출 실패로 간주
                breaker.record_failure();

                EnrichmentResult {
                    status,
                    data: None,
                    cached: false,
                }
            }
        }
    }

    async fn write_cache<T: Serialize>(
        &self,
        cache_key: &str,
        result: &EnrichmentResult<T>,
        ttl_seconds: u64,
    ) {
        match serde_json::to_string(result) {
            Ok(json) => {
                if let Err(err) = self.redis.set(cache_key, &json, ttl_seconds).await {
                    tracing::warn!(
                        %cache_key,
                        error = ?err,
                        "Enrichment 결과 Cache 저장 실패"
                    );
                }
            }

            Err(err) => {
                tracing::warn!(
                    %cache_key,
                    error = %err,
                    "Enrichment 결과 직렬화 실패"
                );
            }
        }
    }

    // Circuit Breaker가 호출 허용하지 않는다는 결과를 반환
    fn breaker_open_result<P: EnrichmentProvider>(
        &self,
        provider: &P,
    ) -> EnrichmentResult<P::Output> {
        EnrichmentResult {
            status: EnrichmentStatus::Error(format!(
                "{} 서킷브레이커가 Provider 호출을 허용하지 않아 호출을 건너뜀",
                provider.name()
            )),
            data: None,
            cached: false,
        }
    }

    // RawEvent에서 존재하는 IOC만 병렬로 조회하고 Evidence로 취합
    pub async fn enrich_event(&self, event: RawEvent) -> Evidence {
        let iocs = event.enrichment_iocs();

        let (enrichment_src_ip, enrichment_dst_domain, enrichment_file_sha256) = tokio::join!(
            async {
                match iocs.src_ip {
                    Some(ip) => Some(self.enrich_src_ip(ip).await),
                    None => None,
                }
            },
            async {
                match iocs.dst_domain {
                    Some(domain) => Some(self.enrich_dst_domain(domain).await),
                    None => None,
                }
            },
            async {
                match iocs.file_sha256 {
                    Some(hash) => Some(self.enrich_file_sha256(hash).await),
                    None => None,
                }
            },
        );

        Evidence {
            event,
            enrichment_src_ip,
            enrichment_dst_domain,
            enrichment_file_sha256,
        }
    }
}
