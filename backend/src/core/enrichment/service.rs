/*
Event
    ↓
IOC Field 추출 (event.rs의 enrichment_iocs())
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

[캐시 / Lock / Provider 호출 흐름]
각 IOC마다 먼저 Redis/Dragonfly 캐시를 확인함

  캐시 조회
    ├─ Hit
    │    → 저장된 enrichment 결과 반환
    │
    ├─ Unavailable (Redis/Dragonfly 장애)
    │    → Provider 호출하지 않고 CacheUnavailable 반환
    │
    └─ Miss (캐시에 결과가 없음)
         ↓
       Circuit Breaker 확인
         ├─ 호출 불가(Open 상태에서 쿨다운 30초 이내이거나, HalfOpen 상태에서 시험 요청이 진행 중인 상태)
         │    → Provider 호출하지 않고 Error 반환
         │
         └─ 호출 가능
              ↓
            분산 Lock 획득 시도
              ├─ Lock 획득
              │    ↓
              │  캐시 재확인
              │    ├─ Hit
              │    │    → Lock 해제 후 저장된 결과 반환
              │    │
              │    ├─ Unavailable (Redis/Dragonfly 장애)
              │    │    → Lock 해제 후 CacheUnavailable 반환
              │    │
              │    └─ Miss
              │         ↓
              │       Provider 호출 직전 Circuit Breaker 재확인 (쿨다운이 지나면 시험 요청 1개만 통과)
              │         ├─ 호출 불가 → Provider 호출하지 않고 Error 반환
              │         └─ 호출 가능 → Provider 호출
              │                        ├─ Success  → 캐시 저장
              │                        ├─ NotFound → 캐시 저장
              │                        └─ 실패     → 캐시 저장하지 않음
              │         ↓
              │       Lock 해제 후 EnrichmentResult 반환
              │
              ├─ Lock 획득 실패 (다른 요청이 Lock을 보유 중)
              │    ↓
              │  캐시 결과를 기다림 (100ms마다 확인, 최대 3초)
              │    ├─ Hit         → 저장된 결과 반환
              │    ├─ Unavailable → CacheUnavailable 반환
              │    └─ 3초 초과    → Timeout 반환
              │
              └─ Lock 시도 자체가 에러 (Redis/Dragonfly 장애)
                   → Provider 호출하지 않고 CacheUnavailable 반환

Lock을 획득한 요청도 실제 Provider 호출 직전에 캐시를 한 번 더 확인함
처음 캐시를 확인한 뒤 Lock을 획득하기 전에, 다른 요청이 Provider 호출을 끝내고
캐시에 결과를 저장한 뒤 Lock을 해제했을 수 있기 때문임

예:

요청 A                         요청 B
  │                              │
  │ 캐시 → MISS                   │ 캐시 → MISS
  │                              │
  │                              │ Lock 획득
  │                              │ Provider API 호출
  │                              │ 캐시 저장
  │                              │ Lock 해제
  │ Lock 획득                     │
  │                              │
  │ 캐시 다시 확인                 │
  │ → HIT                        │
  │                              │
  └→ Provider API 호출 안 함       │

B가 Lock을 쥐고 있는 동안 A가 Lock을 시도하면 획득에 실패하고,
A는 Lock을 다시 시도하지 않고 캐시에 결과가 저장되기를 기다림

즉, 같은 IOC에 대한 중복 외부 API 호출을 분산 Lock과 캐시 재확인으로 줄임

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

// try_read_cache()의 조회 결과
enum CacheLookup<T> {
    Hit(T),      // Redis/Dragonfly 캐시에 값이 있고 파싱 성공
    Miss,        // Redis/Dragonfly는 정상 응답했지만 값이 없음 (값 파싱 실패도 Miss 취급)
    Unavailable, // Redis/Dragonfly 자체가 응답하지 않거나 에러
}

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
        match self.try_read_cache::<P::Output>(&cache_key).await {
            CacheLookup::Hit(mut cached) => {
                cached.cached = true;
                return cached;
            }
            // Redis 장애 시에는 Provider(외부 API)를 호출하지 않고 즉시 반환
            CacheLookup::Unavailable => return Self::cache_unavailable_result(),
            CacheLookup::Miss => {}
        }

        // 2. 서킷브레이커 상태 확인 (비소비형 — 여기서 probe를 "써버리지" 않음)
        if !breaker.peek_allow_request() {
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
                // 처음 캐시를 확인한 뒤 Lock을 획득하기 전에 다른 요청이 캐시를 채웠을 가능성을 한 번 더 확인
                let recheck = self.try_read_cache::<P::Output>(&cache_key).await;

                if !matches!(recheck, CacheLookup::Miss) {
                    // Hit 또는 Unavailable: provider를 호출하지 않으므로 락을 해제하고 반환
                    if let Err(err) = self.redis.unlock(&lock_key, &owner).await {
                        tracing::warn!(
                            provider = provider.name(),
                            %ioc,
                            error = ?err,
                            "Lock 해제 실패"
                        );
                    }

                    return match recheck {
                        CacheLookup::Hit(mut cached) => {
                            cached.cached = true;
                            cached
                        }
                        _ => Self::cache_unavailable_result(),
                    };
                }

                // Lock 획득 후 캐시를 다시 확인했지만 여전히 결과가 없음 → 실제 API 호출
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
                // 제한 시간 안에 결과가 없으면 Provider를 직접 호출하지 않고 Timeout을 반환함
                self.wait_for_result::<P::Output>(&cache_key).await
            }

            Err(err) => {
                // Redis 자체 장애
                // 이전에는 여기서 Provider를 직접 호출했는데, Redis 장애 시
                // 모든 동시 요청이 외부 API로 폭주하는 원인이었음
                // 캐시를 쓸 수 없는 상태에서는 Provider를 호출하지 않고 즉시 실패로 처리
                tracing::warn!(
                    provider = provider.name(),
                    %ioc,
                    error = ?err,
                   "Redis Lock 시도 실패, 캐시 불가용으로 처리(Provider 호출 안 함)"
                );

                Self::cache_unavailable_result()
            }
        }
    }

    async fn try_read_cache<T: DeserializeOwned>(
        &self,
        cache_key: &str,
    ) -> CacheLookup<EnrichmentResult<T>> {
        match self.redis.get(cache_key).await {
            Ok(None) => CacheLookup::Miss, // redis/dragonfly 에 없는 경우

            Ok(Some(json)) => match serde_json::from_str(&json) {
                Ok(value) => CacheLookup::Hit(value),

                Err(err) => {
                    tracing::warn!(
                        %cache_key,
                        error = %err, // Display(사람이 읽기 좋은 문자열 형태)
                        "Cache 값 파싱 실패"
                    );
                    CacheLookup::Miss
                }
            },

            Err(err) => {
                // Redis 자체가 응답하지 않는 것을 Miss로 취급하면 동시 요청 전부가 외부 API로 폭주함
                // 이를 막기 위해 Unavailable 처리
                tracing::warn!(
                    %cache_key,
                    error = ?err, // {:?} 형태, 구조체 내부까지 다 보여줌
                    "Cache 조회 실패"
                );
                CacheLookup::Unavailable
            }
        }
    }

    // 락을 얻지 못한 경우 다른 요청이 cache를 채우기를 잠시 기다림
    //
    // polling 중 cache가 채워지면 해당 결과를 반환하고,
    // 제한 시간까지 결과가 없으면 Provider를 직접 호출하지 않고 Timeout으로 반환함
    // (이전에는 여기서 Provider를 직접 호출했는데, Lock 보유자가 실패하면 대기자 전체가 한꺼번에 호출해 외부 API가 폭주할 수 있었음)
    async fn wait_for_result<T: DeserializeOwned>(&self, cache_key: &str) -> EnrichmentResult<T> {
        let attempts = LOCK_MAX_WAIT_MS / LOCK_POLL_INTERVAL_MS;

        for _ in 0..attempts {
            sleep(Duration::from_millis(LOCK_POLL_INTERVAL_MS)).await;

            match self.try_read_cache::<T>(cache_key).await {
                CacheLookup::Hit(mut cached) => {
                    cached.cached = true;
                    return cached;
                }
                // 폴링 중 Redis 장애를 감지하면 3초를 다 기다리지 않고 즉시 반환
                CacheLookup::Unavailable => return Self::cache_unavailable_result(),
                CacheLookup::Miss => {}
            }
        }

        // 제한 시간 초과: Lock 보유자의 결과가 캐시에 없음
        // 대기자가 한꺼번에 Provider를 호출하면 폭주하므로 직접 호출하지 않고 Timeout으로 반환
        EnrichmentResult {
            status: EnrichmentStatus::Timeout,
            data: None,
            cached: false,
        }
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
        // 실제 provider 호출 직전, 여기서만 probe를 "소비"함(CAS 발생 지점을 한 곳으로 고정)
        // allow_request()가 true를 반환한 이후에는 provider.call()이 반드시 실행되고, 그 결과가 반드시
        // record_success()/record_failure()로 이어지므로 probe 소비와 보고가 항상 짝을 이룸
        // false면 provider를 호출하지 않고 반환하므로 probe도 소비되지 않음
        if !breaker.allow_request() {
            return self.breaker_open_result(provider);
        }

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

    // Redis/Dragonfly 장애로 캐시를 확인할 수 없을 때 반환하는 결과
    fn cache_unavailable_result<T>() -> EnrichmentResult<T> {
        EnrichmentResult {
            status: EnrichmentStatus::CacheUnavailable,
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

// cargo test --lib enrichment::service
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: 캐시 불가용 결과는 Provider 데이터 없이 CacheUnavailable 상태를 반환한다
    #[test]
    fn cache_unavailable_result_has_no_data() {
        let result = EnrichmentService::cache_unavailable_result::<AbuseIpDbData>();

        assert_eq!(result.status, EnrichmentStatus::CacheUnavailable);
        assert!(result.data.is_none());
        assert!(!result.cached);
    }
}
