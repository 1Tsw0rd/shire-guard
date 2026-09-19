use std::net::IpAddr;

use async_trait::async_trait;

// Hickory DNS Resolver의 DNS 서버 연결 설정
use hickory_resolver::TokioResolver; // Tokio 런타임에서 비동기로 DNS 조회를 수행하는 Resolver
use hickory_resolver::config::{
    ConnectionConfig, // ConnectionConfig: UDP/TCP 등 DNS 통신 방식과 포트 설정
    NameServerConfig, // NameServerConfig: 실제 사용할 DNS 서버(IP)와 연결 설정
    ResolverConfig,   // ResolverConfig: 사용할 DNS 서버 목록 등 Resolver 전체 설정
};
use hickory_resolver::net::runtime::TokioRuntimeProvider; // Tokio 기반 DNS 네트워크 연결을 제공하는 runtime provider

use serde::{Deserialize, Serialize};

use crate::common::error::AppError;
use crate::core::enrichment::provider::{EnrichmentProvider, EnrichmentStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsData {
    pub resolved_ips: Vec<String>,
}

pub struct DnsProvider {
    resolver: TokioResolver,
}

impl DnsProvider {
    pub fn new(dns_host: &str, dns_port: u16) -> Result<Self, AppError> {
        let ip: IpAddr = dns_host.parse().map_err(|_| {
            AppError::Internal(format!(
                "DNS_RESOLVER_HOST가 올바른 IP가 아닙니다: {dns_host}"
            ))
        })?;

        // DNS 서버와 통신할 때 사용할 연결 방식을 생성
        let mut conn = ConnectionConfig::udp();
        conn.port = dns_port;
        // 실제로 사용할 DNS 서버 접속 정보를 생성
        let name_server = NameServerConfig::new(ip, true, vec![conn]);
        // Resolver가 사용할 DNS 서버 목록을 구성
        let config = ResolverConfig::from_name_servers(vec![name_server]);
        // 위에서 만든 DNS 서버 설정을 사용하는 비동기 DNS Resolver 생성
        let resolver = TokioResolver::builder_with_config(config, TokioRuntimeProvider::default())
            .build()
            .map_err(|err| {
                AppError::Internal(format!("DNS Resolver 생성에 실패했습니다: {err}"))
            })?;

        Ok(Self { resolver })
    }
}

#[async_trait]
impl EnrichmentProvider for DnsProvider {
    type Output = DnsData; // DNS 조회 결과는 DnsData 구조체에 담아서 반환

    // Provider를 식별하기 위한 이름: Redis 키(enrichment:{name}:{ioc})에 사용
    fn name(&self) -> &'static str {
        "dns"
    }
    // 이 Provider의 결과를 Redis 등에 캐시할 TTL(Time To Live)
    fn ttl_seconds(&self) -> u64 {
        3600 // 1시간(3600초)
    }

    async fn call(&self, ioc: &str) -> Result<Self::Output, EnrichmentStatus> {
        match self.resolver.lookup_ip(ioc).await {
            Ok(lookup) => {
                let resolved_ips = lookup.iter().map(|ip| ip.to_string()).collect();
                Ok(DnsData { resolved_ips })
            }
            Err(err) if err.is_nx_domain() || err.is_no_records_found() => {
                // NXDOMAIN: 해당 도메인이 존재하지 않음
                // NoRecordsFound: 도메인은 존재할 수 있지만 조회할 레코드가 없음
                Err(EnrichmentStatus::NotFound)
            }
            Err(err) => Err(EnrichmentStatus::Error(err.to_string())), // 위에서 처리하지 않은 나머지 DNS 오류
        }
    }
}

// cargo test --lib enrichment::providers::dns
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: 잘못된 IP 형식이면 에러를 반환한다
    #[test]
    fn new_rejects_invalid_host() {
        let result = DnsProvider::new("not-an-ip", 53);
        assert!(result.is_err());
    }

    // 시나리오 2: 올바른 IP면 생성에 성공한다
    #[test]
    fn new_accepts_valid_host() {
        let result = DnsProvider::new("1.1.1.1", 53);
        assert!(result.is_ok());
    }

    // 시나리오 3: 실제 DNS 서버에 정상 도메인을 조회하면 IP 주소를 반환한다
    // 네트워크 필요하므로 #[ignore]로 기본 `cargo test`에서는 제외하고 수동 검증 시에만 실행하도록 함
    // (`cargo test --lib -- --ignored`)
    // CI 동작 중 cargo test에서 매번 동작하도록 하지 않게 함
    #[tokio::test]
    #[ignore]
    async fn resolves_existing_domain() {
        // 테스트용 공개 DNS 서버
        let provider = DnsProvider::new("1.1.1.1", 53).unwrap();

        // 실제 DNS 조회
        let result = provider.call("example.com").await.unwrap();

        // 실제 조회 결과 출력
        println!(
            "DNS lookup result for example.com: {:?}",
            &result.resolved_ips
        );
        for ip in &result.resolved_ips {
            println!("  - {ip}");
        }
        /*
        DNS lookup result for example.com: ["2606:4700:10::6814:179a", "2606:4700:10::ac42:93f3", "172.66.147.243", "104.20.23.154"]
        - 2606:4700:10::6814:179a
        - 2606:4700:10::ac42:93f3
        - 172.66.147.243
        - 104.20.23.154
        */

        // 최소 하나의 IP가 반환되어야 함
        assert!(!result.resolved_ips.is_empty());
    }

    // 시나리오 4: 존재하지 않는 도메인을 조회하면 NotFound를 반환한다
    // CI 동작 중 cargo test에서 매번 동작하도록 하지 않게 함
    #[tokio::test]
    #[ignore]
    async fn returns_not_found_for_unknown_domain() {
        let provider = DnsProvider::new("1.1.1.1", 53).unwrap();

        let result = provider
            .call("this-domain-definitely-does-not-exist.example")
            .await;

        println!("DNS lookup result for unknown domain: {result:?}"); // DNS lookup result for unknown domain: Err(NotFound)

        assert!(matches!(result, Err(EnrichmentStatus::NotFound)));
    }
}
