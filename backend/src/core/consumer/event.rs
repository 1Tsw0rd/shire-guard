// 포트폴리오 테스트를 위해 정의한 표준 RAW EVENT 모델
//
// 평탄화된 단일 구조체로, 이벤트 타입마다 서로 다른 필드가 존재할 수 있다는 전제를 반영
// event_id/event_type/timestamp는 모든 이벤트가 공통으로 갖는 메타데이터라 필수 필드로 두고,
// 나머지 필드들은 그대로 보존한다.
//
// 추가로, Enrichment가 실제로 조사하는 3개 IOC(src_ip/dst_domain/file_sha256)만 명시적 필드로 둔다.(enrichment_iocs() 참고)
//
// 현재 테스트용 표준 이벤트 예시:
// - file_download: src_ip, dst_domain, file_name, file_sha256, file_size_bytes
// - login_failure: src_ip, username, attempt_count, target_service
// - dns_beacon:    src_ip, dst_domain, hostname, request_interval_seconds

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawEvent {
    pub event_id: String, // 보안장비/센서마다 이벤트 ID 형식이 다를 수 있어 String으로 정의(예: UUID, 숫자, 문자열 등)
    pub event_type: String,
    pub timestamp: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_sha256: Option<String>,

    // 위 3개 IOC를 제외한 원본 이벤트의 모든 필드가 여기 그대로 보존됨
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

// Enrichment가 조사할 IOC들을 한 번에 추출한 결과
// RawEvent가 어떤 이벤트 타입인지와 무관하게, 존재하는 필드만 여기 채워짐
pub struct EnrichmentIocs<'a> {
    pub src_ip: Option<&'a str>,
    pub dst_domain: Option<&'a str>,
    pub file_sha256: Option<&'a str>,
}

impl RawEvent {
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn enrichment_iocs(&self) -> EnrichmentIocs<'_> {
        EnrichmentIocs {
            src_ip: self.src_ip.as_deref(),
            dst_domain: self.dst_domain.as_deref(),
            file_sha256: self.file_sha256.as_deref(),
        }
    }
}

// cargo test --lib event
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: file_download 이벤트에서 IOC 3개는 명시적 필드로, 나머지는 extra로 파싱된다
    #[test]
    fn parses_file_download_event() {
        let json = r#"{
            "event_id": "evt-1",
            "event_type": "file_download",
            "timestamp": "2026-09-08T10:15:00Z",
            "src_ip": "185.220.101.45",
            "dst_domain": "cdn-update-service.net",
            "file_name": "invoice_2026.exe",
            "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "file_size_bytes": 245760
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");

        assert_eq!(event.event_id, "evt-1");
        assert_eq!(event.src_ip.as_deref(), Some("185.220.101.45"));
        assert_eq!(
            event.file_sha256.as_deref(),
            Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        // file_name, file_size_bytes는 이제 extra에 담김
        assert_eq!(
            event.extra.get("file_name"),
            Some(&serde_json::json!("invoice_2026.exe"))
        );
        assert_eq!(
            event.extra.get("file_size_bytes"),
            Some(&serde_json::json!(245760))
        );
    }

    // 시나리오 2: login_failure 이벤트는 dst_domain/file_sha256 없이 파싱되고,
    // username/attempt_count/target_service는 extra로 들어간다
    #[test]
    fn parses_login_failure_event() {
        let json = r#"{
            "event_id": "evt-2",
            "event_type": "login_failure",
            "timestamp": "2026-09-08T10:17:32Z",
            "src_ip": "185.220.101.45",
            "username": "admin",
            "attempt_count": 12,
            "target_service": "ssh"
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");

        assert_eq!(event.dst_domain, None);
        assert_eq!(event.file_sha256, None);
        assert_eq!(
            event.extra.get("attempt_count"),
            Some(&serde_json::json!(12))
        );
        assert_eq!(
            event.extra.get("username"),
            Some(&serde_json::json!("admin"))
        );
    }

    // 시나리오 3: dns_beacon 이벤트가 정상 파싱되는지 검증
    #[test]
    fn parses_dns_beacon_event() {
        let json = r#"{
            "event_id": "evt-3",
            "event_type": "dns_beacon",
            "timestamp": "2026-09-08T12:30:44Z",
            "src_ip": "198.51.100.77",
            "dst_domain": "telemetry-sync.info",
            "hostname": "WKS-1183",
            "request_interval_seconds": 60
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");

        assert_eq!(event.src_ip.as_deref(), Some("198.51.100.77"));
        assert_eq!(
            event.extra.get("hostname"),
            Some(&serde_json::json!("WKS-1183"))
        );
    }

    // 시나리오 4: 필수 메타데이터(event_id/event_type/timestamp)가 없으면 파싱이 실패
    #[test]
    fn missing_required_metadata_fails() {
        let json = r#"{"src_ip": "1.2.3.4"}"#;

        let result: Result<RawEvent, _> = serde_json::from_str(json);

        assert!(result.is_err());
    }

    // 시나리오 5: 정의되지 않은 이벤트 타입이 들어와도 event_type을 String으로 받기 때문에
    // 역직렬화에 실패하지 않고, RawEvent에 명시하지 않은 필드는 extra에 보존된다
    #[test]
    fn unknown_event_type_preserves_extra_fields() {
        let json = r#"{
            "event_id": "evt-5",
            "event_type": "port_scan",
            "timestamp": "2026-09-08T13:00:00Z",
            "src_ip": "10.0.0.5",
            "target_port": 22,
            "scan_type": "syn"
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");

        assert_eq!(event.event_type, "port_scan");
        assert_eq!(event.src_ip.as_deref(), Some("10.0.0.5"));
        assert_eq!(event.extra.get("target_port"), Some(&serde_json::json!(22)));
        assert_eq!(
            event.extra.get("scan_type"),
            Some(&serde_json::json!("syn"))
        );
    }

    // 시나리오 6: enrichment_iocs()가 존재하는 필드만 정확히 추출하는지 검증
    #[test]
    fn enrichment_iocs_extracts_only_present_fields() {
        let json = r#"{
            "event_id": "evt-6",
            "event_type": "login_failure",
            "timestamp": "2026-09-08T10:17:32Z",
            "src_ip": "185.220.101.45",
            "username": "admin",
            "attempt_count": 12,
            "target_service": "ssh"
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");
        let iocs = event.enrichment_iocs();

        assert_eq!(iocs.src_ip, Some("185.220.101.45"));
        assert_eq!(iocs.dst_domain, None);
        assert_eq!(iocs.file_sha256, None);
    }

    // 시나리오 7: 원본에 없던 필드는 직렬화 시에도 나타나지 않는다 (skip_serializing_if 검증)
    #[test]
    fn absent_ioc_fields_are_skipped_on_serialize() {
        let json = r#"{
            "event_id": "evt-7",
            "event_type": "login_failure",
            "timestamp": "2026-09-08T10:17:32Z",
            "src_ip": "185.220.101.45",
            "username": "admin",
            "attempt_count": 12,
            "target_service": "ssh"
        }"#;

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패");
        let serialized = serde_json::to_string(&event).expect("직렬화 실패");
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();

        assert!(parsed.get("dst_domain").is_none());
        assert!(parsed.get("file_sha256").is_none());
        assert_eq!(parsed.get("username"), Some(&serde_json::json!("admin")));
    }
}
