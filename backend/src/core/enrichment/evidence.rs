use serde::Serialize;

use crate::core::consumer::event::RawEvent;
use crate::core::enrichment::provider::EnrichmentResult;
use crate::core::enrichment::providers::{
    abuseipdb::AbuseIpDbData, dns::DnsData, virustotal::VirusTotalData,
};

// RawEvent 하나를 조사한 결과를 취합하는 구조체
// 원본 이벤트(RawEvent) 전체를 그대로 보존하면서, Enrichment 결과를 enrichment_* 필드로 나란히 붙인다.
#[derive(Debug, Clone, Serialize)]
pub struct Evidence {
    #[serde(flatten)]
    pub event: RawEvent,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment_src_ip: Option<EnrichmentResult<AbuseIpDbData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment_dst_domain: Option<EnrichmentResult<DnsData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment_file_sha256: Option<EnrichmentResult<VirusTotalData>>,
}

// cargo test --lib evidence
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::enrichment::provider::EnrichmentStatus;

    // 시나리오 1: Evidence를 JSON으로 직렬화할 때
    // - event(RawEvent) 내용이 최상위로 펼쳐지는지 (#[serde(flatten)] 확인)
    // - enrichment_*가 None이면 그 필드가 JSON에서 빠지는지 (skip_serializing_if 확인)
    #[test]
    fn evidence_flattens_raw_event_and_attaches_enrichment_fields() {
        let event: RawEvent = serde_json::from_str(
            r#"{
                "event_id": "evt-1",
                "event_type": "login_failure",
                "timestamp": "2026-09-08T10:17:32Z",
                "src_ip": "185.220.101.45",
                "username": "admin",
                "attempt_count": 12,
                "target_service": "ssh"
            }"#,
        )
        .unwrap();

        let evidence = Evidence {
            event,
            enrichment_src_ip: None,
            enrichment_dst_domain: None,
            enrichment_file_sha256: None,
        };

        let json = serde_json::to_string(&evidence).expect("직렬화 실패");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // RawEvent의 필드가 최상위에 그대로 존재
        assert_eq!(parsed.get("event_id"), Some(&serde_json::json!("evt-1")));
        assert_eq!(
            parsed.get("src_ip"),
            Some(&serde_json::json!("185.220.101.45"))
        );
        assert_eq!(parsed.get("username"), Some(&serde_json::json!("admin")));

        // "event"라는 필드명 자체는 사라짐(flatten)
        assert!(parsed.get("event").is_none());

        // enrichment_*가 전부 None이면 필드 자체가 없어야 함
        assert!(parsed.get("enrichment_src_ip").is_none());
    }

    // 시나리오 2: enrichment_src_ip가 Some일 때
    // - "enrichment_src_ip"라는 이름으로 JSON에 나타나는지
    // - 그 안의 값(abuse_confidence_score)이 정확히 들어가는지
    #[test]
    fn evidence_includes_enrichment_result_when_present() {
        let event: RawEvent = serde_json::from_str(
            r#"{
                "event_id": "evt-2",
                "event_type": "login_failure",
                "timestamp": "2026-09-08T10:17:32Z",
                "src_ip": "185.220.101.45"
            }"#,
        )
        .unwrap();

        let evidence = Evidence {
            event,
            enrichment_src_ip: Some(EnrichmentResult {
                status: EnrichmentStatus::Success,
                data: Some(AbuseIpDbData {
                    abuse_confidence_score: 100,
                    total_reports: 3319,
                }),
                cached: false,
            }),
            enrichment_dst_domain: None,
            enrichment_file_sha256: None,
        };

        let json = serde_json::to_string(&evidence).expect("직렬화 실패");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed.get("enrichment_src_ip").is_some());
        assert_eq!(
            parsed["enrichment_src_ip"]["data"]["abuse_confidence_score"],
            serde_json::json!(100)
        );
    }
}
