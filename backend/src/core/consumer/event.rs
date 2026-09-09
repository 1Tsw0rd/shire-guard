// 포트폴리오 테스트를 위해 정의한 표준 RAW EVENT 모델
//
// 실제 운영 환경에서는 외부 보안 장비/센서마다 서로 다른 필드와
// 추가 필드를 가진 이벤트가 들어올 수 있으므로, 특정 이벤트 구조를 Rust enum으로 강제하지 않는다.
//
// 하지만 이 파일은 포트폴리오 테스트에서 사용할 3종 RAW EVENT의 구조를 Rust 타입으로 정의하고,
// Consumer가 전달받은 JSON을 해당 이벤트 타입으로 역직렬화할 수 있도록 한다.
//
// 이 파일은 이벤트 타입과 필드 구조만 정의하며, 메시지 수신이나 이후 이벤트 처리 로직은 담당하지 않는다.
//
// 현재 테스트용 표준 이벤트:
// - file_download
// - login_failure
// - dns_beacon

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")] // enum을 JSON의 어떤 필드로 구분할지 지정
pub enum RawEvent {
    #[serde(rename = "file_download")]
    // JSON의 event_type이 "file_download"이면 이 variant로 역직렬화
    FileDownload {
        event_id: String, // 보안장비/센서마다 이벤트 ID 형식이 다를 수 있어 String으로 정의(예: UUID, 숫자, 문자열 등)
        timestamp: String,
        src_ip: String,
        dst_domain: String,
        file_name: String,
        file_sha256: String,
        file_size_bytes: u64,
    },

    #[serde(rename = "login_failure")]
    LoginFailure {
        event_id: String,
        timestamp: String,
        src_ip: String,
        username: String,
        attempt_count: u32,
        target_service: String,
    },

    #[serde(rename = "dns_beacon")]
    DnsBeacon {
        event_id: String,
        timestamp: String,
        src_ip: String,
        dst_domain: String,
        hostname: String,
        request_interval_seconds: u32,
    },
}

// cargo test --lib event
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: file_download JSON이 RawEvent::FileDownload으로 정상 역직렬화되는지 검증한다.
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

        let event: RawEvent = serde_json::from_str(json).expect("파싱 실패"); // JSON 문자열을 RawEvent enum으로 역직렬화

        match event {
            RawEvent::FileDownload {
                event_id, src_ip, ..
            } => {
                assert_eq!(event_id, "evt-1");
                assert_eq!(src_ip, "185.220.101.45");
            }
            _ => panic!("FileDownload로 파싱되어야 함"),
        }
    }

    // 시나리오 2: login_failure JSON이 RawEvent::LoginFailure으로 정상 역직렬화되는지 검증한다.
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

        match event {
            RawEvent::LoginFailure { attempt_count, .. } => {
                assert_eq!(attempt_count, 12);
            }
            _ => panic!("LoginFailure로 파싱되어야 함"),
        }
    }

    // 시나리오 3: dns_beacon JSON이 RawEvent::DnsBeacon으로 정상 역직렬화되는지 검증한다.
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

        match event {
            RawEvent::DnsBeacon { hostname, .. } => {
                assert_eq!(hostname, "WKS-1183");
            }
            _ => panic!("DnsBeacon으로 파싱되어야 함"),
        }
    }

    // 시나리오 4: 지원하지 않는 event_type을 입력하면 역직렬화에 실패하는지 검증한다.
    #[test]
    fn unknown_event_type_fails() {
        let json = r#"{"event_id": "evt-4", "event_type": "unknown_type"}"#;

        let result: Result<RawEvent, _> = serde_json::from_str(json);

        assert!(result.is_err());
    }
}
