# Shire Guard

## 표준 RAW EVENT

파이프라인 테스트에 사용하는 표준 이벤트 3종 (`./scripts/test.sh event <type>`로 전송 가능).

```json
// 1. file_download — AbuseIPDB + DNS + VirusTotal 전부 검증
{
  "event_id": "evt-20260908-001",
  "event_type": "file_download",
  "timestamp": "2026-09-08T10:15:00Z",
  "source_ip": "185.220.101.45",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "file_size_bytes": 245760
}

// 2. login_failure — AbuseIPDB만 검증
{
  "event_id": "evt-20260908-002",
  "event_type": "login_failure",
  "timestamp": "2026-09-08T10:17:32Z",
  "source_ip": "185.220.101.45",
  "username": "admin",
  "attempt_count": 12,
  "target_service": "ssh"
}

// 3. dns_beacon — AbuseIPDB + DNS 검증 (파일 없음)
{
  "event_id": "evt-20260908-003",
  "event_type": "dns_beacon",
  "timestamp": "2026-09-08T12:30:44Z",
  "source_ip": "198.51.100.77",
  "destination_domain": "telemetry-sync.info",
  "hostname": "WKS-1183",
  "request_interval_seconds": 60
}
```