-- 샘플 플레이북 (개발 및 데모용, 첫 기동 시 1회 실행)
INSERT INTO playbooks (name, description, enabled, nodes) VALUES
(
  'Enrichment 조회 실패 이벤트 보류',
  'Provider(AbuseIPDB, DNS, VirusTotal) 또는 Redis/Dragonfly 서버 오류로 IOC 조회에 실패한 이벤트를 HOLD 처리',
  TRUE,
  $json$[
    { "type": "condition", "name": "IOC 조회 실패 및 이벤트 HOLD",
      "when": { "or": [
        { "and": [
          { "field": "enrichment_src_ip.status.status", "op": "ne", "value": "Success" },
          { "field": "enrichment_src_ip.status.status", "op": "ne", "value": "NotFound" } ] },
        { "and": [
          { "field": "enrichment_dst_domain.status.status", "op": "ne", "value": "Success" },
          { "field": "enrichment_dst_domain.status.status", "op": "ne", "value": "NotFound" } ] },
        { "and": [
          { "field": "enrichment_file_sha256.status.status", "op": "ne", "value": "Success" },
          { "field": "enrichment_file_sha256.status.status", "op": "ne", "value": "NotFound" } ] } ] } },
    { "type": "response", "action": "HOLD" }
  ]$json$::jsonb
),
(
  '고위험 IP 차단',
  '이벤트 종류와 관계없이 AbuseIPDB 80점 이상 IP의 이벤트를 DENY 처리',
  TRUE,
  $json$[
    { "type": "condition", "name": "고위험 IP",
      "when": { "field": "enrichment_src_ip.data.abuse_confidence_score", "op": "gte", "value": 80 } },
    { "type": "response", "action": "DENY" }
  ]$json$::jsonb
),
(
  '애매한 파일 AI 분석',
  'VirusTotal 악성 탐지가 1~9개인 파일 다운로드를 AI로 분석하고, 위험도에 따라 DENY/HOLD/DETECT 처리',
  FALSE,  -- Ollama 실행 후 활성화
  $json$[
    { "type": "condition", "name": "파일 다운로드 + 애매한 구간",
      "when": { "and": [
        { "field": "event_type", "op": "eq", "value": "file_download" },
        { "field": "enrichment_file_sha256.data.malicious", "op": "gte", "value": 1 },
        { "field": "enrichment_file_sha256.data.malicious", "op": "lt", "value": 10 } ] } },
    { "type": "ai_response",
      "model": "qwen2.5:14b-instruct",
      "prompt": "보안 이벤트와 enrichment 결과를 분석하고 위험도를 판단하라.",
      "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }
  ]$json$::jsonb
),
(
  '고위험 실행파일 차단',
  '.exe 파일 다운로드 중 VirusTotal 악성 탐지 10개 이상이거나, AbuseIPDB 80점 이상 IP의 100000바이트 초과 파일을 DENY 처리',
  TRUE,
  $json$[
    { "type": "condition", "name": "파일 다운로드만",
      "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
    { "type": "condition", "name": "악성 탐지 또는 고위험 IP의 대용량 파일",
      "when": { "or": [
        { "field": "enrichment_file_sha256.data.malicious", "op": "gte", "value": 10 },
        { "and": [
          { "field": "enrichment_src_ip.data.abuse_confidence_score", "op": "gte", "value": 80 },
          { "field": "file_size_bytes", "op": "gt", "value": 100000 } ] } ] } },
    { "type": "condition", "name": "실행 파일",
      "when": { "field": "file_name", "op": "ends_with", "value": ".exe" } },
    { "type": "response", "action": "DENY" }
  ]$json$::jsonb
),
(
  '차단 도메인 비컨 차단',
  '차단 대상 도메인(c2.devil.example)으로 주기적 통신하는 DNS beacon 이벤트를 DENY 처리',
  TRUE,
  $json$[
    { "type": "condition", "name": "DNS beacon만",
      "when": { "field": "event_type", "op": "eq", "value": "dns_beacon" } },
    { "type": "condition", "name": "차단 도메인",
      "when": { "field": "dst_domain", "op": "eq", "value": "c2.devil.example" } },
    { "type": "response", "action": "DENY" }
  ]$json$::jsonb
);

-- 다수결 테스트용 플레이북 샘플
--
-- 테스트 이벤트 예시
--   {
--     "event_id": "evt-test-1",
--     "event_type": "login_failure",
--     "timestamp": "2026-10-06T10:00:00Z",
--     "source_ip": "118.25.6.39",
--     "username": "admin",
--     "attempt_count": 12,
--     "target_service": "ssh",
--     "test_votes": "D1,D2,H1"
--   }
--
-- 저장된 로그의 playbook_result.action 기대 결과
--   "test_votes": "D1,D2,H1"     -> DENY   (DENY 2/3)
--   "test_votes": "H1,H2,D1"     -> HOLD   (HOLD 2/3)
--   "test_votes": "T1,T2,D1"     -> DETECT (DETECT 2/3)
--   "test_votes": "D1,H1,T1"     -> HOLD   (과반 없음 -> HOLD)
--   "test_votes": "D1,D2,T1,T2"  -> HOLD   (2:2 동률, 과반 아님 -> HOLD)
INSERT INTO playbooks (name, description, enabled, nodes) VALUES
('[TEST] D1', '다수결 테스트: test_votes에 D1 포함 시 DENY', TRUE,
 $json$[{ "type": "condition", "name": "D1", "when": { "field": "test_votes", "op": "contains", "value": "D1" } },
        { "type": "response", "action": "DENY" }]$json$::jsonb),
('[TEST] D2', '다수결 테스트: test_votes에 D2 포함 시 DENY', TRUE,
 $json$[{ "type": "condition", "name": "D2", "when": { "field": "test_votes", "op": "contains", "value": "D2" } },
        { "type": "response", "action": "DENY" }]$json$::jsonb),
('[TEST] H1', '다수결 테스트: test_votes에 H1 포함 시 HOLD', TRUE,
 $json$[{ "type": "condition", "name": "H1", "when": { "field": "test_votes", "op": "contains", "value": "H1" } },
        { "type": "response", "action": "HOLD" }]$json$::jsonb),
('[TEST] H2', '다수결 테스트: test_votes에 H2 포함 시 HOLD', TRUE,
 $json$[{ "type": "condition", "name": "H2", "when": { "field": "test_votes", "op": "contains", "value": "H2" } },
        { "type": "response", "action": "HOLD" }]$json$::jsonb),
('[TEST] T1', '다수결 테스트: test_votes에 T1 포함 시 DETECT', TRUE,
 $json$[{ "type": "condition", "name": "T1", "when": { "field": "test_votes", "op": "contains", "value": "T1" } },
        { "type": "response", "action": "DETECT" }]$json$::jsonb),
('[TEST] T2', '다수결 테스트: test_votes에 T2 포함 시 DETECT', TRUE,
 $json$[{ "type": "condition", "name": "T2", "when": { "field": "test_votes", "op": "contains", "value": "T2" } },
        { "type": "response", "action": "DETECT" }]$json$::jsonb);