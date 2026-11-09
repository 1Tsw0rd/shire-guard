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
      "model": "gemma4:12b",
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

-- 다수결 테스트용 플레이북 샘플(기본 비활성)
--
-- 테스트 방법
--   1. 기존 플레이북을 모두 끄고 [TEST-VOTE] 플레이북만 활성화 (Backend 시작 전에 실행)
--        UPDATE playbooks SET enabled = (name LIKE '[TEST-VOTE]%');
--   2. loadgen --votes 옵션으로 이벤트 전송
--        예: ./loadgen.exe login_failure 40001 10 10 --case safe --votes D1,D2,H1
--   3. Backend 실행
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
--     "test_votes": "D1,D2,H1"  <<-- 다수결 테스트용 name 필드 값, 쉼표로 구분된 플레이북 코드
--   }
--
-- 저장된 로그의 playbook_result.action 기대 결과
--   "test_votes": "D1,D2,H1"     -> DENY   (DENY 2/3)
--   "test_votes": "H1,H2,D1"     -> HOLD   (HOLD 2/3)
--   "test_votes": "T1,T2,D1"     -> DETECT (DETECT 2/3)
--   "test_votes": "D1,H1,T1"     -> HOLD   (과반 없음 -> HOLD)
--   "test_votes": "D1,D2,T1,T2"  -> HOLD   (2:2 동률, 과반 아님 -> HOLD)
INSERT INTO playbooks (name, description, enabled, nodes) VALUES
('[TEST-VOTE] D1', '다수결 테스트: test_votes에 D1 포함 시 DENY', FALSE,
 $json$[{ "type": "condition", "name": "D1", "when": { "field": "test_votes", "op": "contains", "value": "D1" } },
        { "type": "response", "action": "DENY" }]$json$::jsonb),
('[TEST-VOTE] D2', '다수결 테스트: test_votes에 D2 포함 시 DENY', FALSE,
 $json$[{ "type": "condition", "name": "D2", "when": { "field": "test_votes", "op": "contains", "value": "D2" } },
        { "type": "response", "action": "DENY" }]$json$::jsonb),
('[TEST-VOTE] H1', '다수결 테스트: test_votes에 H1 포함 시 HOLD', FALSE,
 $json$[{ "type": "condition", "name": "H1", "when": { "field": "test_votes", "op": "contains", "value": "H1" } },
        { "type": "response", "action": "HOLD" }]$json$::jsonb),
('[TEST-VOTE] H2', '다수결 테스트: test_votes에 H2 포함 시 HOLD', FALSE,
 $json$[{ "type": "condition", "name": "H2", "when": { "field": "test_votes", "op": "contains", "value": "H2" } },
        { "type": "response", "action": "HOLD" }]$json$::jsonb),
('[TEST-VOTE] T1', '다수결 테스트: test_votes에 T1 포함 시 DETECT', FALSE,
 $json$[{ "type": "condition", "name": "T1", "when": { "field": "test_votes", "op": "contains", "value": "T1" } },
        { "type": "response", "action": "DETECT" }]$json$::jsonb),
('[TEST-VOTE] T2', '다수결 테스트: test_votes에 T2 포함 시 DETECT', FALSE,
 $json$[{ "type": "condition", "name": "T2", "when": { "field": "test_votes", "op": "contains", "value": "T2" } },
        { "type": "response", "action": "DETECT" }]$json$::jsonb);

-- AI 대응 노드 테스트용 플레이북 샘플(기본 비활성)
--
-- 테스트 방법
--   1. 기존 플레이북을 모두 끄고 [TEST-AI] 플레이북만 활성화 (Backend 시작 전에 실행)
--        UPDATE playbooks SET enabled = (name LIKE '[TEST-AI]%');
--   2. loadgen ai 명령으로 이벤트 전송 (test_ai 필드에 판정할 플레이북 코드가 들어감)
--   3. Backend 실행
--
-- 플레이북 구성 및 조건 간단 예시
--   file_download         test_ai: "G1"        -> G1 (gemma4)
--   process_execution     test_ai: "G2"        -> G2 (gemma4)
--   email_attachment      test_ai: "G3"        -> G3 (gemma4)
--   login_failure         test_ai: "P1"        -> P1 (phi4)
--   port_scan             test_ai: "P2"        -> P2 (phi4)
--   privilege_escalation  test_ai: "G4,P3,P4"  -> G4 (gemma4) + P3 (phi4) + P4 (phi4) 다수결
INSERT INTO playbooks (name, description, enabled, nodes) VALUES
('[TEST-AI] G1', 'AI 테스트: file_download + test_ai에 G1 포함 시 AI 분석 (gemma4)', FALSE,
 $json$[{ "type": "condition", "name": "파일 다운로드", "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
        { "type": "condition", "name": "G1", "when": { "field": "test_ai", "op": "contains", "value": "G1" } },
        { "type": "ai_response", "model": "gemma4:12b",
          "prompt": "파일 다운로드 이벤트다. 다운로드된 파일의 VirusTotal 판정과 Sigma 탐지를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] G2', 'AI 테스트: process_execution + test_ai에 G2 포함 시 AI 분석 (gemma4)', FALSE,
 $json$[{ "type": "condition", "name": "프로세스 실행", "when": { "field": "event_type", "op": "eq", "value": "process_execution" } },
        { "type": "condition", "name": "G2", "when": { "field": "test_ai", "op": "contains", "value": "G2" } },
        { "type": "ai_response", "model": "gemma4:12b",
          "prompt": "프로세스 실행 이벤트다. 실행된 파일의 VirusTotal 판정과 Sigma 탐지를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] G3', 'AI 테스트: email_attachment + test_ai에 G3 포함 시 AI 분석 (gemma4)', FALSE,
 $json$[{ "type": "condition", "name": "이메일 첨부", "when": { "field": "event_type", "op": "eq", "value": "email_attachment" } },
        { "type": "condition", "name": "G3", "when": { "field": "test_ai", "op": "contains", "value": "G3" } },
        { "type": "ai_response", "model": "gemma4:12b",
          "prompt": "이메일 첨부 이벤트다. 송신 IP의 AbuseIPDB 조회 결과와 목적지 도메인의 DNS 조회 결과를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] G4', 'AI 테스트: privilege_escalation + test_ai에 G4 포함 시 AI 분석 (gemma4, 다수결)', FALSE,
 $json$[{ "type": "condition", "name": "권한 상승", "when": { "field": "event_type", "op": "eq", "value": "privilege_escalation" } },
        { "type": "condition", "name": "G4", "when": { "field": "test_ai", "op": "contains", "value": "G4" } },
        { "type": "ai_response", "model": "gemma4:12b",
          "prompt": "권한 상승 이벤트다. 이벤트에 포함된 모든 근거를 종합하여 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] P1', 'AI 테스트: login_failure + test_ai에 P1 포함 시 AI 분석 (phi4)', FALSE,
 $json$[{ "type": "condition", "name": "로그인 실패", "when": { "field": "event_type", "op": "eq", "value": "login_failure" } },
        { "type": "condition", "name": "P1", "when": { "field": "test_ai", "op": "contains", "value": "P1" } },
        { "type": "ai_response", "model": "phi4:14b",
          "prompt": "로그인 실패 이벤트다. 출발지 IP의 AbuseIPDB 조회 결과와 로그인 시도 횟수를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] P2', 'AI 테스트: port_scan + test_ai에 P2 포함 시 AI 분석 (phi4)', FALSE,
 $json$[{ "type": "condition", "name": "포트 스캔", "when": { "field": "event_type", "op": "eq", "value": "port_scan" } },
        { "type": "condition", "name": "P2", "when": { "field": "test_ai", "op": "contains", "value": "P2" } },
        { "type": "ai_response", "model": "phi4:14b",
          "prompt": "포트 스캔 이벤트다. 출발지 IP의 AbuseIPDB 조회 결과와 스캔 규모를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] P3', 'AI 테스트: privilege_escalation + test_ai에 P3 포함 시 AI 분석 (phi4, 다수결)', FALSE,
 $json$[{ "type": "condition", "name": "권한 상승", "when": { "field": "event_type", "op": "eq", "value": "privilege_escalation" } },
        { "type": "condition", "name": "P3", "when": { "field": "test_ai", "op": "contains", "value": "P3" } },
        { "type": "ai_response", "model": "phi4:14b",
          "prompt": "권한 상승 이벤트다. 관련 파일의 VirusTotal 판정과 Sigma 탐지를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb),
('[TEST-AI] P4', 'AI 테스트: privilege_escalation + test_ai에 P4 포함 시 AI 분석 (phi4, 다수결)', FALSE,
 $json$[{ "type": "condition", "name": "권한 상승", "when": { "field": "event_type", "op": "eq", "value": "privilege_escalation" } },
        { "type": "condition", "name": "P4", "when": { "field": "test_ai", "op": "contains", "value": "P4" } },
        { "type": "ai_response", "model": "phi4:14b",
          "prompt": "권한 상승 이벤트다. 출발지 IP의 AbuseIPDB 조회 결과를 중심으로 위험도를 판단하라.",
          "actions": { "HIGH": "DENY", "MEDIUM": "HOLD", "LOW": "DETECT" } }]$json$::jsonb);