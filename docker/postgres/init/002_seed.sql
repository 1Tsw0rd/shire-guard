-- 샘플 플레이북 (개발·데모용, 첫 기동 시 1회 실행)
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
  '파일 다운로드 차단',
  '모든 파일 다운로드 이벤트를 DENY 처리',
  TRUE,
  $json$[
    { "type": "condition", "name": "파일 다운로드만",
      "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
    { "type": "response", "action": "DENY" }
  ]$json$::jsonb
),
(
  '애매한 파일 AI 분석',
  'VirusTotal 악성 탐지가 1~9개인 파일 다운로드를 AI로 분석하고, 위험도에 따라 HOLD/DENY/DETECT 처리',
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
      "actions": { "HIGH": "HOLD", "MEDIUM": "DENY", "LOW": "DETECT" } }
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
);