# 플레이북 설계

> **Playbook Engine**
> 보안 이벤트에 등록된 플레이북을 병렬 평가하고, 조건을 만족한 플레이북의 `action`을 다수결로 집계하여 최종 대응을 결정한다.

```text
RAW EVENT
   │
   ▼
 Vector
   │
   ▼
 Enrichment
   │
   ▼
┌──────────────────────────────┐
│       Playbook Engine        │
│                              │
│  ① 활성 Playbook 병렬 평가   │
│  ② Condition AND/OR 평가     │
│  ③ Response / AI Response    │
│  ④ Action 결과 집계          │
│  ⑤ 과반수(> 50%) 결정        │
└──────────────────────────────┘
   │
   ├── Redis / Dragonfly
   │      └─ AI 결과 캐시
   │
   ├── Ollama
   │      └─ AI 분석
   │
   ▼
OpenSearch + ClickHouse
```

### 핵심 동작

| 항목                      | 내용                                                           |
| ----------------------- | ------------------------------------------------------------ |
| **Playbook 저장**         | PostgreSQL                                                   |
| **Runtime 캐시**          | 메모리(`RwLock<Arc<Vec<Playbook>>>`), TTL 없음, CRUD 시 전체 교체 |
| **평가 방식**               | 활성 Playbook 병렬 평가                                            |
| **조건 평가**               | Playbook 내부 Condition은 `AND`, Condition 내부는 `AND / OR` 중첩 가능 |
| **종료 노드**               | `Response` 또는 `AI Response` 정확히 1개                           |
| **Action**              | `DENY` / `DETECT` / `HOLD`                                   |
| **AI 처리** | Redis/Dragonfly 조회 → 없으면 Ollama 호출(같은 요청 1회, 동시 1개) → 성공 시 TTL 캐시 / 캐시 장애 시 Ollama 호출 없이 에러(→ HOLD) |
| **최종 결정**               | 특정 Action이 전체 결과의 **50% 초과** 시 해당 Action                     |
| **과반수 없음**              | `HOLD`                                                       |
| **조건을 만족한 Playbook 없음** | 원본 이벤트 그대로 저장 (`playbook_result` 없음)                         |
| **판정 결과가 1개 이상 존재**     | 원본 이벤트 + `playbook_result` 저장                                |

### 실행 흐름

```text
Playbook Cache
      │
      ├──────── Playbook 1 ──→ Action
      ├──────── Playbook 2 ──→ Action
      ├──────── Playbook 3 ──→ Action
      └──────── Playbook N ──→ Action
                              │
                              ▼
                       Action 집계
                              │
                    ┌─────────┴─────────┐
                    │                   │
              과반수(>50%)          과반수 없음
                    │                   │
                    ▼                   ▼
              해당 Action             HOLD
```

### 플레이북 구조

```text
Condition 0..N
      │
      ▼
Response / AI Response 1개
```

**Node**

* `condition` : 이벤트 조건 평가
* `response` : 고정 Action 결정
* `ai_response` : AI 위험도 분석 후 Action 결정

**결과**

* 조건 불일치 → 해당 Playbook 기록하지 않음
* 조건 일치 + Action 결정 → `playbook_result.playbooks[]`에 기록
* 최종 결과는 OpenSearch / ClickHouse에 저장

## 0. 플레이북 동작 상세

### 1) 도커 설치 단계
- OpenSearch 전용 템플릿 / ClickHouse create문 / Postgres 샘플 플레이북 설정

### postgres
```sql
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS playbooks (
    id         BIGSERIAL PRIMARY KEY,          -- 플레이북 ID
    name       VARCHAR(100) NOT NULL,          -- 플레이북 이름
    description VARCHAR(1000) NOT NULL DEFAULT '', -- 플레이북 설명
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,  -- 활성 여부
    is_delete  BOOLEAN NOT NULL DEFAULT FALSE, -- 논리 삭제 여부
    nodes      JSONB NOT NULL CHECK (jsonb_typeof(nodes) = 'array'),
                                               -- 플레이북 노드 배열
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 생성 시각
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- 수정 시각
);

DROP TRIGGER IF EXISTS tr_playbooks_updated_at ON playbooks;

CREATE TRIGGER tr_playbooks_updated_at
    BEFORE UPDATE ON playbooks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

샘플 insert
```sql
INSERT INTO playbooks (name, nodes) VALUES (
  '파일 다운로드 차단',
  $json$[
    { "type": "condition", "name": "파일 다운로드만",
      "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
    { "type": "response", "action": "DENY" }
  ]$json$::jsonb
);
```

### opensearch
```json
PUT _index_template/event-logs
{
  "index_patterns": ["event-logs-*"],   // 인덱스 이름: event-logs-YYYY.MM.DD (날짜는 ingested_at 기준)
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,          // 단일 노드
      "index.mapping.ignore_malformed": true  // 숫자·날짜·IP 타입이 안 맞아도 문서를 거부하지 않고 그 필드만 인덱싱 제외(이 설정이 없으면 타입 맞지 않는 필드가 있는 경우 해당 로그 하나를 통째로 날리게 됨)
    },
    "mappings": {
      "dynamic_templates": [
        // 아래 properties에 없는 문자열 필드는 keyword로 저장 (text 아님)
        { "strings_as_keyword": {
            "match_mapping_type": "string",
            "mapping": { "type": "keyword", "ignore_above": 1024 } } } // 값이 1024자를 넘으면 인덱싱하지 않음
      ],
      "properties": {
        // ── 이벤트 원본 (발신 측이 채움) ──
        "event_id":        { "type": "keyword", "ignore_above": 1024 },  // 이벤트 ID
        "event_type":      { "type": "keyword", "ignore_above": 1024 },  // 이벤트 종류 (file_download 등)
        "timestamp":       { "type": "date" },                           // 이벤트 발생 시각 (발신 측 값, 신뢰하지 않음)
        "src_ip":          { "type": "ip" },                             // 출발 IP
        "dst_domain":      { "type": "keyword", "ignore_above": 1024 }, // 목적지 도메인
        "file_name":       { "type": "keyword", "ignore_above": 1024 }, // 파일명
        "file_sha256":     { "type": "keyword", "ignore_above": 1024 }, // 파일 SHA-256
        "file_size_bytes": { "type": "long" },                           // 파일 크기(byte)

        // ── 백엔드가 찍는 값 ──
        "ingested_at":     { "type": "date" },                           // 백엔드 처리 시작 시각

        // ── enrichment (나머지 필드는 동적 매핑) ──
        "enrichment_dst_domain": { "properties": { "data": { "properties": {
          "resolved_ips": { "type": "ip" }                               // DNS 조회 결과 IP 목록
        } } } },

        // ── 플레이북 판정 (결정한 플레이북이 1개 이상일 때만 존재) ──
        "playbook_result": { "properties": {
          "action":       { "type": "keyword" },                         // 최종 결정: DENY | DETECT | HOLD
          "evaluated":    { "type": "integer" },                         // action을 결정한 플레이북 수
          "processed_at": { "type": "date" },                            // 플레이북 판정 완료 시각
          "playbooks": {
            "type": "nested",                                            // 플레이북별 조건을 같은 항목에서 검색하려고 nested 사용
            "properties": {
              "playbook_id": { "type": "long" },                         // 플레이북 ID
              "name":        { "type": "keyword" },                      // 판정 당시 플레이북 이름
              "action":      { "type": "keyword" },                      // 해당 플레이북의 결정
              "when":        { "type": "keyword", "ignore_above": 4096 },// 조건을 문자열로 렌더링한 값 (과거 기록용)
              "ai_result": { "properties": {                             // AI 대응 노드일 때만
                "status": { "type": "keyword" },                         // success | error
                "risk":   { "type": "keyword" },                         // HIGH | MEDIUM | LOW (success 시)
                "reason": { "type": "keyword", "ignore_above": 4096 },   // 판단 근거 (success 시)
                "error":  { "type": "keyword" },                         // timeout | connection_failed | http_error | invalid_response | cache_unavailable (error 시)
                "model":  { "type": "keyword" }                          // 사용한 모델
              } }
            }
          }
        } }
      }
    }
  }
}
```

### clickhouse
```sql
CREATE TABLE IF NOT EXISTS shire.event_logs (
    event_id         String,                            -- 이벤트 ID (클릭하우스는 일반 RDBMS와 다르므로 별도 id 컬럼 사용 안함)
    event_type       LowCardinality(String),            -- 이벤트 종류 (file_download 등)
    event_time       DateTime64(3, 'UTC'),              -- 이벤트 발생 시각 (timestamp 파싱값)
    ingested_at      DateTime64(3, 'UTC'),              -- 백엔드 처리 시작 시각 (OpenSearch의 ingested_at과 같은 값)
    playbook_action  LowCardinality(Nullable(String)),  -- 플레이북 최종 결정: DENY / DETECT / HOLD (결정한 플레이북이 없으면 NULL)
    payload          JSON                               -- 원본 + enrichment + playbook_result 전체 (OpenSearch 문서와 동일)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)                        -- 월 단위 파티션
ORDER BY (event_type, event_time, event_id);             -- 정렬 키
```


### 2) Rust Axum 실행 시
- Postgres playbook 테이블에서 활성 플레이북(enabled=true, is_delete=false)을 로드하여
  메모리(RwLock<Arc<Vec<Playbook>>>)에 통째로 보관 (TTL 없음)
- 매번 Postgres를 조회하지 않음
- 로드 실패 처리: Postgres 접속/쿼리 실패는 기동 실패(fail-fast), 개별 플레이북의 JSON 파싱 실패는 그 플레이북만 건너뛰고 warn 로그

### 3) 플레이북 노드 동작
- 플레이북 구성: 조건 노드 0개 이상 + 종료 노드(대응 또는 AI 대응) 정확히 1개(맨 끝)
- 활성 플레이북(enabled=true, 삭제되지 않은 것)은 병렬로 평가하고, 모두 끝난 뒤 다수결로 집계

- 조건 노드는 AND로 직렬 연결
  - 하나라도 거짓이면 해당 플레이북은 해당 없음(기록하지 않음)

- 대응 노드(AI 대응 포함)에서 action이 결정된 경우 "playbook_result"의 playbooks[]에 해당 플레이북 추가

- AI 대응 노드: Redis/Dragonfly에서 ai 분석 결과 조회
  - 있으면 사용하고,
  - Redis/Dragonfly 조회 장애면 Ollama를 호출하지 않고 cache_unavailable 에러 반환
  - 없으면 Ollama 호출 (캐시 키가 같은 요청은 1번만 호출, 동시 호출 1개로 제한) 후 정상 응답만 7일 저장
  - 요청 하나의 전체 시간 상한 60초, 초과 시 timeout 에러
  - 키: `sha256(model + prompt + Evidence(원본 + enrichment 결과), event_id·timestamp 제외)`

### 4) 최종적으로 OpenSearch와 ClickHouse에 저장되는 로그
- 결정한 플레이북이 없는 경우: 원본 로그
- 결정한 플레이북이 1개 이상인 경우: 원본 로그 + `playbook_result`

### 5) 플레이북 API
- 신규 생성 → Postgres insert → 캐시 갱신(refresh)
- 수정 → Postgres update → 캐시 갱신(refresh)
- 삭제 → Postgres update(`is_delete` 컬럼으로 논리적 삭제) → 캐시 갱신(refresh)
- 캐시 갱신은 Postgres 커밋 성공 이후 수행하며, 활성 플레이북 전체를 다시 조회해 통째로 교체함 (개별 갱신 없음)

## 1. 플레이북 다수결 기능

- 플레이북 기본 동작
- 플레이북마다 action 결과가 다른 경우, 과반수(50%)를 초과하는 action으로 결정
- 식: 특정 action의 결정 수 / 실제로 action을 결정한 playbook 수 > 0.5

예시)
- DENY 2개 + DETECT 1개 + HOLD 1개 = 과반수를 DENY가 넘지 못했으므로 최종 HOLD
- DENY 2개 + DETECT 2개 + HOLD 3개 = HOLD
- DENY 2개 + DETECT 2개 + HOLD 1개 = HOLD ← 과반수가 없으므로
- DENY 1개 + DETECT 3개 + HOLD 2개 = HOLD ← DETECT가 과반수 50%를 넘지 못함

## 2. 노드는 3개

### 2.1 조건 노드

- 조건 노드들은 AND로 연결
- 조건 설정 요소: 필드명, 연산자, 값
- 조건은 여러 개 추가 가능하며 and, or 활용 가능

연산자
- 공통: 필드 존재(exists) / 미존재(not_exists)
- 숫자: eq ne gt gte lt lte
- 문자열: eq ne contains starts_with ends_with
- IP: eq ne in_cidr not_in_cidr
- 값의 타입이 맞지 않는 경우에도 거짓

```json
{
  "type": "condition",
  "name": "사내 대역 또는 신뢰 도메인",
  "when": {
    "or": [
      { "field": "src_ip", "op": "in_cidr", "value": "10.0.0.0/8" },
      { "and": [
          { "field": "dst_domain", "op": "not_exists" },
          { "field": "enrichment_src_ip.data.abuse_confidence_score", "op": "lt", "value": 20 }
      ] }
    ]
  }
}
```

중첩 조건 예시
```json
{ "or": [
    { "and": [A, B] },
    { "or": [ { "and": [C, D] }, E ] }
] }
```

**참고사항**
- 포트폴리오이므로 모든 필드 종류를 구현하지 않음
- 예: 배열 value 처리, 대소문자 구분 등은 제외

### 2.2 대응 노드

- action 설정 가능: DENY | DETECT | HOLD

```json
{
  "type": "response",
  "action": "DENY"
}
```

### 2.3 AI 대응 노드

- Ollama 응답이 빠를 것으로 기대하지 않음
- 용도: 다수의 조건 노드 뒤 마지막 AI 대응으로 적용 (복잡한 로그 분석을 AI에게 위임)
- `actions`에 없는 risk는 해당 없음

- model
- prompt
- actions: risk 하나에 action 하나
  - AI 에러 발생 시 매핑과 무관하게 HOLD 처리
  - 매핑값: DENY | HOLD | DETECT

- 입력: prompt는 system 메시지로, Evidence(원본 + enrichment, event_id·timestamp 제외)는 user 메시지로 전달 (분석 지침과 분석 대상 데이터를 구분)
- temperature 0 적용 (같은 입력에는 같은 답이 나오도록 함)
- model은 호출할 모델(AiModel enum, 허용 목록 2개)을 지정하는 값임
- actions 매핑은 모델에 전달하지 않음
```json
{
  "type": "ai_response",
  "model": "qwen2.5:14b-instruct",
  "prompt": "보안 이벤트와 enrichment 결과를 분석하고 위험도를 판단하라.",
  "actions": {
    "HIGH": "HOLD",
    "MEDIUM": "DENY"
  }
}
```

- 출력: `risk`(HIGH | MEDIUM | LOW)와 `reason`만 사용, Ollama `format` schema로 강제

성공
```json
"ai_result": {
  "status": "success",
  "risk": "HIGH",
  "reason": "파일의 악성 탐지 비율이 높고 관련 IOC의 위험도가 높게 확인되었습니다.",
  "model": "qwen2.5:14b-instruct"
}
```

실패
- `error` 값은 `timeout | connection_failed | http_error | invalid_response | cache_unavailable`로 고정
- 에러 시 `risk`, `reason` 필드는 포함하지 않음

```json
"ai_result": { "status": "error", "error": "timeout", "model": "qwen2.5:14b-instruct" }

"ai_result": { "status": "error", "error": "cache_unavailable", "model": "qwen2.5:14b-instruct" }
```

## 3. action 종류

- DENY: 차단
- HOLD: 보류 (사람이 검토)
- DETECT: 탐지 기록만 (차단하지 않음)


## 4. Playbook result json
- 어떤 플레이북도 결정하지 않은 이벤트는 그대로 통과하며 `playbook_result`가 붙지 않음
- action을 결정한 플레이북이 1개 이상인 경우에만 원본 로그에 `playbook_result` 필드 생성

```
"playbook_result": {
  "action": String,        // 플레이북 최종 결정: DENY | DETECT | HOLD
  "evaluated": Integer,    // action을 결정한 플레이북 수 (= playbooks 길이)
  "processed_at": String,  // 엔진이 찍은 판정 시각 (ISO 8601)
  "playbooks": [           // action을 결정한 플레이북 목록
    {
      "playbook_id": Integer,  // 플레이북 ID
      "name": String,          // 플레이북 이름 (판정 당시 이름)
      "action": String,        // 해당 플레이북의 결정: DENY | HOLD | DETECT
      "when": String,          // 조건 노드들을 AND로 이어 붙인 문자열 (조건 노드가 없으면 생략)
      "ai_result": {           // AI 대응 노드일 때만
        "status": String,      // success | error
        "risk": String,        // HIGH | MEDIUM | LOW (성공 시)
        "reason": String,      // 판단 근거 (성공 시)
        "error": String,       // 에러 원인 (에러 시): timeout | connection_failed | http_error | invalid_response | cache_unavailable
        "model": String        // 사용한 모델
      }
    }
  ]
}
```

`when` 문자열 규칙
- 조건 하나: `(필드 연산자 값)`, `exists`/`not_exists`는 값 없이 `(필드 연산자)`로 표기
- and/or 그룹: `(A and B)`, `(A or B)`
- 조건 노드가 여러 개인 경우 각 노드의 문자열을 ` and `로 이어 붙임

예시 - DENY 2개, DETECT 2개로 2/4 = 0.5이므로 과반수 아님 → 최종 HOLD
```json
"playbook_result": {
  "action": "HOLD",
  "evaluated": 4,
  "processed_at": "2026-09-22T10:15:03Z",
  "playbooks": [
    {
      "playbook_id": 1,
      "name": "악성 파일 차단",
      "action": "DENY",
      "when": "(event_type eq file_download) and (enrichment_file_sha256.data.malicious gte 10)"
    },
    {
      "playbook_id": 2,
      "name": "애매한 파일 AI 분석",
      "action": "DENY",
      "when": "(event_type eq file_download) and (enrichment_file_sha256.data.malicious gte 1)",
      "ai_result": {
        "status": "success",
        "risk": "MEDIUM",
        "reason": "파일의 악성 탐지 비율이 높고 관련 IOC의 위험도가 높게 확인되었습니다.",
        "model": "qwen2.5:14b-instruct"
      }
    },
    {
      "playbook_id": 3,
      "name": "사내 대역 예외",
      "action": "DETECT",
      "when": "(src_ip in_cidr 10.0.0.0/8)"
    },
    {
      "playbook_id": 4,
      "name": "저위험 기록",
      "action": "DETECT",
      "when": "(dst_domain not_exists)",
      "ai_result": {
        "status": "success",
        "risk": "LOW",
        "reason": "...",
        "model": "qwen2.5:14b-instruct"
      }
    }
  ]
}
```

## 5. 예시

### 1-1) 플레이북: 조건 노드 1개 + 대응 노드 1개
```json
{
  "id": 1,
  "name": "파일 다운로드 차단",
  "enabled": true,
  "nodes": [
    {
      "type": "condition",
      "name": "파일 다운로드만",
      "when": {
        "field": "event_type",
        "op": "eq",
        "value": "file_download"
      }
    },
    {
      "type": "response",
      "action": "DENY"
    }
  ]
}
```

### 1-2) 1-1 결과 playbook_result
```json
"playbook_result": {
  "action": "DENY",
  "evaluated": 1,
  "processed_at": "2026-09-22T10:15:03Z",
  "playbooks": [
    {
      "playbook_id": 1,
      "name": "파일 다운로드 차단",
      "action": "DENY",
      "when": "(event_type eq file_download)"
    }
  ]
}
```

### 2-1) 플레이북: 조건 노드 1개 + AI 대응 노드 1개
```json
{
  "id": 2,
  "name": "애매한 파일 AI 분석",
  "enabled": true,
  "nodes": [
    {
      "type": "condition",
      "name": "파일 다운로드 + 애매한 구간",
      "when": {
        "and": [
          {
            "field": "event_type",
            "op": "eq",
            "value": "file_download"
          },
          {
            "field": "enrichment_file_sha256.data.malicious",
            "op": "gte",
            "value": 1
          },
          {
            "field": "enrichment_file_sha256.data.malicious",
            "op": "lt",
            "value": 10
          }
        ]
      }
    },
    {
      "type": "ai_response",
      "model": "qwen2.5:14b-instruct",
      "prompt": "보안 이벤트와 enrichment 결과를 분석하고 위험도를 판단하라.",
      "actions": {
        "HIGH": "HOLD",
        "MEDIUM": "DENY",
        "LOW": "DETECT"
      }
    }
  ]
}
```

### 2-2) 2-1의 결과 playbook_result
```json
"playbook_result": {
  "action": "DENY",
  "evaluated": 1,
  "processed_at": "2026-09-22T10:15:03Z",
  "playbooks": [
    {
      "playbook_id": 2,
      "name": "애매한 파일 AI 분석",
      "action": "DENY",
      "when": "((event_type eq file_download) and (enrichment_file_sha256.data.malicious gte 1) and (enrichment_file_sha256.data.malicious lt 10))",
      "ai_result": {
        "status": "success",
        "risk": "MEDIUM",
        "reason": "파일의 악성 탐지 비율이 높고 관련 IOC의 위험도가 높게 확인되었습니다.",
        "model": "qwen2.5:14b-instruct"
      }
    }
  ]
}
```

### 3-1) 플레이북: 조건 노드 3개 + 대응 노드 1개
```json
{
  "id": 3,
  "name": "고위험 실행파일 차단",
  "enabled": true,
  "nodes": [
    {
      "type": "condition",
      "name": "파일 다운로드만",
      "when": {
        "field": "event_type",
        "op": "eq",
        "value": "file_download"
      }
    },
    {
      "type": "condition",
      "name": "악성 탐지 또는 고위험 IP의 대용량 파일",
      "when": {
        "or": [
          {
            "field": "enrichment_file_sha256.data.malicious",
            "op": "gte",
            "value": 10
          },
          {
            "and": [
              {
                "field": "enrichment_src_ip.data.abuse_confidence_score",
                "op": "gte",
                "value": 80
              },
              {
                "field": "file_size_bytes",
                "op": "gt",
                "value": 100000
              }
            ]
          }
        ]
      }
    },
    {
      "type": "condition",
      "name": "실행 파일",
      "when": {
        "field": "file_name",
        "op": "ends_with",
        "value": ".exe"
      }
    },
    {
      "type": "response",
      "action": "DENY"
    }
  ]
}
```

### 3-2) 3-1의 결과 playbook_result: 다수결 DENY (DENY 2개, HOLD 1개)
```json
"playbook_result": {
  "action": "DENY",
  "evaluated": 3,
  "processed_at": "2026-09-22T10:15:03Z",
  "playbooks": [
    {
      "playbook_id": 1,
      "name": "파일 다운로드 차단",
      "action": "DENY",
      "when": "(event_type eq file_download)"
    },
    {
      "playbook_id": 2,
      "name": "애매한 파일 AI 분석",
      "action": "HOLD",
      "when": "((event_type eq file_download) and (enrichment_file_sha256.data.malicious gte 1) and (enrichment_file_sha256.data.malicious lt 10))",
      "ai_result": {
        "status": "success",
        "risk": "HIGH",
        "reason": "파일의 악성 탐지 비율이 높고 관련 IOC의 위험도가 높게 확인되었습니다.",
        "model": "qwen2.5:14b-instruct"
      }
    },
    {
      "playbook_id": 3,
      "name": "고위험 실행파일 차단",
      "action": "DENY",
      "when": "(event_type eq file_download) and ((enrichment_file_sha256.data.malicious gte 10) or ((enrichment_src_ip.data.abuse_confidence_score gte 80) and (file_size_bytes gt 100000))) and (file_name ends_with .exe)"
    }
  ]
}
```

### 4) 번외: HOLD된 playbook_result (DENY 1개, HOLD 1개)
```json
"playbook_result": {
  "action": "HOLD",
  "evaluated": 2,
  "processed_at": "2026-09-22T10:16:41Z",
  "playbooks": [
    {
      "playbook_id": 1,
      "name": "파일 다운로드 차단",
      "action": "DENY",
      "when": "(event_type eq file_download)"
    },
    {
      "playbook_id": 2,
      "name": "애매한 파일 AI 분석",
      "action": "HOLD",
      "when": "((event_type eq file_download) and (enrichment_file_sha256.data.malicious gte 1) and (enrichment_file_sha256.data.malicious lt 10))",
      "ai_result": {
        "status": "error",
        "error": "timeout",
        "model": "qwen2.5:14b-instruct"
      }
    }
  ]
}
```