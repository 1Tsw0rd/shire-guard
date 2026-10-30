# API Documentation

**한국어** | [English](API.en.md)

**문서 링크** | [README](../README.md) | [개발 가이드](DEVELOPMENT.md) | [데모 문서](DEMO.md)

Shire Guard의 API를 한눈에 확인하고, Request·Response 상세 내용으로 이동할 수 있도록 정리한 문서

## Base URL

| 대상 | 주소 | 용도 |
|---|---|---|
| Vector | `http://localhost:8081` | 이벤트 수집 |
| Rust Backend | `http://localhost:3001` | 서버 상태 확인, Shire Guard API |

- 포트는 `docker/.env.example` 기본값 기준으로 작성함 (Vector: `VECTOR_PORT`, Rust Backend: 코드에 `3001` 고정)

```bash
# Rust Backend: 라우트 정의 찾기 (저장소 루트에서 실행)
grep -rn "\.route(" backend/src

# Vector: 이벤트 수신 설정 찾기
grep -n -A3 "type = \"http_server\"" docker/vector/vector.toml
```

## API Overview

| 기능 | 대상 | API Method & URL | Request | Response |
|---|---|---|---|---|
| 이벤트 수집 | Vector | `POST /` | [Request](#event-ingest-request) | [Response](#event-ingest-response) |
| 서버 상태 확인 | Rust Backend | `GET /health` | [Request](#health-request) | [Response](#health-response) |
| Metric 조회 | Rust Backend | `GET /metrics` | [Request](#metrics-request) | [Response](#metrics-response) |
| Playbook API | Rust Backend | 구현 예정 | - | - |
| 로그 조회 API | Rust Backend | 구현 예정 | - | - |
| 보고서 생성 API | Rust Backend | 구현 예정 | - | - |

## 공통 응답 (Rust Backend API)

- Rust Backend의 Shire Guard API(Playbook·로그 조회·보고서 생성)에 적용되는 응답 형식
- 이벤트 수집(Vector), `/health`, `/metrics`는 이 형식을 따르지 않음 (각 API 상세 참고)

### 성공 - 단일 Data 포함

```json
{
  "success": true,
  "status": 200,
  "data": {}
}
```

### 성공 - 목록 Data와 meta 포함

```json
{
  "success": true,
  "status": 200,
  "data": [],
  "meta": {
    "count": 2,
    "total": 5,
    "page": 1,
    "size": 2,
    "total_pages": 3
  }
}
```

- `count`: 현재 페이지에 포함된 데이터 개수
- `total`: 전체 검색 결과 개수
- `page`: 현재 페이지 번호
- `size`: 페이지당 데이터 개수
- `total_pages`: 전체 페이지 수

### 성공 - Data 없음

```json
{
  "success": true,
  "status": 201
}
```

### 실패

```json
{
  "success": false,
  "status": 404,
  "error": {
    "code": "NOT_FOUND",
    "message": "존재하지 않는 API입니다."
  }
}
```

| HTTP 상태 | code | 의미 |
|---:|---|---|
| 400 | `BAD_REQUEST` | 잘못된 요청 |
| 403 | `FORBIDDEN` | 접근 권한 부족 |
| 404 | `NOT_FOUND` | 대상 또는 API가 없음 |
| 422 | `VALIDATION_ERROR` | 요청 값 검증 실패 |
| 500 | `INTERNAL_SERVER_ERROR` | 서버 내부 오류 |

### 오류 정보 제공 범위

- 보안을 위해 서버 내부의 상세 오류 원인은 Response에 노출하지 않음
- Rust Backend의 모든 응답에는 `x-request-id` Header가 포함되며, 같은 ID로 백엔드 로그에서 상세 원인 확인 가능

---
---
---
---
---

## 1. 이벤트 수집 API

외부 보안 장비·서비스가 Shire Guard로 이벤트를 보내는 입력 API (Vector가 수신)

<a id="event-ingest-request"></a>
### 이벤트 수집 Request

```http
POST http://localhost:8081/
Content-Type: application/json
```

```json
{
  "event_id": "evt-1",
  "event_type": "file_download",
  "timestamp": "2026-10-09T20:47:39Z",
  "source_ip": "144.48.243.18",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f",
  "file_size_bytes": 245760
}
```

- **[참고사항]** API 테스트할 때는 Vector는 요청 본문을 줄 단위로 읽으므로, JSON은 줄바꿈 없이 **한 줄로** 작성해야 함 (위 예시처럼 여러 줄로 보내면 `400` 오류 반환)
```json
{"event_id":"evt-bruno-04","event_type":"login_failure","timestamp":"2026-10-21T00:00:00Z","source_ip":"185.220.101.45","username":"admin","attempt_count":12,"target_service":"ssh"}
```

| 필드 | 필수 | 설명 |
|---|---|---|
| `event_id` | O | 이벤트 ID (문자열) |
| `timestamp` | O | 이벤트 발생 시각 (문자열) |
| `event_type` | | 이벤트 종류, 없으면 Vector가 `unknown`으로 채움 |
| `source_ip` | | 출발지 IP → AbuseIPDB로 평판 조회 |
| `destination_domain` | | 목적지 도메인 → DNS 조회 |
| `file_sha256` | | 파일 해시 → VirusTotal로 평판 조회 |
| 그 외 필드 | | 원본 그대로 저장, 플레이북 조건에 사용 가능 |

- 필수 필드가 없거나 형식이 맞지 않는 이벤트는 버리지 않고 PostgreSQL `storage_failures`에 원문 보관
- 아래 필드는 Vector가 자동으로 추가하므로 보내지 않음: `ingested_at`(수집 시각), `path`, `source_type`

**이벤트 종류별 예시**

```json
// 로그인 실패 (AbuseIPDB만 조회)
{
  "event_id": "evt-2",
  "event_type": "login_failure",
  "timestamp": "2026-10-09T20:47:39Z",
  "source_ip": "185.220.101.45",
  "username": "admin",
  "attempt_count": 12,
  "target_service": "ssh"
}
```

```json
// DNS 비컨 (AbuseIPDB + DNS 조회)
{
  "event_id": "evt-3",
  "event_type": "dns_beacon",
  "timestamp": "2026-10-09T20:47:39Z",
  "source_ip": "198.51.100.77",
  "destination_domain": "telemetry-sync.info",
  "hostname": "WKS-1183",
  "request_interval_seconds": 60
}
```

**curl 예시** (Git Bash에서 실행)

```bash
curl -X POST http://localhost:8081/ -H "Content-Type: application/json" -d '{"event_id":"evt-1","event_type":"login_failure","timestamp":"2026-10-09T20:47:39Z","source_ip":"185.220.101.45","username":"admin","attempt_count":12,"target_service":"ssh"}'
```

<a id="event-ingest-response"></a>
### 이벤트 수집 Response

| HTTP 상태 | 의미 |
|---:|---|
| 200 | Kafka(또는 RedPanda) 저장까지 확인됨 (응답 본문 없음) |
| 400 | JSON 형식 오류 (`{"code": 400, "message": "Failed decoding body: ..."}`) |
| 404 | 경로 오류 (`/` 이외의 경로) |
| 그 외 | 저장 실패, 보낸 쪽에서 재시도 필요 |

- Vector는 브로커 저장이 확인된 뒤에만 200을 응답 (acknowledgements 설정)
- 브로커에 연결할 수 없으면 응답 없이 대기 (Vector가 저장 확인을 기다리기 때문)

---

## 2. 서버 상태 API

<a id="health-request"></a>
### 서버 상태 확인 Request

```http
GET http://localhost:3001/health
```

<a id="health-response"></a>
### 서버 상태 확인 Response

```text
ok
```

- 공통 응답 형식이 아닌 문자열 그대로 응답

<a id="metrics-request"></a>
### Metric 조회 Request

```http
GET http://localhost:3001/metrics
```

<a id="metrics-response"></a>
### Metric 조회 Response

```text
# HELP shireguard_cache_key_count Enrichment 캐시(Redis/Dragonfly)에 저장된 키 개수
# TYPE shireguard_cache_key_count gauge
shireguard_cache_key_count 0

# HELP shireguard_config_active_broker 현재 MESSAGE_BROKER 설정값 (해당 broker label만 1로 노출)
# TYPE shireguard_config_active_broker gauge
shireguard_config_active_broker{broker="redpanda"} 1

# HELP shireguard_config_active_cache 현재 CACHE_BACKEND 설정값 (해당 cache label만 1로 노출)
# TYPE shireguard_config_active_cache gauge
shireguard_config_active_cache{cache="dragonfly"} 1

# HELP shireguard_consumer_broker_connected Consumer와 Broker 현재 연결 상태 (0=끊김, 1=연결됨)
# TYPE shireguard_consumer_broker_connected gauge
shireguard_consumer_broker_connected 1

# HELP shireguard_consumer_events_failed_total RawEvent 역직렬화에 실패한 이벤트 수 (빈 메시지 포함)
# TYPE shireguard_consumer_events_failed_total counter
shireguard_consumer_events_failed_total 0

# HELP shireguard_consumer_events_parsed_total RawEvent 역직렬화에 성공한 이벤트 수
# TYPE shireguard_consumer_events_parsed_total counter
shireguard_consumer_events_parsed_total 0

# HELP shireguard_consumer_messages_received_total Consumer가 브로커(Kafka/RedPanda)로부터 수신한 메시지 수
# TYPE shireguard_consumer_messages_received_total counter
shireguard_consumer_messages_received_total 0

# HELP shireguard_storage_events_sent_total OpenSearch와 ClickHouse에 모두 저장 완료된 로그 수
# TYPE shireguard_storage_events_sent_total counter
shireguard_storage_events_sent_total 0

# HELP shireguard_storage_rejected_total OpenSearch가 doc 자체 문제(400)로 거절해 storage_failures에 기록한 로그 수 (정상이면 0)
# TYPE shireguard_storage_rejected_total counter
shireguard_storage_rejected_total 0
```

- Prometheus 수집용 텍스트 형식, Prometheus가 15초마다 수집

---

## 3. Playbook API (구현 예정)

- 플레이북 생성 / 수정 / 삭제 / 조회

## 4. 로그 조회 API (구현 예정)

- OpenSearch에 저장된 이벤트 로그와 플레이북 판정 결과 조회 / 조건 검색

## 5. 보고서 생성 API (구현 예정)

- ClickHouse 집계로 HWPX / DOCX 보고서 생성