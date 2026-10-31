# API Documentation

[한국어](API.md) | **English**

**Document Links** | [README](../README.en.md) | [Development Guide](DEVELOPMENT.en.md) | [Demo Document](DEMO.en.md)

A document organized so that you can check the Shire Guard API at a glance and move to the detailed Request·Response contents

## Base URL

| Target | Address | Purpose |
|---|---|---|
| Vector | `http://localhost:8081` | Event collection |
| Rust Backend | `http://localhost:3001` | Server status check, Shire Guard API |

- Ports are written based on the default values in `docker/.env.example` (Vector: `VECTOR_PORT`, Rust Backend: fixed to `3001` in the code)

```bash
# Rust Backend: find route definitions (run from the repository root)
grep -rn "\.route(" backend/src

# Vector: find the event receiving configuration
grep -n -A3 "type = \"http_server\"" docker/vector/vector.toml
```

## API Overview

| Function | Target | API Method & URL | Request | Response |
|---|---|---|---|---|
| Event collection | Vector | `POST /` | [Request](#event-ingest-request) | [Response](#event-ingest-response) |
| Server status check | Rust Backend | `GET /health` | [Request](#health-request) | [Response](#health-response) |
| Metric query | Rust Backend | `GET /metrics` | [Request](#metrics-request) | [Response](#metrics-response) |
| Playbook API | Rust Backend | Planned | - | - |
| Log query API | Rust Backend | Planned | - | - |
| Report generation API | Rust Backend | Planned | - | - |

## Common Response (Rust Backend API)

- The response format applied to the Shire Guard API of the Rust Backend (Playbook · log query · report generation)
- Event collection (Vector), `/health`, and `/metrics` do not follow this format (see each API's details)

### Success - Includes Single Data

```json
{
  "success": true,
  "status": 200,
  "data": {}
}
```

### Success - Includes List Data and meta

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

- `count`: number of data items included in the current page
- `total`: total number of search results
- `page`: current page number
- `size`: number of data items per page
- `total_pages`: total number of pages

### Success - No Data

```json
{
  "success": true,
  "status": 201
}
```

### Failure

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

- `message` is returned in Korean as it is by the server (meaning: "This API does not exist.")

| HTTP Status | code | Meaning |
|---:|---|---|
| 400 | `BAD_REQUEST` | Invalid request |
| 403 | `FORBIDDEN` | Insufficient access permission |
| 404 | `NOT_FOUND` | The target or API does not exist |
| 422 | `VALIDATION_ERROR` | Request value validation failed |
| 500 | `INTERNAL_SERVER_ERROR` | Internal server error |

### Scope of Error Information Provided

- For security, the detailed cause of internal server errors is not exposed in the Response
- Every response of the Rust Backend includes the `x-request-id` Header, and the detailed cause can be checked in the backend logs with the same ID

---
---
---
---
---

## 1. Event Collection API

An input API through which external security devices·services send events to Shire Guard (received by Vector)

<a id="event-ingest-request"></a>
### Event Collection Request

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

- **[Note]** When testing the API, Vector reads the request body line by line, so the JSON must be written **on a single line** without line breaks (if sent on multiple lines like the example above, a `400` error is returned)
```json
{"event_id":"evt-bruno-04","event_type":"login_failure","timestamp":"2026-10-21T00:00:00Z","source_ip":"185.220.101.45","username":"admin","attempt_count":12,"target_service":"ssh"}
```

| Field | Required | Description |
|---|---|---|
| `event_id` | O | Event ID (string) |
| `timestamp` | O | Time the event occurred (string) |
| `event_type` | | Event type; if missing, Vector fills it with `unknown` |
| `source_ip` | | Source IP → reputation lookup with AbuseIPDB |
| `destination_domain` | | Destination domain → DNS lookup |
| `file_sha256` | | File hash → reputation lookup with VirusTotal |
| Other fields | | Stored as the original, can be used in playbook conditions |

- Events that are missing required fields or have an invalid format are not discarded; the original is kept in PostgreSQL `storage_failures`
- The fields below are added automatically by Vector, so do not send them: `ingested_at` (collection time), `path`, `source_type`

**Examples by Event Type**

```json
// Login failure (AbuseIPDB lookup only)
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
// DNS beacon (AbuseIPDB + DNS lookup)
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

**curl Example** (run in Git Bash)

```bash
curl -X POST http://localhost:8081/ -H "Content-Type: application/json" -d '{"event_id":"evt-1","event_type":"login_failure","timestamp":"2026-10-09T20:47:39Z","source_ip":"185.220.101.45","username":"admin","attempt_count":12,"target_service":"ssh"}'
```

<a id="event-ingest-response"></a>
### Event Collection Response

| HTTP Status | Meaning |
|---:|---|
| 200 | Confirmed as stored in Kafka (or RedPanda) (no response body) |
| 400 | JSON format error (`{"code": 400, "message": "Failed decoding body: ..."}`) |
| 404 | Path error (a path other than `/`) |
| Others | Storage failed; the sender needs to retry |

- Vector responds with 200 only after storage in the broker is confirmed (acknowledgements setting)
- If it cannot connect to the broker, it waits without responding (because Vector waits for the storage confirmation)

---

## 2. Server Status API

<a id="health-request"></a>
### Server Status Check Request

```http
GET http://localhost:3001/health
```

<a id="health-response"></a>
### Server Status Check Response

```text
ok
```

- Responds with the plain string, not the common response format

<a id="metrics-request"></a>
### Metric Query Request

```http
GET http://localhost:3001/metrics
```

<a id="metrics-response"></a>
### Metric Query Response

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

- Text format for Prometheus collection; Prometheus collects it every 15 seconds
- The `# HELP` descriptions are returned in Korean as they are by the server. Their meanings are as follows:

| Metric | Meaning of `# HELP` |
|---|---|
| `shireguard_cache_key_count` | Number of keys stored in the Enrichment cache (Redis/Dragonfly) |
| `shireguard_config_active_broker` | Current MESSAGE_BROKER setting value (only the corresponding broker label is exposed as 1) |
| `shireguard_config_active_cache` | Current CACHE_BACKEND setting value (only the corresponding cache label is exposed as 1) |
| `shireguard_consumer_broker_connected` | Current connection status between the Consumer and the Broker (0=disconnected, 1=connected) |
| `shireguard_consumer_events_failed_total` | Number of events that failed RawEvent deserialization (including empty messages) |
| `shireguard_consumer_events_parsed_total` | Number of events that succeeded in RawEvent deserialization |
| `shireguard_consumer_messages_received_total` | Number of messages the Consumer received from the broker (Kafka/RedPanda) |
| `shireguard_storage_events_sent_total` | Number of logs whose storage completed in both OpenSearch and ClickHouse |
| `shireguard_storage_rejected_total` | Number of logs that OpenSearch rejected due to a problem with the doc itself (400) and that were recorded in storage_failures (0 if normal) |

---

## 3. Playbook API (Planned)

- Playbook create / update / delete / query

## 4. Log Query API (Planned)

- Query / conditional search of event logs and playbook decision results stored in OpenSearch

## 5. Report Generation API (Planned)

- Generate HWPX / DOCX reports with ClickHouse aggregation
