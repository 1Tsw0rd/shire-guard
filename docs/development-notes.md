## 임시 아키텍처
                  RAW EVENT
                      │
                      ▼
                 Vector
               (Normalize)
                      │
                      ▼
                    Kafka/RedPanda
                      │
                      ▼
              ┌─────────────────┐
              │ Rust Enrichment │
              └───────┬─────────┘
          ┌───────────┼───────────┐
          ▼           ▼           ▼
     AbuseIPDB       DNS      VirusTotal
     (IP 평판)      (도메인/    (해시/URL/
                    A레코드)    IP 평판)
          │           │           │
          └───────────┼───────────┘
                      ▼
              Enriched Evidence
           (검증된 사실 — Source of Truth)
           ※ IP/해시 판정 결과는 Redis/Dragonfly에 캐싱
                      │
                      ▼
                Playbook Engine
        (설정 조회는 Moka 인메모리 캐시 활용 — 로드맵)
                      │
                      ▼
                Condition Node
              (명백한 케이스 판별)
                      │
              ┌───────┴───────┐
              ▼               ▼
         명백한 케이스        애매한 케이스
              │               │
              ▼               ▼
       Block / Pass    ┌─────────────────┐
       (룰 기반 즉시)   │  AI Analysis Node │
              │        ├─────────────────┤
              │        │ Model: [선택]     │
              │        │  Qwen2.5-14B      │
              │        │  Foundation-Sec-8B│
              │        │                   │
              │        │ Prompts: [+ 추가]  │
              │        └─────────┬─────────┘
              │                  ▼
              │           risk == HIGH ?
              │                  │
              │          ┌───────┴───────┐
              │          ▼               ▼
              │        Block            Pass
              │          │               │
              └──────────┴───────┬───────┘
                                 ▼
                         Playbook Log 저장
                    (action, action_source,
                     ai_reason, risk, timestamp)
                                 │
                         ┌───────┴───────┐
                         ▼               ▼
                   OpenSearch       ClickHouse
                  (조회/검색용)      (집계/통계용)

## 모니터링 아키텍처

     ┌────────────────┐       ┌──────────────────────────────┐
     │    cAdvisor    │       │       Monitored Services     │
     │                │       │                              │
     │ 모든 컨테이너의  │       │ Vector                       │
     │ 리소스 수집      │       │ Kafka / RedPanda            │
     │                │       │ Redis / Dragonfly            │
     │ CPU / Memory   │       │ PostgreSQL                   │
     │ Network        │       │ OpenSearch                   │
     │ UP / DOWN      │       │ ClickHouse                   │
     └───────┬────────┘       │ Rust Axum                    │
             │                │ Ollama                       │
             │                │                              │
             │                │ Pipeline Metrics             │
             │                └──────────────┬───────────────┘
             │                               │
             └───────────────┬───────────────┘
                             │
                   Prometheus scrape (Metrics)
                             │
                             ▼
                    ┌──────────────────────┐
                    │      Prometheus      │
                    │    메트릭 수집 · 저장  │
                    └──────────┬───────────┘
                               │
                         PromQL 조회
                               │
                               ▼
                    ┌──────────────────────┐
                    │       Grafana        │
                    │      시각화 · 알림     │
                    └──────────────────────┘

## 보고서 생성 - hwpx
* 단건 이벤트 보고서
* 집계(주간) 이벤트 보고서

## 임시 Playbook Log (모든 경로가 여기로 합류)
```json
{
  "event_id": "evt-143348",
  "action": "BLOCK_IP",
  "action_source": "condition",     // "condition" 또는 "ai_analysis"
  "ai_reason": null,                // condition이면 null, ai_analysis면 report.summary
  "risk": "HIGH",
  "timestamp": "..."
}
```
action_source로 룰 기반 차단인지 AI 판단 차단인지 구분
ai_reason은 AI Analysis Node를 거친 경우에만 채워짐 (룰 기반이면 null)

## 임시 AI Analysis Node 스펙
```json
{
  "node_type": "ai_analysis",
  "model": "qwen2.5-14b",          // 또는 "foundation-sec-8b" — 선택 가능
  "custom_prompts": [               // + 버튼으로 자유 추가
    "IP가 특정 국가에서 접속한 경우 지리적 리스크를 추가 평가하세요."
  ],
  "output_schema": {
    "risk": "HIGH|MEDIUM|LOW",
    "summary": "string",
    "analysis": "string",
    "recommendations": ["string"]
  }
}
```
모델은 Qwen 단독 또는 Foundation-Sec 단독 중 하나만 선택 (2단계 검증은 제외)
custom_prompts는 기본 시스템 프롬프트에 이어붙여서 사용
출력은 항상 동일한 report 스키마로 강제

### AI Analysis Node 실행 시 입력 구성

노드 스펙(모델, 프롬프트 설정)은 정적이며, 실제 실행 시점에 
Enriched Evidence(해당 이벤트의 동적 데이터)와 결합되어 AI에게 전달된다.

  최종 프롬프트 = base_system_prompt + custom_prompts + Enriched Evidence

---

## ⚙️ 환경 변수 (.env)

민감 정보(비밀번호 등)와 포트 설정은 docker/.env에서 관리.
최초 실행 전 아래처럼 파일을 만들어야 함:

```bash
cp docker/.env.example docker/.env
# 값 채우기
```

## 🐳 AI 모델 (Ollama)

### 0. GPU 패스스루 확인
> 참고용, 문제 발생 시 점검용 (Docker가 GPU 접근 가능한지 확인)

```bash
docker run --rm --gpus all nvidia/cuda:12.4.0-base-ubuntu22.04 nvidia-smi
```

### 1. Ollama 컨테이너 실행

```bash
docker compose -f docker-compose.yml -f docker-compose.ollama.yml up -d ollama
```

### 2. 모델 pull

Qwen2.5-14B, Foundation-Sec-8B 두 모델을 설치한다.

```bash
docker exec -it ollama ollama pull qwen2.5:14b-instruct
docker exec -it ollama ollama pull hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF
```

### 3. 설치된 모델 목록 확인

```bash
docker exec -it ollama ollama list
```

### 4. 통신 방법

실제 서비스 코드에서 사용할 방식 — REST API 호출

```bash
curl http://localhost:11434/api/chat -d '{
  "model": "qwen2.5:14b-instruct",
  "messages": [{"role": "user", "content": "..."}],
  "stream": false,
  "keep_alive": "30m"
}'
```

**요청 필드 설명**

| 필드 | 설명 |
|---|---|
| `model` | 호출할 모델 이름. 설치된 2개 모델 중 하나를 지정 |
| `messages` | 대화 내용을 배열로 전달. `role`(발화자)과 `content`(내용)로 구성 |
| `stream` | 응답을 실시간 스트리밍할지 여부. `false`면 전체 응답 완성 후 한 번에 반환 |
| `keep_alive` | 응답 후 모델을 GPU 메모리에 유지할 시간. `"30m"`이면 재요청 시 재로딩 없이 즉시 응답 |

**모델 지정값**

| 모델 | `model` 값 |
|---|---|
| Qwen2.5-14B | `qwen2.5:14b-instruct` |
| Foundation-Sec-8B | `hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF` |

**`role` 종류**

| role | 의미 |
|---|---|
| `system` | 모델 행동을 지시하는 시스템 프롬프트 |
| `user` | 사용자 질문 |
| `assistant` | 모델의 이전 답변 (대화 이력을 이어갈 때 사용) |

## 📡 Vector (이벤트 정규화)

원본 로그/이벤트를 받아(source) 정규화한 뒤(transform) 원하는 곳으로 전달(sink)하는 파이프라인 도구.
Logstash의 input-filter-output과 동일한 개념.

| 단계 | 역할 |
|---|---|
| Source | 이벤트 수신 (HTTP, 파일, Kafka 등). 들어온 raw 데이터를 **decoding**으로 구조화 |
| Transform | 필드 변환/정규화 (VRL) |
| Sink | 이벤트 전달 (console, Kafka, Elasticsearch 등). 내보낼 때 **encoding**으로 부호화 |

### 1. 컨테이너 실행
```bash
docker compose -f docker-compose.yml -f docker-compose.vector.yml up -d vector
```

### 2. 설정 파일 검증 (선택)
```bash
docker exec -it vector vector validate --config /etc/vector/vector.toml
```

### 3. 테스트 이벤트 전송
```bash
curl -X POST http://localhost:8081 \
  -H "Content-Type: application/json" \
  -d '{
  "event_id": "evt-20260908-001",
  "timestamp": "2026-09-08T10:15:00Z",
  "source_ip": "185.220.101.45",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "file_size_bytes": 245760
}'
```

### 4. 결과 확인
```bash
docker logs -f vector
```

### 5. Health 상태 확인
브라우저 또는 curl로 확인: http://localhost:8686/health
`{"ok":true}` 응답이면 정상.

### 참고 명령어
```bash
docker exec -it vector vector --version                              # 버전 확인
docker exec -it vector vector graph --config /etc/vector/vector.toml # 파이프라인 구조 확인
```

## 📨 Kafka (KRaft, Zookeeper 미사용)

분산 이벤트 스트리밍 플랫폼. Producer가 보낸 메시지를 토픽 단위로 저장하고,
Consumer가 순서대로 읽어가는 큐 역할. 시스템 간 비동기 연결점.

Vector가 Producer, Rust Enrichment가 Consumer 역할을 담당. 별도 Producer 구현 불필요.

| 리스너 이름 | 역할 | 프로토콜 |
|---|---|---|
| CLIENT | Vector 등 클라이언트 접속용 (9092) | PLAINTEXT |
| CONTROLLER | 브로커 간 메타데이터 통신 (9093) | PLAINTEXT |

- 단일 브로커 → 복제 계수(replication factor) 1 고정 (3 이상은 브로커 3개 이상 필요)
- 로컬 개발 환경 → SSL/SASL 암호화 미적용 (운영 배포 시 적용 필요)
- 토픽 자동 생성 비활성화 → 수동 생성 필요
- 컨테이너 간 통신은 Docker 내장 DNS로 서비스명(`kafka`) 사용, `localhost` 아님

### 1. 전체 서비스 실행
```bash
cd docker
docker compose -f docker-compose.yml  -f docker-compose.kafka.yml up -d kafka
```

### 2. 토픽 생성 (최초 1회)
```bash
docker exec -it kafka //opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic test_topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. 토픽 확인
```bash
docker exec -it kafka //opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 4. 컨슈머 대기 (별도 터미널)
```bash
docker exec -it kafka //opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test_topic \
  --from-beginning
```

### 5. Vector로 이벤트 전송 → Kafka 전달 확인
```bash
curl -X POST http://localhost:8081 \
  -H "Content-Type: application/json" \
  -d '{
  "event_id": "evt-20260908-001",
  "timestamp": "2026-09-08T10:15:00Z",
  "source_ip": "185.220.101.45",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "file_size_bytes": 245760
}'
```
컨슈머 화면에 변환된 JSON이 뜨면 Vector → Kafka 파이프라인 정상.

### 참고: 소비 상태 확인

```bash
# 토픽 오프셋(쌓인 메시지 수) 확인
docker exec -it kafka //opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic test_topic

# 컨슈머 그룹 소비 상태(Lag) 확인 — --group으로 실행한 컨슈머만 추적 가능
docker exec -it kafka //opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group test-group
```

## 📨 RedPanda (Kafka 프로토콜 호환 브로커)

Kafka API와 호환되는 C++ 기반 이벤트 스트리밍 브로커.
- Kafka 프로토콜 호환
- Kafka 대체 브로커로 사용 가능
- 단일 바이너리 기반의 경량 구조

| 리스너 종류 | 역할 | 포트 |
|---|---|---|
| internal | Docker 네트워크 안(Vector 등)에서 접속 | 9092 (고정) |
| external | 호스트 머신에서 직접 접속 | `${REDPANDA_PORT}`(19092, 가변) |

- 단일 브로커, `--mode dev-container`로 개발용 사전 설정 적용
- Kafka와 달리 환경변수가 아닌 `redpanda start` 커맨드라인 인자로 직접 설정 (`--kafka-addr`, `--advertise-kafka-addr` 등)
- 토픽 생성은 `rpk`(RedPanda 전용 CLI)로 수행
- 컨테이너 간 통신은 Docker 내장 DNS로 서비스명(`redpanda`) + internal 포트(9092) 사용

### 1. 컨테이너 실행
```bash
cd docker
docker compose -f docker-compose.yml -f docker-compose.redpanda.yml up -d redpanda
```

### 2. 클러스터 상태 확인
```bash
docker exec -it redpanda rpk cluster health
docker exec -it redpanda rpk cluster info
```

### 3. 토픽 생성 (최초 1회)
```bash
docker exec -it redpanda rpk topic create test_topic
```
> 참고: docker-compose.redpanda.yml > command > `--mode dev-container`는 토픽 자동 생성이 기본 활성화돼 있어, 수동 생성 없이 Vector가 첫 이벤트를 보내는 순간 자동으로 만들어지기도 함. 위 명령은 명시적으로 미리 만들어두고 싶을 때 사용.

### 4. 토픽 자동 생성 비활성화
```bash
docker exec -it redpanda rpk cluster config set auto_create_topics_enabled false
```

### 5. 토픽 확인
```bash
docker exec -it redpanda rpk topic list
```

### 6. 컨슈머 대기 (별도 터미널)
```bash
docker exec -it redpanda rpk topic consume test_topic
```

### 7. Vector로 이벤트 전송 → RedPanda 전달 확인
```bash
curl -X POST http://localhost:8081 \
  -H "Content-Type: application/json" \
  -d '{
  "event_id": "evt-20260908-001",
  "timestamp": "2026-09-08T10:15:00Z",
  "source_ip": "185.220.101.45",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "file_size_bytes": 245760
}'
```
컨슈머 화면에 변환된 JSON이 뜨면 Vector → RedPanda 파이프라인 정상.

### 참고: 소비 상태 확인
```bash
# 토픽 상세 정보(파티션, 오프셋 등) 확인
docker exec -it redpanda rpk topic describe test_topic

# 컨슈머 그룹 소비 상태(Lag) 확인
docker exec -it redpanda rpk group describe test-group
```

## 🔎 OpenSearch (조회/검색 저장소)

오픈소스 분산 검색·분석 엔진으로, 로그와 이벤트 데이터를 색인하고 검색하는 데 사용.

- Elasticsearch 계열의 검색 엔진
- JSON 문서 기반 검색
- OpenSearch Dashboards 제공

Vector가 정규화한 이벤트를 저장. Kibana 대신 OpenSearch Dashboards 사용.

### 실행
```bash
docker compose -f docker-compose.yml  -f docker-compose.opensearch.yml up -d opensearch opensearch-dashboards
```

### 확인 (OpenSearch Dashboards)
1. http://localhost:5601 접속
2. Dev Tools 메뉴에서 쿼리 실행:
```
GET shire-events/_search
{
  "query": { "match_all": {} }
}
```


## 📊 ClickHouse (집계/통계 저장소)

컬럼 기반 OLAP DB. 대량 데이터의 집계/통계 쿼리에 최적화, 
단건 수정/삭제는 비효율적이라 Append-only 로그성 데이터에 적합.

동일 이벤트를 정형 필드로 저장, 향후 대시보드/집계 쿼리용.

### 컨테이너 실행
```bash
docker compose -f docker-compose.yml  -f docker-compose.clickhouse.yml up -d clickhouse
```

### 테이블 자동 생성
docker/clickhouse/init/ 폴더의 SQL이 최초 실행 시 자동 적용됨.

### 확인 (Play UI)
1. http://localhost:8123/play 접속
2. 로그인: CLICKHOUSE_USER / CLICKHOUSE_PASSWORD (docker/.env 참고)
3. 쿼리 실행:
```sql
SELECT * FROM shire.security_events
```

## 📈 Grafana (모니터링, 추후 본격 활용 예정)

현재 컨테이너만 띄워둔 상태. ClickHouse 집계 데이터가 쌓이면 
통합 대시보드로 활용 예정. (http://localhost:3000, admin/admin)

### 컨테이너 실행
```bash
docker compose -f docker-compose.yml  -f docker-compose.grafana.yml up -d grafana
```

## 🐘 PostgreSQL (Playbook 설정 저장소)

오픈소스 객체 관계형 데이터베이스로, 구조화된 데이터를 안정적으로 저장하고 관리할 수 있음.

Playbook 정의(조건 노드, AI 분석 노드 설정)를 저장

### 1. 컨테이너 실행
```bash
docker compose -f docker-compose.yml -f docker-compose.postgres.yml up -d postgres
```

### 2. 접속 확인
```bash
docker exec -it postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

### 3. 연결 문자열
`backend/.env`의 `DATABASE_URL`에서 관리 (docker/.env와 별개, Rust 앱이 직접 읽는 값).


## 🧰 Redis / Dragonfly (캐싱)

인메모리 Key-Value 저장소. Dragonfly는 Redis 프로토콜과 호환되는 대체 구현체.
빠른 조회가 필요한 데이터에 사용. 기본은 휘발성이지만 AOF 등으로 영속화도 가능.

AbuseIPDB/VirusTotal 등 외부 API 조회 결과(IP 평판, 파일 해시 판정)를 캐싱해 
동일 IOC의 중복 조회를 방지하고 API rate limit을 절약. 재시작 후에도 데이터가 
유지되도록 영속 볼륨(AOF) 사용.

### 1. 컨테이너 실행
```bash
docker compose -f docker-compose.yml -f docker-compose.redis.yml up -d redis
# 또는
docker compose -f docker-compose.yml -f docker-compose.dragonfly.yml up -d dragonfly
```

### 2. CLI 접속
```bash
docker exec -it redis redis-cli -a ${REDIS_PASSWORD}
# Dragonfly도 redis-cli 프로토콜 호환이라 동일하게 접속 가능
docker exec -it dragonfly redis-cli -a ${DRAGONFLY_PASSWORD}
```

### 3. 기본 명령어
```bash
SET key value EX 60      # key에 value 저장, 60초 후 만료
GET key                  # key 값 조회
DEL key                  # key 삭제
TTL key                  # 남은 만료 시간(초) 확인
PING                     # 연결 확인 (PONG 응답)
```

### 4. 연결 정보
`backend/.env`의 `CACHE_BACKEND`, `REDIS_*`/`DRAGONFLY_*` 값에서 관리.

좋아, 일관성 유지가 맞는 판단이야 — 이미 9개 서비스가 `:latest`인데 Prometheus 하나만 버전 고정하면 오히려 스타일이 들쭉날쭉해 보여. 나중에 전체를 한 번에 버전 고정하는 리팩터링을 하고 싶으면 그때 몰아서 하는 게 낫고.

development-notes.md 스타일 그대로 맞춰서 Prometheus 섹션 작성할게.


## 📈 Prometheus (메트릭 수집기)

오픈소스 모니터링 시스템으로, 각 서비스에서 제공하는 메트릭(metric)​을 주기적으로 가져와 시계열 데이터로 저장
(Grafana 혼자서는 데이터를 수집하지 못하고, Prometheus 같은 데이터 소스가 있어야 시각화할 대상이 생김)

로그처럼 상세한 사건 내용을 저장하는 것이 아니라,
- CPU 사용량
- 메모리 사용량
- 요청 수
- 처리 성공/실패 횟수
- 이벤트 처리량

같은 숫자 형태의 상태 정보를 시간에 따라 수집하고 조회하는 데 사용

Vector/Kafka/RedPanda/Rust 등 파이프라인 각 단계가 "몇 개를 처리했는지"를 
Prometheus가 계속 가져가 쌓아두면, Grafana에서 "Vector는 100개를 보냈는데 
Rust는 0개를 받았다"처럼 구간별 수치를 비교해 이상 징후를 발견할 수 있음.

### 1. 컨테이너 실행
```bash
docker compose -f docker-compose.yml -f docker-compose.prometheus.yml up -d prometheus
```

### 2. 웹 UI 접속
```
http://localhost:9090
```
상단 메뉴의 Status → Targets에서, 등록된 각 서비스가 정상적으로 수집되고 있는지(`UP`) 확인 가능.

### 3. 스크래핑 대상 설정
`docker/prometheus/prometheus.yml`에서 관리. 어떤 서비스의 어떤 주소에서 
메트릭을 가져올지 여기에 등록해야 Prometheus가 수집을 시작함

### 4. 기본 쿼리 확인
웹 UI의 Graph 탭에서 쿼리 입력 후 실행 가능:
```
up
```
등록된 모든 대상의 생존 여부(1=정상, 0=응답 없음) 한눈에 확인


맞아, cAdvisor도 자체 웹 UI가 있어.

## UI 확인

```
http://localhost:8082
```

접속하면 cAdvisor 기본 대시보드가 뜨고, 여기서:
- 전체 호스트의 CPU/메모리/디스크/네트워크 사용량
- 실행 중인 각 컨테이너 목록과 개별 리소스 사용량 그래프

를 바로 확인할 수 있어. 다만 이 UI는 "지금 이 순간의 스냅샷"만 보여주고 과거 이력을 저장하지 않아서, 진짜 시계열 추적/알림은 Prometheus가 이 데이터를 가져가서 Grafana로 보여주는 쪽이 훨씬 유용해. 지금은 "제대로 데이터를 만들어내고 있는지" 육안으로 확인하는 용도로 쓰면 돼.


## 📊 cAdvisor (컨테이너 리소스 모니터링)

Google이 만든 오픈소스 도구로, 실행 중인 모든 Docker 컨테이너의 
CPU/메모리/디스크/네트워크 사용량을 자동으로 수집해 Prometheus 포맷으로 노출함

컨테이너 하나만 띄우면 Kafka, Vector, Rust 등 다른 모든 서비스의 리소스
상태를 별도 설정 없이 한 번에 모니터링 대상으로 만들 수 있음
(서비스별로 각각 exporter를 붙일 필요 없음)

호스트 시스템(파일시스템, cgroup 등)에 깊이 접근해야 하는 특성상
`privileged: true`와 다수의 읽기 전용 마운트가 필요함

### 1. 컨테이너 실행
```bash
docker compose -f docker-compose.yml -f docker-compose.cadvisor.yml up -d cadvisor
```

### 2. 웹 UI 접속
```
# Web UI
http://localhost:8082

# Metrics 확인
http://localhost:8082/metrics
```

### 3. Prometheus 연동 (다음 단계)
`docker/prometheus/prometheus.yml`에 스크래핑 대상으로 등록하면,
cAdvisor가 수집한 데이터를 Prometheus가 가져가 장기 보관하고
Grafana에서 시계열 그래프로 확인 가능해짐
```
