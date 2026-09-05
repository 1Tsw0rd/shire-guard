## 임시 아키텍처
                  RAW EVENT
                      │
                      ▼
                 Vector
               (Normalize)
                      │
                      ▼
                    Kafka
                      │
                      ▼
              ┌───────────────┐
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
                      │
                      ▼
                Playbook Engine
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
                  Elasticsearch      ClickHouse
                  (조회/검색용)      (집계/통계용)

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
docker compose -f docker/docker-compose.ai.yml up -d
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
docker compose -f docker/docker-compose.vector.yml up -d
```

### 2. 설정 파일 검증 (선택)
```bash
docker exec -it vector vector validate --config /etc/vector/vector.toml
```

### 3. 테스트 이벤트 전송
```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{"ip": "203.0.113.45", "failures": 342, "successes": 1, "username": "admin"}'
```

### 4. 결과 확인
```bash
docker logs -f vector
```
`ip → source_ip`, `failures → failed_count`, `successes → success_count`로 변환되어 출력됨.

### 참고 명령어
```bash
docker exec -it vector vector --version                              # 버전 확인
docker exec -it vector vector graph --config /etc/vector/vector.toml # 파이프라인 구조 확인
```

## 📨 Kafka (KRaft, Zookeeper 미사용)

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
docker compose up -d
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
  -d '{"ip":"203.0.113.45","failures":342,"successes":1,"username":"admin"}'
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

## 🔎 OpenSearch (조회/검색 저장소)

Vector가 정규화한 이벤트를 저장. Kibana 대신 OpenSearch Dashboards 사용.

### 실행
```bash
docker compose up -d
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

동일 이벤트를 정형 필드로 저장, 향후 대시보드/집계 쿼리용.

### 테이블 자동 생성
docker/clickhouse/init/ 폴더의 SQL이 최초 실행 시 자동 적용됨.

### 확인 (Play UI)
1. http://localhost:8123/play 접속
2. 로그인: CLICKHOUSE_USER / CLICKHOUSE_PASSWORD (docker/.env 참고)
3. 쿼리 실행:
```sql
SELECT * FROM shire_guard.security_events
```

## 📈 Grafana (모니터링, 추후 본격 활용 예정)

현재 컨테이너만 띄워둔 상태. ClickHouse 집계 데이터가 쌓이면 
통합 대시보드로 활용 예정. (http://localhost:3000, admin/admin)