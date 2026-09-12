# Grafana 대시보드 & Alert 읽는 법

> Shire Guard의 Grafana Dashboard와 Alert를 읽을 때 반드시 알아둘 참고 메모.
> 2026-09-12~13 세션에서 실제로 재현·검증한 Vector / Prometheus 동작을 기준으로 작성한다.

## 핵심 원칙

**대시보드 숫자는 항상 "지금 이 순간의 진실"이라고 생각하면 안 된다.**

같은 숫자라도 다음에 따라 의미가 달라질 수 있다.

1. 관측 시점
2. metric의 종류 — counter / gauge / rate / increase
3. Grafana time range
4. Prometheus scrape 시점
5. Vector `prometheus_exporter`의 활동 기반 metric expiration 여부
6. Vector 재시작 여부

따라서 **패널 하나만 보고 정상/비정상을 단정하지 않는다.**
특히 Kafka / RedPanda 관련 패널은 반드시 다른 지표와 교차 확인한다.

---

# 1. 기본 파이프라인

전체 구조는 다음과 같다.

```text
RAW EVENT
   ↓
Vector
   ├──────────────→ OpenSearch (테스트용)
   ├──────────────→ ClickHouse (테스트용)
   ↓
Kafka / RedPanda
   ↓
Rust Consumer
   ↓
Enrichment / Processing
```

장애를 볼 때는 **어느 구간에서 이벤트 흐름이 끊겼는지**를 좁혀간다.

---

# 2. Dashboard 기본 읽는 순서

장애가 의심되면 다음 순서로 본다.

```text
1. Selected Broker
2. Broker Connected
3. Backend Up
4. Pipeline Flow (Vector → Broker → Rust)
5. Disk Buffer / Kafka·RedPanda Sink 지표
6. Alerts
```

단, `Selected Broker`는 생존 상태가 아니라 **설정값**이라는 점을 먼저 기억한다.

---

# 3. Selected Broker

## 의미

현재 설정에서 어떤 Broker가 active broker로 선택되어 있는지 보여준다.

예:

```text
kafka
```

또는

```text
redpanda
```

## 주의

이 패널이 `green`이라고 해서 Broker가 현재 살아 있다는 뜻은 아니다.

```text
Selected Broker = 설정상 선택된 Broker
Broker Connected = 실제 Consumer 연결 상태
```

실제 Broker 장애 판단은 `Broker Connected` 및 Broker 서비스 상태에서 확인한다.

---

# 4. Broker Connected

## 의미

Rust Backend 서버의 Consumer가 현재 Broker에 연결되어 있는지를 보여준다.

```text
CONNECTED
DISCONNECTED
```

## 읽는 법

`CONNECTED`면 Consumer 관점의 Broker 연결은 정상이다.

`DISCONNECTED`면 Consumer와 Broker 사이의 연결 문제를 의심한다.

단, 이것만으로 **Vector → Broker 전송까지 정상이라고 단정하지 않는다.**
Vector의 송신 상태는 Pipeline 및 Sink 지표에서 별도로 확인한다.

---

# 5. Backend Up

## 의미

Prometheus가 `backend` job으로 스크레이프하는 Rust Axum 서버의 생존 상태다.

```text
Axum 가즈아!!
Axum 죽음
```

`Axum 죽음`이면 Rust backend / consumer 프로세스 자체의 생존 여부부터 확인한다.

Backend가 죽어 있으면 뒤쪽 Consumer metric이 사라지거나 관련 Alert가 연쇄적으로 발생할 수 있다.

---

# 6. Pipeline Flow (Vector → Broker → Rust)

## 의미

이 패널은 전체 이벤트 흐름의 핵심 상태를 보는 패널이다.

```text
Vector 입력
   ↓
Broker 전달
   ↓
Rust Consumer 수신
```

핵심은 **어느 구간부터 0이 되었는지**다.

예:

```text
Vector 입력 > 0
Broker 전달 = 0
```

→ Vector → Broker 구간을 의심한다.

```text
Broker에는 데이터가 들어옴
Rust Consumer 수신 = 0
```

→ Broker → Rust 구간을 의심한다.

---

# 7. Sink Reliability / Buffering

현재 Sink 관련 패널은 서로 다른 의미를 가진다.

---

## 7.1 Disk Buffer Usage

대상:

- OpenSearch
- ClickHouse

### 쿼리 개념

```text
vector_buffer_size_bytes
/
vector_buffer_max_size_bytes
× 100
```

### 신뢰도

**높음. 현재 실제 disk buffer 점유율을 보는 용도로 사용한다.**

### 읽는 법

```text
0%에 가까움
→ 현재 buffer에 쌓인 이벤트가 거의 없음
```

```text
사용률 상승
→ 목적지 장애 / 처리 지연 등으로 이벤트가 disk buffer에 쌓이는 중
```

목적지가 복구되면 flush가 진행되면서 사용률이 내려가는 것이 정상적인 흐름이다.

### 재시작 관련 주의

현재 구성에서 `/var/lib/vector`가 별도 volume으로 보존되도록 되어 있다면,
Vector 프로세스가 재시작되어도 disk buffer 파일을 유지하고 재시작 후 flush를 이어갈 수 있다.

즉 **Vector 재시작과 disk buffer 데이터 보존은 같은 문제가 아니다.**

### `sent ≈ received`라고 flush 완료로 판단하지 말 것

이번 세션에서 실제로 다음과 같은 상황을 재현했다.

```text
count() = 83690 확인
→ “다 flush됐다”고 판단
→ 재확인했더니 68690
→ 당황
→ 몇 초 뒤 다시 확인하니 98690
```

처음 보였던 `83690`은 최종 flush 완료값이 아니라 **그 순간의 스냅샷**이었다.
Vector 재시작 시점에 disk buffer에 남아 있던 잔여 이벤트가 이후에도 계속 목적지로 flush되면서
실제 최종 count가 더 증가했다.

따라서 `vector_component_sent_events_total`과 입력 이벤트 수가 비슷해졌다는 이유만으로
**“목적지까지 전부 확정적으로 저장됐다”라고 판단하면 안 된다.**
이번 실측에서는 `sent` 지표와 목적지의 실제 row count가 같은 시점에 완료되지 않았다.

목적지 `count()`로 flush 완료 여부를 검증할 때는 한 번의 스냅샷을 최종값으로 단정하지 말고,
**시간을 두고 다시 조회해서 숫자가 더 이상 증가하지 않는지 확인한다.**

> 실전 교훈: `sent ≈ received`는 “전송 흐름이 진행되고 있다”는 신호일 뿐,
> 목적지의 최종 저장 완료를 증명하는 값으로 취급하지 않는다.

---


### Disk Buffer가 목적지 복구 후에도 줄지 않을 때

목적지(OpenSearch / ClickHouse)가 다시 살아나고 DNS도 정상적으로 resolve되는데도
disk buffer가 줄지 않는다면, **이번 세션에서 확인한 환경에서는 Vector 쪽 HTTP sink의 재시도 backoff가
아직 대기 중인 상황**을 먼저 의심했다.

목적지 컨테이너를 재시작하는 것만으로는 Vector 쪽의 기존 backoff 타이머가 초기화되지 않았고,
Vector 자체를 재시작하면 다음 재시도 사이클을 다시 시작하면서 buffer flush가 재개되는 것을 확인했다.

확인 순서:

```bash
# 1. 목적지 DNS 확인
docker exec -it vector getent hosts <목적지서비스명>

# 2. 최근 Vector 로그 확인
docker logs --tail 20 vector | grep -i <sink명>

# 3. 그래도 buffer가 줄지 않으면 Vector 재시작 고려
```

> **이번 세션의 실측 교훈:** 목적지가 살아났는데도 buffer가 그대로라는 이유만으로
> 목적지 컨테이너 자체가 아직 고장났다고 단정하지 않는다. Vector의 재시도 backoff 상태를 함께 확인한다.

## 7.2 Kafka / RedPanda Send Reliability

### 가장 중요한 주의사항

이 패널은 **현재 memory buffer 점유율이 아니다.**

현재 표시값은 개념적으로 다음과 같다.

```text
vector_component_discarded_events_total
/
vector_buffer_max_size_events
× 100
```

따라서 이 값은 **discard / failure activity를 보는 보조적인 reliability 지표**로 사용한다.

### 왜 buffer 사용률이 아닌가?

실측한 현재 Vector 환경에서는 Kafka 타입 sink가 memory buffer를 사용하더라도
이벤트가 disk buffer처럼 오래 buffer에 머무르는 방식으로 보이지 않고,
producer(librdkafka) 쪽으로 전달된 뒤 전송 실패가 발생하면 discard로 집계되는 동작을 확인했다.

따라서:

```text
max_events
```

를 가지고

```text
현재 buffer가 몇 % 찼는가?
```

를 계산하면 안 된다.

### 반드시 기억할 점

`discarded_events_total`은 **누적 counter**이므로 한 번 증가하면 Vector가 재시작되기 전까지 내려가지 않는다.

그래서:

```text
150%
300%
```

처럼 `100%`를 초과할 수도 있다.

이 값은 **buffer capacity percentage가 아니다.**

### 활동 기반 metric expiration

현재 Vector `prometheus_exporter` 설정에서는 일정 기간 갱신되지 않은 metric이
리포팅 대상에서 만료될 수 있다.

따라서 discard 활동이 한동안 없으면 Grafana에서:

```text
0
```

또는

```text
No data → or vector(0) 때문에 0처럼 보임
```

으로 보일 수 있다.

이것을 다음처럼 해석하면 안 된다.

```text
"과거 discard가 전부 사라졌다"
```

실제로는 **현재 Prometheus에 그 metric series가 관측되지 않는 것**일 수 있다.

> 비유하면 흉터가 사라진 게 아니라 잠시 조명이 꺼진 것이다.

### 과거 발생 여부 확인

Grafana time range를 넓혀 본다.

예:

```text
Last 1 hour
Last 6 hours
```

현재 시점에 metric이 보이지 않아도 Prometheus에 저장된 과거 시계열이 있다면
활동이 발생했던 구간의 spike를 확인할 수 있다.


### discard는 즉시 발생하지 않고 지연될 수 있다

Kafka / RedPanda가 죽은 직후에는 이벤트가 곧바로 `discarded`로 집계되지 않을 수 있다.
이번 세션의 실측에서는 producer 내부 큐(`vector_kafka_queue_messages`)에 이벤트가 먼저 쌓이고
재시도를 반복한 뒤, 재시도가 소진되는 시점부터 `vector_component_discarded_events_total`이
증가하기 시작했다.

따라서 sink를 막자마자:

```text
Send Reliability = 0%
Discarded Events Trend = 0
```

이라고 해서 **“장애 영향이 없다”라고 바로 판단하면 안 된다.**
실측 환경에서는 discard 판정까지 시간이 걸릴 수 있으므로 **1~2분 정도 기다린 뒤 다시 확인**한다.

### 결론

이 패널은:

```text
값 상승
→ discard / failure activity가 관측되었다.
```

정도로 읽는다.

다음 판단은 반드시 `Kafka/RedPanda Produced Rate`와 `Discarded Events Trend`를 같이 본다.

---

## 7.3 Kafka / RedPanda Produced Rate

### 쿼리 개념

```text
rate(vector_kafka_produced_messages_total[5m])
```

단위:

```text
cps (count per second)
```

### 의미

Kafka / RedPanda sink가 **실제로 broker production에 성공한 이벤트의 처리율**을 본다.

이 패널은 성공률(%)이 아니다.

```text
Produced Rate = broker에 성공적으로 생산된 이벤트 처리율
```

### 읽는 법

```text
값 > 0
→ 해당 시간 창에서 성공적인 broker production이 실제로 관측됨
```

`값 > 0`은 해당 sink가 실제로 broker에 생산 성공했다는 강한 근거다.

반대로:

```text
값 = 0
```

이면 **그 시간 창에서 성공적인 production이 관측되지 않았다는 뜻**이지,
그 숫자 하나만으로 Broker 장애를 확정하는 것은 아니다.

특히 metric expiration / scrape 상태와 함께 확인한다.

### 권장 교차 확인

```text
Produced Rate
        ↕
Discarded Events Trend
        ↕
Broker Connected
        ↕
Vector 로그
```

---

## 7.4 Discarded Events Trend

### 쿼리 개념

```text
increase(vector_component_discarded_events_total[1m])
```

대상:

- Kafka
- RedPanda
- OpenSearch
- ClickHouse

### 의미

**최근 1분 동안 discard counter가 얼마나 증가했는지**를 본다.

즉 누적 총량이 아니라 **최근 discard 활동**을 보기 위한 패널이다.

예:

```text
Kafka = 0
RedPanda = 1520
```

이면 해당 시간 구간에서 RedPanda sink의 discard 증가가 관측됐다는 의미다.

### 장점

`Send Reliability`보다 **현재 장애가 실제로 발생하고 있는 시점**을 읽기에 적합하다.

### 주의

활동 기반 metric expiration 및 scrape 간격의 영향을 완전히 무시할 수는 없다.
그래도 누적 counter 자체를 그대로 보여주는 것보다는 최근 활동을 판단하기에 적합하다.

`or vector(0)`은 metric이 없을 때 화면에 0을 만들기 위한 처리다.

따라서 `0`을 볼 때도:

```text
현재 1분간 discard 증가가 없었음
```

과

```text
metric이 관측되지 않아 vector(0)이 선택됨
```

을 구분한다.

---

# 8. Service UP / Prometheus Scrape Targets

## Service UP

컨테이너 생존 상태는 cAdvisor의 `container_last_seen` 계열 지표로 확인한다.

서비스가 실제로 떠 있는지 빠르게 보는 용도다.

## Prometheus Scrape Targets

Prometheus가 직접 scrape하는 target의 `up` 상태를 표로 본다.

```text
1 = UP
0 = DOWN
```

`Service UP`과 목적이 완전히 같지는 않다.

- `Service UP` → 컨테이너가 살아 있는지
- `Prometheus Scrape Targets` → Prometheus가 해당 target을 정상 scrape하는지

따라서 애매할 때 둘을 함께 본다.

---

# 9. Alert 설계 원칙

현재 Alert를 이해할 때 다음 원칙을 기억한다.

## 9.1 `noDataState: OK`

`up{job=...} == 0` 형태의 Alert는 정상 상태에서 query result가 empty vector가 될 수 있다.

따라서 `noDataState`를 잘못 설정하면 정상인데도 `No Data` 상태로 표시될 수 있다.

현재 Alert는 이런 상황을 정상 상태로 처리할 수 있도록 `noDataState: OK`를 사용한다.

---

## 9.2 Active Broker gating

Kafka / RedPanda를 동시에 모니터링하더라도 **현재 사용하지 않는 Broker가 죽었다고 Alert를 발생시키면 안 된다.**

그래서 다음과 같은 active broker 조건을 사용한다.

```text
shireguard_config_active_broker{broker="kafka"} == 1
```

또는

```text
shireguard_config_active_broker{broker="redpanda"} == 1
```

즉:

```text
Active Broker만 장애 판단에 참여
```

하는 구조다.

---

## 9.3 `or vector(0)`

Kafka / RedPanda metric은 활동 기반 expiration과 결합될 수 있으므로,
query 결과가 아예 없어지는 상황을 고려해 일부 PromQL에:

```promql
or vector(0)
```

를 사용한다.

다만 이것은 **정상임을 증명하는 함수가 아니다.**
단지 empty result를 0으로 표현하기 위한 장치다.

---

# 10. Alert 읽는 법

현재 핵심 Alert는 다음 4개다.

1. `Service Down`
2. `Vector to Broker Silent Failure`
3. `Broker to Rust Silent Failure`
4. `Pipeline Flow Degraded`

Alert는 단순 서비스 생존뿐 아니라 **파이프라인이 조용히 멈추는 silent failure**를 찾기 위한 것이다.

---

# 11. Service Down

## 의미

주요 서비스의 Prometheus `up` 상태가 내려간 경우 발생한다.

기본 대상:

- backend
- vector
- prometheus
- cadvisor
- active broker (Kafka 또는 RedPanda)

Kafka / RedPanda는 `shireguard_config_active_broker` 조건으로 선택된 Broker만 감시한다.

## 발생 시

먼저:

```text
Backend Up
Prometheus Scrape Targets
Broker Connected
```

를 확인한다.

## Alert 특성

현재 `for: 1m`이므로 순간적인 상태 변화보다 **지속되는 장애**를 잡도록 되어 있다.

---

# 12. Vector to Broker Silent Failure

## 의미

Vector에는 이벤트가 들어오는데,
active broker 방향으로 Vector가 보내는 흐름이 일정 시간 동안 0으로 보이는 상황을 잡는다.

개념적으로:

```text
Vector input > 0
AND
active broker 방향 Vector sent rate == 0
```

## 중요한 이유

Broker 컨테이너 자체는 살아 있어도:

```text
Vector → Broker
```

구간이 멈출 수 있다.

이런 문제는 단순 `Service Down`만으로는 잡히지 않을 수 있다.

## 발생 시 확인 순서

```text
Selected Broker
↓
Broker Connected
↓
Kafka / RedPanda Produced Rate
↓
Discarded Events Trend
↓
Vector logs
```

---

# 13. Broker to Rust Silent Failure

## 의미

Broker에는 실제 메시지가 들어오는데 Rust Consumer가 받지 않는 상황을 잡는다.

개념적으로:

```text
Broker input rate > 0
AND
Rust Consumer received rate == 0
```

즉:

```text
RAW EVENT
  ↓
Vector       정상
  ↓
Broker       정상
  ↓
Rust         수신 0  ← 문제
```

## 발생 시 확인

```text
Backend Up
↓
Broker Connected
↓
Broker production / topic input metric
↓
shireguard_consumer_messages_received_total
↓
Rust logs
```

이 Alert는 현재 구조에서 **Broker까지 데이터가 들어왔는데 Consumer가 조용히 멈춘 상황**을 잡는 데 특히 중요하다.

---

# 14. Pipeline Flow Degraded

## 의미

파이프라인의 핵심 흐름 중 하나 이상이 끊긴 상황을 넓게 감지하는 종합 Alert다.

개념적으로 다음과 같은 상태를 포함한다.

```text
Vector input > 0
AND
Vector → Broker == 0
```

또는

```text
Broker input > 0
AND
Rust Consumer received == 0
```

즉 세부 원인을 확정하는 Alert라기보다:

```text
"전체 pipeline flow가 어딘가에서 비정상적으로 끊겼다"
```

를 먼저 알려주는 역할이다.

따라서 이 Alert만 보고 원인을 결정하지 말고,
`Vector to Broker Silent Failure` / `Broker to Rust Silent Failure` 및 Dashboard panel을 함께 본다.

---

# 15. Alert가 동시에 여러 개 뜨는 경우

Alert 여러 개가 동시에 firing된다고 해서 반드시 장애가 여러 개라는 뜻은 아니다.

하나의 장애가 여러 조건을 연쇄적으로 만족시킬 수 있다.

예:

```text
Rust Consumer 장애
    ↓
Broker → Rust 수신 0
    ↓
Broker to Rust Silent Failure
    ↓
Pipeline Flow Degraded
```

따라서 **가장 하위의 실제 장애 지점을 찾는 것**이 중요하다.

### 다중 sink 장애 동시 검증 사례

이번 세션에서는 **Kafka와 ClickHouse를 동시에 정지시킨 상태에서 부하를 발생시키는 테스트**도 수행했다.
그 상태에서도 RedPanda와 OpenSearch가 영향을 받지 않고 정상 처리를 유지하는 것을 실측으로 확인했다.

이는 fan-out 구조에서 sink별 buffering / `drop_newest` 정책을 적용했을 때
**단일 장애뿐 아니라 다중 동시 장애 상황에서도 sink 간 장애가 격리될 수 있음을 확인한 사례**다.

즉 한 sink의 장애가 다른 정상 sink의 처리까지 자동으로 멈춘다고 가정하지 말고,
각 sink의 실제 metric과 destination 결과를 별도로 확인한다.

---

# 16. 트러블슈팅 체크리스트

지표가 이상하게 보일 때는 다음 순서로 확인한다.

## 1) Grafana보다 raw metric 먼저 확인

```bash
curl localhost:9598/metrics
```

Dashboard에서 보이는 값을 그대로 믿지 말고,
가능하면 Vector의 raw metric을 먼저 확인한다.

---

## 2) 컨테이너 상태 확인

```bash
docker compose ps <service>
```

특히 `STATUS`의 `Up X` 시간이 예상보다 짧다면 최근 재시작 여부를 의심한다.

---

## 3) Vector 로그 확인

```bash
docker logs vector | grep -i <sink명>
```

예:

```bash
docker logs vector | grep -i redpanda
```

또는:

```bash
docker logs vector | grep -i kafka
```

---

## 4) Windows Git Bash 경로 변환 주의

Windows Git Bash에서 컨테이너 내부 절대 경로를 `docker exec`에 넘길 때
MSYS 경로 변환이 개입할 수 있다.

필요하면:

```text
//etc/vector/vector.toml
```

처럼 `//`로 시작하거나:

```bash
MSYS_NO_PATHCONV=1
```

를 사용한다.

---

## 5) Counter가 갑자기 0이 되었는지 확인

`discarded_events_total` 같은 counter가 갑자기 처음부터 시작한 것처럼 보이면
**Vector 재시작 여부를 먼저 확인한다.**

실제로는 다음처럼 확인한다.

```bash
docker compose ps vector
```

여기서 `STATUS`의 `Up X` 시간을 확인한다. 예를 들어 `Up About a minute`처럼
`Up` 시간이 비정상적으로 짧다면 최근 재시작을 의심한다.
`CREATED` 시각은 컨테이너 생성 시각이고, 현재 실행이 얼마나 지속됐는지는 `STATUS`의 `Up` 시간이 더 직접적인 단서다.

구분:

```text
metric expiration
→ reporting series가 일시적으로 사라짐

Vector restart
→ metric counter 자체가 다시 시작됨
```

둘은 완전히 다른 현상이다.

---

# 17. 장애 상황별 빠른 판단표

| 관측 상태 | 우선 의심 구간 |
|---|---|
| Backend Up = 0 | Rust backend / consumer 프로세스 |
| Broker Connected = DISCONNECTED | Consumer ↔ Broker 연결 |
| Vector input > 0, Broker 전달 = 0 | Vector → Broker |
| Broker input > 0, Rust received = 0 | Broker → Rust |
| Disk Buffer Usage 상승 | OpenSearch / ClickHouse 목적지 장애 또는 처리 지연 |
| Discarded Events Trend spike | 해당 sink에서 실제 discard 활동 |
| Produced Rate > 0 | 해당 sink에서 실제 broker production 성공 활동 관측 |
| Send Reliability 값 상승 | 과거 또는 최근 discard activity 의심 — 현재 buffer 점유율로 해석 금지 |
| Alert 여러 개 동시 발생 | 하나의 근본 장애가 여러 조건을 연쇄 만족했는지 확인 |

---

# 18. 한 줄 요약

```text
Selected Broker
→ "어느 Broker를 쓰도록 설정했나?"

Broker Connected
→ "Rust Consumer가 Broker에 연결돼 있나?"

Backend Up
→ "Rust Axum / backend가 살아 있나?"

Pipeline Flow
→ "이벤트가 Vector → Broker → Rust로 실제 흐르고 있나?"

Disk Buffer Usage
→ "OpenSearch / ClickHouse에 못 보내고 쌓인 이벤트가 얼마나 있나?"

Kafka/RedPanda Send Reliability
→ "해당 sink에서 discard / failure activity가 있었나?"

Produced Rate
→ "실제로 broker production 성공이 관측되고 있나?"

Discarded Events Trend
→ "최근 1분 동안 실제 discard 증가가 있었나?"

Alerts
→ "파이프라인의 어느 구간이 조용히 멈췄나?"
```
