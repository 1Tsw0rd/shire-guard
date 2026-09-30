// Kafka/RedPanda에서 RAW EVENT 메시지를 수신하는 Consumer
//
// config.rs에서 생성된 BrokerConfig를 전달받아 Consumer를 초기화하고,
// 지정된 topic을 구독한 뒤 메시지를 계속 수신한다.
//
// 또한 librdkafka의 통계 callback을 통해 Broker 연결 상태를 확인하고,
// Prometheus metric과 로그로 연결 상태 변화를 기록한다.
//
// 처리 흐름:
// BrokerConfig
//      ↓
// StreamConsumer 생성
//      ↓
// Topic 구독
//      ↓
// 메시지 수신
//      ↓
// JSON payload 추출
//      ↓
// RawEvent 역직렬화
//      ├── 성공 → Enrichment로 전달
//      └── 실패 → 오류 로그 기록 후 해당 메시지 처리 종료

use std::sync::Arc;

// ClientContext: librdkafka가 발생시키는 각종 이벤트/통계를 받을 수 있는 Context 기능
// Consumer: Topic 구독할 때 사용하는 기능
// ConsumerContext: Consumer 전용 Context 만들 때 사용
// StreamConsumer: Kafka 메시지를 비동기 스트림 형태로 계속 수신하는 Consumer 구현체
// Statistics: librdkafka가 주기적으로 전달하는 Consumer 브로커 통계 정보
// ClientConfig: Kafka Consumer의 bootstrap 서버, group.id, 통계 주기 등의 설정을 구성하는 Builder
// Message: Kafka 메시지에서 payload 등을 꺼내기 위해 사용하는 trait
use rdkafka::client::ClientContext;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::statistics::Statistics;
use rdkafka::{ClientConfig, Message};

use crate::common::error::AppError;
use crate::common::metrics::Metrics;
use crate::config::BrokerConfig;
use crate::core::consumer::event::RawEvent;
use crate::core::enrichment::service::EnrichmentService;
use crate::core::playbook::engine::PlaybookEngine;

use tokio::sync::Semaphore;

/*
PROCESS_CONCURRENCY 상수 설명
- 동시에 처리할 이벤트 수 (Enrichment 실행 + Playbook 평가 구간)
- 이벤트 하나가 Redis/Dragonfly 왕복을 기다리는 동안 다른 이벤트를 함께 처리해서 대기 시간을 겹치게 함

성능 테스트 결과 (release 빌드, 쌓인 이벤트를 소비만 하는 조건, IOC 고정으로 전부 캐시 히트):
  직렬(1)   : 약 1.4K/s
  256       : 약 62K/s
  512       : 약 94K/s   (256 대비 +52%)
  1024      : 약 99K/s   (512 대비 +5%, 측정 오차 범위 수준)
  2048      : 약 105K/s 성능을 보여줬지만, 테스트 당시 Redis/Dragonfly 응답 지연으로 타임아웃 발생, 약 58만 건 처리 후 정지
=> 512 이상부터는 증가폭이 크지 않지만, 여유를 더 확보하고자 1024를 기본값으로 확정함

[참고사항]
포트폴리오에선 실제 외부 장비의 검증된 이벤트를 수신할 수 없어,
임의의 테스트 이벤트를 처리하는데
실제 운영하는 장비로 개발할 경우 Enrichment 처리는 제거되는 기능이지만
Redis/Dragonfly에 평판정보를 캐싱하여 Playbook에서 평판처리를 하는 기능을 구현할 경우
Enrichment 처리 로직과 비슷하기 때문에

Enrichment 처리 -> Playbook 평가 동작을 병렬로 처리하는 기능을 구현하고 적용함
*/
const PROCESS_CONCURRENCY: usize = 1024;

// librdkafka가 statistics.interval.ms 주기로 통계를 콜백해줄 때 받는 커스텀 컨텍스트
pub struct KafkaConsumerContext {
    metrics: Arc<Metrics>,
}

impl ClientContext for KafkaConsumerContext {
    fn stats(&self, statistics: Statistics) {
        // nodeid >= 0인 것만 실제 브로커로 취급 (nodeid -1은 부트스트랩용 가짜 항목)
        // 그중 하나라도 state == "UP"이면 연결된 것으로 판단
        let is_connected = statistics
            .brokers
            .values()
            .filter(|b| b.nodeid >= 0)
            .any(|b| b.state == "UP");

        let connected_value = if is_connected { 1 } else { 0 }; // Prometheus Gauge에 넣을 0/1 값
        let previous_value = self.metrics.consumer_broker_connected.get();

        // 상태가 실제로 바뀐 시점에만 로그 (매 15초마다 반복 안 찍히게)
        if previous_value != connected_value {
            if is_connected {
                tracing::info!("[BROKER CONNECTED] Kafka/RedPanda 브로커 연결됨");
            } else {
                tracing::warn!("[BROKER DISCONNECTED] Kafka/RedPanda 브로커 연결 끊김");
            }
        }

        // set(): Gauge의 현재 값을 직접 설정하는 메서드
        self.metrics.consumer_broker_connected.set(connected_value);
    }
}

// Kafka Consumer에서 사용할 Context가 ConsumerContext trait도 구현하도록 선언
//
// ConsumerContext에는 rebalance, commit callback 등 Consumer 동작과 관련된 여러 callback이 정의되어 있다.
// 지금은 이런 callback을 직접 커스터마이징할 필요가 없기 때문에 trait에서 제공하는 기본 구현을 그대로 사용한다.
// 따라서 별도의 메서드를 구현하지 않고 빈 블록으로 둔다.
//
// "이 타입은 Consumer 컨텍스트로 쓸 수 있다"는 선언 자체가 핵심
impl ConsumerContext for KafkaConsumerContext {}

pub fn build_consumer(
    config: &BrokerConfig,
    metrics: Arc<Metrics>,
) -> Result<StreamConsumer<KafkaConsumerContext>, AppError> {
    // 우리가 만든 KafkaConsumerContext 객체 생성
    // Context 안에 Prometheus Metrics를 넣어둔다.
    // 여기 담긴 metrics가 나중에 stats() 콜백 안에서 게이지를 갱신하는 데 쓰인다.
    let context = KafkaConsumerContext { metrics };

    // Kafka Consumer를 생성하고 초기화
    let consumer: StreamConsumer<KafkaConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest") // 저장된 offset이 없으면 가장 오래된 메시지부터 읽음
        .set("statistics.interval.ms", "15000") // Prometheus scrape 주기(15s)와 맞춤
        // 일반적인 Consumer 생성(create) 대신
        // 위에서 우리가 만든 KafkaConsumerContext 연결해서 Consumer 생성(create_with_context)
        // 이렇게 생성하면 librdkafka가 통계 정보를 전달할 때(statistics.interval.ms 주기)
        // Consumer는 자동으로 context의 stats()를 호출해줌
        .create_with_context(context)
        .map_err(|e| AppError::Internal(format!("Kafka Consumer 생성 실패: {e}")))?;

    tracing::info!(
        broker = ?config.broker,
        brokers = %config.brokers,
        "[CONSUMER CREATED] Kafka Consumer 객체 생성 성공"
    );

    Ok(consumer)
}

pub async fn run(
    consumer: StreamConsumer<KafkaConsumerContext>,
    topic: String,
    metrics: Arc<Metrics>,
    enrichment: Arc<EnrichmentService>,
    engine: Arc<PlaybookEngine>,
) {
    // Topic 구독
    if let Err(e) = consumer.subscribe(&[&topic]) {
        tracing::error!(error = %e, %topic, "[TOPIC SUBSCRIBE FAILED] 토픽 구독 실패");
        return;
    }

    tracing::info!(%topic, "[TOPIC SUBSCRIBED] 메시지 수신 대기 중...");

    // 동시에 처리할 수 있는 이벤트 수를 제한하는 Semaphore
    // Semaphore는 정해진 개수의 permit(허가증)을 가지고 있으며,
    // 이벤트 하나가 처리될 때 permit 하나를 빌려주고 처리가 끝나면 반환함
    // 여기서는 PROCESS_CONCURRENCY만큼 permit을 생성하여,
    // 동시에 처리할 수 있는 이벤트 수를 제한함
    //
    // recv()는 Kafka/RedPanda에서 메시지 하나를 받아오는 메서드인데,
    // 처리 중인 이벤트가 PROCESS_CONCURRENCY에 도달하면 permit 획득(acquire_owned)에서 대기하게 되고,
    // 그 사이 루프는 다음 recv()를 호출하지 못하므로 처리 중인 이벤트 수를 초과하여 Task 생성되는 것을 방지함
    // 밀린 이벤트는 이 프로세스 메모리가 아니라 Kafka/RedPanda 쪽에 그대로 쌓임
    let semaphore = Arc::new(Semaphore::new(PROCESS_CONCURRENCY));

    // 메시지 수신 루프
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                metrics.consumer_messages_received.inc();

                let Some(payload) = msg.payload() else {
                    metrics.consumer_events_failed.inc();
                    tracing::warn!("[EMPTY MESSAGE] 빈 메시지 수신, 건너뜀");
                    continue;
                };

                match serde_json::from_slice::<RawEvent>(payload) {
                    Ok(event) => {
                        metrics.consumer_events_parsed.inc();
                        tracing::debug!(?event, "[EVENT PARSED] 이벤트 수신 및 파싱 성공");

                        // permit은 반드시 spawn 전에 획득해야 함
                        // spawn은 비동기 작업을 별도의 Task로 실행하는 것을 의미함
                        // spawn 이후에 permit을 획득하면 입력이 빠를 때 Task가 제한 없이 생성될 수 있음
                        let Ok(permit) = semaphore.clone().acquire_owned().await else {
                            tracing::error!(
                                "[SEMAPHORE CLOSED] Semaphore가 닫혀 consumer를 종료함"
                            );
                            return;
                        };

                        let enrichment = enrichment.clone();
                        let engine = engine.clone();

                        // 병렬 처리 실행
                        tokio::spawn(async move {
                            let _permit = permit; // Task가 종료되면 permit이 drop되어 Semaphore에 자동 반환됨
                            process_event(event, &enrichment, &engine).await;
                        });
                    }
                    Err(e) => {
                        metrics.consumer_events_failed.inc();
                        tracing::warn!(error = %e, "[EVENT PARSE FAILED] 이벤트 역직렬화 실패, 메시지 폐기");
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "[MESSAGE RECV FAILED] Kafka 메시지 수신 실패");
            }
        }
    }
}

// 이벤트 하나를 처리: Enrichment 실행 -> Playbook 평가
async fn process_event(event: RawEvent, enrichment: &EnrichmentService, engine: &PlaybookEngine) {
    // Enrichment 실행
    let evidence = enrichment.enrich_event(event).await;
    tracing::debug!(
        event_id = %evidence.event.event_id,
        "[ENRICHMENT COMPLETE] 조사 완료"
    );
    tracing::trace!(?evidence, "[EVIDENCE] {:?}", evidence);

    // Playbook 평가
    // TODO 저장 배선 단계에서 이 결과를 OpenSearch/ClickHouse 저장으로 교체
    match engine.run(&evidence).await {
        Some(result) => {
            tracing::debug!(
                event_id = %evidence.event.event_id,
                action = ?result.action,
                evaluated = result.evaluated,
                "[PLAYBOOK DECIDED] Playbook 판정 완료"
            );
            match serde_json::to_string(&result) {
                Ok(json) => {
                    tracing::debug!(playbook_result = %json, "[PLAYBOOK RESULT]")
                }
                Err(e) => {
                    tracing::warn!(error = %e, "playbook_result 직렬화 실패")
                }
            }
        }
        None => {
            tracing::debug!(
                event_id = %evidence.event.event_id,
                "[PLAYBOOK NO DECISION] 결정한 Playbook 없음, 원본 그대로 통과"
            );
        }
    }
}
