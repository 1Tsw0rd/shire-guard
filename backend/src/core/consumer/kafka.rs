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

// librdkafka가 statistics.interval.ms 주기로 통계를 콜백해줄 때 받는 커스텀 컨텍스트
pub struct KafkaConsumerContext {
    metrics: Arc<Metrics>,
}

impl ClientContext for KafkaConsumerContext {
    fn stats(&self, statistics: Statistics) {
        // nodeid >= 0인 것만 실제 브로커로 취급 (nodeid -1은 부트스트랩용 가짜 항목)
        // 그중 하나라도 state == "UP"이면 연결된 것으로 판단
        let is_connected  = statistics
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


pub fn build_consumer(config: &BrokerConfig, metrics: Arc<Metrics>,) -> Result<StreamConsumer<KafkaConsumerContext>, AppError> {
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

pub async fn run(consumer: StreamConsumer<KafkaConsumerContext>, topic: String, metrics: Arc<Metrics>) {
    // Topic 구독
    if let Err(e) = consumer.subscribe(&[&topic]) {
        tracing::error!(error = %e, %topic, "[TOPIC SUBSCRIBE FAILED] 토픽 구독 실패");
        return;
    }

    tracing::info!(%topic, "[TOPIC SUBSCRIBED] 메시지 수신 대기 중...");

    // 메시지 수신 루프
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                // inc(): 카운터를 1 증가시킴 — "수집(gather)"이 아니라 "지금 이벤트 하나 처리했다"를 기록하는 동작
                // 동시 호출에도 안전(atomic)
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
