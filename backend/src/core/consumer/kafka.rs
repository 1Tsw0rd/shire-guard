// Kafka/RedPanda에서 RAW EVENT 메시지를 수신하는 Consumer
//
// config.rs에서 생성된 BrokerConfig를 전달받아 Consumer를 초기화하고,
// 지정된 topic을 구독한 뒤 메시지를 계속 수신한다.
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

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};

use crate::common::error::AppError;
use crate::common::metrics::Metrics;
use crate::config::BrokerConfig;
use crate::core::consumer::event::RawEvent;

pub fn build_consumer(config: &BrokerConfig) -> Result<StreamConsumer, AppError> {
    // Kafka Consumer를 생성하고 초기화
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest") // 저장된 offset이 없으면 가장 오래된 메시지부터 읽음
        .create()
        .map_err(|e| AppError::Internal(format!("Kafka Consumer 생성 실패: {e}")))?;

    tracing::info!(
        broker = ?config.broker,
        brokers = %config.brokers,
        "[MESSAGE BROKER CONNECTED] Kafka Consumer 초기화 성공"
    );

    Ok(consumer)
}

pub async fn run(consumer: StreamConsumer, topic: String, metrics: Arc<Metrics>) {
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
