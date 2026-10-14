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
//      ├── 성공 -> Enrichment -> Playbook -> 저장 Task(forwarder.rs)로 전달
//      └── 실패 -> 원문과 사유를 PostgreSQL storage_failures에 보관
//
// Kafka offset commit:
// 메시지를 받으면 offset 추적기(offset.rs)에 등록하고, OpenSearch와 ClickHouse 저장이 끝난 offset까지만 commit 위치로 저장
// (빈 메시지, 역직렬화 실패, 저장 로그 직렬화 실패는 원문과 사유를 PostgreSQL storage_failures에 보관한 뒤 완료 처리)
// librdkafka 자동 offset 저장 설정(enable.auto.offset.store)은 끄고, 브로커 commit은 librdkafka가 주기적으로 수행

use std::sync::{Arc, Mutex};

// ClientContext: librdkafka가 발생시키는 각종 이벤트/통계를 받을 수 있는 Context 기능
// Consumer: Topic 구독할 때 사용하는 기능
// ConsumerContext: Consumer 전용 Context 만들 때 사용
// StreamConsumer: Kafka 메시지를 비동기 스트림 형태로 계속 수신하는 Consumer 구현체
// Statistics: librdkafka가 주기적으로 전달하는 Consumer 브로커 통계 정보
// ClientConfig: Kafka Consumer의 bootstrap 서버, group.id, 통계 주기 등의 설정을 구성하는 Builder
// Message: Kafka 메시지에서 payload 등을 꺼내기 위해 사용하는 trait
// BaseConsumer, Rebalance: rebalance callback(partition 할당/해제 알림)을 받을 때 사용
// TopicPartitionList, Offset: Kafka에 저장할 (topic, partition, offset) 목록을 만들 때 사용
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::statistics::Statistics;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use sqlx::PgPool;

use crate::common::error::AppError;
use crate::common::metrics::Metrics;
use crate::config::BrokerConfig;
use crate::core::consumer::event::RawEvent;
use crate::core::consumer::offset::OffsetTracker;
use crate::core::enrichment::service::EnrichmentService;
use crate::core::playbook::engine::PlaybookEngine;
use crate::core::storage::failures::{FailedLog, save_failures_until_ok};
use crate::core::storage::record::StorageRecord;

use tokio::sync::{Semaphore, mpsc};

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
    tracker: Arc<Mutex<OffsetTracker>>, // rebalance로 partition 할당이 해제되면 해당 partition 기록을 삭제하기 위해 보관
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

        // partition별 commit된 offset과 남은 처리량(lag) 갱신
        // - partition -1은 librdkafka 내부용 항목이라 제외
        // - 아직 commit 전이거나 할당되지 않은 partition은 음수(-1001 등)로 오므로 갱신하지 않음
        for topic in statistics.topics.values() {
            for (&partition, stats) in &topic.partitions {
                if partition < 0 {
                    continue;
                }
                let label = partition.to_string();
                if stats.committed_offset >= 0 {
                    self.metrics
                        .consumer_committed_offset
                        .with_label_values(&[label.as_str()])
                        .set(stats.committed_offset);
                }
                if stats.consumer_lag >= 0 {
                    self.metrics
                        .consumer_lag
                        .with_label_values(&[label.as_str()])
                        .set(stats.consumer_lag);
                }
            }
        }
    }
}

// Kafka Consumer에서 사용할 Context가 ConsumerContext trait도 구현하도록 선언
//
// ConsumerContext에는 rebalance, commit callback 등 Consumer 동작과 관련된 여러 callback이 정의되어 있다.
// 그중 pre_rebalance만 구현: partition 할당이 해제(Revoke)되기 직전에 호출되어 offset 추적기의 해당 partition 기록을 삭제
// (기록은 담당하는 동안에만 유효하므로, 다시 할당되면 새로 기록해야 함 -> offset.rs revoke() 주석 참고)
//
// 이 callback은 수신 루프의 recv() 안에서 실행되며, 추적기 lock을 OffsetCommitter::finish()와 함께 사용함
// finish()가 lock을 잡는 시간은 Batch 2만 건의 이진 탐색(1ms 미만)과 store_offsets(librdkafka 메모리에 기록)뿐이라 대기가 짧음
// (store_offsets는 partition별 짧은 lock만 사용하고 rebalance callback을 기다리지 않으므로 교착이 생기지 않음)
impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                tracing::info!(
                    count = partitions.count(),
                    "[PARTITION ASSIGNED] partition 할당됨"
                );
            }
            Rebalance::Revoke(partitions) => {
                let mut tracker = self.tracker.lock().expect("offset 추적기 lock 실패");
                for partition in partitions.elements() {
                    tracker.revoke(partition.partition());

                    // 더 이상 담당하지 않는 partition의 지표는 갱신되지 않아 예전 값이 남으므로 삭제
                    // (해당 라벨이 아직 없으면 Err를 반환하지만 지울 것이 없다는 뜻이라 무시)
                    let label = partition.partition().to_string();
                    let _ = self
                        .metrics
                        .consumer_committed_offset
                        .remove_label_values(&[label.as_str()]);
                    let _ = self
                        .metrics
                        .consumer_lag
                        .remove_label_values(&[label.as_str()]);
                }
                tracing::warn!(
                    count = partitions.count(),
                    "[PARTITION REVOKED] partition 할당 해제, offset 추적 기록 삭제"
                );
            }
            Rebalance::Error(e) => {
                tracing::error!(error = %e, "[REBALANCE FAILED] rebalance 오류");
            }
        }
    }
}

/*
Kafka offset 저장 담당: 수신 등록(begin)과 저장 완료(finish)를 offset 추적기에 반영하고,
commit 위치가 계산되면 librdkafka에 저장함 (브로커 commit은 librdkafka가 auto.commit.interval.ms 주기로 수행)

kafka.rs(수신 Task)와 forwarder.rs(저장 Task)가 함께 사용하므로 추적기를 Mutex로 감쌈
*/
pub struct OffsetCommitter {
    consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
    tracker: Arc<Mutex<OffsetTracker>>,
    topic: String,
}

impl OffsetCommitter {
    pub fn new(
        consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
        tracker: Arc<Mutex<OffsetTracker>>,
        topic: String,
    ) -> Self {
        Self {
            consumer,
            tracker,
            topic,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    // 메시지 수신 시 등록 (수신 루프에서 받은 순서대로 호출)
    pub fn begin(&self, partition: i32, offset: i64) {
        self.tracker
            .lock()
            .expect("offset 추적기 lock 실패")
            .begin(partition, offset);
    }

    // 저장 완료 반영 후, 계산된 commit 위치를 librdkafka에 저장
    // lock을 잡은 채로 저장해야 동시에 끝난 두 Batch의 저장 순서가 뒤바뀌어 commit 위치가 뒤로 가는 일이 없음
    pub fn finish(&self, done: impl IntoIterator<Item = (i32, i64)>) {
        let mut tracker = self.tracker.lock().expect("offset 추적기 lock 실패");
        let positions = tracker.finish(done);
        if positions.is_empty() {
            return;
        }

        let mut offsets = TopicPartitionList::with_capacity(positions.len());
        for (partition, position) in positions {
            // position은 "다음에 읽을 offset"이고, store_offsets는 넘긴 값을 그대로 저장함 (+1 하지 않음)
            if let Err(e) =
                offsets.add_partition_offset(&self.topic, partition, Offset::Offset(position))
            {
                tracing::error!(error = %e, partition, position, "[OFFSET LIST FAILED] commit 위치 목록 생성 실패");
                return;
            }
        }

        // 할당이 해제된 partition은 librdkafka가 거절함 (rebalance 직후라면 정상)
        // 이 경우 해당 로그는 commit되지 않으므로, 다시 할당된 뒤 commit 위치부터 재처리됨 (중복은 log_id로 제거)
        // store_offsets는 모든 partition이 실패할 때만 Err를 반환하므로, 일부 partition의 실패는 항목별로 확인
        if let Err(e) = self.consumer.store_offsets(&offsets) {
            tracing::warn!(error = %e, "[OFFSET STORE FAILED] commit 위치 저장 실패, 재할당 후 재처리됨");
            return;
        }
        for element in offsets.elements() {
            if let Err(e) = element.error() {
                tracing::warn!(
                    error = %e,
                    partition = element.partition(),
                    "[OFFSET STORE FAILED] partition commit 위치 저장 실패, 재할당 후 재처리됨"
                );
            }
        }
    }
}

/*
Consumer가 처리할 수 없는 메시지 보관: 빈 메시지, RawEvent 역직렬화 실패, 저장 로그 직렬화 실패
원문(payload)과 사유(reason)를 PostgreSQL storage_failures에 저장한 뒤 offset 완료 처리
(저장이 끝나야 commit되므로 버려지는 메시지 없음, 담당자는 storage_failures에서 원인 확인)

log_id: kafka-{topic}-{partition}-{offset}
- event_id와 ingested_at을 얻을 수 없으므로 Kafka 위치로 생성 (해싱하지 않아 사람이 읽을 수 있음)
- 재처리로 같은 메시지가 다시 실패해도 같은 값이라 한 건만 유지
*/
pub struct ConsumerFailures {
    postgres: PgPool,
    metrics: Arc<Metrics>,
    committer: Arc<OffsetCommitter>,
}

impl ConsumerFailures {
    pub fn new(postgres: PgPool, metrics: Arc<Metrics>, committer: Arc<OffsetCommitter>) -> Self {
        Self {
            postgres,
            metrics,
            committer,
        }
    }

    // storage_failures에 저장(성공할 때까지 재시도) 후 완료 처리
    async fn record(&self, partition: i32, offset: i64, reason: &str, payload: &[u8]) {
        let log_id = format!("kafka-{}-{}-{}", self.committer.topic(), partition, offset);
        let failure = FailedLog {
            log_id: &log_id,
            reason,
            payload,
            partition,
            offset,
        };

        save_failures_until_ok(&self.postgres, &self.metrics, &[failure]).await;
        self.committer.finish([(partition, offset)]);
    }
}

pub fn build_consumer(
    config: &BrokerConfig,
    metrics: Arc<Metrics>,
    tracker: Arc<Mutex<OffsetTracker>>,
) -> Result<StreamConsumer<KafkaConsumerContext>, AppError> {
    // 우리가 만든 KafkaConsumerContext 객체 생성
    // Context 안에 Prometheus Metrics를 넣어둔다.
    // 여기 담긴 metrics가 나중에 stats() 콜백 안에서 게이지를 갱신하는 데 쓰인다.
    let context = KafkaConsumerContext { metrics, tracker };

    // Kafka Consumer를 생성하고 초기화
    let consumer: StreamConsumer<KafkaConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest") // 저장된 offset이 없으면 가장 오래된 메시지부터 읽음
        .set("statistics.interval.ms", "15000") // Prometheus scrape 주기(15s)와 맞춤
        .set("enable.auto.offset.store", "false") // offset은 저장이 끝난 위치만 OffsetCommitter가 직접 저장 (받는 즉시 자동 저장하면 저장 전 로그가 commit되어 유실 가능)
        .set("enable.auto.commit", "true") // 저장된 offset을 브로커에 commit하는 것은 librdkafka가 주기적으로 수행 (기본값을 명시)
        .set("auto.commit.interval.ms", "5000")
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
    consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
    committer: Arc<OffsetCommitter>,
    failures: Arc<ConsumerFailures>,
    metrics: Arc<Metrics>,
    enrichment: Arc<EnrichmentService>,
    engine: Arc<PlaybookEngine>,
    storage_tx: mpsc::Sender<StorageRecord>,
) {
    // Topic 구독
    let topic = committer.topic();
    if let Err(e) = consumer.subscribe(&[topic]) {
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

                // 이 메시지의 Kafka 위치 (이벤트 로그를 OpenSearch/ClickHouse에 저장 완료 후 커밋할 위치를 계산할 때 사용)
                let partition = msg.partition();
                let offset = msg.offset();
                committer.begin(partition, offset);

                let Some(payload) = msg.payload() else {
                    metrics.consumer_events_failed.inc();
                    tracing::warn!(
                        partition,
                        offset,
                        "[EMPTY MESSAGE] 빈 메시지 수신, storage_failures에 보관"
                    );
                    let reason = "빈 메시지 (payload 없음)".to_string();
                    if !spawn_failure_record(
                        &semaphore,
                        &failures,
                        partition,
                        offset,
                        reason,
                        Vec::new(),
                    )
                    .await
                    {
                        return;
                    }
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
                        let storage_tx = storage_tx.clone();
                        let failures = failures.clone();

                        // 병렬 처리 실행
                        tokio::spawn(async move {
                            let _permit = permit; // Task가 종료되면 permit이 drop되어 Semaphore에 자동 반환됨
                            process_event(
                                event,
                                partition,
                                offset,
                                &enrichment,
                                &engine,
                                &storage_tx,
                                &failures,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        metrics.consumer_events_failed.inc();
                        tracing::warn!(error = %e, partition, offset, "[EVENT PARSE FAILED] 이벤트 역직렬화 실패, storage_failures에 보관");
                        let reason = format!("RawEvent 역직렬화 실패: {e}");
                        if !spawn_failure_record(
                            &semaphore,
                            &failures,
                            partition,
                            offset,
                            reason,
                            payload.to_vec(),
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "[MESSAGE RECV FAILED] Kafka 메시지 수신 실패");
            }
        }
    }
}

// 처리할 수 없는 메시지를 storage_failures에 보관하는 Task 실행
// DB 저장을 기다리는 동안 수신 루프가 멈추지 않도록 별도 Task로 실행하고,
// permit을 먼저 받아 정상 이벤트와 같은 동시 처리 제한을 받음 (잘못된 메시지가 몰려도 Task가 무한히 생기지 않음)
// Semaphore가 닫혔으면 false를 반환하고 수신 루프를 종료함
async fn spawn_failure_record(
    semaphore: &Arc<Semaphore>,
    failures: &Arc<ConsumerFailures>,
    partition: i32,
    offset: i64,
    reason: String,
    payload: Vec<u8>,
) -> bool {
    let Ok(permit) = semaphore.clone().acquire_owned().await else {
        tracing::error!("[SEMAPHORE CLOSED] Semaphore가 닫혀 consumer를 종료함");
        return false;
    };

    let failures = failures.clone();
    tokio::spawn(async move {
        let _permit = permit; // 보관이 끝나면 permit 반환
        failures.record(partition, offset, &reason, &payload).await;
    });
    true
}

// 이벤트 하나를 처리: Enrichment 실행 -> Playbook 평가 -> 이벤트 로그를 전달 Task로 넘김(전달된 로그는 OpenSearch/ClickHouse에 저장 작업에 사용됨)
async fn process_event(
    event: RawEvent,
    partition: i32,
    offset: i64,
    enrichment: &EnrichmentService,
    engine: &PlaybookEngine,
    storage_tx: &mpsc::Sender<StorageRecord>,
    failures: &ConsumerFailures,
) {
    // Enrichment 실행
    let evidence = enrichment.enrich_event(event).await;
    tracing::debug!(
        event_id = %evidence.event.event_id,
        "[ENRICHMENT COMPLETE] 조사 완료"
    );
    tracing::trace!(?evidence, "[EVIDENCE] {:?}", evidence);

    // Playbook 평가 (결정한 Playbook이 없으면 None -> 원본 로그 그대로 저장)
    let playbook_result = engine.run(&evidence).await;
    match &playbook_result {
        Some(result) => tracing::debug!(
            event_id = %evidence.event.event_id,
            action = ?result.action,
            evaluated = result.evaluated,
            "[PLAYBOOK DECIDED] Playbook 판정 완료"
        ),
        None => tracing::debug!(
            event_id = %evidence.event.event_id,
            "[PLAYBOOK NO DECISION] 결정한 Playbook 없음, 원본 그대로 저장"
        ),
    }

    // 저장할 로그 한 건 생성
    let record = match StorageRecord::new(&evidence, playbook_result.as_ref(), partition, offset) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                event_id = %evidence.event.event_id,
                error = %e,
                "[STORAGE RECORD FAILED] 저장 로그 직렬화 실패, storage_failures에 보관"
            );
            // JSON 직렬화가 실패한 상태라 원문 대신 Debug 출력(Rust 구조체 표기)을 보관
            let reason = format!("저장 로그 직렬화 실패: {e}");
            let payload = format!("{evidence:?} / playbook_result: {playbook_result:?}");
            failures
                .record(partition, offset, &reason, payload.as_bytes())
                .await;
            return;
        }
    };

    // 전달 Task로 넘김: 채널이 가득 차면 여기서 대기 -> 처리 Task의 permit이 반납되지 않아 Kafka 소비도 멈춤 (backpressure)
    // 넘기지 못하면 완료 처리하지 않음 -> commit되지 않으므로 재시작 후 다시 처리됨
    if storage_tx.send(record).await.is_err() {
        tracing::error!(
            event_id = %evidence.event.event_id,
            "[STORAGE CHANNEL CLOSED] 전달 Task가 종료되어 로그를 넘기지 못함"
        );
    }
}
