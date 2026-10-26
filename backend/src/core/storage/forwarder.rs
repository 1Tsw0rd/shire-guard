/*
전달 Task: 처리 Task들이 채널로 보낸 로그를 Batch로 만들어 OpenSearch -> ClickHouse 순서로 저장

<< 릴레이 배치 저장 방식 >>

[기본 동작]
Kafka
  ↓
Rust Playbook
  ↓
Logical Batch
  ↓
OpenSearch
  ↓ 성공한 로그만
ClickHouse
  ↓
둘 다 저장 완료
  ↓
Kafka Commit

[장애 케이스 1: OpenSearch가 특정 로그 거절]
OpenSearch
9,997 성공
3 실패
     ↓
3개 제외
     ↓
ClickHouse
9,997 저장
     ↓
실패한 이벤트 3개 postgres 저장 (log_id 중복 시 무시)
     ↓
Kafka commit

1. OpenSearch Bulk 저장 (문서별 결과 확인)
200/201 -> 저장됨
400     -> 거절 (문서 자체 문제, 다시 보내도 같은 결과)
429/5xx -> 그 문서만 재시도
그 외   -> 설정·환경 문제로 보고 에러 로그 후 재시도
요청 자체 실패 -> 남은 문서 전체 재시도
    ↓
2. ClickHouse INSERT (OpenSearch에 저장된 로그만, 실패하면 전체 재시도)
    ↓
3. PostgreSQL storage_failures (거절된 로그가 있을 때만, 실패하면 재시도)
    ↓
4. 지표 갱신

- 결과: 모든 로그는 "OpenSearch와 ClickHouse 둘 다 저장" 또는 "둘 다 없이 storage_failures에 기록" 중 하나
- 재시도는 성공할 때까지 계속함 (버리면 유실), 대기 시간은 100ms부터 2배씩 최대 30초
- backpressure: 저장이 막히면, 세마포어 자리가 없으므로 Kafka 소비가 멈춤 (메모리가 무한히 늘지 않음)
- 같은 로그를 다시 저장해도 중복되지 않음: OpenSearch는 같은 _id로 덮어씀, ClickHouse는 ReplacingMergeTree가 병합 시 하나만 남김,
  PostgreSQL은 ON CONFLICT DO NOTHING

- 이 기능은 OpenSearch와 ClickHouse 로그를 동일하게 하며, 유실률 0%를 목표로 한다.
*/

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
// mpsc(multi-producer, single-consumer)
// 여러 처리 Task가 하나의 channel(tx)에 StorageRecord를 넣고,
// 전달 Task 하나가 receiver(rx)에서 StorageRecord를 꺼내는 구조
//
// 처리 Task 1 ──┐
// 처리 Task 2 ──┤
// 처리 Task 3 ──┼──→ channel ──→ forwarder Task
//     ...    ──┘
use tokio::sync::{Semaphore, mpsc};
// Instant: 특정 시점을 나타내는 구조체
// timeout_at: 지정한 Instant까지 기다리다가, 그 시간이 지나면 Err 반환
use tokio::time::{Instant, timeout_at};

use crate::common::clients::clickhouse::ClickHouseClient;
use crate::common::clients::opensearch::{BulkDoc, OpenSearchClient};
use crate::common::metrics::Metrics;
use crate::core::consumer::kafka::OffsetCommitter;
use crate::core::storage::failures::{FailedLog, save_failures_until_ok};
use crate::core::storage::record::StorageRecord;
use crate::core::storage::retry::RetryDelay;

pub const CHANNEL_CAPACITY: usize = 40_000; // 채널에 쌓일 수 있는 최대 로그 수 (Batch 2개분)

const BATCH_SIZE: usize = 20_000; // Batch 1개의 최대 로그 수
const BATCH_MAX_WAIT: Duration = Duration::from_millis(200); // Batch를 완성하기까지 최대 대기 시간
const BATCH_MAX_BYTES: usize = 32 * 1024 * 1024; // Batch 1개의 최대 크기(바이트): 로그 수가 BATCH_SIZE에 못 미쳐도 이 크기에 도달하면 Batch를 마감
const MAX_IN_FLIGHT: usize = 8; // 동시에 저장 중일 수 있는 Batch 수

pub struct StorageForwarder {
    opensearch: OpenSearchClient,
    clickhouse: ClickHouseClient,
    postgres: PgPool,
    metrics: Arc<Metrics>,
    committer: Arc<OffsetCommitter>,
}

// OpenSearch가 거절한 로그와 거절 사유
struct Rejected<'a> {
    record: &'a StorageRecord,
    reason: String,
}

impl StorageForwarder {
    pub fn new(
        opensearch: OpenSearchClient,
        clickhouse: ClickHouseClient,
        postgres: PgPool,
        metrics: Arc<Metrics>,
        committer: Arc<OffsetCommitter>,
    ) -> Self {
        Self {
            opensearch,
            clickhouse,
            postgres,
            metrics,
            committer,
        }
    }

    // 전달 Task 실행: 채널이 닫힐 때까지 Batch 만들어 저장 Task로 넘김
    pub async fn run(self, mut rx: mpsc::Receiver<StorageRecord>) {
        let forwarder = Arc::new(self);
        let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(MAX_IN_FLIGHT));

        tracing::info!("[STORAGE FORWARDER STARTED] 전달 Task 시작");

        while let Some(batch) = collect_batch(&mut rx).await {
            // 채널이 닫힐 때까지 batch 수집 작업하고, Some(batch) 조건에 부합되면
            // DB 작업을 위한 세마포어 사용권을 받아야 함
            // 세마포어 자리 없으면 날 때까지 대기 (그동안 채널이 차면 처리 Task도 대기)
            let Ok(permit) = semaphore.clone().acquire_owned().await else {
                tracing::error!("[STORAGE SEMAPHORE CLOSED] Semaphore가 닫혀 전달 Task를 종료함");
                return;
            };

            let forwarder = Arc::clone(&forwarder);
            tokio::spawn(async move {
                let _permit = permit; // 저장이 끝나면 사용권 반환
                forwarder.store_batch(batch).await;
            });
        }

        tracing::warn!("[STORAGE FORWARDER CLOSED] 채널이 닫혀 전달 Task를 종료함");
    }

    // Batch 1개 저장: OpenSearch -> ClickHouse -> (거절된 로그가 있으면) PostgreSQL -> Kafka commit 위치 갱신
    async fn store_batch(&self, batch: Vec<StorageRecord>) {
        // OpenSearch
        let (stored, rejected) = self.index_opensearch(&batch).await;

        // ClickHouse
        if !stored.is_empty() {
            self.insert_clickhouse(&stored).await;
        }

        // PostgreSQL
        if !rejected.is_empty() {
            self.save_rejected(&rejected).await;
        }

        self.metrics.storage_events_sent.inc_by(stored.len() as u64);
        self.metrics.storage_rejected.inc_by(rejected.len() as u64);

        tracing::debug!(
            stored = stored.len(),
            rejected = rejected.len(),
            "[STORAGE BATCH DONE] Batch 저장 완료"
        );

        self.committer
            .finish(batch.iter().map(|record| (record.partition, record.offset)));
    }

    // 1. OpenSearch: 모든 문서가 저장되거나 거절될 때까지 반복
    // 반환: (저장된 로그: clickhouse 저장용, 거절된 로그: postgres 저장용)
    async fn index_opensearch<'a>(
        &self,
        batch: &'a [StorageRecord],
    ) -> (Vec<&'a StorageRecord>, Vec<Rejected<'a>>) {
        // StorageRecord를 직접 소유하지 않고 참조(&'a)하여 Vec에 저장
        // Rejected 내부의 참조도 'a 수명을 가지며 Vec에 Rejected를 저장

        let mut stored = Vec::with_capacity(batch.len()); // ClickHouse 저장용으로 전달할 로그들 저장 공간
        let mut rejected = Vec::new(); // OpenSearch에서 거절된 로그 목록
        let mut pending: Vec<&StorageRecord> = batch.iter().collect(); // 아직 저장되지 않은 로그
        let mut delay = RetryDelay::new();

        loop {
            let docs: Vec<BulkDoc> = pending
                .iter()
                .map(|&record| BulkDoc {
                    index: &record.index_name,     // String -> &String
                    id: &record.log_id,            // String -> &String
                    source: record.doc.as_bytes(), // String -> &[u8] (as_bytes()가 이미 참조 반환)
                })
                .collect();

            let started = Instant::now();
            let result = self.opensearch.bulk_index(&docs).await;
            self.observe_request("opensearch", started);

            match result {
                Ok(results) => {
                    let mut retry = Vec::new();
                    let mut unexpected: Option<(u16, Option<String>)> = None;

                    // zip(): pending의 로그와 results의 응답을 같은 순서끼리 묶음
                    // 예: [record, B, C] + [item, 400, 429] -> [(record, item), (B, 400), (C, 429)]
                    for (record, item) in pending.into_iter().zip(results) {
                        // OpenSearch Response를 상태에 따라 분류
                        match classify(item.status) {
                            DocResult::Stored => stored.push(record), // 정상 저장된 로그는 stored에 추가
                            DocResult::Rejected => rejected.push(Rejected {
                                // 거절된 로그는 이유와 함께 rejected에 추가하여 나중에 PostgreSQL의 storage_failures에 저장
                                record,
                                reason: item.error.unwrap_or_default(),
                            }),
                            DocResult::Retry => retry.push(record), // 일시적 오류이므로 다음 요청에서 다시 시도
                            DocResult::Unexpected => {
                                // 예상하지 못한 상태도 로그를 남기고 일단 재시도
                                unexpected.get_or_insert((item.status, item.error)); // 첫 건만 기록: 원인이 보통 설정·환경 문제로 전 문서에 공통이라, 문서별로 남기면 로그가 폭주하여 1건만 저장
                                retry.push(record);
                            }
                        }
                    }

                    // 예상하지 못한 상태가 있는 경우
                    if let Some((status, error)) = unexpected {
                        tracing::error!(
                            status,
                            error = ?error,
                            "[OPENSEARCH UNEXPECTED STATUS] 예상하지 못한 문서 응답 (설정·환경 확인 필요), 재시도"
                        );
                    }

                    pending = retry; // 재시도해야 할 로그만 다음 loop의 pending으로 사용
                }

                Err(err) => {
                    // Bulk 요청 자체가 실패했으므로 pending 전체를 그대로 다시 시도
                    tracing::warn!(
                        error = ?err,
                        count = pending.len(),
                        retry_in_ms = delay.current().as_millis() as u64,
                        "[OPENSEARCH BULK RETRY] Bulk 요청 실패, 남은 문서 전체 재시도"
                    );
                }
            }

            if pending.is_empty() {
                break;
            }

            // OpenSearch 재시도 횟수 +1 (doc 건수가 아니라 재요청 1번당 1)
            self.metrics
                .storage_retries
                .with_label_values(&["opensearch"])
                .inc();

            // wait(): 처음에는 RETRY_INITIAL_DELAY만큼 대기하고 이후 대기 시간을 2배로 변경
            delay.wait().await;
        }

        // OpenSearch에 저장 성공한 doc 수만큼 증가 (거절 400 doc는 제외)
        self.metrics
            .storage_db_events
            .with_label_values(&["opensearch"])
            .inc_by(stored.len() as u64);

        if !rejected.is_empty() {
            tracing::warn!(
                count = rejected.len(),
                first_log_id = %rejected[0].record.log_id,
                first_reason = %rejected[0].reason,
                "[OPENSEARCH REJECTED] 문서 자체 문제로 거절됨, storage_failures에 기록"
            );
        }

        (stored, rejected)
    }

    // 2. ClickHouse: OpenSearch에 저장된 로그만 INSERT, 성공할 때까지 반복
    async fn insert_clickhouse(&self, records: &[&StorageRecord]) {
        let rows = build_clickhouse_rows(records);
        let mut delay = RetryDelay::new();

        loop {
            let started = Instant::now();
            // INSERT는 본문 소유권을 가져가므로, 재시도에 대비해 복사본을 전달
            let result = self
                .clickhouse
                .insert_json_rows("event_logs", rows.clone())
                .await;
            self.observe_request("clickhouse", started);

            match result {
                Ok(()) => break,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        count = records.len(),
                        retry_in_ms = delay.current().as_millis() as u64,
                        "[CLICKHOUSE INSERT RETRY] INSERT 실패, 재시도"
                    );
                }
            }

            self.metrics
                .storage_retries
                .with_label_values(&["clickhouse"])
                .inc();
            delay.wait().await;
        }

        // ClickHouse에 저장 성공한 행 수만큼 증가 (INSERT는 전부 성공 or 전부 실패라 records 전체 건수)
        self.metrics
            .storage_db_events
            .with_label_values(&["clickhouse"])
            .inc_by(records.len() as u64);
    }

    // 3. PostgreSQL: 거절된 로그를 storage_failures에 기록, 성공할 때까지 반복 (재시도 로직은 failures.rs)
    async fn save_rejected(&self, rejected: &[Rejected<'_>]) {
        let failures: Vec<FailedLog> = rejected
            .iter()
            .map(|r| FailedLog {
                log_id: &r.record.log_id,
                reason: &r.reason,
                payload: r.record.doc.as_bytes(),
                partition: r.record.partition,
                offset: r.record.offset,
            })
            .collect();

        save_failures_until_ok(&self.postgres, &self.metrics, &failures).await;
    }

    // DB 요청 1번에 걸린 시간을 지표에 기록
    fn observe_request(&self, db: &str, started: Instant) {
        self.metrics
            .storage_request_seconds
            .with_label_values(&[db]) // db 라벨 값에 해당하는 메트릭을 선택
            // started.elapsed(): 요청 시작부터 현재까지 경과한 시간을 Duration으로 계산
            // as_secs_f64(): Duration을 초 단위 f64 값으로 변환
            .observe(started.elapsed().as_secs_f64()); // 측정한 요청 시간을 Histogram에 기록
    }
}

// OpenSearch 문서별 상태 코드를 처리 방식으로 분류
#[derive(Debug, PartialEq, Eq)]
enum DocResult {
    Stored,     // 200, 201: 저장됨
    Rejected,   // 400: 문서 자체 문제 (매핑 충돌, 필드 수 초과 등), 다시 보내도 같은 결과
    Retry,      // 429, 5xx: OpenSearch가 일시적으로 처리하지 못함
    Unexpected, // 그 외 (404 등): 문서가 아니라 설정·환경 문제로 추정
}

fn classify(status: u16) -> DocResult {
    match status {
        200 | 201 => DocResult::Stored,
        400 => DocResult::Rejected,
        429 | 500..=599 => DocResult::Retry,
        _ => DocResult::Unexpected,
    }
}

/*
ClickHouse 저장을 위한 메모리 공간 확보 및 INSERT 본문 생성 (JSONEachRow: 한 줄에 한 행)
{"log_id":"<log_id>","payload":<doc>}
{"log_id":"<log_id>","payload":<doc>}

- doc은 이미 JSON이므로 다시 직렬화하지 않고 그대로 이어 붙임
*/
fn build_clickhouse_rows(records: &[&StorageRecord]) -> Vec<u8> {
    const PREFIX: &str = r#"{"log_id":""#;
    const MIDDLE: &str = r#"","payload":"#;
    const SUFFIX: &str = "}\n";

    // ClickHouse INSERT를 위해 본문 전체 크기만큼 메모리를 미리 확보
    let fixed_len = PREFIX.len() + MIDDLE.len() + SUFFIX.len();
    let capacity = records
        .iter()
        .map(|r| fixed_len + r.log_id.len() + r.doc.len())
        .sum();

    let mut rows = String::with_capacity(capacity);
    for record in records {
        rows.push_str(PREFIX);
        rows.push_str(&record.log_id);
        rows.push_str(MIDDLE);
        rows.push_str(&record.doc);
        rows.push_str(SUFFIX);
    }
    rows.into_bytes() // insert_json_rows가 Vec<u8>를 받아서 타입 변환 
}

// OpenSearch Bulk 요청 본문에서 로그 1건이 차지하는 크기(바이트)
fn bulk_bytes(record: &StorageRecord) -> usize {
    record.doc.len() + record.index_name.len() + record.log_id.len()
}

// 채널에서 로그를 꺼내 Batch 하나를 만듬
// 아래 3가지 중 하나가 되면 지금까지 모은 로그로 Batch를 마감
//   1. 로그 수가 BATCH_SIZE에 도달
//   2. 로그 크기 합이 BATCH_MAX_BYTES에 도달 (마지막에 넣은 로그 1건만큼 넘을 수 있음)
//   3. 첫 로그를 받은 뒤 BATCH_MAX_WAIT가 지남 (로그가 적게 들어올 때 너무 오래 기다리지 않도록)
// 채널이 닫혔으면 남은 로그만으로 마감하고, 남은 로그도 없으면 None
//
// 예: 일반 로그(약 1.3KB)만 들어오면 20,000건(약 27MB)에서 마감
//     큰 로그(약 22KB)가 10% 섞이면 약 9,600건(32MB)에서 마감
async fn collect_batch(rx: &mut mpsc::Receiver<StorageRecord>) -> Option<Vec<StorageRecord>> {
    // 첫 로그는 들어올 때까지 기다림 (채널이 닫혔고 남은 로그도 없으면 None 반환)
    let first = rx.recv().await?;
    let mut bytes = bulk_bytes(&first);
    let mut batch = Vec::with_capacity(BATCH_SIZE); // BATCH_SIZE건을 담을 공간을 미리 확보
    batch.push(first);

    // BATCH_SIZE 도달 / BATCH_MAX_BYTES 도달 / BATCH_MAX_WAIT 초과 / 채널 닫힘 중 하나가 되면 지금까지 모은 batch를 반환
    let deadline = Instant::now() + BATCH_MAX_WAIT;
    while batch.len() < BATCH_SIZE && bytes < BATCH_MAX_BYTES {
        // 로그를 1건씩 꺼내며 크기를 더함 (여러 건을 한 번에 꺼내면 크기 상한을 크게 넘을 수 있음)
        // timeout_at: deadline이 지나면 더 기다리지 않고 Err 반환
        match timeout_at(deadline, rx.recv()).await {
            Ok(Some(record)) => {
                bytes += bulk_bytes(&record);
                batch.push(record);
            }
            Ok(None) => break, // 채널 닫힘: 지금까지 모은 만큼 사용
            Err(_) => break,   // 시간 초과: 지금까지 모은 만큼 사용
        }
    }

    Some(batch)
}

// cargo test --lib storage::forwarder
#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64) -> StorageRecord {
        StorageRecord {
            log_id: format!("log-{offset}"),
            index_name: "event-logs-2026.10.09".to_string(),
            doc: format!(r#"{{"event_id":"evt-{offset}"}}"#),
            partition: 0,
            offset,
        }
    }

    // 시나리오 1: BATCH_SIZE건보다 많이 쌓여 있으면 BATCH_SIZE건씩 나눠 묶는다
    #[tokio::test]
    async fn collect_batch_splits_at_batch_size() {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        for i in 0..(BATCH_SIZE as i64 + 10) {
            tx.send(record(i)).await.unwrap();
        }

        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), BATCH_SIZE);
        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 10);
    }

    // 시나리오 2: BATCH_SIZE건이 안 차면 BATCH_MAX_WAIT만큼 기다린 뒤 모인 만큼으로 Batch 완성한다
    #[tokio::test]
    async fn collect_batch_returns_partial_batch_after_wait() {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        tx.send(record(1)).await.unwrap();
        tx.send(record(2)).await.unwrap();

        let started = Instant::now();
        let batch = collect_batch(&mut rx).await.unwrap();

        assert_eq!(batch.len(), 2);
        assert!(started.elapsed() >= BATCH_MAX_WAIT);
        drop(tx);
    }

    // 시나리오 3: 채널이 닫혔고 남은 로그가 없으면 None
    #[tokio::test]
    async fn collect_batch_returns_none_when_channel_closed() {
        let (tx, mut rx) = mpsc::channel::<StorageRecord>(CHANNEL_CAPACITY);
        drop(tx);

        assert!(collect_batch(&mut rx).await.is_none());
    }

    // 시나리오 4: 채널이 닫혀도 남아 있던 로그는 기다리지 않고 Batch로 반환한다
    #[tokio::test]
    async fn collect_batch_flushes_remaining_records_on_close() {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        tx.send(record(1)).await.unwrap();
        drop(tx);

        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 1);
    }

    // 시나리오 5: OpenSearch 문서별 상태 코드를 처리 방식으로 분류한다
    #[test]
    fn classifies_document_status() {
        assert_eq!(classify(200), DocResult::Stored);
        assert_eq!(classify(201), DocResult::Stored);
        assert_eq!(classify(400), DocResult::Rejected);
        assert_eq!(classify(429), DocResult::Retry);
        assert_eq!(classify(500), DocResult::Retry);
        assert_eq!(classify(503), DocResult::Retry);
        assert_eq!(classify(404), DocResult::Unexpected);
        assert_eq!(classify(409), DocResult::Unexpected);
    }

    // 시나리오 6: ClickHouse 본문은 한 줄에 {"log_id":...,"payload":<doc>} 한 행이고, 각 줄이 올바른 JSON이다
    #[test]
    fn builds_clickhouse_rows_as_json_each_row() {
        let (a, b) = (record(1), record(2));
        let rows = build_clickhouse_rows(&[&a, &b]);
        let text = String::from_utf8(rows.clone()).unwrap();

        assert_eq!(
            text,
            concat!(
                r#"{"log_id":"log-1","payload":{"event_id":"evt-1"}}"#,
                "\n",
                r#"{"log_id":"log-2","payload":{"event_id":"evt-2"}}"#,
                "\n",
            )
        );
        for line in text.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(value["payload"].is_object());
        }
        assert_eq!(rows.capacity(), rows.len()); // 미리 계산한 크기가 실제 크기와 같음
    }

    // 시나리오 7: 로그가 커서 BATCH_SIZE건이 안 되어도 BATCH_MAX_BYTES에 도달하면 Batch를 마감한다
    #[tokio::test]
    async fn collect_batch_splits_at_max_bytes() {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);

        // 문서 1건을 상한의 1/4 크기로 만들어 6건 전송 -> 4건째에 상한 도달
        let big_doc = "x".repeat(BATCH_MAX_BYTES / 4);
        for i in 0..6 {
            let mut r = record(i);
            r.doc = big_doc.clone();
            tx.send(r).await.unwrap();
        }
        drop(tx);

        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 4);
        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 2);
        assert!(collect_batch(&mut rx).await.is_none());
    }
}
