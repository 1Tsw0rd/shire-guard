/*
전달 Task: 처리 Task들이 채널로 보낸 로그를 묶어서 vector-storage로 전송

  처리 Task 1024개 ──(StorageRecord)──▶ 채널 (최대 CHANNEL_CAPACITY건)
                                          │
                                          ▼
                       Batch 만들기: BATCH_MAX_EVENTS건 또는 BATCH_MAX_WAIT 중 먼저 되는 쪽
                                          │
                                          ▼
                       Batch마다 별도 Task로 전송 (동시 최대 MAX_IN_FLIGHT개)
                                          │
                         ┌────────────────┴────────────────┐
                        2xx                 그 외 (4xx, 5xx, 연결 실패)
                     전달 완료               대기 후 재시도(무한, 버리지 않음)

- 처리와 전달을 분리하는 이유
  vector-storage는 디스크 버퍼/ack 설정에 따라 응답까지 시간이 걸릴 수 있음
  처리 Task가 응답을 기다리면 처리량이 초당 약 1천 건(1024개 / 1초)으로 떨어짐

- backpressure
  전달이 막히면 채널이 가득 차고, 처리 Task가 대기하면서 Kafka 소비도 멈춤 (메모리가 무한히 늘지 않음)

- 4xx를 버리지 않는 이유
  버리면 유실이므로, 고칠 때까지 재시도하며 에러로 알림
  (vector-storage가 memory + ack라 DB 저장 실패도 Rust까지 전달됨)
*/

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::CONTENT_TYPE; // HTTP 요청의 Content-Type 헤더 이름
// mpsc(multi-producer, single-consumer)
// 여러 처리 Task가 하나의 channel(tx)에 StorageRecord를 넣고,
// 전달 Task 하나가 receiver(rx)에서 StorageRecord를 꺼내는 구조
//
// 처리 Task 1 ──┐
// 처리 Task 2 ──┤
// 처리 Task 3 ──┼──→ channel ──→ forwarder Task
//     ...      ──┘
use tokio::sync::{Semaphore, mpsc};
// Instant: 특정 시점을 나타내는 구조체
// timeout_at: 지정한 Instant까지 기다리다가, 그 시간이 지나면 Err 반환
use tokio::time::{Instant, sleep, timeout_at};

use crate::common::error::AppError;
use crate::common::metrics::Metrics;
use crate::core::storage::record::StorageRecord;

pub const CHANNEL_CAPACITY: usize = 10_000; // 채널에 쌓을 수 있는 최대 StorageRecord 수

const BATCH_MAX_EVENTS: usize = 1_000; // Batch로 전송할 최대 StorageRecord 수
const BATCH_MAX_WAIT: Duration = Duration::from_millis(200); // Batch로 전송할 최대 대기 시간
const MAX_IN_FLIGHT: usize = 400; // 동시에 전송할 수 있는 Batch 수

const HTTP_TIMEOUT: Duration = Duration::from_secs(30); // vector-storage에 전송할 때 HTTP 요청 제한 시간
const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100); // vector-storage에 전송 실패 시 재시도 대기 시간
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30); // vector-storage에 전송 실패 시 재시도 최대 대기 시간

pub struct StorageForwarder {
    http: reqwest::Client,
    url: String,
    metrics: Arc<Metrics>,
}

impl StorageForwarder {
    pub fn new(url: String, metrics: Arc<Metrics>) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| AppError::Internal(format!("Storage HTTP client 생성 실패: {e}")))?;

        Ok(Self { http, url, metrics })
    }

    // 전달 Task 실행
    pub async fn run(self, mut rx: mpsc::Receiver<StorageRecord>) {
        let forwarder = Arc::new(self);
        // 동시에 전송할 수 있는 Batch 수를 제한하기 위해 Semaphore 사용
        // MAX_IN_FLIGHT개의 permit을 가진 Semaphore를 만들고, Batch 전송 Task가 시작될 때 permit을 하나 가져가고, 끝나면 반납
        let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT));

        tracing::info!(url = %forwarder.url, "[STORAGE FORWARDER STARTED] 전달 Task 시작");

        while let Some(batch) = collect_batch(&mut rx).await {
            // 동시 전송이 MAX_IN_FLIGHT개면 자리가 날 때까지 대기 (그동안 채널이 차서 처리 Task도 대기)
            let Ok(permit) = semaphore.clone().acquire_owned().await else {
                tracing::error!("[STORAGE SEMAPHORE CLOSED] Semaphore가 닫혀 전달 Task를 종료함");
                return;
            };

            let forwarder = Arc::clone(&forwarder);
            tokio::spawn(async move {
                let _permit = permit; // 전송이 끝나면 자리 반환
                forwarder.send_batch(batch).await;
            });
        }

        tracing::warn!("[STORAGE FORWARDER CLOSED] 채널이 닫혀 전달 Task를 종료함");
    }

    // Batch 하나를 2xx 성공 응답을 받을 때까지 전송
    async fn send_batch(&self, batch: Vec<StorageRecord>) {
        let count = batch.len() as u64; // Batch 안의 StorageRecord 수

        let total_len = batch.iter().map(|r| r.line.len()).sum(); // Batch 안의 모든 로그(line)의 바이트 크기를 합산
        let mut body = Vec::with_capacity(total_len); // HTTP Body에 들어갈 전체 크기만큼 Vec의 메모리를 미리 확보

        // extend_from_slice: Vec에 다른 slice를 이어 붙임
        // 예시: {"id":1}\n{"id":2}\n{"id":3}\n
        for record in &batch {
            body.extend_from_slice(&record.line);
        }

        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            let result = self
                .http
                .post(&self.url)
                .header(CONTENT_TYPE, "application/x-ndjson")
                .body(body.clone())
                .send()
                .await;

            match result {
                // 2xx: 정상 처리
                Ok(resp) if resp.status().is_success() => {
                    self.metrics.storage_events_sent.inc_by(count); // inc_by: metrics의 counter를 count만큼 증가
                    tracing::debug!(count, "[STORAGE BATCH SENT] Batch 저장 완료");
                    return;
                }

                // 4xx: 클라이언트 요청 오류
                Ok(resp) if resp.status().is_client_error() => {
                    let status = resp.status();
                    let detail = resp.text().await.unwrap_or_default();
                    tracing::error!(
                        %status,
                        count,
                        %detail,
                        retry_in_ms = delay.as_millis() as u64,
                        "[STORAGE BATCH REJECTED] vector-storage가 요청을 거절함 (설정 확인 필요), 재시도"
                    );
                }

                // 5xx: vector-storage 측 오류 응답
                Ok(resp) => {
                    tracing::warn!(
                        status = %resp.status(),
                        count,
                        retry_in_ms = delay.as_millis() as u64,
                        "[STORAGE BATCH RETRY] vector-storage 오류 응답, 재시도"
                    );
                }

                // 연결 실패, 응답 시간 초과
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        count,
                        retry_in_ms = delay.as_millis() as u64,
                        "[STORAGE BATCH RETRY] vector-storage 연결 실패, 재시도"
                    );
                }
            }

            self.metrics.storage_retries.inc(); // inc: metrics의 counter를 1 증가
            sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX_DELAY); // 지수적 backoff: 재시도 대기 시간을 2배로 늘리되, RETRY_MAX_DELAY를 넘지 않음
        }
    }
}

// 채널에서 Batch 하나를 만듦
async fn collect_batch(rx: &mut mpsc::Receiver<StorageRecord>) -> Option<Vec<StorageRecord>> {
    let mut batch = Vec::with_capacity(BATCH_MAX_EVENTS);

    // recv_many(): 채널에서 여러 로그를 한 번에 꺼냄(최대 BATCH_MAX_EVENTS 건)
    // 현재 batch의 남은 칸까지만 가져올 수 있음
    // 채널에 로그가 없으면 로그가 들어올 때까지 대기
    // 반환값이 0이면 채널이 닫혔다는 의미 -> None 반환
    if rx.recv_many(&mut batch, BATCH_MAX_EVENTS).await == 0 {
        return None;
    }

    let deadline = Instant::now() + BATCH_MAX_WAIT;
    // BATCH_MAX_EVENTS 건을 모으거나
    // BATCH_MAX_WAIT만큼 기다리거나
    // 둘 중 먼저 되는 쪽에서 Batch 완성
    while batch.len() < BATCH_MAX_EVENTS {
        let remaining = BATCH_MAX_EVENTS - batch.len();
        match timeout_at(deadline, rx.recv_many(&mut batch, remaining)).await {
            Ok(0) => break,  // 채널 닫힘: 모은 것만 전송
            Ok(_) => {}      // 더 받음, 계속
            Err(_) => break, // 대기 시간 초과: Batch 완성
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
            line: format!("{{\"offset\":{offset}}}\n").into_bytes(),
            partition: 0,
            offset,
        }
    }

    // 시나리오 1: BATCH_MAX_EVENTS건보다 많이 쌓여 있으면 BATCH_MAX_EVENTS건씩 나눠 묶는다
    // 채널에 총 1,010건이 있으면
    // 첫 번째 Batch는 최대 1,000건을 가져가고,
    // 남은 10건은 두 번째 Batch에서 그대로 가져간다.
    #[tokio::test]
    async fn collect_batch_splits_at_max_events() {
        // channel 생성
        // tx(Sender): 테스트용 StorageRecord를 채널에 넣는 쪽
        // rx(Receiver): collect_batch()가 StorageRecord를 꺼내는 쪽
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);

        // 0부터 1,009까지 총 1,010개의 테스트용 StorageRecord를 채널에 넣음
        for i in 0..(BATCH_MAX_EVENTS as i64 + 10) {
            tx.send(record(i)).await.unwrap();
        }

        // 첫 번째 Batch: 1,000건
        assert_eq!(
            collect_batch(&mut rx).await.unwrap().len(),
            BATCH_MAX_EVENTS
        );
        // 채널에 남아 있던 10건
        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 10);
    }

    // 시나리오 2: 채널에 2건만 있고 1,000건이 채워지지 않으면, 최대 BATCH_MAX_WAIT 시간만큼 기다린 뒤 현재 모인 2건으로 Batch를 완성한다.
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

    // 시나리오 3: 채널에 남은 로그가 0건인 상태에서 Sender가 닫히면, 가져올 로그가 없으므로 collect_batch()는 None을 반환한다.
    #[tokio::test]
    async fn collect_batch_returns_none_when_channel_closed() {
        let (tx, mut rx) = mpsc::channel::<StorageRecord>(CHANNEL_CAPACITY);
        drop(tx);

        // 채널도 닫혀 있고 남은 로그도 없으므로 None 반환
        assert!(collect_batch(&mut rx).await.is_none());
    }

    // 시나리오 4: 채널에 로그 1건이 남아 있는 상태에서 Sender가 닫히면, 추가 로그를 기다리지 않고 남아 있던 1건으로 Batch를 완성한다.
    #[tokio::test]
    async fn collect_batch_flushes_remaining_records_on_close() {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        tx.send(record(1)).await.unwrap();
        drop(tx);

        // 남아 있던 1건을 기다리지 않고 Batch로 반환
        assert_eq!(collect_batch(&mut rx).await.unwrap().len(), 1);
    }
}
