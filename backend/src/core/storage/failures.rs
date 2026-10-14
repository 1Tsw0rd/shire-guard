/*
저장하지 못한 로그를 PostgreSQL의 storage_failures 테이블에 insert

보관 대상과 log_id:
- OpenSearch가 문서 자체 문제(400)로 거절한 로그의 log_id = sha256(event_id|ingested_at)로 적용
  (OpenSearch _id 와 ClickHouse log_id와 같은 값)
- Consumer가 처리할 수 없는 메시지(빈 메시지, 역직렬화 실패 등)의 log_id = kafka-{topic}-{partition}-{offset}
  (event_id와 ingested_at을 얻을 수 없으므로 Kafka 위치로 생성, 재처리돼도 같은 값이라 한 건만 유지)

payload는 받은 바이트 그대로 BYTEA로 저장 (깨진 JSON, NUL 바이트, 잘못된 UTF-8도 거부되지 않음)
조회: SELECT log_id, reason, bytea_to_text_safe(payload) FROM storage_failures; (함수는 docker/postgres/init/001_init.sql)
*/

use sqlx::PgPool;

use crate::common::metrics::Metrics;
use crate::core::storage::retry::RetryDelay;

pub struct FailedLog<'a> {
    pub log_id: &'a str,
    pub reason: &'a str, // 실패 사유 (OpenSearch error.reason, 역직렬화 에러 메시지 등)
    pub payload: &'a [u8], // 저장하려던 로그 원문 (바이트 그대로)
    pub partition: i32,  // 원본 이벤트의 Kafka partition 번호
    pub offset: i64,     // 원본 이벤트의 Kafka offset
}

/*
저장하지 못한 로그들을 Bulk insert로 저장

UNNEST: 컬럼별 배열의 같은 위치에 있는 값을 하나의 행으로 묶음
  log_ids    = ["a1", "a2"]
  reasons    = ["r1", "r2"]      ->   ("a1", "r1", ...)
  ...                                ("a2", "r2", ...)
*/
pub async fn save_failures(pool: &PgPool, failures: &[FailedLog<'_>]) -> Result<(), sqlx::Error> {
    if failures.is_empty() {
        return Ok(());
    }

    // 한 번 순회하면서 UNNEST에 사용할 컬럼별 배열을 각각 생성
    // 예: failures 2건 -> log_ids["a1", "a2"], reasons["r1", "r2"], ...
    let mut log_ids = Vec::with_capacity(failures.len());
    let mut reasons = Vec::with_capacity(failures.len());
    let mut payloads = Vec::with_capacity(failures.len());
    let mut partitions = Vec::with_capacity(failures.len());
    let mut offsets = Vec::with_capacity(failures.len());

    for f in failures {
        log_ids.push(f.log_id);
        reasons.push(f.reason);
        payloads.push(f.payload);
        partitions.push(f.partition);
        offsets.push(f.offset);
    }

    sqlx::query!(
        r#"
        INSERT INTO storage_failures (log_id, reason, payload, kafka_partition, kafka_offset)
        SELECT log_id, reason, payload, kafka_partition, kafka_offset
        FROM UNNEST($1::text[], $2::text[], $3::bytea[], $4::int4[], $5::int8[])
            AS t(log_id, reason, payload, kafka_partition, kafka_offset)
        ON CONFLICT (log_id) DO NOTHING
        "#,
        &log_ids as &[&str],
        &reasons as &[&str],
        &payloads as &[&[u8]],
        &partitions,
        &offsets
    )
    .execute(pool)
    .await?;

    Ok(())
}

// 저장에 성공할 때까지 재시도 (실패 로그를 버리면 유실이므로), 재시도할 때마다 storage_retries{db="postgres"} 증가
pub async fn save_failures_until_ok(pool: &PgPool, metrics: &Metrics, failures: &[FailedLog<'_>]) {
    let mut delay = RetryDelay::new();

    loop {
        match save_failures(pool, failures).await {
            Ok(()) => return,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    count = failures.len(),
                    retry_in_ms = delay.current().as_millis() as u64,
                    "[STORAGE FAILURES RETRY] storage_failures 저장 실패, 재시도"
                );
            }
        }

        // PostgreSQL 재시도 횟수를 메트릭 카운터에 1 증가
        metrics
            .storage_retries
            .with_label_values(&["postgres"])
            .inc();

        delay.wait().await;
    }
}

// cargo test --lib storage::failures -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    const BATCH_TEST_LOG_IDS: [&str; 2] =
        ["test-storage-failures-1111", "test-storage-failures-2222"]; // 시나리오 1 전용
    const NUL_TEST_LOG_ID: &str = "test-storage-failures-3333"; // 시나리오 3 전용
    const RAW_TEST_LOG_IDS: [&str; 5] = [
        "test-storage-failures-4444",
        "test-storage-failures-5555",
        "test-storage-failures-6666",
        "test-storage-failures-7777",
        "test-storage-failures-8888",
    ]; // 시나리오 4 전용

    async fn test_pool() -> PgPool {
        dotenvy::dotenv().ok();
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL 필요");
        PgPool::connect(&url).await.expect("PostgreSQL 연결 실패")
    }

    // 지정한 log_id의 테스트 데이터만 삭제
    async fn cleanup(pool: &PgPool, log_ids: &[&str]) {
        sqlx::query!(
            "DELETE FROM storage_failures WHERE log_id = ANY($1)",
            log_ids as &[&str]
        )
        .execute(pool)
        .await
        .unwrap();
    }

    fn failed(
        log_id: &'static str,
        reason: &'static str,
        payload: &'static str,
    ) -> FailedLog<'static> {
        FailedLog {
            log_id,
            reason,
            payload: payload.as_bytes(),
            partition: 0,
            offset: 100,
        }
    }

    /*
    시나리오 1: 여러 건을 한 번에 저장하고, 같은 log_id를 다시 저장하면 무시된다

    1번째 저장:
    - 1111 저장
    - 2222 저장

    2번째 저장:
    - 1111을 다른 reason과 payload로 다시 저장 시도하지만 ON CONFLICT (log_id) DO NOTHING으로 무시
    - 기존 1111의 reason과 payload가 유지되는지 확인
    */
    #[tokio::test]
    #[ignore]
    async fn saves_batch_and_ignores_duplicates() {
        let pool = test_pool().await;
        cleanup(&pool, &BATCH_TEST_LOG_IDS).await;

        save_failures(
            &pool,
            &[
                failed(
                    BATCH_TEST_LOG_IDS[0],
                    "mapper_parsing_exception",
                    r#"{"event_id":"evt-1"}"#,
                ),
                failed(
                    BATCH_TEST_LOG_IDS[1],
                    "illegal_argument_exception",
                    r#"{"event_id":"evt-2"}"#,
                ),
            ],
        )
        .await
        .unwrap();

        // 이미 저장된 1111은 log_id가 같으므로 무시됨
        save_failures(&pool, &[failed(BATCH_TEST_LOG_IDS[0], "다른 사유", "{}")])
            .await
            .unwrap();

        let rows = sqlx::query!(
            r#"SELECT log_id, reason, bytea_to_text_safe(payload) AS "payload!"
               FROM storage_failures
               WHERE log_id = ANY($1)
               ORDER BY log_id"#,
            &BATCH_TEST_LOG_IDS as &[&str]
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        cleanup(&pool, &BATCH_TEST_LOG_IDS).await;

        // 1111, 2222 두 건만 저장되고 중복 1111은 추가되지 않음
        assert_eq!(rows.len(), 2);

        // 1111은 두 번째 INSERT가 무시되어 첫 번째 데이터가 유지됨
        assert_eq!(rows[0].reason, "mapper_parsing_exception");
        assert_eq!(rows[0].payload, r#"{"event_id":"evt-1"}"#);

        // 2222도 정상적으로 저장됨
        assert_eq!(rows[1].reason, "illegal_argument_exception");
        assert_eq!(rows[1].payload, r#"{"event_id":"evt-2"}"#);
    }

    /*
    시나리오 2: 저장할 로그가 없으면 DB를 건드리지 않고 성공
    */
    #[tokio::test]
    #[ignore]
    async fn empty_list_is_ok() {
        let pool = test_pool().await;

        assert!(save_failures(&pool, &[]).await.is_ok());
    }

    // 시나리오 3: JSON 이스케이프 \u0000(NUL 문자를 나타내는 6글자 문자열)이 든 문서도 원문 그대로 저장되고, JSON으로 해석하면 NUL 문자가 된다
    // (실제 NUL 바이트(0x00)가 든 원문은 시나리오 4의 5555에서 확인)
    #[tokio::test]
    #[ignore]
    async fn saves_payload_with_nul_character() {
        let pool = test_pool().await;
        cleanup(&pool, &[NUL_TEST_LOG_ID]).await;

        let payload = r#"{"event_id":"evt-1","note":"a\u0000b"}"#;
        save_failures(
            &pool,
            &[failed(NUL_TEST_LOG_ID, "mapper_parsing_exception", payload)],
        )
        .await
        .unwrap();

        let row = sqlx::query!(
            r#"SELECT bytea_to_text_safe(payload) AS "payload!" FROM storage_failures WHERE log_id = $1"#,
            NUL_TEST_LOG_ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        cleanup(&pool, &[NUL_TEST_LOG_ID]).await;

        let value: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
        assert_eq!(value["note"], "a\u{0}b"); // 해석하면 a + NUL + b
    }

    /*
    시나리오 4: 깨진 원문도 바이트 그대로 저장되고, bytea_to_text_safe()로 사람이 읽을 수 있게 조회된다

    4444: 깨진 JSON          -> 그대로 저장, 문자열로 조회
    5555: NUL 바이트          -> TEXT, JSON 컬럼이면 거부되는 값, escape 표기(\000)로 조회
    6666: 잘못된 UTF-8(0xff) -> escape 표기(\377)로 조회
    7777: 빈 메시지           -> 빈 바이트로 저장
    8888: 한글                -> 깨지지 않고 그대로 조회
    */
    #[tokio::test]
    #[ignore]
    async fn saves_raw_bytes_as_is() {
        let pool = test_pool().await;
        cleanup(&pool, &RAW_TEST_LOG_IDS).await;

        let payloads: [&[u8]; 5] = [
            br#"{"event_id":"#,
            b"a\0b",
            b"a\xffb",
            b"",
            "송장.exe".as_bytes(),
        ];
        let failures: Vec<FailedLog> = RAW_TEST_LOG_IDS
            .iter()
            .zip(payloads)
            .map(|(&log_id, payload)| FailedLog {
                log_id,
                reason: "RawEvent 역직렬화 실패",
                payload,
                partition: 0,
                offset: 100,
            })
            .collect();

        save_failures(&pool, &failures).await.unwrap();

        let rows = sqlx::query!(
            r#"SELECT payload, bytea_to_text_safe(payload) AS "text!"
               FROM storage_failures
               WHERE log_id = ANY($1)
               ORDER BY log_id"#,
            &RAW_TEST_LOG_IDS as &[&str]
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        cleanup(&pool, &RAW_TEST_LOG_IDS).await;

        // 저장한 바이트와 조회한 바이트가 정확히 같음
        let stored: Vec<&[u8]> = rows.iter().map(|r| r.payload.as_slice()).collect();
        assert_eq!(stored, payloads);

        // 조회용 문자열: 정상 UTF-8은 그대로, 깨진 바이트는 escape 표기
        let texts: Vec<&str> = rows.iter().map(|r| r.text.as_str()).collect();
        assert_eq!(
            texts,
            [r#"{"event_id":"#, r"a\000b", r"a\377b", "", "송장.exe"]
        );
    }
}
