use sqlx::PgPool;

pub struct FailedLog<'a> {
    pub log_id: &'a str,
    pub reason: &'a str,  // OpenSearch가 보낸 거절 사유 (error.reason)
    pub payload: &'a str, // 저장하려던 로그 JSON
    pub partition: i32,   // 원본 이벤트의 Kafka partition 번호
    pub offset: i64,      // 원본 이벤트의 Kafka offset
}

/*
거절된 로그들을 Bulk insert로 저장

UNNEST: 컬럼별 배열의 같은 위치에 있는 값을 하나의 행으로 묶음
  log_ids    = ["a1", "a2"]
  reasons    = ["r1", "r2"]      →   ("a1", "r1", ...)
  ...                                ("a2", "r2", ...)
*/
pub async fn save_failures(pool: &PgPool, failures: &[FailedLog<'_>]) -> Result<(), sqlx::Error> {
    if failures.is_empty() {
        return Ok(());
    }

    // 한 번 순회하면서 UNNEST에 사용할 컬럼별 배열을 각각 생성
    // 예: failures 2건 → log_ids["a1", "a2"], reasons["r1", "r2"], ...
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
        SELECT log_id, reason, payload::json, kafka_partition, kafka_offset
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int4[], $5::int8[])
            AS t(log_id, reason, payload, kafka_partition, kafka_offset)
        ON CONFLICT (log_id) DO NOTHING
        "#,
        &log_ids as &[&str],
        &reasons as &[&str],
        &payloads as &[&str],
        &partitions,
        &offsets
    )
    .execute(pool)
    .await?;

    Ok(())
}

// cargo test --lib storage::failures -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    const BATCH_TEST_LOG_IDS: [&str; 2] =
        ["test-storage-failures-0001", "test-storage-failures-0002"]; // 시나리오 1 전용
    const NUL_TEST_LOG_ID: &str = "test-storage-failures-0003"; // 시나리오 3 전용

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
            payload,
            partition: 0,
            offset: 100,
        }
    }

    /*
    시나리오 1: 여러 건을 한 번에 저장하고, 같은 log_id를 다시 저장하면 무시된다

    1번째 저장:
    - 0001 저장
    - 0002 저장

    2번째 저장:
    - 0001을 다른 reason과 payload로 다시 저장 시도하지만 ON CONFLICT (log_id) DO NOTHING으로 무시
    - 기존 0001의 reason과 payload가 유지되는지 확인
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

        // 이미 저장된 0001은 log_id가 같으므로 무시됨
        save_failures(&pool, &[failed(BATCH_TEST_LOG_IDS[0], "다른 사유", "{}")])
            .await
            .unwrap();

        let rows = sqlx::query!(
            r#"SELECT log_id, reason, payload::text AS "payload!"
               FROM storage_failures
               WHERE log_id = ANY($1)
               ORDER BY log_id"#,
            &BATCH_TEST_LOG_IDS as &[&str]
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        cleanup(&pool, &BATCH_TEST_LOG_IDS).await;

        // 0001, 0002 두 건만 저장되고 중복 0001은 추가되지 않음
        assert_eq!(rows.len(), 2);

        // 0001은 두 번째 INSERT가 무시되어 첫 번째 데이터가 유지됨
        assert_eq!(rows[0].reason, "mapper_parsing_exception");
        assert_eq!(rows[0].payload, r#"{"event_id":"evt-1"}"#);

        // 0002도 정상적으로 저장됨
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

    // 시나리오 3: \u0000(NUL 문자)이 든 문서도 원문 그대로 저장된다, \u0000은 NUL 문자로, 화면에서는 눈에 보이지 않는 제어 문자
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
            r#"SELECT payload::text AS "payload!" FROM storage_failures WHERE log_id = $1"#,
            NUL_TEST_LOG_ID
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        cleanup(&pool, &[NUL_TEST_LOG_ID]).await;

        let value: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
        assert_eq!(value["note"], "a\u{0}b"); // 해석하면 a + NUL + b
    }
}
