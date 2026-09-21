-- vector.toml 테스트용
-- CREATE TABLE IF NOT EXISTS shire.security_events (
--     event_type String,
--     source_ip String,
--     failed_count UInt32,
--     success_count UInt32,
--     username String,
--     timestamp DateTime64(3)
-- ) ENGINE = MergeTree()
-- ORDER BY timestamp; No newline at end of file

-- 테이블 생성
CREATE TABLE IF NOT EXISTS shire.event_logs (
    event_id         String,                            -- 이벤트 ID (클릭하우스는 일반 RDBMS와 다르므로 별도 id 컬럼 사용 안함)
    event_type       LowCardinality(String),            -- 이벤트 종류 (file_download 등)
    event_time       DateTime64(3, 'UTC'),              -- 이벤트 발생 시각 (timestamp 파싱값)
    ingested_at      DateTime64(3, 'UTC'),              -- 백엔드 처리 시작 시각 (OpenSearch의 ingested_at과 같은 값)
    playbook_action  LowCardinality(Nullable(String)),  -- 플레이북 최종 결정: DENY / DETECT / HOLD (결정한 플레이북이 없으면 NULL)
    payload          JSON                               -- 원본 + enrichment + playbook_result 전체 (OpenSearch 문서와 동일)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)                        -- 월 단위 파티션
ORDER BY (event_type, event_time, event_id);             -- 정렬 키
