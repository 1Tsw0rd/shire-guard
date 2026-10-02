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
    event_id         String,                            -- 이벤트 ID (같은 이벤트의 로그가 여러 건일 수 있음)
    event_type       LowCardinality(String),            -- 이벤트 종류 (file_download 등)
    timestamp        DateTime64(3, 'UTC'),              -- 이벤트 발생 시각 (이벤트 발신 측 기준 시각)
    ingested_at      DateTime64(3, 'UTC'),              -- 수집 시각 (수신 Vector가 기록한 시각) 
    playbook_action  LowCardinality(Nullable(String)),  -- 플레이북 최종 결정: DENY / DETECT / HOLD (플레이북 처리가 없으면 NULL)
    payload          JSON                               -- 원본 + enrichment + playbook_result 전체 (OpenSearch 문서와 동일)
) ENGINE = MergeTree                                    -- 로그는 계속 쌓음 (중복 제거 안 함)
PARTITION BY toYYYYMM(ingested_at)                      -- 월 단위 파티션 (OpenSearch 인덱스와 같은 수집 시각 기준)
ORDER BY (ingested_at, event_id);                       -- 정렬 키 (날짜 범위 조회가 많으므로 ingested_at을 포함시킴)