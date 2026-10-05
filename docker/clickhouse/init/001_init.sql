-- vector.toml 테스트용
-- CREATE TABLE IF NOT EXISTS shire.security_events (
--     event_type String,
--     source_ip String,
--     failed_count UInt32,
--     success_count UInt32,
--     username String,
--     timestamp DateTime64(3)
-- ) ENGINE = MergeTree()
-- ORDER BY timestamp;

-- 테이블 생성
CREATE TABLE IF NOT EXISTS shire.event_logs (
    log_id   String,                                  -- 로그 ID: sha256(event_id + ingested_at), 중복 판단 기준 (OpenSearch _id와 같은 값)
    payload  JSON(                                    -- 원본 + enrichment + playbook_result 전체 (OpenSearch 문서와 동일)
        -- 자주 조회되거나 파티션, 정렬 키에 사용하는 JSON 경로에 타입 힌트를 지정
        -- 타입 힌트가 지정된 경로는 타입이 고정된 subcolumn으로 저장되어 전용 컬럼과 같은 성능으로 조회됨
        -- (힌트가 없는 경로는 타입이 정해지지 않은 Dynamic으로 저장되어 상대적으로 느림)
        ingested_at              DateTime64(9, 'UTC'),    -- 수집 시각 (수신 Vector가 기록, 파티션·정렬 키에 사용)
        event_id                 String,                  -- 이벤트 ID (같은 이벤트의 로그가 여러 건일 수 있음)
        event_type               LowCardinality(String),  -- 이벤트 종류 (file_download 등)
        `playbook_result.action` LowCardinality(Nullable(String))   -- 플레이북 최종 결정: DENY / DETECT / HOLD (결정 없으면 NULL)
    )
) ENGINE = ReplacingMergeTree                         -- 같은 log_id는 병합 시 하나만 남김 (정확한 조회는 FINAL)
PARTITION BY toYYYYMM(payload.ingested_at)            -- 월 단위 파티션 (OpenSearch 인덱스와 같은 수집 시각 기준)
ORDER BY (payload.ingested_at, log_id);               -- 정렬 키 = 중복 판단 기준, ingested_at이 앞이라 시간 범위 조회에 유리
