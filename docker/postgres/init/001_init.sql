-- updated_at 자동 갱신 function 추가
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 테이블 생성
CREATE TABLE IF NOT EXISTS playbooks (
    id         BIGSERIAL PRIMARY KEY,          -- 플레이북 ID
    name       VARCHAR(100) NOT NULL,          -- 플레이북 이름
    description VARCHAR(1000) NOT NULL DEFAULT '', -- 플레이북 설명
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,  -- 활성 여부
    is_delete  BOOLEAN NOT NULL DEFAULT FALSE, -- 논리 삭제 여부
    nodes      JSONB NOT NULL CHECK (jsonb_typeof(nodes) = 'array'),
                                               -- 플레이북 노드 배열
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 생성 시각
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- 수정 시각
);

-- OpenSearch가 document 자체 문제(400)로 거절한 이벤트 로그 보관 (OpenSearch·ClickHouse에는 저장되지 않음)
-- 재처리로 같은 로그가 다시 거절돼도 log_id PK로 한 건만 유지 (INSERT ... ON CONFLICT (log_id) DO NOTHING)
CREATE TABLE IF NOT EXISTS storage_failures (
    log_id          TEXT PRIMARY KEY,                -- OpenSearch _id·ClickHouse log_id와 같은 값
    reason          TEXT NOT NULL,                   -- OpenSearch error.reason
    payload         JSON NOT NULL,                   -- 저장하려던 로그 원본
    kafka_partition INTEGER NOT NULL,                -- 원본 이벤트의 Kafka 위치 (추적용)
    kafka_offset    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW() -- UTC로 저장, 조회 시 세션 시간대로 표시, 예: 2026-10-07 00:10:01.501+00
);

CREATE INDEX IF NOT EXISTS idx_storage_failures_created_at ON storage_failures (created_at);

-- 트리거 설정
DROP TRIGGER IF EXISTS tr_playbooks_updated_at ON playbooks;

CREATE TRIGGER tr_playbooks_updated_at
    BEFORE UPDATE ON playbooks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();