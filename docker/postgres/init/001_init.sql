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

-- 저장하지 못한 로그 보관 (OpenSearch·ClickHouse에는 저장되지 않음)
-- 1. OpenSearch가 document 자체 문제(400)로 거절한 로그      → log_id: OpenSearch _id·ClickHouse log_id와 같은 값
-- 2. Consumer가 처리할 수 없는 메시지(빈 메시지, 역직렬화 실패 등) → log_id: kafka-{topic}-{partition}-{offset}
-- 재처리로 같은 로그가 다시 실패해도 log_id PK로 한 건만 유지 (INSERT ... ON CONFLICT (log_id) DO NOTHING)
CREATE TABLE IF NOT EXISTS storage_failures (
    log_id          TEXT PRIMARY KEY,                -- 1번: sha256(event_id|ingested_at), 2번: kafka-{topic}-{partition}-{offset}
    reason          TEXT NOT NULL,                   -- 실패 사유 (OpenSearch error.reason, 역직렬화 에러 메시지 등)
    payload         BYTEA NOT NULL,                  -- 저장하려던 로그 원문 (바이트 그대로, 깨진 JSON·NUL·잘못된 UTF-8도 저장됨, 빈 메시지는 빈 바이트)
    kafka_partition INTEGER NOT NULL,                -- 원본 이벤트의 Kafka 위치 (추적용)
    kafka_offset    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW() -- UTC로 저장, 조회 시 세션 시간대로 표시, 예: 2026-10-07 00:10:01.501+00
);

CREATE INDEX IF NOT EXISTS idx_storage_failures_created_at ON storage_failures (created_at);

-- BYTEA 원문 조회용: 정상 UTF-8이면 문자열 그대로, 깨진 바이트(NUL, 0xff 등)가 있으면 escape 표기(\000, \377)로 반환
-- (BYTEA 기본 출력은 16진수(\x7b22...)라 읽을 수 없고, encode(..., 'escape')는 한글까지 \354\206\241처럼 바꿔서 이 함수를 사용)
-- 예: SELECT log_id, reason, bytea_to_text_safe(payload) FROM storage_failures;
-- 정상 JSON인 1번 로그는 JSON으로 바꿔 필드 조회 가능: bytea_to_text_safe(payload)::json -> 'event_id'
--   (->>는 값에 \u0000이 들어 있으면 PostgreSQL이 텍스트로 바꾸지 못해 에러 → 이 경우 -> 사용)
-- character_not_in_repertoire(SQLSTATE 22021): UTF-8 변환 실패 오류만 처리하고, 그 외 오류는 그대로 발생시킴
-- STRICT: NULL 입력이면 함수를 실행하지 않고 NULL 반환
CREATE OR REPLACE FUNCTION bytea_to_text_safe(b BYTEA)
RETURNS TEXT AS $$
BEGIN
    RETURN convert_from(b, 'UTF8');
EXCEPTION
    WHEN character_not_in_repertoire THEN
        RETURN encode(b, 'escape');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


-- 트리거 설정
DROP TRIGGER IF EXISTS tr_playbooks_updated_at ON playbooks;

CREATE TRIGGER tr_playbooks_updated_at
    BEFORE UPDATE ON playbooks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();