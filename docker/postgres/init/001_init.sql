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

-- 이벤트 처리 과정에서 OpenSearch와 ClickHouse에 저장하지 못한 로그 보관
-- 1. OpenSearch가 document 자체 문제(400)로 거절한 로그      -> log_id: OpenSearch _id와 ClickHouse log_id와 같은 값
-- 2. Consumer가 처리할 수 없는 메시지(빈 메시지, 역직렬화 실패 등) -> log_id: kafka-{topic}-{partition}-{offset}
-- 재처리로 같은 로그가 다시 실패해도 log_id PK로 한 건만 유지 (INSERT ... ON CONFLICT (log_id) DO NOTHING)
CREATE TABLE IF NOT EXISTS storage_failures (
    log_id          TEXT PRIMARY KEY,                -- 1번: sha256(event_id|ingested_at), 2번: kafka-{topic}-{partition}-{offset}
    reason          TEXT NOT NULL,                   -- 실패 사유 (OpenSearch error.reason, 역직렬화 에러 메시지 등)
    payload         BYTEA NOT NULL,                  -- 저장하려던 로그 원문 (데이터를 문자열로 해석하지 않고 바이트 그대로 저장, 깨진 JSON/NUL/잘못된 UTF-8도 저장됨, 빈 메시지는 빈 바이트)
    kafka_partition INTEGER NOT NULL,                -- 원본 이벤트의 Kafka 위치 (추적용)
    kafka_offset    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW() -- UTC로 저장, 조회 시 세션 시간대로 표시, 예: 2026-10-07 00:10:01.501+00
);

CREATE INDEX IF NOT EXISTS idx_storage_failures_created_at ON storage_failures (created_at);

-- BYTEA 원문 조회용 함수
-- UTF-8 문자열로 변환할 수 있으면 문자열 그대로 반환(convert_from(b, 'UTF8');)
-- 잘못된 UTF-8 바이트나 NUL 등으로 TEXT 변환에 실패하면 원본 바이트 전체를 escape 표기로 변환해 반환(encode(b, 'escape');)
-- escape는 사람이 읽을 수 있도록 특수한 바이트를 이스케이프 표기로 변환(\354\206\241처럼)
--
-- character_not_in_repertoire: 문자 인코딩으로 표현할 수 없는 문자가 있을 때 발생하는 PostgreSQL 예외 조건(에러 코드: SQLSTATE 22021)
-- LANGUAGE: 함수 구현 언어를 지정하고, plpgsql 사용(plpgsql: PostgreSQL에서 사용하는 절차적 SQL 프로그래밍 언어)
--
-- 함수 변동성 분류: 함수의 결과가 얼마나 안정적으로 유지되는지 PostgreSQL에 알림
-- - IMMUTABLE: 동일한 입력이면 언제 실행하더라도 동일한 결과를 반환(예: abs(-3) -> 항상 3)
-- - STABLE: 동일한 입력이면 하나의 SQL 문 안에서 동일한 결과를 반환한다고 선언(예: now() -> 같은 SQL 문 안에서는 항상 같은 시각)
-- - VOLATILE: 하나의 SQL문 안에서도 호출할 때마다 결과가 달라질 수 있다고 선언(예: random())
--
-- STABLE을 사용한 이유는 convert_from()는 STABLE 등급, encode()는 IMMUTABLE 등급인데, 
-- 함수의 등급은 그 안에서 호출하는 함수 중 가장 약한 등급을 넘으면 안 되므로 convert_from()에 맞춰 STABLE로 선언
--
-- STRICT: 입력 값이 NULL이면 함수를 실행하지 않고 NULL 반환
CREATE OR REPLACE FUNCTION bytea_to_text_safe(b BYTEA)
RETURNS TEXT AS $$
BEGIN
    RETURN convert_from(b, 'UTF8');
EXCEPTION
    WHEN character_not_in_repertoire THEN
        RETURN encode(b, 'escape');
END;
$$ LANGUAGE plpgsql STABLE STRICT;

-- 트리거 설정
DROP TRIGGER IF EXISTS tr_playbooks_updated_at ON playbooks;

CREATE TRIGGER tr_playbooks_updated_at
    BEFORE UPDATE ON playbooks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();