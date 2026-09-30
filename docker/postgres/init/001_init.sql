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

-- 트리거 설정
DROP TRIGGER IF EXISTS tr_playbooks_updated_at ON playbooks;

CREATE TRIGGER tr_playbooks_updated_at
    BEFORE UPDATE ON playbooks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();