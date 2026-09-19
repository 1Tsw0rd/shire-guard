use redis::{Client, aio::ConnectionManager};

use crate::common::error::AppError;

#[derive(Clone)]
pub struct RedisClient {
    connection: ConnectionManager,
}

impl RedisClient {
    pub async fn new(client: Client) -> Result<Self, AppError> {
        let connection = client.get_connection_manager().await.map_err(|err| {
            AppError::Internal(format!("Redis/Dragonfly 연결에 실패했습니다: {}", err))
        })?;

        Ok(Self { connection })
    }

    // Enrichment 결과 캐시 조회
    pub async fn get(&self, key: &str) -> Result<Option<String>, AppError> {
        let mut connection = self.connection.clone();

        redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(|err| AppError::Internal(format!("Redis 데이터 조회에 실패했습니다: {}", err)))
    }

    // Enrichment 결과 저장. provider별 TTL이 다르므로 항상 인자로 강제
    pub async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<(), AppError> {
        let mut connection = self.connection.clone();

        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("EX")
            .arg(ttl_seconds)
            .query_async::<()>(&mut connection)
            .await
            .map_err(|err| AppError::Internal(format!("Redis 데이터 저장에 실패했습니다: {}", err)))
    }

    // 분산 락 획득: SET key value NX EX ttl (단일 원자적 커맨드)
    // SETNX + EXPIRE로 분리하면 그 사이 크래시 시 TTL 없는 락이 영구 잔존할 수 있음
    // 반환값: 락 획득 성공 여부 (이미 다른 요청이 락을 쥐고 있으면 false)
    pub async fn try_lock(
        &self,
        key: &str,
        owner_value: &str,
        ttl_seconds: u64,
    ) -> Result<bool, AppError> {
        let mut connection = self.connection.clone();

        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(owner_value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut connection)
            .await
            .map_err(|err| AppError::Internal(format!("Redis 락 획득에 실패했습니다: {}", err)))?;

        // NX 성공 시 "OK", 이미 락이 있으면 nil(None)
        Ok(result.is_some())
    }

    // 분산 락 해제: 현재 값이 내가 넣은 owner_value와 일치할 때만 원자적으로 삭제
    // GET→DEL을 분리하면 그 사이 TTL 만료로 다른 요청이 새로 획득한 락을
    // 내가 잘못 지워버리는 사고가 날 수 있어, Lua(EVAL)로 확인+삭제를 한 번에 처리
    pub async fn unlock(&self, key: &str, owner_value: &str) -> Result<(), AppError> {
        const UNLOCK_SCRIPT: &str = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;

        let mut connection = self.connection.clone();

        let deleted: i64 = redis::cmd("EVAL")
            .arg(UNLOCK_SCRIPT)
            .arg(1) // KEYS 개수
            .arg(key)
            .arg(owner_value)
            .query_async(&mut connection)
            .await
            .map_err(|err| AppError::Internal(format!("Redis 락 해제에 실패했습니다: {}", err)))?;

        if deleted == 0 {
            // 내 소유가 아니었다는 뜻 — TTL 만료로 이미 풀렸거나 다른 요청이 재획득한 상태
            // 에러는 아니지만 원인 분석용으로 남겨둠
            tracing::warn!(%key, "락 해제 시도했으나 소유권이 이미 없었습니다(TTL 만료 또는 재획득)");
        }

        Ok(())
    }

    // Enrichment 캐시에 저장된 키 개수 조회(Grafana 대시보드 표시용)
    pub async fn dbsize(&self) -> Result<i64, AppError> {
        let mut connection = self.connection.clone();

        // DBSIZE는 전체 Key 개수 반환
        redis::cmd("DBSIZE")
            .query_async(&mut connection)
            .await
            .map_err(|err| AppError::Internal(format!("Redis DBSIZE 조회에 실패했습니다: {}", err)))
    }
}
