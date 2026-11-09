/*
store.rs: 활성 플레이북을 런타임에 캐싱하는 저장소

이 파일이 하는 일
- Postgres playbooks 테이블에서 활성(enabled=true, is_delete=false) 플레이북을 읽어
  Playbook::from_json으로 파싱/검증 (실패한 행은 건너뛰고 warn 로그)
- 파싱된 목록을 RwLock<Arc<Vec<Playbook>>>로 들고 있다가, 이벤트 처리 시 snapshot()으로 제공
- snapshot()은 Arc만 clone하고 lock을 즉시 해제하므로, 평가(병렬 처리) 동안 lock 경합이 없음
- Playbook CRUD 후에는 refresh()로 전체를 다시 로드해 통째로 교체 (개별 갱신 없음)
- TTL 없음: Playbook CRUD 성공 후 refresh()로 캐시를 즉시 갱신하므로 별도 만료가 필요 없음
*/

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::core::playbook::nodes::Playbook;

// Postgres에서 활성 플레이북 목록을 읽어 파싱한다.
// 개별 행의 nodes 파싱이 실패해도 전체를 실패시키지 않고 그 행만 건너뛴다.
async fn load_active_playbooks(pool: &PgPool) -> Result<Vec<Playbook>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id, name, enabled, nodes
        FROM playbooks
        WHERE enabled = true AND is_delete = false
        ORDER BY id
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut playbooks = Vec::with_capacity(rows.len()); // rows.len()만큼 용량(capacity)을 미리 확보
    for row in rows {
        // id와 enabled는 Copy 타입이지만, name은 String이라 원본을 유지하기 위해 clone해서 전달하고 tracing에서 사용
        match Playbook::from_json(row.id, row.name.clone(), row.enabled, row.nodes) {
            Ok(playbook) => playbooks.push(playbook),
            Err(e) => {
                tracing::warn!(
                    playbook_id = row.id,
                    name = %row.name,
                    error = %e,
                "Playbook Load 실패, 건너뜀"
                );
            }
        }
    }

    Ok(playbooks)
}

// Vec는 활성 Playbook 목록을 저장하고, 각 프로세스의 메모리에 별도로 보관함
// Arc는 동일 프로세스 내 여러 Task가 같은 Playbook 목록을 공유하도록 함
// RwLock은 여러 읽기는 동시에 허용하고(읽기 Lock은 동시에 존재 가능),
// 쓰기는 하나만 허용하도록(쓰기 Lock은 하나만 존재 가능) 동시 접근을 제어함
pub struct PlaybookStore {
    inner: RwLock<Arc<Vec<Playbook>>>,
}

impl PlaybookStore {
    // 서버 기동 시 최초 로드. Postgres 접속/쿼리 자체가 실패하면 에러를 반환함
    // 호출자가 에러를 전파하면 서버 기동 단계에서 fail-fast(문제 발생 시 즉시 실패 처리)로 처리할 수 있음
    pub async fn load(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let playbooks = load_active_playbooks(pool).await?;
        tracing::info!(count = playbooks.len(), "활성 Playbook 목록 Load 완료");
        Ok(Self {
            inner: RwLock::new(Arc::new(playbooks)),
        })
    }

    // 현재 활성 Playbook 목록의 Arc를 복제해서 반환하고, 읽기 Lock은 즉시 해제
    // RwLock의 읽기 Lock은 내부의 Arc<Vec<Playbook>>에 대한 접근을 제어
    pub async fn snapshot(&self) -> Arc<Vec<Playbook>> {
        self.inner.read().await.clone()
    }

    // Postgres에서 활성 Playbook 목록을 다시 읽어와 전체를 통째로 교체함
    // Playbook 생성/수정/삭제 API에서 처리
    pub async fn refresh(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let playbooks = load_active_playbooks(pool).await?;
        let count = playbooks.len();
        // *로 쓰기 Lock 내부의 실제 Arc<Vec<Playbook>> 값에 접근하여 새 값으로 교체
        *self.inner.write().await = Arc::new(playbooks);
        tracing::info!(count, "Playbook Cache 갱신 완료");
        Ok(())
    }
}
