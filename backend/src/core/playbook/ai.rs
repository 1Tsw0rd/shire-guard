/*
ai.rs: AI 대응 노드의 실행 담당
- AI 대응 노드는 복잡한 판단이 필요한 소수 케이스에만 쓰이는 노드이고, Ollama 응답이 느려 캐싱을 사용함

이 파일이 하는 일
- 캐시 키 생성: sha256(model + prompt + Evidence, event_id/timestamp 제외)
- Redis/Dragonfly 조회 → 있으면 그대로 반환
- 없으면 Ollama(/api/chat)를 호출해서 risk/reason을 받아옴
- 성공(success) 결과만 Redis/Dragonfly에 TTL 7일로 저장 (에러는 저장하지 않음: 일시 장애가 TTL 동안 고정되는 것을 막기 위함)
- Redis/Dragonfly 장애 시에는 Ollama를 호출하지 않고 CacheUnavailable 에러 반환
- 같은 요청이 동시에 여러 개 들어오면 Ollama는 1번만 호출 (single-flight)

용어
- in_flight: 현재 진행 중인 AI 요청을 기록해 두는 Map (키: 캐시 키)
- leader: 같은 요청 중 가장 먼저 도착해서 실제로 Ollama를 호출하는 요청
- waiter: 같은 요청 중 나중에 도착해서 leader의 결과를 기다렸다가 그대로 받는 요청
- single-flight: 같은 요청이 동시에 여러 번 들어와도 실제 작업은 1번만 하고, 나머지는 그 결과를 받는 방식

[전체 흐름]
  AI 요청 - 아래 전체 과정에 AI_CALL_TIMEOUT(60초) 적용
    ↓
  Redis/Dragonfly 캐시 조회
    ├─ Hit  → 저장된 결과 반환
    ├─ 장애 → CacheUnavailable 반환 (Ollama 호출 안 함)
    └─ Miss
         ↓
       in_flight 확인
         ├─ 같은 요청이 이미 진행 중 → waiter: leader의 결과를 기다렸다가 그대로 반환
         └─ 진행 중인 요청이 없음    → leader
                                          ↓
                                       캐시 재확인 (직전 leader가 방금 저장했을 수 있음)
                                          ↓
                                       Ollama 순번 대기 (동시 1개)
                                          ↓
                                       Ollama 호출
                                          ↓
                                       Success → 캐시 저장 / Error → 저장 안 함
                                          ↓
                                       in_flight에서 leader가 자기 키 제거 후, 결과를 waiter들과 공유

[예시] 같은 요청 3개가 동시에 도착
  A: 캐시 Miss → 가장 먼저 도착 → leader → Ollama 호출(10초) → 결과를 캐시에 저장
  B: 캐시 Miss → 같은 요청이 진행 중 → waiter → A의 결과를 받아 반환
  C: 캐시 Miss → 같은 요청이 진행 중 → waiter → A의 결과를 받아 반환
  10초 뒤 도착한 D: 캐시 Hit → 바로 반환

[시간 제한] AI_CALL_TIMEOUT(60초) 하나만 사용
- 캐시 조회, 순번 대기, Ollama 호출, waiter 대기가 모두 이 시간 안에 포함됨
- 초과하면 Timeout 에러 반환 → engine.rs에서 해당 플레이북의 결정을 HOLD로 처리
- Redis/Dragonfly가 에러를 반환하면 Ollama를 호출하지 않고 CacheUnavailable 에러 반환
  (장애 시 에러가 반환되기까지 10초 이상 걸릴 수 있고, 이 시간도 AI_CALL_TIMEOUT 60초에 포함됨)
- leader가 timeout으로 취소되면 남은 waiter 중 하나가 새 leader가 되어 다시 시도

[에러 처리]
- 모든 AI 에러는 engine.rs에서 해당 플레이북의 결정을 HOLD로 처리 (다수결에서는 한 표)
- 에러는 캐시에 저장하지 않으며, 같은 시점에 기다리던 waiter에게는 그대로 전달됨
- 다음 요청은 새로 시도함

주의할 점
- Ollama는 콜드 스타트 시 모델 로드에 16~22초가 걸렸음 (실측: qwen 22초, Foundation-Sec 16초)
  -> keep_alive로 모델을 메모리에 유지해 재요청 비용을 줄임
- LLM이 만든 JSON은 risk/reason 필드 순서가 모델마다 다를 수 있음
  -> serde_json은 키 이름으로 매칭하므로 순서는 문제되지 않음. 필드 누락/오타만 InvalidResponse로 처리
- message.content는 JSON "문자열"로 옴 (이미 파싱된 객체가 아님) -> 한 번 더 파싱 필요
*/

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

// OnceCell: 같은 요청의 결과를 한 번만 만들고 여러 Task가 공유할 때 사용
// Semaphore: 동시에 실행할 수 있는 작업의 개수를 제한할 때 사용
use tokio::sync::{OnceCell, Semaphore};

// timeout: 비동기 작업이 일정 시간 이상 걸리면 시간 초과로 종료할 때 사용
use tokio::time::timeout;

use crate::common::clients::redis::RedisClient;
use crate::common::error::AppError;
use crate::core::enrichment::evidence::Evidence;
use crate::core::playbook::nodes::{AiModel, Risk};

// AI 대응 노드 실행 결과 (playbook_result.playbooks[].ai_result에 그대로 직렬화됨)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AiResult {
    Success {
        risk: Risk,
        reason: String,
        model: AiModel,
    },
    Error {
        error: AiError,
        model: AiModel,
    },
}

impl AiResult {
    fn error(error: AiError, model: AiModel) -> Self {
        Self::Error { error, model }
    }
}

// AI 호출 실패 원인
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiError {
    Timeout,
    ConnectionFailed,
    HttpError,
    InvalidResponse,
    CacheUnavailable, // Redis/Dragonfly 장애로 캐시를 확인할 수 없는 경우
}

// 같은 캐시 키로 진행 중인 AI 요청을 관리하는 Map
// Redis/Dragonfly 캐시에 결과가 없으면 이 Map에서 같은 요청이 이미 진행 중인지 확인함
// Map에 해당 key가 없으면 새로운 OnceCell을 등록하고, 해당 Task가 leader가 되어 Ollama 호출을 수행함
// Map에 해당 key가 이미 있으면 기존 OnceCell을 가져와 waiter가 되어 leader의 결과를 기다림
// 따라서 같은 key에 대한 중복 Ollama 호출을 방지함
// Mutex로 Map에 대한 동시 접근을 보호하고,
// Arc<OnceCell<AiResult>>를 통해 하나의 AI 결과를 leader와 여러 waiter가 공유함
type InFlightMap = Mutex<HashMap<String, Arc<OnceCell<AiResult>>>>;

// leader가 자신이 InFlightMap에 등록한 요청 정보를 기억하고,
// 작업이 끝날 때 해당 요청을 Map에서 자동으로 제거하기 위한 Guard
// 정상 반환, 에러, panic, timeout에 의한 Future 취소 등으로 Guard가 drop되면 정리 작업을 수행함
// waiter는 InFlightGuard를 사용하지 않고, 이미 clone한 Arc<OnceCell<AiResult>>를 통해 leader의 결과를 기다림
// 따라서 InFlightMap에서 key가 제거되어도 waiter가 가진 Arc가 존재하는 동안 OnceCell은 유지됨
struct InFlightGuard {
    map: Arc<InFlightMap>,         // 여러 Task가 공유하는 InFlightMap
    key: String,                   // leader가 등록한 요청의 캐시 키
    cell: Arc<OnceCell<AiResult>>, // leader가 등록한 OnceCell, waiter와 공유됨
}

// InFlightGuard가 스코프를 벗어나면 Drop::drop이 자동으로 호출됨
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Mutex를 잡고 있던 스레드가 panic하면 poisoned 상태가 될 수 있지만,
        // 여기서는 요청 정리가 목적이므로 poisoned 상태에서도 내부 Map을 꺼내 계속 처리함
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());

        // ptr은 pointer(포인터), eq는 equal(같다)의 의미
        // Arc::ptr_eq()는 내부의 AiResult 값이 같은지 비교하는 것이 아니라,
        // 두 Arc가 동일한 OnceCell 객체를 가리키는지 비교함
        // Map의 cell이 내가 등록한 cell과 같은 객체일 때만 제거함
        // leader가 취소되면 Guard가 key를 삭제하고, 기다리던 waiter 중 1개가 같은 cell로 이어서 leader가 됨
        // 그 사이 같은 key로 새 요청이 새 cell을 등록할 수 있는데,
        // 이어받은 leader가 끝나면서 key를 지우면 새 요청의 cell까지 지워지므로 이를 막음
        if map
            .get(&self.key)
            .is_some_and(|c| Arc::ptr_eq(c, &self.cell))
        {
            map.remove(&self.key);
        }
    }
}

// Ollama의 POST /api/chat 응답에서 필요한 필드만 파싱하는 구조체
#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    message: OllamaMessage,
}

#[derive(Debug, Deserialize)]
struct OllamaMessage {
    content: String, // JSON 문자열 (예: "{\"risk\":\"HIGH\",\"reason\":\"...\"}") - 한 번 더 파싱 필요
}

// content를 파싱해서 얻는 AI 판단 결과
// risk/reason 필드 순서는 모델마다 다를 수 있지만 serde_json은 키로 매칭하므로 문제없음
#[derive(Debug, Deserialize)]
struct AiJudgment {
    risk: Risk,
    reason: String,
}

#[derive(Clone)]
pub struct AiClient {
    http: reqwest::Client, // timeout은 call()의 AI_CALL_TIMEOUT이 관리하므로 여기서는 설정하지 않음
    redis: RedisClient,
    base_url: String, // 예: "http://localhost:11434"
    in_flight: Arc<InFlightMap>,
    ollama_semaphore: Arc<Semaphore>,
}

impl AiClient {
    pub fn new(redis: RedisClient, base_url: String) -> Result<Self, AppError> {
        // Ollama에 동시에 보내는 요청 수
        // Ollama의 OLLAMA_NUM_PARALLEL 기본값이 1이라 같은 값으로 맞춤 (Ollama 설정을 올리면 이 값도 같이 올려야 함)
        const OLLAMA_MAX_CONCURRENCY: usize = 1;

        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AppError::Internal(format!("Ollama HTTP client 생성 실패: {e}")))?;

        Ok(Self {
            http,
            redis,
            base_url,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            ollama_semaphore: Arc::new(Semaphore::new(OLLAMA_MAX_CONCURRENCY)),
        })
    }

    // AI 대응 노드의 진입점: 처리 전체(캐시 조회 ~ Ollama 호출)에 시간 상한을 적용
    pub async fn call(&self, model: AiModel, prompt: &str, evidence: &Evidence) -> AiResult {
        // AI 요청 하나가 끝나기까지의 전체 상한 (캐시 조회 + 순번 대기 + Ollama 호출 + waiter 대기)
        // 초과하면 Timeout 에러를 반환하고, engine에서 해당 플레이북의 결정을 HOLD로 처리함
        // 산정 근거: 콜드 스타트 실측 qwen 22초, Foundation-Sec 16초 + 순번 대기 시간 → 60초
        const AI_CALL_TIMEOUT: Duration = Duration::from_secs(60);

        match timeout(AI_CALL_TIMEOUT, self.call_inner(model, prompt, evidence)).await {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(model = model.as_str(), "AI 호출 시간 초과");
                AiResult::error(AiError::Timeout, model)
            }
        }
    }

    // 캐시 확인 → (같은 키가 진행 중이면 그 결과 대기) → leader가 Ollama 호출
    async fn call_inner(&self, model: AiModel, prompt: &str, evidence: &Evidence) -> AiResult {
        let key = match cache_key(model, prompt, evidence) {
            Ok(k) => k,
            // Evidence 직렬화 실패는 사실상 발생하지 않지만, 방어적으로 처리
            Err(e) => {
                tracing::warn!(error = %e, "AI 캐시 키 생성 실패");
                return AiResult::error(AiError::InvalidResponse, model);
            }
        };

        // 1. 캐시 확인 (Hit이면 저장된 결과, Redis/Dragonfly 장애면 CacheUnavailable 에러)
        if let Some(result) = self.lookup_cache(&key, model).await {
            return result;
        }

        // 2. 같은 키로 진행 중인 요청이 있으면 그 cell을 공유하고, 없으면 새로 생성
        // std Mutex는 lock → 맵 조작 → 해제까지만 쓰고 그 안에서 await 하지 않음
        let cell = {
            let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            Arc::clone(
                // entry(): Map에서 해당 key의 Entry를 가져옴
                // or_insert_with(): key가 이미 있으면 기존 값을 사용하고, 없으면 새로운 Arc<OnceCell<AiResult>>를 만들어 Map에 저장함
                map.entry(key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };

        // 3. 가장 먼저 도착한 요청 하나만 leader로 run_leader를 실행하고, 나머지는 그 결과를 기다림
        // get_or_init(): OnceCell에 값이 있으면 기존 값을 사용하고, 없으면 초기화 함수를 실행해 값을 저장함
        // 같은 cell을 사용하는 여러 Task 중 하나만 run_leader()를 실행하고, 나머지는 그 결과가 저장될 때까지 기다림
        cell.get_or_init(|| self.run_leader(key, Arc::clone(&cell), model, prompt, evidence))
            .await
            .clone()
    }

    // Redis/Dragonfly 캐시 조회
    // Hit → Some(저장된 결과) / 장애 → Some(CacheUnavailable 에러) / Miss(값 없음, 파싱 실패 포함) → None
    async fn lookup_cache(&self, key: &str, model: AiModel) -> Option<AiResult> {
        match self.redis.get(key).await {
            Ok(Some(json)) => match serde_json::from_str::<AiResult>(&json) {
                Ok(cached) => Some(cached),
                Err(e) => {
                    tracing::warn!(%key, error = %e, "AI 캐시 값 파싱 실패, 캐시 미스로 처리");
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(%key, error = ?e, "AI 캐시 조회 실패, Ollama를 호출하지 않음");
                Some(AiResult::error(AiError::CacheUnavailable, model))
            }
        }
    }

    // leader의 실제 처리: 캐시 재확인 → Ollama 순번 대기 → Ollama 호출 → 성공만 캐시 저장
    // 함수가 끝나면 guard가 drop되면서 in_flight에서 자기 키가 제거됨 (캐시 저장이 끝난 뒤)
    async fn run_leader(
        &self,
        key: String,
        cell: Arc<OnceCell<AiResult>>,
        model: AiModel,
        prompt: &str,
        evidence: &Evidence,
    ) -> AiResult {
        // AI 결과 캐시 TTL: 7일 (같은 이벤트면 항상 같은 판정이 맞다는 전제)
        const AI_CACHE_TTL_SECONDS: u64 = 60 * 60 * 24 * 7;

        // InFlightGuard를 생성해 leader의 요청 정보를 보관함
        // _guard가 스코프에 살아 있는 동안 요청을 관리하고,
        // run_leader()가 끝나면 _guard가 자동으로 drop되어 InFlightMap에서 해당 요청을 정리함
        let _guard = InFlightGuard {
            map: Arc::clone(&self.in_flight),
            key: key.clone(),
            cell,
        };

        // 직전 leader가 방금 작업을 끝내고 성공 결과를 캐시에 저장했을 수 있으므로 한 번 더 확인
        // 첫 번째 캐시 조회에서는 Miss였지만, 그 직후 다른 leader가 결과를 저장하고 InFlightMap의 key를 제거하면
        // 이 요청은 새로운 leader가 될 수 있음
        // 재확인에서 Cache Hit이면 이미 계산된 결과를 그대로 반환하여 불필요한 Ollama 재호출을 방지함
        // 이 재확인은 Redis 캐시를 확인하는 것이며, InFlightGuard의 Arc::ptr_eq()와는 역할이 다름
        //   - 캐시 재확인: 이미 생성된 AI 결과를 다시 Ollama에서 계산하는 것을 방지
        //   - Arc::ptr_eq(): 오래된 leader가 새로운 leader의 InFlightMap 항목을 삭제하는 것을 방지
        if let Some(result) = self.lookup_cache(&key, model).await {
            return result;
        }

        // Ollama 동시 호출 제한: 순번이 올 때까지 대기 (대기 시간 상한은 call()의 AI_CALL_TIMEOUT)
        // acquire(): Semaphore에서 Ollama 호출을 위한 permit(허가권) 하나를 얻음
        // 사용 가능한 permit이 없으면 다른 요청이 permit을 반납할 때까지 기다림
        let Ok(permit) = self.ollama_semaphore.acquire().await else {
            tracing::error!("Ollama Semaphore가 닫힘");
            return AiResult::error(AiError::ConnectionFailed, model);
        };

        let result = self.call_ollama(model, prompt, evidence).await;
        drop(permit); // 캐시 저장 전에 Ollama 순번을 먼저 반환

        // success만 캐시 저장 (error를 캐시하면 일시 장애가 TTL 7일 동안 고정되므로 저장하지 않음)
        if matches!(result, AiResult::Success { .. }) {
            match serde_json::to_string(&result) {
                Ok(json) => {
                    if let Err(e) = self.redis.set(&key, &json, AI_CACHE_TTL_SECONDS).await {
                        tracing::warn!(%key, error = ?e, "AI 결과 캐시 저장 실패");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AI 결과 직렬화 실패");
                }
            }
        }

        result
    }

    // 실제 Ollama HTTP 호출: 캐시 로직과 분리해서 이 함수는 "호출 자체"만 책임짐
    async fn call_ollama(&self, model: AiModel, prompt: &str, evidence: &Evidence) -> AiResult {
        // 호출이 끝난 뒤 Ollama가 모델을 메모리(RAM/GPU)에 유지하는 시간 (AI 결과를 저장하는 옵션이 아님)
        // 이 시간 안에 다시 요청이 오면 모델 로드(콜드 스타트)를 건너뜀
        const OLLAMA_KEEP_ALIVE: &str = "30m";

        // 캐시 키와 같은 입력(event_id/timestamp 제외)을 전송함
        let evidence_json = match evidence_input(evidence) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "Evidence 직렬화 실패");
                return AiResult::error(AiError::InvalidResponse, model);
            }
        };

        let body = json!({
            "model": model.as_str(),
            "messages": [
                { "role": "system", "content": prompt }, // AI에게 주는 상위 지침 또는 요청사항
                { "role": "user", "content": evidence_json }, // AI가 분석할 실제 Evidence 데이터
            ],
            // Ollama가 응답을 이 JSON 스키마에 맞춰 생성하도록 요청 (risk/reason만 사용)
            "format": {
                "type": "object",
                "properties": {
                    "risk": { "type": "string", "enum": ["HIGH", "MEDIUM", "LOW"] },
                    "reason": { "type": "string" }
                },
                "required": ["risk", "reason"] // 응답 JSON에 risk와 reason 필드를 반드시 포함
            },
            // temperature 0: 같은 입력에는 같은 답이 나오도록 무작위성을 제거 (완전한 결정성은 캐시로 보장)
            "options": { "temperature": 0 },
            "stream": false, // 응답을 실시간 조각 단위로 받지 않고, 완성된 응답을 한 번에 받음
            "keep_alive": OLLAMA_KEEP_ALIVE,
        });

        let response = match self
            .http
            .post(format!("{}/api/chat", self.base_url))
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(model = model.as_str(), error = %e, "Ollama 요청 실패");
                return AiResult::error(AiError::ConnectionFailed, model);
            }
        };

        if !response.status().is_success() {
            tracing::warn!(
                model = model.as_str(),
                status = %response.status(),
                "Ollama가 오류 응답을 반환함"
            );
            return AiResult::error(AiError::HttpError, model);
        }

        let chat_response: OllamaChatResponse = match response.json().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(model = model.as_str(), error = %e, "Ollama 응답 파싱 실패");
                return AiResult::error(AiError::InvalidResponse, model);
            }
        };

        // content(JSON 문자열)를 한 번 더 파싱 — 필드 순서는 모델마다 달라도 무관
        let judgment: AiJudgment = match serde_json::from_str(&chat_response.message.content) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(
                    model = model.as_str(),
                    error = %e,
                    content = %chat_response.message.content,
                    "Ollama 응답 JSON 파싱 실패 (risk/reason 형식 불일치)"
                );
                return AiResult::error(AiError::InvalidResponse, model);
            }
        };

        AiResult::Success {
            risk: judgment.risk,
            reason: judgment.reason,
            model,
        }
    }
}

// Evidence를 JSON 문자열로 변환하되 event_id/timestamp는 제외
// 캐시 키 계산과 Ollama 전송이 같은 입력을 쓰도록 하는 공용 함수
// 제외하는 이유: 이 둘은 이벤트마다 항상 달라서 넣으면 캐시가 절대 히트하지 않고, AI 판정에도 필요하지 않음
fn evidence_input(evidence: &Evidence) -> Result<String, serde_json::Error> {
    let mut value = serde_json::to_value(evidence)?;
    if let Some(obj) = value.as_object_mut() {
        // value가 Object라면 수정할 수 있는 상태로 빌림
        obj.remove("event_id");
        obj.remove("timestamp");
    }

    // serde_json 기본(=preserve_order 미사용) 상태에서는 객체 키가 알파벳 순으로 정렬되어 직렬화되므로
    // 같은 내용이면 항상 같은 문자열이 나옴
    serde_json::to_string(&value)
}

// 캐시 키 생성: sha256(model + prompt + Evidence, event_id/timestamp 제외)
fn cache_key(
    model: AiModel,
    prompt: &str,
    evidence: &Evidence,
) -> Result<String, serde_json::Error> {
    let normalized = evidence_input(evidence)?;

    let mut hasher = Sha256::new(); // 빈 해시 계산기 생성
    hasher.update(model.as_str().as_bytes()); // 모델 이름을 해시 계산에 입력
    hasher.update(b"\0");
    // 구분자: 조각을 그냥 이어붙이면 서로 다른 (model,prompt) 조합이 같은 바이트 스트림이 될 수 있는 경계 문제를 방지
    // 예: model="AB", prompt="CD" → "ABCD"
    //     model="A",  prompt="BCD" → "ABCD"  (서로 다른 입력인데 결과가 같아짐)
    // \0은 model/prompt/JSON 문자열 어디에도 나타나지 않는 값이라 경계 표시로 사용
    hasher.update(prompt.as_bytes()); // 이어서 prompt를 해시 계산에 입력
    hasher.update(b"\0");
    hasher.update(normalized.as_bytes()); // 이어서 정규화된 Evidence JSON을 해시 계산에 입력

    let digest = hasher.finalize(); // 지금까지 입력한 모든 데이터를 바탕으로 SHA-256 최종 결과를 반환 (32 바이트)

    // SHA-256 결과는 32바이트의 바이너리 값이므로 사람이 읽기 좋은 16진수 문자열로 변환
    // 예: [0x9f, 0x86, 0xd0, ...] → "9f86d0..."
    let hex: String = digest.iter().map(|byte| format!("{:02x}", byte)).collect();
    // 최종 캐시 키 예: "ai:9f86d081..."
    Ok(format!("ai:{hex}"))
}

// cargo test --lib playbook::ai
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::consumer::event::RawEvent;

    fn sample_evidence(event_id: &str, timestamp: &str) -> Evidence {
        let event: RawEvent = serde_json::from_str(&format!(
            r#"{{
                "event_id": "{event_id}",
                "event_type": "file_download",
                "timestamp": "{timestamp}",
                "src_ip": "1.2.3.4",
                "file_name": "invoice.exe"
            }}"#
        ))
        .expect("테스트 이벤트 파싱 실패");

        Evidence {
            event,
            enrichment_src_ip: None,
            enrichment_dst_domain: None,
            enrichment_file_sha256: None,
        }
    }

    // 시나리오 1: 같은 model/prompt/evidence면 캐시 키가 항상 같다
    #[test]
    fn cache_key_is_deterministic() {
        let e1 = sample_evidence("evt-1", "2026-09-26T10:00:00Z");
        let e2 = sample_evidence("evt-1", "2026-09-26T10:00:00Z");

        let k1 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e1).unwrap();
        let k2 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e2).unwrap();

        assert_eq!(k1, k2);
    }

    // 시나리오 2: event_id/timestamp만 다르면 캐시 키는 같아야 한다
    #[test]
    fn cache_key_ignores_event_id_and_timestamp() {
        let e1 = sample_evidence("evt-1", "2026-09-26T10:00:00Z");
        let e2 = sample_evidence("evt-9999", "2026-09-27T23:59:59Z");

        let k1 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e1).unwrap();
        let k2 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e2).unwrap();

        assert_eq!(k1, k2);
    }

    // 시나리오 3: model이 다르면 캐시 키도 달라야 한다
    #[test]
    fn cache_key_differs_by_model() {
        let e = sample_evidence("evt-1", "2026-09-26T10:00:00Z");

        let k1 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e).unwrap();
        let k2 = cache_key(AiModel::FoundationSec8bReasoning, "분석하라", &e).unwrap();

        assert_ne!(k1, k2);
    }

    // 시나리오 4: prompt가 다르면 캐시 키도 달라야 한다
    #[test]
    fn cache_key_differs_by_prompt() {
        let e = sample_evidence("evt-1", "2026-09-26T10:00:00Z");

        let k1 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e).unwrap();
        let k2 = cache_key(AiModel::Qwen25Instruct14b, "다르게 분석하라", &e).unwrap();

        assert_ne!(k1, k2);
    }

    // 시나리오 5: 실제 Ollama 응답(qwen)처럼 risk가 먼저 오는 순서도 파싱된다
    #[test]
    fn parses_content_with_risk_first() {
        let content = r#"{"risk":"HIGH","reason":"실행 파일 확장자가 의심스러움"}"#;
        let judgment: AiJudgment = serde_json::from_str(content).expect("파싱 실패");
        assert_eq!(judgment.risk, Risk::High);
    }

    // 시나리오 6: 실제 Ollama 응답(Foundation-Sec)처럼 reason이 먼저 오는 순서도 파싱된다
    // (필드 순서는 모델마다 다르지만 serde_json은 키로 매칭하므로 문제없음)
    #[test]
    fn parses_content_with_reason_first() {
        let content = r#"{"reason":"사용자가 파일 누락을 신고함","risk":"HIGH"}"#;
        let judgment: AiJudgment = serde_json::from_str(content).expect("파싱 실패");
        assert_eq!(judgment.risk, Risk::High);
    }

    // 시나리오 7: risk 값이 스키마에 없는 값이면 파싱 실패 (InvalidResponse로 이어짐)
    #[test]
    fn invalid_risk_value_fails_to_parse() {
        let content = r#"{"risk":"매우높음","reason":"..."}"#;
        let result: Result<AiJudgment, _> = serde_json::from_str(content);
        assert!(result.is_err());
    }

    // 시나리오 8: AiResult가 문서에 정의된 JSON 형태(status 태그)로 직렬화된다
    #[test]
    fn ai_result_serializes_with_status_tag() {
        let success = AiResult::Success {
            risk: Risk::High,
            reason: "테스트".to_string(),
            model: AiModel::Qwen25Instruct14b,
        };
        let value = serde_json::to_value(&success).unwrap();
        assert_eq!(value["status"], "success");
        assert_eq!(value["risk"], "HIGH");

        let error = AiResult::Error {
            error: AiError::Timeout,
            model: AiModel::Qwen25Instruct14b,
        };
        let value = serde_json::to_value(&error).unwrap();
        assert_eq!(value["status"], "error");
        assert_eq!(value["error"], "timeout");
    }

    // 시나리오 9: event_id/timestamp를 제외한 Evidence 내용이 다르면 캐시 키도 달라야 한다
    #[test]
    fn cache_key_differs_by_evidence_content() {
        let e1 = sample_evidence("evt-1", "2026-09-26T10:00:00Z"); // file_name: "invoice.exe"

        let event2: RawEvent = serde_json::from_str(
            r#"{
            "event_id": "evt-1",
            "event_type": "file_download",
            "timestamp": "2026-09-26T10:00:00Z",
            "src_ip": "1.2.3.4",
            "file_name": "malware.exe"
        }"#,
        )
        .expect("테스트 이벤트 파싱 실패");
        let e2 = Evidence {
            event: event2,
            enrichment_src_ip: None,
            enrichment_dst_domain: None,
            enrichment_file_sha256: None,
        };

        let k1 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e1).unwrap();
        let k2 = cache_key(AiModel::Qwen25Instruct14b, "분석하라", &e2).unwrap();

        assert_ne!(k1, k2);
    }

    // 시나리오 10: CacheUnavailable이 문서 형식(snake_case)으로 직렬화된다
    #[test]
    fn cache_unavailable_error_serializes_as_snake_case() {
        let error = AiResult::Error {
            error: AiError::CacheUnavailable,
            model: AiModel::Qwen25Instruct14b,
        };
        let value = serde_json::to_value(&error).unwrap();
        assert_eq!(value["status"], "error");
        assert_eq!(value["error"], "cache_unavailable");
    }

    // 시나리오 11: guard가 drop되면 자기 cell을 in_flight 맵에서 제거한다
    #[test]
    fn guard_removes_own_entry_on_drop() {
        let map: Arc<InFlightMap> = Arc::new(Mutex::new(HashMap::new()));
        let cell = Arc::new(OnceCell::new());
        map.lock()
            .unwrap()
            .insert("k".to_string(), Arc::clone(&cell));

        {
            let _guard = InFlightGuard {
                map: Arc::clone(&map),
                key: "k".to_string(),
                cell,
            };
        }

        assert!(map.lock().unwrap().is_empty());
    }

    // 시나리오 12: 같은 키로 새로 만들어진 다른 cell은 guard가 지우지 않는다
    #[test]
    fn guard_does_not_remove_newer_cell_with_same_key() {
        let map: Arc<InFlightMap> = Arc::new(Mutex::new(HashMap::new()));
        let old_cell = Arc::new(OnceCell::new());
        let new_cell = Arc::new(OnceCell::new());
        map.lock()
            .unwrap()
            .insert("k".to_string(), Arc::clone(&new_cell));

        {
            let _guard = InFlightGuard {
                map: Arc::clone(&map),
                key: "k".to_string(),
                cell: old_cell,
            };
        }

        assert!(map.lock().unwrap().contains_key("k"));
    }
}
