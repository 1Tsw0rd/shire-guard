/*
ai.rs: AI 대응 노드의 실행 담당 (Ollama 호출 + Redis 캐시)
- AI 대응 노드는 복잡한 판단이 필요한 소수 케이스에만 쓰이는 노드이고, 응답이 느려 캐싱을 사용함

이 파일이 하는 일
- 캐시 키 생성: sha256(model + prompt + Evidence, event_id/timestamp 제외)
- Redis/Dragonfly 조회 → 있으면 그대로 반환 (호출 비용 절약)
- 없으면 Ollama(/api/chat)를 호출해서 risk/reason을 받아옴
- 성공(success) 결과만 Redis에 TTL 7일로 저장 (에러는 캐시하지 않음: 일시 장애가 TTL 동안 고정되는 것을 막기 위함)
- AiResult(성공/실패)는 여기서 정의

주의할 점
- Ollama는 콜드 스타트 시 모델 로드에 20초 이상 걸릴 수 있음 (실측: qwen 22초, Foundation-Sec 16초)
  -> HTTP timeout을 넉넉히(90초) 잡고, keep_alive로 모델을 메모리에 유지해 재요청 비용을 줄임
- LLM이 만든 JSON은 risk/reason 필드 순서가 모델마다 다를 수 있음
  -> serde_json은 키 이름으로 매칭하므로 순서는 문제되지 않음. 필드 누락/오타만 InvalidResponse로 처리
- message.content는 JSON "문자열"로 옴 (이미 파싱된 객체가 아님) -> 한 번 더 파싱 필요
*/

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::common::clients::redis::RedisClient;
use crate::common::error::AppError;
use crate::core::enrichment::evidence::Evidence;
use crate::core::playbook::nodes::{AiModel, Risk};

// AI 결과 캐시 TTL: 7일 (같은 이벤트면 항상 같은 판정이 맞다는 전제)
// AI 분석 결과(JSON)를 Redis/Dragonfly에 저장해 두는 시간
const AI_CACHE_TTL_SECONDS: u64 = 60 * 60 * 24 * 7;

// Ollama 콜드 스타트(모델 로드) + 응답 생성까지 고려한 여유 timeout(초)
const OLLAMA_TIMEOUT: Duration = Duration::from_secs(90);

// Ollama가 요청에 사용한 AI 모델을 메모리(RAM/GPU)에 계속 로드해 두는 시간
// AI 응답 결과를 저장하는 옵션이 아님
// 요청이 끝난 뒤에도 모델을 OLLAMA_KEEP_ALIVE 시간 동안 메모리에 유지하여,
// 그 안에 다시 요청이 들어오면 모델을 다시 로드하는 콜드 스타트를 피함
// OLLAMA_KEEP_ALIVE 시간 동안 추가 요청이 없으면 Ollama가 모델을 메모리에서 언로드할 수 있음
const OLLAMA_KEEP_ALIVE: &str = "30m";

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

// AI 호출 실패 원인 (고정된 4종류만 사용)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiError {
    Timeout,
    ConnectionFailed,
    HttpError,
    InvalidResponse,
}

// Ollama의 POST /api/chat 응답에서 필요한 필드만 파싱
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
    http: reqwest::Client,
    redis: RedisClient,
    base_url: String, // 예: "http://localhost:11434"
}

impl AiClient {
    pub fn new(redis: RedisClient, base_url: String) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .timeout(OLLAMA_TIMEOUT)
            .build()
            .map_err(|e| AppError::Internal(format!("Ollama HTTP client 생성 실패: {e}")))?;

        Ok(Self {
            http,
            redis,
            base_url,
        })
    }

    // AI 대응 노드의 진입점: Redis/Dragonfly 캐시 확인 -> 없으면 Ollama 호출 -> 성공만 Redis/Dragonfly 캐시 저장
    pub async fn call(&self, model: AiModel, prompt: &str, evidence: &Evidence) -> AiResult {
        let key = match cache_key(model, prompt, evidence) {
            Ok(k) => k,
            // Evidence 직렬화 실패는 사실상 발생하지 않지만, 방어적으로 처리
            Err(e) => {
                tracing::warn!(error = %e, "AI 캐시 키 생성 실패");
                return AiResult::Error {
                    error: AiError::InvalidResponse,
                    model,
                };
            }
        };

        // 1. 캐시 확인 (Redis 자체 장애는 캐시 미스로 취급하고 계속 진행)
        match self.redis.get(&key).await {
            Ok(Some(json)) => match serde_json::from_str::<AiResult>(&json) {
                // Redis에 캐시된 JSON 값이 있다면 AiResult로 파싱
                Ok(cached) => return cached,
                Err(e) => {
                    tracing::warn!(%key, error = %e, "AI 캐시 값 파싱 실패, 새로 호출");
                }
            },
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(%key, error = ?e, "AI 캐시 조회 실패, 새로 호출");
            }
        }

        // 2. 캐시 미스 → Ollama 호출
        let result = self.call_ollama(model, prompt, evidence).await;

        // 3. success만 캐시 저장 (error를 캐시하면 일시 장애가 TTL 7일 동안 고정되므로 저장하지 않음)
        // result가 AiResult::Success이면 캐시 저장
        // { .. }는 Success 내부 필드의 값은 확인하지 않는다는 의미(뼈대만 확인)
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
        let evidence_json = match serde_json::to_string(evidence) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "Evidence 직렬화 실패");
                return AiResult::Error {
                    error: AiError::InvalidResponse,
                    model,
                };
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
                let error = if e.is_timeout() {
                    AiError::Timeout
                } else {
                    AiError::ConnectionFailed
                };
                tracing::warn!(model = model.as_str(), error = %e, "Ollama 요청 실패");
                return AiResult::Error { error, model };
            }
        };

        if !response.status().is_success() {
            tracing::warn!(
                model = model.as_str(),
                status = %response.status(),
                "Ollama가 오류 응답을 반환함"
            );
            return AiResult::Error {
                error: AiError::HttpError,
                model,
            };
        }

        let chat_response: OllamaChatResponse = match response.json().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(model = model.as_str(), error = %e, "Ollama 응답 파싱 실패");
                return AiResult::Error {
                    error: AiError::InvalidResponse,
                    model,
                };
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
                return AiResult::Error {
                    error: AiError::InvalidResponse,
                    model,
                };
            }
        };

        AiResult::Success {
            risk: judgment.risk,
            reason: judgment.reason,
            model,
        }
    }
}

// 캐시 키 생성: sha256(model + prompt + Evidence, event_id/timestamp 제외)
// event_id/timestamp를 빼는 이유: 이 둘은 이벤트마다 항상 달라서 그대로 넣으면 캐시가 절대 히트하지 않음
fn cache_key(
    model: AiModel,
    prompt: &str,
    evidence: &Evidence,
) -> Result<String, serde_json::Error> {
    let mut value = serde_json::to_value(evidence)?;
    if let Some(obj) = value.as_object_mut() {
        // value가 Object라면 수정할 수 있는 상태로 빌림
        obj.remove("event_id");
        obj.remove("timestamp");
    }

    // serde_json 기본(=preserve_order 미사용) 상태에서는 객체 키가 알파벳 순으로 정렬되어 직렬화되므로
    // 같은 내용이면 항상 같은 문자열이 나옴
    let normalized = serde_json::to_string(&value)?;

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
}
