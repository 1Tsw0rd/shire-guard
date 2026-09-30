/*
플레이북 실행 엔진(노드 순회 + 다수결 집계)
- 활성 Playbook 전체를 이벤트 하나에 대해 병렬로 평가(JoinSet)
- Playbook마다 Node를 순서대로 순회
- Node는 AND로 직렬 연결
- 대응 노드에서 Playbook action 결정
- 동일한 이벤트 처리가 중복되는 Playbook의 결정을 모아 다수결로 최종 action 결정
- Playbook 동작 결과들은 수합 후 PlaybookResult로 조립됨

- JoinSet을 쓰는 이유: Playbook마다 소요 시간 편차가 존재
  각자 별도 Task로 spawn하면 느린 플레이북이 다른 플레이북을 막지 않고,
  특정 태스크가 패닉해도(JoinError) 나머지 결과에는 영향 없음
*/

use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::core::enrichment::evidence::Evidence;
use crate::core::playbook::ai::{AiClient, AiResult};
use crate::core::playbook::condition;
use crate::core::playbook::nodes::{Action, AiModel, Node, Playbook, RiskActions};
use crate::core::playbook::store::PlaybookStore;

// 최종 판정 결과 (Playbook 조건에 부합되어 최종 처리된 원본 로그에 이 필드가 그대로 붙음)
#[derive(Debug, Clone, Serialize)]
pub struct PlaybookResult {
    pub action: Action,
    pub evaluated: usize,
    pub processed_at: String, // ISO 8601, 예: "2026-09-27T10:15:03Z"
    pub playbooks: Vec<PlaybookDecision>,
}

// action을 결정한 Playbook 1개의 기록
#[derive(Debug, Clone, Serialize)]
pub struct PlaybookDecision {
    pub playbook_id: i64,
    pub name: String,
    pub action: Action,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<String>, // 조건 노드가 없으면 생략됨
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ai_result: Option<AiResult>, // ai_response 노드일 때만 추가
}

pub struct PlaybookEngine {
    store: PlaybookStore,
    ai_client: AiClient,
}

impl PlaybookEngine {
    pub fn new(store: PlaybookStore, ai_client: AiClient) -> Self {
        Self { store, ai_client }
    }

    pub fn store(&self) -> &PlaybookStore {
        &self.store
    }

    // 이벤트 하나를 활성 Playbook 전체에 대해 평가하고
    // 다수결로 최종 결과를 수합
    // 결정한 Plabbook이 하나도 없으면 None이며, 원본 로그 그대로 통과하고 playbook_result 필드는 미생성됨
    pub async fn run(&self, evidence: &Evidence) -> Option<PlaybookResult> {
        // 활성 Palybook 목록 가져오기
        let playbooks = self.store.snapshot().await;
        if playbooks.is_empty() {
            return None;
        }

        // Evidence를 직렬화하여 serde_json::Value(JSON 객체)로 변환
        let event_value = match serde_json::to_value(evidence) {
            Ok(v) => Arc::new(v),
            Err(e) => {
                tracing::error!(error = %e, "Evidence 직렬화 실패, 플레이북 평가를 건너뜀");
                return None;
            }
        };

        // AI 노드 호출 시 Ollama에 보낼 원본 Evidence가 필요해서 별도로 한 번만 clone해 Arc로 공유
        // (event_value는 condition 평가용으로 직렬화된 Value, evidence는 AI 호출용 원본)

        let evidence = Arc::new(evidence.clone());

        let mut set = JoinSet::new();
        // playbook을 clone해서 각 병렬 task가 복제된 값을 소유하도록 전달
        for playbook in playbooks.iter().cloned() {
            let event_value = Arc::clone(&event_value);
            let evidence = Arc::clone(&evidence);
            let ai_client = self.ai_client.clone();
            set.spawn(async move {
                evaluate_playbook(playbook, event_value, evidence, ai_client).await
            });
        }

        // Playbook 결과 다수결 집계
        let mut decisions = Vec::new();

        // JoinSet에서 병렬 실행 중인 Task 중 먼저 완료된 것부터 하나씩 결과를 가져옴
        // 모든 Task의 결과를 처리하면 join_next()가 None을 반환하여 반복 종료
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Some(decision)) => decisions.push(decision),
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, "Playbook 평가 Task 실패(panic 등), 해당 Playbook은 결과에서 제외");
                }
            }
        }

        if decisions.is_empty() {
            return None;
        }

        let action = majority_vote(&decisions);
        Some(PlaybookResult {
            action,
            evaluated: decisions.len(),
            // 현재 시각을 UTC 기준 RFC 3339 형식(초 단위)으로 저장하고, 끝에 Z를 붙임(예: "2026-09-28T07:30:03Z")
            processed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            playbooks: decisions,
        })
    }
}

// 다수결 집계 및 판정: 특정 action의 결정 수 / 전체 결정 수 > 0.5 인 경우에만 그 action, 아니면 HOLD
fn majority_vote(decisions: &[PlaybookDecision]) -> Action {
    // decisions.len()은 usize이므로, 비율 계산을 위해 f64로 변환
    let total_count = decisions.len() as f64;

    let mut deny_count = 0usize; // 초기값 0으로 세팅
    let mut detect_count = 0usize;

    for d in decisions {
        match d.action {
            Action::Deny => deny_count += 1,
            Action::Detect => detect_count += 1,
            Action::Hold => {}
        }
    }

    if (deny_count as f64) / total_count > 0.5 {
        Action::Deny
    } else if (detect_count as f64) / total_count > 0.5 {
        Action::Detect
    } else {
        Action::Hold
    }
}

// Playbook Node에서 처리한 결과를 나타내는 enum
// prompt와 actions는 Node에서 빌린 참조이므로 참조의 유효 기간을 'a로 표현
#[derive(Debug)]
enum WalkOutcome<'a> {
    NoMatch,    // 조건 노드 중 하나라도 거짓인 경우
    NoTerminal, // 대응 노드(response | ai_response)를 만나지 못한 경우

    Response {
        // 일반 대응 노드(response)의 결과
        action: Action,
        when: Option<String>,
    },

    AiResponse {
        // AI 대응 노드(ai_response)의 결과
        model: AiModel,
        prompt: &'a str,
        actions: &'a RiskActions,
        when: Option<String>,
    },
}

// Playbook의 Node를 순서대로 순회하는 로직 (I/O 없음)
// - 조건 노드: 평가 결과가 false면 즉시 NoMatch, true면 when 문자열을 누적하고 계속 진행
// - 대응 노드(response | ai_response)를 만나면 결과를 반환하고 즉시 종료
// - 루프가 끝날 때까지 대응 노드를 만나지 못하면 NoTerminal
fn walk_playbook<'a>(nodes: &'a [Node], event_value: &Value) -> WalkOutcome<'a> {
    let mut when_parts: Vec<String> = Vec::new();

    for node in nodes {
        match node {
            Node::Condition { when, .. } => {
                if !condition::eval(when, event_value) {
                    // 조건 평가 결과가 false인 경우
                    return WalkOutcome::NoMatch;
                }
                // 현재 조건 하나를 문자열로 변환하여 when_parts에 저장
                // 예:
                // (event_type eq file_download)
                // (file_name ends_with .exe)
                when_parts.push(condition::render_when(when));
            }
            Node::Response { action } => {
                return WalkOutcome::Response {
                    action: *action, // 역참조하여 실제 Action 값 저장
                    // 여러 조건이 저장되어 있다면 join_when()에서 " and "로 연결
                    // 예:
                    // "(event_type eq file_download) and (file_name ends_with .exe)"
                    when: join_when(&when_parts),
                };
            }
            Node::AiResponse {
                model,
                prompt,
                actions,
            } => {
                return WalkOutcome::AiResponse {
                    model: *model,
                    prompt,
                    actions,
                    when: join_when(&when_parts),
                };
            }
        }
    }

    WalkOutcome::NoTerminal
}

// 조건 노드가 여러 개면 각 노드의 when 문자열을 " and "로 이어붙임
// 예:
// ["(event_type eq file_download)", "(file_name ends_with .exe)"]
//                  ↓
// "(event_type eq file_download) and (file_name ends_with .exe)"
// 조건 노드가 없으면 None을 반환
fn join_when(parts: &[String]) -> Option<String> {
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" and "))
    }
}

// 개별 Playbook 평가 함수
// walk_playbook의 결과에 따라 필요한 경우 AI를 호출하고, 해당 Playbook의 최종 PlaybookDecision을 생성
async fn evaluate_playbook(
    playbook: Playbook,
    event_value: Arc<Value>,
    evidence: Arc<Evidence>,
    ai_client: AiClient,
) -> Option<PlaybookDecision> {
    let playbook_id = playbook.id;
    let name = playbook.name.clone();

    match walk_playbook(&playbook.nodes, &event_value) {
        WalkOutcome::NoMatch | WalkOutcome::NoTerminal => None,

        WalkOutcome::Response { action, when } => Some(PlaybookDecision {
            playbook_id,
            name,
            action,
            when,
            ai_result: None,
        }),

        WalkOutcome::AiResponse {
            model,
            prompt,
            actions,
            when,
        } => {
            let ai_result = ai_client.call(model, prompt, &evidence).await;
            let action = resolve_ai_action(&ai_result, actions);

            action.map(|action| PlaybookDecision {
                playbook_id,
                name,
                action,
                when,
                ai_result: Some(ai_result),
            })
        }
    }
}

// AI 결과의 Risk를 Playbook에 설정된 Action으로 변환
// - Success: RiskActions에서 해당 Risk의 Action을 조회
// - Error: 항상 HOLD 반환
fn resolve_ai_action(ai_result: &AiResult, actions: &RiskActions) -> Option<Action> {
    match ai_result {
        AiResult::Success { risk, .. } => actions.get(*risk),
        AiResult::Error { .. } => Some(Action::Hold),
    }
}

// cargo test --lib playbook::engine
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::playbook::ai::AiError;
    use crate::core::playbook::nodes::Risk;
    use serde_json::json;

    fn nodes_from(json_str: &str) -> Vec<Node> {
        serde_json::from_str(json_str).expect("테스트 노드 파싱 실패")
    }

    // ── walk_playbook ──
    // 시나리오 1: 조건이 참이면 종료 노드(response)에서 결정된다
    #[test]
    fn walk_stops_at_response_when_conditions_pass() {
        let nodes = nodes_from(
            r#"[
            { "type": "condition", "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
            { "type": "response", "action": "DENY" }
        ]"#,
        );
        let event = json!({ "event_type": "file_download" });

        match walk_playbook(&nodes, &event) {
            WalkOutcome::Response { action, when } => {
                assert_eq!(action, Action::Deny);
                assert_eq!(when.as_deref(), Some("(event_type eq file_download)"));
            }
            other => panic!("Response여야 함: {other:?}"),
        }
    }

    // 시나리오 2: 조건이 거짓이면 NoMatch
    #[test]
    fn walk_returns_no_match_when_condition_fails() {
        let nodes = nodes_from(
            r#"[
            { "type": "condition", "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
            { "type": "response", "action": "DENY" }
        ]"#,
        );
        let event = json!({ "event_type": "login_failure" });

        assert!(matches!(
            walk_playbook(&nodes, &event),
            WalkOutcome::NoMatch
        ));
    }

    // 시나리오 3: 조건 노드가 여러 개면 when이 " and "로 이어붙는다
    #[test]
    fn walk_joins_multiple_condition_whens_with_and() {
        let nodes = nodes_from(
            r#"[
            { "type": "condition", "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
            { "type": "condition", "when": { "field": "file_name", "op": "ends_with", "value": ".exe" } },
            { "type": "response", "action": "DENY" }
        ]"#,
        );
        let event = json!({ "event_type": "file_download", "file_name": "invoice.exe" });

        match walk_playbook(&nodes, &event) {
            WalkOutcome::Response { when, .. } => {
                assert_eq!(
                    when.as_deref(),
                    Some("(event_type eq file_download) and (file_name ends_with .exe)")
                );
            }
            other => panic!("Response여야 함: {other:?}"),
        }
    }

    // 시나리오 4: 조건 노드가 없으면 when은 None
    #[test]
    fn walk_has_no_when_without_condition_nodes() {
        let nodes = nodes_from(r#"[ { "type": "response", "action": "DENY" } ]"#);
        let event = json!({});

        match walk_playbook(&nodes, &event) {
            WalkOutcome::Response { when, .. } => assert_eq!(when, None),
            other => panic!("Response여야 함: {other:?}"),
        }
    }

    // 시나리오 5: 종료 노드가 없으면(조건 노드만 있음) NoTerminal
    #[test]
    fn walk_returns_no_terminal_without_response_node() {
        let nodes = nodes_from(
            r#"[
            { "type": "condition", "when": { "field": "event_type", "op": "eq", "value": "file_download" } }
        ]"#,
        );
        let event = json!({ "event_type": "file_download" });

        assert!(matches!(
            walk_playbook(&nodes, &event),
            WalkOutcome::NoTerminal
        ));
    }

    // 시나리오 6: 종료 노드 뒤에 다른 노드가 더 있어도 첫 종료 노드에서 멈춘다 (엔진 방어 규칙)
    #[test]
    fn walk_stops_at_first_terminal_even_if_more_nodes_follow() {
        let nodes = nodes_from(
            r#"[
            { "type": "response", "action": "DENY" },
            { "type": "response", "action": "HOLD" }
        ]"#,
        );
        let event = json!({});

        match walk_playbook(&nodes, &event) {
            WalkOutcome::Response { action, .. } => assert_eq!(action, Action::Deny),
            other => panic!("Response여야 함: {other:?}"),
        }
    }

    // 시나리오 7: ai_response 노드를 만나면 model/prompt/actions/when을 그대로 전달한다
    #[test]
    fn walk_returns_ai_response_outcome() {
        let nodes = nodes_from(
            r#"[
            { "type": "ai_response", "model": "qwen2.5:14b-instruct", "prompt": "분석하라",
              "actions": { "HIGH": "HOLD" } }
        ]"#,
        );
        let event = json!({});

        match walk_playbook(&nodes, &event) {
            WalkOutcome::AiResponse {
                model,
                prompt,
                actions,
                when,
            } => {
                assert_eq!(model, AiModel::Qwen25Instruct14b);
                assert_eq!(prompt, "분석하라");
                assert_eq!(actions.get(Risk::High), Some(Action::Hold));
                assert_eq!(when, None);
            }
            other => panic!("AiResponse여야 함: {other:?}"),
        }
    }

    // ── resolve_ai_action ──

    // 시나리오 8: 성공 + risk가 actions에 있으면 그 action
    #[test]
    fn resolve_ai_action_maps_known_risk() {
        let actions: RiskActions =
            serde_json::from_str(r#"{ "HIGH": "HOLD", "MEDIUM": "DENY" }"#).unwrap();
        let result = AiResult::Success {
            risk: Risk::High,
            reason: "테스트".into(),
            model: AiModel::Qwen25Instruct14b,
        };
        assert_eq!(resolve_ai_action(&result, &actions), Some(Action::Hold));
    }

    // 시나리오 9: 성공이지만 risk가 actions에 없으면 None(해당 없음)
    #[test]
    fn resolve_ai_action_none_for_unmapped_risk() {
        let actions: RiskActions = serde_json::from_str(r#"{ "HIGH": "HOLD" }"#).unwrap();
        let result = AiResult::Success {
            risk: Risk::Low,
            reason: "테스트".into(),
            model: AiModel::Qwen25Instruct14b,
        };
        assert_eq!(resolve_ai_action(&result, &actions), None);
    }

    // 시나리오 10: 에러는 매핑과 무관하게 항상 HOLD (actions가 비어 있어도)
    #[test]
    fn resolve_ai_action_error_is_always_hold() {
        let actions: RiskActions = serde_json::from_str(r#"{}"#).unwrap();
        let result = AiResult::Error {
            error: AiError::Timeout,
            model: AiModel::Qwen25Instruct14b,
        };
        assert_eq!(resolve_ai_action(&result, &actions), Some(Action::Hold));
    }

    // ── majority_vote ──

    fn decision(action: Action) -> PlaybookDecision {
        PlaybookDecision {
            playbook_id: 1,
            name: "테스트".into(),
            action,
            when: None,
            ai_result: None,
        }
    }

    // 시나리오 11: 문서 예시 - DENY2 + DETECT1 + HOLD1 = 과반수 없음 -> HOLD
    #[test]
    fn majority_vote_no_majority_falls_back_to_hold() {
        let decisions = vec![
            decision(Action::Deny),
            decision(Action::Deny),
            decision(Action::Detect),
            decision(Action::Hold),
        ];
        assert_eq!(majority_vote(&decisions), Action::Hold);
    }

    // 시나리오 12: 문서 예시 - DENY2 + DETECT2 = 2/4=0.5로 과반수 아님 -> HOLD
    #[test]
    fn majority_vote_exactly_half_is_not_majority() {
        let decisions = vec![
            decision(Action::Deny),
            decision(Action::Deny),
            decision(Action::Detect),
            decision(Action::Detect),
        ];
        assert_eq!(majority_vote(&decisions), Action::Hold);
    }

    // 시나리오 13: 결정이 하나뿐이고 DENY면 1/1 > 0.5 -> DENY
    #[test]
    fn majority_vote_single_decision_wins() {
        let decisions = vec![decision(Action::Deny)];
        assert_eq!(majority_vote(&decisions), Action::Deny);
    }

    // 시나리오 14: DENY2 + HOLD1 = 2/3 ≈ 0.667 > 0.5 -> DENY
    #[test]
    fn majority_vote_two_thirds_wins() {
        let decisions = vec![
            decision(Action::Deny),
            decision(Action::Deny),
            decision(Action::Hold),
        ];
        assert_eq!(majority_vote(&decisions), Action::Deny);
    }
}
