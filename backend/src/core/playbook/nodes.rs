/*
nodes.rs: 플레이북 설정 타입 (Postgres playbooks.nodes JSONB를 파싱한 결과)

이 파일이 하는 일
- 플레이북 설정의 "모양"을 정의하고(파싱), 저장·로드 전에 구조를 "검증"함
- 조건 평가는 condition.rs, 플레이북 실행과 다수결은 engine.rs에서 함

플레이북 구조
  Playbook (id, name, enabled)
    └── nodes: 조건 노드 0개 이상 + 종료 노드 1개(맨 끝)

  1. 조건 노드 (Node::Condition)
     - name, when(조건식 Expr)
     - 조건식이 거짓이면 이 플레이북은 해당 없음
     - 조건 노드끼리는 AND로 연결됨
  2. 대응 노드 (Node::Response)
     - action(Action): DENY | DETECT | HOLD
  3. AI 대응 노드 (Node::AiResponse)
     - model(AiModel), prompt, actions(RiskActions: risk → action)
     - actions에 없는 risk는 해당 없음

조건식 (Expr)
  { "and": [...] } | { "or": [...] } | 조건 하나 (중첩 가능)
  조건 하나 = field + op(Op) + value  (exists / not_exists는 value 없음)

주요 타입
  Action       플레이북/노드가 내리는 결정 (DENY | DETECT | HOLD)
  Risk         AI가 판단하는 위험도 (HIGH | MEDIUM | LOW)
  AiModel      사용할 수 있는 AI 모델 (Ollama 태그)
  RiskActions  AI 대응 노드의 risk → action 매핑
  Op           조건 연산자 (eq, gte, contains, in_cidr ...)
  Condition    조건 하나
  Expr         조건식 (and / or / 조건)
  Node         플레이북 노드 3종
  Playbook     플레이북 (from_json으로 파싱 + validate로 검증)

설정 JSON 예시
  [
    { "type": "condition", "name": "파일 다운로드만",
      "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
    { "type": "response", "action": "DENY" }
  ]
*/

use serde::{Deserialize, Serialize};

// 플레이북/노드가 내리는 결정
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")] // JSON 직렬화/역직렬화 시 enum 값을 대문자 형식(DENY, DETECT, HOLD)으로 사용
pub enum Action {
    Deny,
    Detect,
    Hold,
}

// AI가 판단하는 위험도
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Risk {
    High,
    Medium,
    Low,
}

// 사용할 수 있는 AI 모델 (Ollama 태그)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AiModel {
    #[serde(rename = "qwen2.5:14b-instruct")]
    Qwen25Instruct14b,
    #[serde(rename = "hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF")]
    FoundationSec8bReasoning,
}

impl AiModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Qwen25Instruct14b => "qwen2.5:14b-instruct",
            Self::FoundationSec8bReasoning => "hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF",
        }
    }
}

// AI 대응 노드의 risk -> action 매핑("HIGH": "HOLD" 형태)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskActions {
    // rename: JSON 키를 대문자로 지정
    // default: JSON에서 생략되면 None 사용
    // skip_serializing_if: None이면 JSON 직렬화에서 생략
    #[serde(rename = "HIGH", default, skip_serializing_if = "Option::is_none")]
    pub high: Option<Action>,
    #[serde(rename = "MEDIUM", default, skip_serializing_if = "Option::is_none")]
    pub medium: Option<Action>,
    #[serde(rename = "LOW", default, skip_serializing_if = "Option::is_none")]
    pub low: Option<Action>,
}

impl RiskActions {
    pub fn get(&self, risk: Risk) -> Option<Action> {
        match risk {
            Risk::High => self.high,
            Risk::Medium => self.medium,
            Risk::Low => self.low,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.high.is_none() && self.medium.is_none() && self.low.is_none()
    }
}

// 조건 연산자
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")] // 소문자 + snake 형식: not_exists
pub enum Op {
    Exists,    // 필드가 존재하는지 확인
    NotExists, // 필드가 존재하지 않는지 확인

    Eq, // 값이 같은지 비교
    Ne, // 값이 다른지 비교

    Gt,  // 값이 큰지 비교 (>)
    Gte, // 값이 크거나 같은지 비교 (>=)
    Lt,  // 값이 작은지 비교 (<)
    Lte, // 값이 작거나 같은지 비교 (<=)

    Contains,   // 문자열에 특정 값이 포함되어 있는지 확인
    StartsWith, // 문자열이 특정 값으로 시작하는지 확인
    EndsWith,   // 문자열이 특정 값으로 끝나는지 확인

    InCidr,    // IP가 CIDR 대역에 포함되는지 확인
    NotInCidr, // IP가 CIDR 대역에 포함되지 않는지 확인
}

// 조건 하나
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub field: String,
    pub op: Op,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>, // exists / not_exists는 비교할 value가 없으므로 None
}

// 조건식: { "and": [...] } | { "or": [...] } | 조건 하나 (중첩 가능)
#[derive(Debug, Clone, Serialize, Deserialize)]
// JSON에 enum 구분용 type 태그를 추가하지 않음
// 사용하지 않으면: {"type": "and", "and": [...]} 형태
// 사용하면:         {"and": [...]} 형태
#[serde(untagged)]
pub enum Expr {
    And { and: Vec<Expr> },
    Or { or: Vec<Expr> },
    Cond(Condition),
}

// 플레이북 노드: 각 플레이북마다 조건노드는 0개 이상이지만, 최소 대응 또는 AI 대응노드가 1개 필수로 필요
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Node {
    Condition {
        // 조건 노드
        #[serde(default)]
        name: Option<String>, // JSON에 없으면 None으로 처리
        when: Expr,
    },
    Response {
        // 대응 노드
        action: Action,
    },
    AiResponse {
        // AI 대응 노드
        model: AiModel,
        prompt: String,
        actions: RiskActions,
    },
}

#[derive(Debug, Clone)]
pub struct Playbook {
    pub id: i64,
    pub name: String,
    pub enabled: bool,
    pub nodes: Vec<Node>,
}

impl Playbook {
    // Postgres의 playbook 테이블 nodes(JSONB) 값을 파싱하고 검증
    pub fn from_json(
        id: i64,
        name: String,
        enabled: bool,
        nodes: serde_json::Value,
    ) -> Result<Self, String> {
        let nodes: Vec<Node> =
            serde_json::from_value(nodes).map_err(|e| format!("nodes 파싱 실패: {e}"))?;
        let playbook = Self {
            id,
            name,
            enabled,
            nodes,
        };
        playbook.validate()?;
        Ok(playbook)
    }

    // 구조 검증: 조건 노드 0개 이상 + 종료 노드 정확히 1개(맨 끝)
    pub fn validate(&self) -> Result<(), String> {
        // split_last(): 벡터에서 마지막 요소 하나와 나머지 요소를 분리하여 반환
        // 예: nodes = [A, B, C, D]
        //     last = D
        //     rest = [A, B, C]
        let (last, rest) = self
            .nodes
            .split_last()
            .ok_or_else(|| "nodes가 비어 있음".to_string())?;

        // enumerate(): 요소와 요소 번호 반환
        // 예: rest = [A, B, C]
        //     i = 0, node = A
        //     i = 1, node = B
        //     i = 2, node = C
        for (i, node) in rest.iter().enumerate() {
            match node {
                Node::Condition { when, .. } => {
                    // .. 의미는 when 외에 나머지는 필요하지 않다는 의미
                    validate_expr(when).map_err(|e| format!("{}번째 노드: {e}", i + 1))?
                }
                _ => {
                    return Err(format!(
                        "{}번째 노드: 종료 노드(response | ai_response)는 맨 끝에만 올 수 있음",
                        i + 1
                    ));
                }
            }
        }

        match last {
            Node::Condition { .. } => {
                Err("마지막 노드는 종료 노드(response | ai_response)여야 함".to_string())
            }
            Node::Response { .. } => Ok(()),
            // model은 enum이라 파싱 단계에서 이미 검증됨
            Node::AiResponse {
                prompt, actions, ..
            } => {
                if prompt.trim().is_empty() {
                    return Err("ai_response: prompt가 비어 있음".to_string());
                }
                if actions.is_empty() {
                    return Err("ai_response: actions에 매핑이 하나도 없음".to_string());
                }
                Ok(())
            }
        }
    }
}

fn validate_expr(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::And { and } => validate_group("and", and),
        Expr::Or { or } => validate_group("or", or),
        Expr::Cond(condition) => validate_condition(condition),
    }
}

fn validate_group(kind: &str, items: &[Expr]) -> Result<(), String> {
    if items.is_empty() {
        return Err(format!("빈 {kind} 그룹"));
    }
    items.iter().try_for_each(validate_expr)
}

// 연산자와 값의 타입이 맞는지 확인
// (CIDR 문자열 형식 검증은 ipnet을 추가하는 condition.rs 단계에서 함)
fn validate_condition(c: &Condition) -> Result<(), String> {
    if c.field.trim().is_empty() {
        return Err("조건의 field가 비어 있음".to_string());
    }

    let value = c.value.as_ref(); // Option<&Value>
    match c.op {
        Op::Exists | Op::NotExists => {
            if value.is_some() {
                return Err(format!("{:?}는 value를 쓰지 않음", c.op));
            }
        }
        Op::Gt | Op::Gte | Op::Lt | Op::Lte => {
            if !value.is_some_and(|v| v.is_number()) {
                return Err(format!("{:?}의 value는 숫자여야 함", c.op));
            }
        }
        Op::Eq | Op::Ne => {
            if !value.is_some_and(|v| v.is_number() || v.is_string()) {
                return Err(format!("{:?}의 value는 숫자 또는 문자열이어야 함", c.op));
            }
        }
        Op::Contains | Op::StartsWith | Op::EndsWith | Op::InCidr | Op::NotInCidr => {
            if !value.is_some_and(|v| v.is_string()) {
                return Err(format!("{:?}의 value는 문자열이어야 함", c.op));
            }
        }
    }
    Ok(())
}

// cargo test --lib playbook::nodes
#[cfg(test)]
mod tests {
    use super::*;

    // JSON 문자열(nodes 배열)을 Playbook으로 파싱하는 유일한 헬퍼
    // (from_json은 serde_json::Value를 받으므로 문자열을 Value로 바꿔서 넘김)
    fn parse(nodes_json: &str) -> Result<Playbook, String> {
        let nodes: serde_json::Value =
            serde_json::from_str(nodes_json).expect("테스트 JSON 문법 오류");
        Playbook::from_json(1, "테스트".into(), true, nodes)
    }

    // 시나리오 1: 조건 노드(and/or 중첩) + 대응 노드가 파싱된다
    #[test]
    fn parses_nested_conditions_and_response() {
        let pb = parse(r#"[
            { "type": "condition", "name": "파일 다운로드만",
              "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
            { "type": "condition", "name": "악성 탐지 또는 고위험 IP의 대용량 파일",
              "when": { "or": [
                { "field": "enrichment_file_sha256.data.malicious", "op": "gte", "value": 10 },
                { "and": [
                  { "field": "enrichment_src_ip.data.abuse_confidence_score", "op": "gte", "value": 80 },
                  { "field": "file_size_bytes", "op": "gt", "value": 100000 } ] } ] } },
            { "type": "response", "action": "DENY" }
        ]"#)
        .expect("파싱 실패");

        assert_eq!(pb.nodes.len(), 3);
    }

    // 시나리오 2: 조건 노드 없이 대응 노드만 있어도 유효하다
    #[test]
    fn response_only_playbook_is_valid() {
        assert!(parse(r#"[ { "type": "response", "action": "DENY" } ]"#).is_ok());
    }

    // 시나리오 3: AI 대응 노드가 파싱되고, risk → action 매핑을 조회할 수 있다
    #[test]
    fn parses_ai_response_actions() {
        let pb = parse(
            r#"[
            { "type": "condition", "name": "파일 다운로드만",
              "when": { "field": "event_type", "op": "eq", "value": "file_download" } },
            { "type": "ai_response",
              "model": "qwen2.5:14b-instruct",
              "prompt": "분석하라",
              "actions": { "HIGH": "HOLD", "MEDIUM": "DENY", "LOW": "DETECT" } }
        ]"#,
        )
        .expect("파싱 실패");

        match &pb.nodes[1] {
            Node::AiResponse { model, actions, .. } => {
                assert_eq!(*model, AiModel::Qwen25Instruct14b);
                assert_eq!(actions.get(Risk::High), Some(Action::Hold));
                assert_eq!(actions.get(Risk::Medium), Some(Action::Deny));
                assert_eq!(actions.get(Risk::Low), Some(Action::Detect));
            }
            other => panic!("ai_response 노드여야 함: {other:?}"),
        }
    }

    // 시나리오 4: 두 번째 모델(Foundation-Sec)도 파싱된다
    #[test]
    fn parses_second_ai_model() {
        let pb = parse(
            r#"[
            { "type": "ai_response",
              "model": "hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF",
              "prompt": "분석하라",
              "actions": { "HIGH": "HOLD" } }
        ]"#,
        )
        .expect("파싱 실패");

        match &pb.nodes[0] {
            Node::AiResponse { model, .. } => {
                assert_eq!(*model, AiModel::FoundationSec8bReasoning);
            }
            other => panic!("ai_response 노드여야 함: {other:?}"),
        }
    }

    // 시나리오 5: as_str()이 serde 태그와 같은 값을 반환한다
    // (캐시 키에 as_str()을 쓰므로 serde rename과 어긋나면 안 됨)
    #[test]
    fn model_tags_match_serde() {
        for model in [
            AiModel::Qwen25Instruct14b,
            AiModel::FoundationSec8bReasoning,
        ] {
            assert_eq!(
                serde_json::to_value(model).unwrap(),
                serde_json::json!(model.as_str())
            );
        }
    }

    // 시나리오 6: 잘못된 설정은 로드 시점에 거부
    // 케이스 구성: (설명, 에러 메시지에 포함되어야 하는 문구, JSON)
    #[test]
    fn invalid_playbooks_are_rejected() {
        let cases: Vec<(&str, &str, &str)> = vec![
            // ── 구조 검증 (validate) ──
            ("빈 nodes", "nodes가 비어 있음", r#"[]"#),
            (
                "마지막이 조건 노드",
                "마지막 노드는 종료 노드",
                r#"[ { "type": "condition", "when": { "field": "a", "op": "eq", "value": 1 } } ]"#,
            ),
            (
                "종료 노드가 중간에 있음",
                "맨 끝에만 올 수 있음",
                r#"[ { "type": "response", "action": "DENY" }, { "type": "response", "action": "DENY" } ]"#,
            ),
            // ── 조건 검증 (validate_condition) ──
            (
                "빈 and 그룹",
                "빈 and 그룹",
                r#"[ { "type": "condition", "when": { "and": [] } },
                     { "type": "response", "action": "DENY" } ]"#,
            ),
            (
                "exists에 value",
                "value를 쓰지 않음",
                r#"[ { "type": "condition", "when": { "field": "a", "op": "exists", "value": 1 } },
                     { "type": "response", "action": "DENY" } ]"#,
            ),
            (
                "gt에 문자열 value",
                "value는 숫자여야 함",
                r#"[ { "type": "condition", "when": { "field": "a", "op": "gt", "value": "x" } },
                     { "type": "response", "action": "DENY" } ]"#,
            ),
            // ── AI 대응 노드 검증 (validate) ──
            (
                "AI actions가 비어 있음",
                "actions에 매핑이 하나도 없음",
                r#"[ { "type": "ai_response", "model": "qwen2.5:14b-instruct", "prompt": "p", "actions": {} } ]"#,
            ),
            (
                "AI prompt가 비어 있음",
                "prompt가 비어 있음",
                r#"[ { "type": "ai_response", "model": "qwen2.5:14b-instruct", "prompt": " ", "actions": { "HIGH": "HOLD" } } ]"#,
            ),
            // ── serde 파싱 단계에서 거부 (에러 문구는 serde가 만듦) ──
            (
                "알 수 없는 노드 type (foo)",
                "unknown variant `foo`",
                r#"[ { "type": "foo" } ]"#,
            ),
            (
                "알 수 없는 op (gtee)",
                "untagged enum Expr",
                r#"[ { "type": "condition", "when": { "field": "a", "op": "gtee", "value": 1 } },
                     { "type": "response", "action": "DENY" } ]"#,
            ),
            (
                "ALLOW는 action이 아님",
                "unknown variant `ALLOW`",
                r#"[ { "type": "response", "action": "ALLOW" } ]"#,
            ),
            (
                "허용되지 않은 model (no-such-model)",
                "unknown variant `no-such-model`",
                r#"[ { "type": "ai_response", "model": "no-such-model", "prompt": "p", "actions": { "HIGH": "HOLD" } } ]"#,
            ),
            (
                "AI actions에 알 수 없는 risk (HGIH)",
                "unknown field `HGIH`",
                r#"[ { "type": "ai_response", "model": "qwen2.5:14b-instruct", "prompt": "p", "actions": { "HGIH": "HOLD" } } ]"#,
            ),
        ];

        for (desc, expected, nodes_json) in cases {
            let err = parse(nodes_json).expect_err(&format!("거부되어야 함: {desc}"));
            assert!(
                err.contains(expected),
                "{desc}: 기대 문구 `{expected}`가 없음, 실제 에러 = {err}"
            );
        }
    }
}
