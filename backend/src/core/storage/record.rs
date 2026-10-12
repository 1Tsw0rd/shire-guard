/*
OpenSearch/ClickHouse에 저장할 이벤트 로그 한 건을 만드는 파일

이벤트 처리 Task가 끝나면 최종 로그를 JSON으로 한 번만 직렬화해서 StorageRecord에 담고,
채널을 통해 전달 Task(forwarder.rs)로 넘김
전송 형식(OpenSearch bulk, ClickHouse 행)은 전송할 때 이 JSON 앞뒤에 문자열만 붙여 만듦 (다시 직렬화하지 않음)

처리된 이벤트 예시:
{
  "event_id": "evt-1",
  "event_type": "file_download",
  "timestamp": "...",
  "ingested_at": "2026-10-03T10:00:01.123456789Z",
  "src_ip": "1.2.3.4",
  "enrichment_src_ip": {...},
  "playbook_result": {...}
}
         ↓
StorageRecord
  log_id     : sha256(event_id|ingested_at)   → OpenSearch _id, ClickHouse log_id 컬럼
  index_name : event-logs-2026.10.03          → OpenSearch 인덱스 (ingested_at의 UTC 날짜)
  doc        : 위 JSON (줄바꿈 없음)
  partition  : Kafka/RedPanda 커밋 위치 계산용
  offset     : Kafka/RedPanda 커밋 위치 계산용
*/

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::enrichment::evidence::Evidence;
use crate::core::playbook::engine::PlaybookResult;

#[derive(Debug)]
pub struct StorageRecord {
    pub log_id: String,
    pub index_name: String,
    pub doc: String, // 저장할 이벤트 JSON (줄바꿈 없음, log_id 필드 없음)
    pub partition: i32,
    pub offset: i64,
}

#[derive(Serialize)]
struct StoredLog<'a> {
    #[serde(flatten)] // RawEvent 평탄화
    evidence: &'a Evidence,
    #[serde(skip_serializing_if = "Option::is_none")]
    // Playbook 판단이 없는 이벤트는 playbook_result 필드가 없음 (Playbook 평가는 모든 이벤트에 실행됨)
    playbook_result: Option<&'a PlaybookResult>,
}

impl StorageRecord {
    pub fn new(
        evidence: &Evidence,
        playbook_result: Option<&PlaybookResult>,
        partition: i32,
        offset: i64,
    ) -> Result<Self, serde_json::Error> {
        let ingested_at = evidence.event.ingested_at.as_str(); // 수신 Vector가 기록한 이벤트 수집 시각 (RawEvent 필수 필드)
        let index_name = build_index_name(ingested_at);
        let log_id = build_log_id(&evidence.event.event_id, ingested_at);

        // StoredLog를 JSON 문자열로 직렬화, 예: {"event_id":"evt-1", ...}
        let doc = serde_json::to_string(&StoredLog {
            evidence,
            playbook_result,
        })?;

        Ok(Self {
            log_id,
            index_name,
            doc,
            partition,
            offset,
        })
    }
}

fn build_log_id(event_id: &str, ingested_at: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(event_id.as_bytes());
    hasher.update(b"|");
    hasher.update(ingested_at.as_bytes());

    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}")) // x는 16진수, 2는 최소 2자리, 0은 빈자리를 0으로 채움
        .collect()
}

// OpenSearch 인덱스 이름: event-logs-YYYY.MM.DD
// 수신 Vector는 ingested_at을 항상 UTC로 기록하므로 (예: 2026-10-03T10:00:01.123456789Z)
fn build_index_name(ingested_at: &str) -> String {
    let date = &ingested_at[..10]; // 앞 10자리(YYYY-MM-DD)를 가져옴
    format!("event-logs-{}", date.replace('-', ".")) // 앞 10글자(2026-10-03)를 그대로 잘라 "-"만 "."으로 바꿈
}

// cargo test --lib storage::record
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::consumer::event::RawEvent;
    use crate::core::playbook::engine::PlaybookDecision;
    use crate::core::playbook::nodes::Action;

    fn sample_evidence() -> Evidence {
        let event: RawEvent = serde_json::from_str(
            r#"{
                "event_id": "evt-1",
                "event_type": "file_download",
                "timestamp": "2026-10-03T10:00:00Z",
                "ingested_at": "2026-10-03T10:00:01.123456789Z",
                "src_ip": "1.2.3.4",
                "file_name": "invoice.exe"
            }"#,
        )
        .expect("테스트 이벤트 파싱 실패");

        Evidence {
            event,
            enrichment_src_ip: None,
            enrichment_dst_domain: None,
            enrichment_file_sha256: None,
        }
    }

    fn parse_doc(record: &StorageRecord) -> serde_json::Value {
        serde_json::from_str(&record.doc).expect("저장 로그 JSON 파싱 실패")
    }

    // 시나리오 1: log_id는 sha256(event_id|ingested_at)로 생성된다
    #[test]
    fn log_id_is_sha256_of_event_id_and_ingested_at() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();

        assert_eq!(
            record.log_id,
            "9bd009ec30efe923c86b7c9265edba088b9af024f8ea6df80bf8ec62e791b6b5"
        );
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset, 100);
    }

    // 시나리오 2: 인덱스 이름은 ingested_at의 날짜로 생성된다
    #[test]
    fn index_name_uses_ingested_at_date() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();

        assert_eq!(record.index_name, "event-logs-2026.10.03");
    }

    // 시나리오 3: 문서는 줄바꿈 없는 JSON이고, 원본 필드는 최상위에 있으며 log_id 필드는 포함하지 않는다
    #[test]
    fn doc_keeps_fields_without_newline_and_log_id() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();
        let value = parse_doc(&record);

        assert!(!record.doc.contains('\n')); // JSON 문서에 줄바꿈 문자(\n)가 포함되지 않았는지 확인
        assert_eq!(value["event_id"], "evt-1");
        assert_eq!(value["ingested_at"], "2026-10-03T10:00:01.123456789Z");
        assert_eq!(value["file_name"], "invoice.exe");
        assert!(value.get("evidence").is_none()); // flatten으로 감싸는 필드명이 없어야 함
        assert!(value.get("log_id").is_none()); // log_id는 OpenSearch _id, ClickHouse 컬럼으로만 저장
    }

    // 시나리오 4: 결정한 Playbook이 없으면 playbook_result 필드가 없다
    #[test]
    fn omits_playbook_result_when_none() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();

        assert!(parse_doc(&record).get("playbook_result").is_none());
    }

    // 시나리오 5: 결정한 Playbook이 있으면 playbook_result가 붙는다
    #[test]
    fn includes_playbook_result_when_present() {
        let result = PlaybookResult {
            action: Action::Deny,
            evaluated: 1,
            processed_at: "2026-10-03T10:00:02Z".to_string(),
            playbooks: vec![PlaybookDecision {
                playbook_id: 1,
                name: "테스트".to_string(),
                action: Action::Deny,
                when: Some("(event_type eq file_download)".to_string()),
                ai_result: None,
            }],
        };

        let record = StorageRecord::new(&sample_evidence(), Some(&result), 0, 100).unwrap();
        let value = parse_doc(&record);

        assert_eq!(value["playbook_result"]["action"], "DENY");
        assert_eq!(value["playbook_result"]["playbooks"][0]["name"], "테스트");
    }
}
