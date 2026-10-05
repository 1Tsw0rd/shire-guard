/*
OpenSearch/ClickHouse에 저장할 이벤트 로그 한 건을 만드는 파일

이벤트 처리 Task가 끝나면 최종 로그를 NDJSON 한 줄로 직렬화 해서 StorageRecord에 담고,
채널을 통해 전달 Task(forwarder.rs)로 넘김

처리된 이벤트 예시:
{
  "event_id": "evt-1",
  "event_type": "file_download",
  "timestamp": "...",
  "src_ip": "1.2.3.4",
  "enrichment_src_ip": {...},
  "playbook_result": {...}
}
         ↓ JSON 직렬화 + "\n"
{"event_id":"evt-1",...,"playbook_result":{...}}\n
         ↓
    StorageRecord
*/

use serde::Serialize;

use crate::core::enrichment::evidence::Evidence;
use crate::core::playbook::engine::PlaybookResult;

/*
전달 Task(forwarder.rs)로 넘기는 로그 1건
- partition/offset은 Rust Axum이 소비한 Kafka/RedPanda Commit 관리용으로 사용
- line은 JSON 한 줄 + 줄바꿈("\n")으로 NDJSON 한 줄로 직렬화된 이벤트 로그

Vec<u8>
┌─────────────────────────────────────┐
│ JSON 바이트들              │ \n │
└─────────────────────────────────────┘
             하나의 Vec<u8>
*/
#[derive(Debug)]
pub struct StorageRecord {
    pub line: Vec<u8>,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Serialize)]
struct StoredLog<'a> {
    #[serde(flatten)] // RawEvent 평탄화
    evidence: &'a Evidence,
    #[serde(skip_serializing_if = "Option::is_none")]
    // 결정한 Playbook이 없는 이벤트가 존재 가능 (Playbook 평가는 모든 이벤트에 실행됨)
    playbook_result: Option<&'a PlaybookResult>,
}

impl StorageRecord {
    pub fn new(
        evidence: &Evidence,
        playbook_result: Option<&PlaybookResult>,
        partition: i32,
        offset: i64,
    ) -> Result<Self, serde_json::Error> {
        // StoredLog를 JSON 바이트(Vec<u8>)로 직렬화
        // 결과 예: {"event_id":"evt-1", ...}
        let mut line = serde_json::to_vec(&StoredLog {
            evidence,
            playbook_result,
        })?;
        // JSON 뒤에 줄바꿈(\n)을 붙여 NDJSON 한 줄로 만듦
        // 결과 예: {"event_id":"evt-1", ...}\n
        line.push(b'\n');

        Ok(Self {
            line,
            partition,
            offset,
        })
    }
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

    fn parse_line(record: &StorageRecord) -> serde_json::Value {
        serde_json::from_slice(&record.line).expect("저장 로그 JSON 파싱 실패")
    }

    // 시나리오 1: 한 줄짜리 JSON이고 끝에만 줄바꿈이 있다 (vector-storage가 한 줄을 이벤트 하나로 나누므로)
    #[test]
    fn line_is_single_json_line_ending_with_newline() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();

        assert_eq!(record.line.last(), Some(&b'\n'));
        assert_eq!(record.line.iter().filter(|&&b| b == b'\n').count(), 1);
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset, 100);
    }

    // 시나리오 2: 원본 필드와 수신 Vector가 넣은 ingested_at이 최상위에 그대로 들어간다
    #[test]
    fn keeps_raw_event_fields_and_ingested_at_at_top_level() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();
        let value = parse_line(&record);

        assert_eq!(value["event_id"], "evt-1");
        assert_eq!(value["ingested_at"], "2026-10-03T10:00:01.123456789Z");
        assert_eq!(value["file_name"], "invoice.exe");
        assert!(value.get("evidence").is_none()); // flatten으로 감싸는 필드명이 없어야 함
    }

    // 시나리오 3: 결정한 Playbook이 없으면 playbook_result 필드가 없다
    #[test]
    fn omits_playbook_result_when_none() {
        let record = StorageRecord::new(&sample_evidence(), None, 0, 100).unwrap();
        let value = parse_line(&record);

        assert!(value.get("playbook_result").is_none());
    }

    // 시나리오 4: 결정한 Playbook이 있으면 playbook_result가 붙는다
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
        let value = parse_line(&record);

        assert_eq!(value["playbook_result"]["action"], "DENY");
        assert_eq!(value["playbook_result"]["playbooks"][0]["name"], "테스트");
    }
}
