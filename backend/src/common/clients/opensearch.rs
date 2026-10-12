use std::time::Duration;

use reqwest::RequestBuilder;
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, de::DeserializeOwned};

use crate::common::error::AppError;

// 조회 요청 1번의 응답 제한 시간
const SEARCH_TIMEOUT: Duration = Duration::from_secs(10);

// Bulk 요청 1번의 응답 제한 시간
const BULK_TIMEOUT: Duration = Duration::from_secs(30);

// Bulk 응답에서 필요한 필드만 받도록 요청
const BULK_RESPONSE_FILTER: &str = "items.*.status,items.*.error.type,items.*.error.reason";

// Bulk로 저장할 문서 1건 (복사하지 않고 빌려서 받음)
pub struct BulkDoc<'a> {
    pub index: &'a str,   // 저장할 인덱스 이름
    pub id: &'a str,      // 문서 _id (같은 _id로 다시 저장하면 덮어씀)
    pub source: &'a [u8], // 문서 JSON
}

// Bulk 응답의 문서 1건 결과
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkItemResult {
    pub status: u16, // 문서별 HTTP 상태 코드 (예: 201 저장, 400 문서 오류, 429 과부하)
    pub error: Option<String>, // 실패 시 OpenSearch가 보낸 오류 내용
}

#[derive(Clone)]
pub struct OpenSearchClient {
    http: reqwest::Client,
    base_url: String,
}

impl OpenSearchClient {
    pub fn new(base_url: String) -> Result<Self, AppError> {
        let http = reqwest::Client::builder().build().map_err(|err| {
            AppError::Internal(format!(
                "OpenSearch 클라이언트 생성에 실패했습니다: {}",
                err
            ))
        })?;

        Ok(Self { http, base_url })
    }

    // 모든 요청의 공통 처리: 전송 → 상태 코드 확인 → 응답 JSON을 T로 해석
    // action: 에러 메시지에 쓸 요청 이름 (예: "Search", "Bulk")
    async fn send<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        action: &str,
    ) -> Result<T, AppError> {
        let response = request.send().await.map_err(|err| {
            AppError::Internal(format!(
                "OpenSearch {} 요청에 실패했습니다: {}",
                action, err
            ))
        })?;

        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default(); // OpenSearch가 보낸 오류 내용
            return Err(AppError::Internal(format!(
                "OpenSearch {} 요청이 실패했습니다 ({}): {}",
                action, status, error_body
            )));
        }

        response.json::<T>().await.map_err(|err| {
            AppError::Internal(format!(
                "OpenSearch {} 응답 해석에 실패했습니다: {}",
                action, err
            ))
        })
    }

    /*
    index에서 문서 조회: POST <index>/_search

    예시:
    POST event-logs-2026.10.08/_search
    {
        "query": {
            "match": {
                "event_type": "login"
            }
        }
    }
    */
    pub async fn search(
        &self,
        index: &str,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value, AppError> {
        // 본문이 있는 GET은 일부 HTTP 클라이언트·프록시가 본문을 버리기 때문에 GET 대신 POST로 처리
        let request = self
            .http
            .post(format!("{}/{}/_search", self.base_url, index))
            .timeout(SEARCH_TIMEOUT)
            .json(query); // query: OpenSearch Query DSL 그대로 (조건 검색, 범위, 정렬, 집계 등)

        self.send(request, "Search").await
    }

    /*
    Bulk 저장

    예시:
    POST /_bulk?filter_path=<필요한 응답 필드>
    {"index":{"_index":"<index>","_id":"<_id>"}}
    {"field":"value"}
    {"index":{"_index":"event-logs-2026.10.08","_id":"123456789"}}
    {"name":"lee","age":25}

    - 첫 번째 줄: 저장할 인덱스와 doc ID
    - 두 번째 줄: 저장할 doc 내용
    */
    pub async fn bulk_index(&self, docs: &[BulkDoc<'_>]) -> Result<Vec<BulkItemResult>, AppError> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        let request = self
            .http
            .post(format!("{}/_bulk", self.base_url))
            .query(&[("filter_path", BULK_RESPONSE_FILTER)])
            .header(CONTENT_TYPE, "application/x-ndjson")
            .timeout(BULK_TIMEOUT)
            .body(build_bulk_body(docs));

        let response: BulkResponse = self.send(request, "Bulk").await?;
        to_bulk_results(response, docs.len())
    }
}

// Bulk 요청 본문 생성 함수
fn build_bulk_body(docs: &[BulkDoc<'_>]) -> Vec<u8> {
    // Bulk action의 고정된 앞부분
    // {"index":{"_index":"
    const PREFIX: &[u8] = br#"{"index":{"_index":""#;

    // index와 doc ID 사이의 고정된 부분
    // ","_id":"
    const MIDDLE: &[u8] = br#"","_id":""#;

    // doc ID 뒤의 고정된 부분 + 줄바꿈
    // "}}\n
    const SUFFIX: &[u8] = b"\"}}\n";

    // 문서 1개를 만들 때 필요한 고정 길이 계산
    // +1은 실제 문서 내용(source) 뒤에 붙는 줄바꿈
    let fixed_len = PREFIX.len() + MIDDLE.len() + SUFFIX.len() + 1;

    // 모든 문서를 붙였을 때 필요한 전체 바이트 크기를 미리 계산
    let capacity = docs
        .iter()
        .map(|d| fixed_len + d.index.len() + d.id.len() + d.source.len())
        .sum();

    // 계산한 크기만큼 메모리를 미리 확보
    let mut body = Vec::with_capacity(capacity);

    // 문서 하나씩 Bulk 형식으로 조립
    for doc in docs {
        // 저장할 index를 지정하는 action 앞부분 추가
        body.extend_from_slice(PREFIX);

        // 실제 index 이름 추가
        body.extend_from_slice(doc.index.as_bytes());

        // doc ID 앞부분 추가
        body.extend_from_slice(MIDDLE);

        // 실제 doc ID 추가
        body.extend_from_slice(doc.id.as_bytes());

        // action 줄 완성 + 줄바꿈
        body.extend_from_slice(SUFFIX);

        // 실제 저장할 문서 내용 추가
        body.extend_from_slice(doc.source);

        // NDJSON 형식에 맞춰 문서 내용 뒤에 줄바꿈
        body.push(b'\n');
    }

    // 완성된 Bulk 요청 body 반환
    // 예: docs가 아래 2개라면
    // index = "logs", id = "123", source = {"name":"kim"}
    // index = "logs", id = "456", source = {"name":"lee"}
    //
    // 최종 body:
    // {"index":{"_index":"logs","_id":"123"}}
    // {"name":"kim"}
    // {"index":{"_index":"logs","_id":"456"}}
    // {"name":"lee"}
    body
}

// Bulk 응답 형태: {"items":[{"index":{"status":201}}, {"index":{"status":400,"error":{...}}}]}
#[derive(Deserialize)]
struct BulkResponse {
    #[serde(default)] // 결과가 없으면 OpenSearch가 items를 생략함
    items: Vec<BulkItem>,
}

#[derive(Deserialize)]
struct BulkItem {
    index: BulkItemBody,
}

#[derive(Deserialize)]
struct BulkItemBody {
    status: u16,
    error: Option<BulkItemError>,
}

#[derive(Deserialize)]
struct BulkItemError {
    #[serde(rename = "type")]
    kind: Option<String>,
    reason: Option<String>,
}

/*
 Bulk 응답을 doc별 저장 결과로 변환

 예시: doc 3개 전송
 요청:
 {"index":{"_index":"logs","_id":"1"}}
 {"name":"kim"}
 {"index":{"_index":"logs","_id":"2"}}
 {"name":"lee"}
 {"index":{"_index":"logs","_id":"3"}}
 {"name":"park"}

 응답:
 {
   "items": [
     { "index": { "status": 201 } },
     { "index": { "status": 201 } },
     { "index": { "status": 400, "error": { "reason": "잘못된 값" } } }
   ]
 }

 보낸 문서마다 결과가 하나씩 반환되므로
 보낸 문서 수와 응답 결과 수가 같아야 각 문서의 결과를 정확히 대응할 수 있음
*/
fn to_bulk_results(response: BulkResponse, sent: usize) -> Result<Vec<BulkItemResult>, AppError> {
    if response.items.len() != sent {
        return Err(AppError::Internal(format!(
            "OpenSearch Bulk 응답 결과 수가 맞지 않습니다: 보낸 문서 {}건, 응답 결과 {}건",
            sent,
            response.items.len()
        )));
    }

    Ok(response
        .items
        .into_iter()
        .map(|item| BulkItemResult {
            status: item.index.status,
            // 오류 내용은 reason을 쓰고, 없으면 type을 씀
            error: item.index.error.and_then(|e| e.reason.or(e.kind)),
        })
        .collect())
}

// cargo test --lib clients::opensearch
#[cfg(test)]
mod tests {
    use super::*;

    fn doc<'a>(id: &'a str, source: &'a str) -> BulkDoc<'a> {
        BulkDoc {
            index: "logs-2026.10.03",
            id,
            source: source.as_bytes(),
        }
    }

    fn bulk_response(json: &str) -> BulkResponse {
        serde_json::from_str(json).expect("테스트 Bulk 응답 파싱 실패")
    }

    // 시나리오 1: Bulk 본문은 문서마다 액션 줄과 문서 줄이 이어진 NDJSON이다
    #[test]
    fn builds_bulk_body_as_ndjson() {
        /*
        {"index":{"_index":"logs-2026.10.03","_id":"a1"}}
        {"n":1}
        {"index":{"_index":"logs-2026.10.03","_id":"a2"}}
        {"n":2}
        */
        let docs = [doc("a1", r#"{"n":1}"#), doc("a2", r#"{"n":2}"#)];
        let body = build_bulk_body(&docs);

        let expected = concat!(
            r#"{"index":{"_index":"logs-2026.10.03","_id":"a1"}}"#,
            "\n",
            r#"{"n":1}"#,
            "\n",
            r#"{"index":{"_index":"logs-2026.10.03","_id":"a2"}}"#,
            "\n",
            r#"{"n":2}"#,
            "\n",
        );
        assert_eq!(String::from_utf8(body.clone()).unwrap(), expected);
        assert_eq!(body.capacity(), body.len()); // 미리 계산한 크기가 실제 크기와 같음
    }

    // 시나리오 2: 문서별 상태 코드와 오류 내용을 보낸 순서대로 돌려준다
    #[test]
    fn returns_status_and_error_in_order() {
        let response = bulk_response(
            r#"{"items":[
                {"index":{"status":201}},
                {"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field"}}},
                {"index":{"status":429,"error":{"type":"es_rejected_execution_exception"}}}
            ]}"#,
        );

        let results = to_bulk_results(response, 3).unwrap();

        assert_eq!(
            results,
            vec![
                BulkItemResult {
                    status: 201,
                    error: None
                },
                BulkItemResult {
                    status: 400,
                    error: Some("failed to parse field".into())
                },
                BulkItemResult {
                    status: 429,
                    error: Some("es_rejected_execution_exception".into())
                },
            ]
        );
    }

    // 시나리오 3: filter_path가 적용되지 않은 전체 응답이어도 필요한 필드만 읽는다
    #[test]
    fn ignores_unused_fields() {
        let response = bulk_response(
            r#"{"took":3,"errors":false,"items":[
                {"index":{"_index":"logs","_id":"a1","_version":1,"result":"created","_shards":{"total":1},"status":201}}
            ]}"#,
        );

        let results = to_bulk_results(response, 1).unwrap();

        assert_eq!(
            results,
            vec![BulkItemResult {
                status: 201,
                error: None
            }]
        );
    }

    // 시나리오 4: 결과 수가 보낸 문서 수와 다르면 에러
    #[test]
    fn item_count_mismatch_is_error() {
        let response = bulk_response(r#"{"items":[{"index":{"status":201}}]}"#);

        assert!(to_bulk_results(response, 2).is_err());
    }

    // 시나리오 5: items가 없는 응답은 결과 0건으로 해석되어 에러
    #[test]
    fn missing_items_is_error() {
        let response = bulk_response("{}");

        assert!(to_bulk_results(response, 1).is_err());
    }
}
