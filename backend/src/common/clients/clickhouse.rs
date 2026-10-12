use std::time::Duration;

use reqwest::{RequestBuilder, Response};

use crate::common::error::AppError;

// 조회 요청 1번의 응답 제한 시간
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

// INSERT 요청 1번의 응답 제한 시간
const INSERT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ClickHouseClient {
    http: reqwest::Client,
    base_url: String,
    database: String,
    user: String,
    password: String,
}

impl ClickHouseClient {
    pub fn new(
        base_url: String,
        database: String,
        user: String,
        password: String,
    ) -> Result<Self, AppError> {
        let http = reqwest::Client::builder().build().map_err(|err| {
            AppError::Internal(format!(
                "ClickHouse 클라이언트 생성에 실패했습니다: {}",
                err
            ))
        })?;

        Ok(Self {
            http,
            base_url,
            database,
            user,
            password,
        })
    }

    // 모든 요청에 database와 인증 정보를 추가하고 요청 결과를 확인
    async fn send(&self, request: RequestBuilder, action: &str) -> Result<Response, AppError> {
        let response = request
            .query(&[("database", self.database.as_str())])
            .basic_auth(&self.user, Some(&self.password))
            .send()
            .await
            .map_err(|err| {
                AppError::Internal(format!(
                    "ClickHouse {} 요청에 실패했습니다: {}",
                    action, err
                ))
            })?;

        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(AppError::Internal(format!(
                "ClickHouse {} 요청이 실패했습니다 ({}): {}",
                action, status, error_body
            )));
        }

        Ok(response)
    }

    /*
    SQL 조회: POST /?database=<DB>&default_format=JSON&param_<이름>=<값>

    예시:
    sql:
    SELECT
        log_id,
        payload.event_id,
        payload.event_type,
        payload.src_ip
    FROM event_logs FINAL
    WHERE payload.event_type = {event_type:String}
    LIMIT 10

    params:
    [("event_type", "file_download")]

    응답:
    {
        "data": [
            {
                "log_id": "9bd0e7c4a12f4e8d...",
                "payload.event_id": "evt-1",
                "payload.event_type": "file_download",
                "payload.src_ip": "192.168.0.10"
            }
        ],
        "rows": 1,
        ...
    }

    - {이름:타입}에 params 값 전달
    - 외부 입력은 SQL에 직접 넣지 않고 params로 전달
    */
    pub async fn query(
        &self,
        sql: &str,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value, AppError> {
        // ClickHouse HTTP는 URL 파라미터 중 "param_"으로 시작하는 것만 SQL의 {이름:타입} 값으로 인식함
        // 호출부는 SQL에 쓴 이름 그대로 넘기고, 여기서 "param_"을 붙임
        // 예: ("event_type", "file_download") → ("param_event_type", "file_download")
        let params: Vec<(String, &str)> = params
            .iter()
            .map(|(name, value)| (format!("param_{}", name), *value))
            .collect();

        let request = self
            .http
            .post(&self.base_url)
            .query(&[("default_format", "JSON")])
            .query(&params)
            .timeout(QUERY_TIMEOUT)
            .body(sql.to_owned());

        self.send(request, "Query")
            .await?
            .json()
            .await
            .map_err(|err| {
                AppError::Internal(format!(
                    "ClickHouse Query 응답 해석에 실패했습니다: {}",
                    err
                ))
            })
    }

    /*
    JSON 행 묶음 저장: POST /?database=<DB>&query=INSERT INTO <table> FORMAT JSONEachRow

    예시:
    rows:
    {"log_id":"9bd0...","payload":{"event_id":"evt-1",...}}
    {"log_id":"a3f1...","payload":{"event_id":"evt-2",...}}

    - 한 줄에 한 행
    - table에 INSERT할 테이블 이름 지정
    - rows는 완성된 JSONEachRow 본문
    */
    pub async fn insert_json_rows(&self, table: &str, rows: Vec<u8>) -> Result<(), AppError> {
        if rows.is_empty() {
            return Ok(());
        }

        let insert = format!("INSERT INTO {} FORMAT JSONEachRow", table);
        let request = self
            .http
            .post(&self.base_url)
            .query(&[
                ("query", insert.as_str()),
                ("date_time_input_format", "best_effort"),
            ])
            .timeout(INSERT_TIMEOUT)
            .body(rows);

        self.send(request, "Insert").await?;
        Ok(())
    }
}

// cargo test --lib clients::clickhouse -- --ignored
#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> ClickHouseClient {
        dotenvy::from_filename("../docker/.env").ok();
        let port = std::env::var("CLICKHOUSE_HTTP_PORT").expect("CLICKHOUSE_HTTP_PORT 필요");
        ClickHouseClient::new(
            format!("http://localhost:{}", port),
            std::env::var("CLICKHOUSE_DB").expect("CLICKHOUSE_DB 필요"),
            std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER 필요"),
            std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD 필요"),
        )
        .unwrap()
    }

    /*
    시나리오 1: params로 전달한 숫자가 SQL 조건에 들어가는지 확인

    SQL:
    SELECT count() AS total
    FROM numbers(10)
    WHERE number < {limit:UInt32}

    params:
    [("limit", "3")]

    실제 조건:
    number < 3

    결과:
    0, 1, 2 → 총 3개
    */
    #[tokio::test]
    #[ignore]
    async fn query_binds_params() {
        let result = test_client()
            .query(
                "SELECT count() AS total FROM numbers(10) WHERE number < {limit:UInt32}",
                &[("limit", "3")],
            )
            .await
            .unwrap();

        assert_eq!(result["data"][0]["total"], 3); // 0, 1, 2
    }

    /*
    시나리오 2: SQL처럼 보이는 값도 SQL로 실행되지 않고 값 그대로 전달되는지 확인

    SQL:
    SELECT {v:String} AS v

    params:
    [("v", "x' OR '1'='1")]

    결과:
    v = "x' OR '1'='1"

    입력값이 SQL 조건으로 실행되지 않고 문자열 값으로 처리됨
    */
    #[tokio::test]
    #[ignore]
    async fn query_treats_injection_as_value() {
        let input = "x' OR '1'='1";
        let result = test_client()
            .query("SELECT {v:String} AS v", &[("v", input)])
            .await
            .unwrap();

        assert_eq!(result["data"][0]["v"], input);
    }

    /*
    시나리오 3: 잘못된 SQL을 실행하면 Err가 반환되는지 확인

    SQL:
    SELECT FROM

    정상적인 SELECT 문이 아니므로 ClickHouse가 오류를 반환함
    */
    #[tokio::test]
    #[ignore]
    async fn query_error_is_err() {
        let result = test_client().query("SELECT FROM", &[]).await;

        assert!(result.is_err());
    }

    /*
    시나리오 4: 존재하지 않는 테이블에 INSERT하면 Err가 반환되는지 확인

    SQL:
    INSERT INTO table_does_not_exist FORMAT JSONEachRow

    본문:
    {"a":1}

    테이블이 존재하지 않으므로 ClickHouse가 오류를 반환함
    */
    #[tokio::test]
    #[ignore]
    async fn insert_into_unknown_table_is_err() {
        let result = test_client()
            .insert_json_rows("table_does_not_exist", b"{\"a\":1}\n".to_vec())
            .await;

        assert!(result.is_err());
    }
}
