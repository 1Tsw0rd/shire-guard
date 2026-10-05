// dbbench: 파이프라인 없이 OpenSearch / ClickHouse에 직접 대량 저장하며 저장 속도를 측정하는 도구
//
// 예시:
//   ./dbbench os 500000 --batch 1000 --concurrency 4
//   ./dbbench os 500000 --batch 1000 --concurrency 4 --os-id false   (OpenSearch 자동 ID)
//   ./dbbench ch 500000 --batch 10000 --concurrency 4
//
// 문서 모양: sample.json(실제 저장된 문서 1건)을 복제하고 event_id, ingested_at만 바꿔 서로 다른 로그로 만듦
// log_id = sha256(event_id|ingested_at) → 파이프라인(vector-storage)과 같은 방식
// 측정 시간에는 문서 생성 시간이 포함되지 않음 (전송할 본문을 미리 다 만든 뒤 측정 시작)

use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, SecondsFormat, Utc};
use clap::{Parser, ValueEnum};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(ValueEnum, Clone, Debug)]
enum Target {
    Os, // OpenSearch
    Ch, // ClickHouse
}

#[derive(Parser, Debug)]
#[command(about = "OpenSearch / ClickHouse 직접 저장 성능 측정 도구")]
struct Args {
    /// 대상: os(OpenSearch) 또는 ch(ClickHouse)
    target: Target,

    /// 총 저장 건수
    total: usize,

    /// 요청 1번에 담을 문서 수
    #[arg(long, default_value_t = 1000)]
    batch: usize,

    /// 동시에 보낼 요청 수
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// 실제 문서 1건 (검색 결과 전체 또는 _source만 있는 JSON 둘 다 가능)
    #[arg(long, default_value = "sample.json")]
    sample: String,

    /// OpenSearch: _id를 log_id로 지정 (false면 OpenSearch 자동 ID)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    os_id: bool,

    #[arg(long, default_value = "http://localhost:9200")]
    os_url: String,

    #[arg(long, default_value = "bench-os")]
    os_index: String,

    #[arg(long, default_value = "http://localhost:8123")]
    ch_url: String,

    #[arg(long, default_value = "shire.bench_event_logs")]
    ch_table: String,

    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    ch_user: String,

    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    ch_password: String,
}

// 요청 1번의 결과
struct BatchResult {
    ok: usize,
    failed: usize,
    error: Option<String>, // 실패 시 첫 오류 내용
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let sample = load_sample(&args.sample);
    let client = reqwest::Client::new();

    // OpenSearch는 테스트 인덱스가 미리 만들어져 있어야 함 (없으면 매핑 없이 자동 생성되어 측정이 무의미해짐)
    if let Target::Os = args.target {
        let url = format!("{}/{}", args.os_url, args.os_index);
        match client.head(&url).send().await {
            Ok(r) if r.status().is_success() => {}
            _ => {
                eprintln!("OpenSearch 인덱스 '{}'가 없음. 먼저 bench-os.json으로 생성할 것", args.os_index);
                std::process::exit(1);
            }
        }
    }

    // 1. 전송할 본문을 미리 모두 생성 (측정 시간에서 제외)
    println!("본문 생성 중... ({}건, 배치 {})", args.total, args.batch);
    let bodies = build_bodies(&args, &sample);

    // 2. 동시 요청 수를 Semaphore로 제한하며 전송
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let mut set = JoinSet::new();
    let started = Instant::now();

    for (count, body) in bodies {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let target = args.target.clone();
        let (os_url, ch_url, ch_table, ch_user, ch_password) = (
            args.os_url.clone(),
            args.ch_url.clone(),
            args.ch_table.clone(),
            args.ch_user.clone(),
            args.ch_password.clone(),
        );

        set.spawn(async move {
            let _permit = permit; // 요청이 끝나면 자리 반환
            match target {
                Target::Os => send_os(&client, &os_url, count, body).await,
                Target::Ch => {
                    send_ch(&client, &ch_url, &ch_table, &ch_user, &ch_password, count, body).await
                }
            }
        });
    }

    // 3. 결과 집계
    let (mut ok, mut failed, mut failed_requests) = (0usize, 0usize, 0usize);
    let mut first_error: Option<String> = None;
    while let Some(joined) = set.join_next().await {
        let r = joined.expect("전송 Task 실패");
        ok += r.ok;
        failed += r.failed;
        if r.failed > 0 {
            failed_requests += 1;
        }
        if first_error.is_none() {
            first_error = r.error;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();

    println!();
    println!("=== 결과 ===");
    match args.target {
        Target::Os => println!("대상: OpenSearch (index {}, _id 지정 {})", args.os_index, args.os_id),
        Target::Ch => println!("대상: ClickHouse (table {})", args.ch_table),
    }
    println!("총 {}건 / 배치 {} / 동시 {}", args.total, args.batch, args.concurrency);
    println!("성공 {} / 실패 {} (실패 요청 {})", ok, failed, failed_requests);
    println!("시간 {:.2}초 / {:.0} docs/s", elapsed, ok as f64 / elapsed);
    if let Some(e) = first_error {
        println!("첫 오류: {e}");
    }
}

// sample.json 읽기: 검색 결과 전체면 첫 문서의 _source를 꺼냄
fn load_sample(path: &str) -> Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path} 읽기 실패: {e}"));
    let value: Value = serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .unwrap_or_else(|e| panic!("{path} JSON 파싱 실패: {e}"));

    let mut doc = match value.pointer("/hits/hits/0/_source") {
        Some(src) => src.clone(),
        None => value,
    };
    assert!(doc.is_object(), "sample 문서가 JSON 객체가 아님");

    // 파이프라인과 같게: OpenSearch 문서에는 log_id 필드가 없음 (_id로만 사용)
    doc.as_object_mut().unwrap().remove("log_id");
    doc
}

// 문서 1건 생성: event_id, ingested_at만 바꾸고 log_id 계산
fn make_doc(sample: &Value, run_id: i64, i: usize, base: chrono::DateTime<Utc>) -> (String, Value) {
    let mut doc = sample.clone();
    let event_id = format!("bench-{run_id}-{i}");
    let ingested_at = (base + Duration::nanoseconds(i as i64))
        .to_rfc3339_opts(SecondsFormat::Nanos, true); // 예: 2026-10-05T10:00:01.123456789Z

    doc["event_id"] = json!(event_id);
    doc["ingested_at"] = json!(ingested_at);
    let log_id = hex::encode(Sha256::digest(format!("{event_id}|{ingested_at}")));
    (log_id, doc)
}

// 배치 단위로 요청 본문 생성
fn build_bodies(args: &Args, sample: &Value) -> Vec<(usize, Vec<u8>)> {
    let run_id = Utc::now().timestamp_millis(); // 실행마다 다른 event_id가 나오도록
    let base = Utc::now();
    let mut bodies = Vec::new();

    let mut i = 0;
    while i < args.total {
        let count = args.batch.min(args.total - i);
        let mut body = Vec::new();

        for j in i..i + count {
            let (log_id, doc) = make_doc(sample, run_id, j, base);
            match args.target {
                // OpenSearch bulk: 액션 줄 + 문서 줄
                Target::Os => {
                    let action = if args.os_id {
                        json!({ "index": { "_index": args.os_index, "_id": log_id } })
                    } else {
                        json!({ "index": { "_index": args.os_index } })
                    };
                    serde_json::to_writer(&mut body, &action).unwrap();
                    body.push(b'\n');
                    serde_json::to_writer(&mut body, &doc).unwrap();
                    body.push(b'\n');
                }
                // ClickHouse JSONEachRow: 한 줄에 행 하나 (log_id 컬럼 + payload)
                Target::Ch => {
                    let row = json!({ "log_id": log_id, "payload": doc });
                    serde_json::to_writer(&mut body, &row).unwrap();
                    body.push(b'\n');
                }
            }
        }

        bodies.push((count, body));
        i += count;
    }
    bodies
}

// OpenSearch _bulk 전송: HTTP 200이어도 문서별 실패가 있을 수 있어 응답의 items를 확인
async fn send_os(client: &reqwest::Client, url: &str, count: usize, body: Vec<u8>) -> BatchResult {
    let resp = client
        .post(format!("{url}/_bulk"))
        .header("Content-Type", "application/x-ndjson")
        .body(body)
        .send()
        .await;

    let resp = match resp {
        Ok(r) => r,
        Err(e) => return BatchResult { ok: 0, failed: count, error: Some(format!("{e:?}")) },
    };

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return BatchResult { ok: 0, failed: count, error: Some(format!("{status}: {text}")) };
    }

    // 응답을 해석하지 못하면 결과를 알 수 없으므로 실패로 처리
    let value: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            return BatchResult { ok: 0, failed: count, error: Some(format!("응답 파싱 실패: {e}")) };
        }
    };

    // errors: false → 전부 성공, true → 문서별 결과 확인, 그 외(필드 없음 등) → 실패
    match value["errors"].as_bool() {
        Some(false) => return BatchResult { ok: count, failed: 0, error: None },
        Some(true) => {}
        None => {
            return BatchResult { ok: 0, failed: count, error: Some(format!("errors 필드 없음: {text}")) };
        }
    }
    // 일부 문서 실패: 문서마다 결과가 하나씩 와야 하므로 개수부터 확인
    let items = match value["items"].as_array() {
        Some(items) if items.len() == count => items,
        Some(items) => {
            return BatchResult {
                ok: 0,
                failed: count,
                error: Some(format!("items 개수 불일치: 기대 {count}, 실제 {}", items.len())),
            };
        }
        None => {
            return BatchResult { ok: 0, failed: count, error: Some("items 필드 없음".to_string()) };
        }
    };
    let mut failed = 0;
    let mut error = None;
    for item in items {
            let result = &item["index"];
        if result["status"].as_u64().unwrap_or(500) >= 300 {
            failed += 1;
            if error.is_none() {
                error = Some(result["error"].to_string());
            }
        }
    }
    BatchResult { ok: count - failed, failed, error }
}

// ClickHouse HTTP INSERT 전송 (JSONEachRow)
async fn send_ch(
    client: &reqwest::Client,
    url: &str,
    table: &str,
    user: &str,
    password: &str,
    count: usize,
    body: Vec<u8>,
) -> BatchResult {
    let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
    let resp = client
        .post(format!("{url}/"))
        .query(&[("query", query.as_str()), ("date_time_input_format", "best_effort")])
        .basic_auth(user, Some(password))
        .body(body)
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => BatchResult { ok: count, failed: 0, error: None },
        Ok(r) => {
            let status = r.status();
            let text = r.text().await.unwrap_or_default();
            BatchResult { ok: 0, failed: count, error: Some(format!("{status}: {text}")) }
        }
        Err(e) => BatchResult { ok: 0, failed: count, error: Some(format!("{e:?}")) },
    }
}