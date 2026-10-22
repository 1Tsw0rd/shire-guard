use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, SecondsFormat, Utc};
// CommandFactory: 구조체 정의로 help 출력용 명령 정보를 생성
// Parser: 터미널 입력값을 파싱해 구조체의 각 필드에 담음
// ValueEnum: enum의 각 항목을 터미널에서 입력할 수 있는 선택지로 사용
use clap::{CommandFactory, Parser, ValueEnum};
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
#[command(about = "OpenSearch / ClickHouse 직접 저장 성능 측정 도구", after_help = HELP_EXTRA)]
struct Args {
    /// 대상: os(OpenSearch) 또는 ch(ClickHouse)
    target: Target,

    /// 총 저장 건수
    total: usize,

    /// 요청 1번에 담을 doc 수
    #[arg(long, default_value_t = 1000)]
    batch: usize,

    /// 동시에 보낼 요청 수
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// 복제할 doc JSON 파일 (생략 시 내장 doc SAMPLE_DOC 사용, 검색 결과 전체 또는 _source만 있는 JSON 둘 다 가능)
    #[arg(long)]
    sample: Option<String>,

    /// OpenSearch: _id에 log_id 지정 여부 (false면 _id 미지정 → OpenSearch가 임의 생성)
    // action = Set: 옵션 뒤에 입력한 값을 읽어 해당 필드에 설정하는 동작
    // 사용한 경우: `--os-id false` → os_id = false
    // 사용하지 않은 경우: `--os-id false` → false를 입력값으로 받지 않아 오류 발생
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    os_id: bool,
    
    /// OpenSearch 주소
    #[arg(long, default_value = "http://localhost:9200")]
    os_url: String,

    /// OpenSearch 테스트 인덱스 (없으면 자동 생성)
    #[arg(long, default_value = "bench-os")]
    os_index: String,

    /// ClickHouse 주소
    #[arg(long, default_value = "http://localhost:8123")]
    ch_url: String,

    /// ClickHouse 테스트 테이블 (없으면 shire.event_logs와 같은 구조로 자동 생성)
    #[arg(long, default_value = "shire.bench_event_logs")]
    ch_table: String,

    /// ClickHouse 사용자 (생략 시 docker/.env의 CLICKHOUSE_USER)
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default", hide_env_values = true)]
    ch_user: String,

    /// ClickHouse 비밀번호 (생략 시 docker/.env의 CLICKHOUSE_PASSWORD)
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "", hide_env_values = true)]
    ch_password: String,
}

// --help 하단에 출력할 사용 예시와 동작 설명
const HELP_EXTRA: &str = "\
예시:
  # OpenSearch / 총 50만 건 / 배치 1000 / 동시성 4 / _id 지정 (log_id)
  dbbench os 500000 --batch 1000 --concurrency 4

  # OpenSearch / 총 50만 건 / 배치 1000 / 동시성 4 / _id 미지정 (OpenSearch가 임의 생성)
  dbbench os 500000 --batch 1000 --concurrency 4 --os-id false

  # ClickHouse / 총 50만 건 / 배치 10000 / 동시성 4
  dbbench ch 500000 --batch 10000 --concurrency 4

  # OpenSearch / 총 10만 건 / 배치 1000 / 동시성 4 / 지정한 JSON 파일의 doc를 샘플로 사용
  dbbench os 100000 --sample my-doc.json

동작:
  - doc 1건을 복제해 총 저장 건수만큼 전송 (--sample 생략 시 내장 doc SAMPLE_DOC 사용)
  - 복제본마다 event_id와 ingested_at 변경
  - OpenSearch _id / ClickHouse log_id: SHA-256(event_id + \"|\" + ingested_at) (--os-id false면 OpenSearch _id 미지정)
  - doc 생성 시간은 측정 시간에서 제외
  - 인덱스/테이블이 없으면 생성, 기존 것이면 그대로 사용
  - ClickHouse 인증: docker/.env의 CLICKHOUSE_USER / CLICKHOUSE_PASSWORD";

// 복제할 기본 doc: 파이프라인이 실제 저장한 doc 1건 (enrichment + playbook_result 포함)
const SAMPLE_DOC: &str = r#"{"event_id":"evt-13055464","event_type":"file_download","timestamp":"2026-10-09T20:47:39Z","ingested_at":"2026-10-09T20:47:39.540062018Z","src_ip":"144.48.243.18","dst_domain":"cdn-update-service.net","file_sha256":"275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f","file_name":"invoice_2026.exe","file_size_bytes":245760,"path":"/","source_type":"http_server","enrichment_src_ip":{"status":{"status":"Success"},"data":{"abuse_confidence_score":100,"total_reports":3626}},"enrichment_dst_domain":{"status":{"status":"NotFound"},"data":null},"enrichment_file_sha256":{"status":{"status":"Success"},"data":{"malicious":66,"suspicious":0,"harmless":0,"undetected":2,"timeout":0,"confirmed-timeout":0,"failure":0,"type-unsupported":7,"sigma_high":0,"sigma_medium":0,"sigma_critical":0,"sigma_low":1}},"playbook_result":{"action":"DENY","evaluated":2,"processed_at":"2026-10-09T20:49:13Z","playbooks":[{"playbook_id":2,"name":"고위험 IP 차단","action":"DENY","when":"(enrichment_src_ip.data.abuse_confidence_score gte 80)"},{"playbook_id":4,"name":"고위험 실행파일 차단","action":"DENY","when":"(event_type eq file_download) and ((enrichment_file_sha256.data.malicious gte 10) or ((enrichment_src_ip.data.abuse_confidence_score gte 80) and (file_size_bytes gt 100000))) and (file_name ends_with .exe)"}]}}"#;

// OpenSearch 실제 인덱스 템플릿 (빌드 시 포함): 테스트 인덱스를 같은 settings/mappings로 생성
const OS_TEMPLATE: &str = include_str!("../../../docker/opensearch/templates/event-logs.json");

// ClickHouse 실제 테이블: 테스트 테이블을 같은 구조로 생성
const CH_SOURCE_TABLE: &str = "shire.event_logs";

// 요청 1번의 결과
struct BatchResult {
    ok: usize,
    failed: usize,
    error: Option<String>, // 실패 시 첫 오류 내용
}

#[tokio::main]
async fn main() {
    // 인자 없이 실행하면 help 출력 후 종료 (.env 로드 전에 확인해야 env 값이 입력으로 간주되지 않음)
    if std::env::args().len() == 1 {
        Args::command().print_help().unwrap();
        return;
    }
    // docker/.env의 CLICKHOUSE_USER / CLICKHOUSE_PASSWORD 사용 (없으면 무시, 이미 설정된 환경변수가 우선)
    let _ = dotenvy::from_path(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/.env"));
    let args = Args::parse();
    let sample = load_sample(args.sample.as_deref());
    let client = reqwest::Client::new();

    // 0. 테스트 인덱스/테이블 준비 (없으면 생성, 실패 시 종료)
    if let Err(e) = match args.target {
        Target::Os => ensure_os_index(&client, &args).await,
        Target::Ch => ensure_ch_table(&client, &args).await,
    } {
        eprintln!("{e}");
        std::process::exit(1);
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

// doc 1건 생성: event_id, ingested_at만 바꾸고 log_id 계산
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
                // OpenSearch bulk: 액션 줄 + doc 줄
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

// OpenSearch 테스트 인덱스 확인: 있으면 그대로 사용, 없으면 실제 템플릿의 settings/mappings로 생성
// (매핑 없이 자동 생성되면 실제와 저장 구조가 달라 측정이 무의미해짐)
async fn ensure_os_index(client: &reqwest::Client, args: &Args) -> Result<(), String> {
    let url = format!("{}/{}", args.os_url, args.os_index);
    let resp = client.head(&url).send().await.map_err(|e| format!("OpenSearch 연결 실패: {e}"))?;
    match resp.status().as_u16() {
        200 => {
            println!("OpenSearch 인덱스 '{}' 사용 (기존 인덱스, 설정이 다르면 삭제 후 재실행)", args.os_index);
            return Ok(());
        }
        404 => {}
        s => return Err(format!("OpenSearch 인덱스 확인 실패: HTTP {s}")),
    }

    let template: Value = serde_json::from_str(OS_TEMPLATE).map_err(|e| format!("템플릿 파싱 실패: {e}"))?;
    let resp = client
        .put(&url)
        .json(&template["template"]) // index_patterns를 제외한 settings + mappings
        .send()
        .await
        .map_err(|e| format!("OpenSearch 연결 실패: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(format!("OpenSearch 인덱스 생성 실패: {status}: {}", resp.text().await.unwrap_or_default()));
    }
    println!("OpenSearch 인덱스 '{}' 생성 (실제 템플릿 기준)", args.os_index);
    Ok(())
}

// ClickHouse 테스트 테이블 확인: 없으면 실제 테이블과 같은 구조로 생성 (IF NOT EXISTS라 있으면 그대로 사용)
async fn ensure_ch_table(client: &reqwest::Client, args: &Args) -> Result<(), String> {
    let query = format!("CREATE TABLE IF NOT EXISTS {} AS {CH_SOURCE_TABLE}", args.ch_table);
    let resp = client
        .post(format!("{}/", args.ch_url))
        .basic_auth(&args.ch_user, Some(&args.ch_password))
        .body(query)
        .send()
        .await
        .map_err(|e| format!("ClickHouse 연결 실패: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(format!("ClickHouse 테이블 생성 실패: {status}: {}", resp.text().await.unwrap_or_default()));
    }
    println!("ClickHouse 테이블 '{}' 사용 ({CH_SOURCE_TABLE}와 같은 구조)", args.ch_table);
    Ok(())
}

// 복제할 doc 읽기: 파일 지정이 없으면 내장 doc 사용, 검색 결과 전체면 첫 doc의 _source를 꺼냄
fn load_sample(path: Option<&str>) -> Value {
    let (name, text) = match path {
        Some(p) => (p, std::fs::read_to_string(p).unwrap_or_else(|e| panic!("{p} 읽기 실패: {e}"))),
        None => ("SAMPLE_DOC", SAMPLE_DOC.to_string()),
    };
    let value: Value = serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .unwrap_or_else(|e| panic!("{name} JSON 파싱 실패: {e}"));

    let mut doc = match value.pointer("/hits/hits/0/_source") {
        Some(src) => src.clone(),
        None => value,
    };
    assert!(doc.is_object(), "sample doc가 JSON 객체가 아님");

    // 파이프라인과 같게: OpenSearch doc에는 log_id 필드가 없음 (_id로만 사용)
    doc.as_object_mut().unwrap().remove("log_id");
    doc
}

// OpenSearch _bulk 전송: HTTP 200이어도 doc별 실패가 있을 수 있어 응답의 items를 확인
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

    // errors: false → 전부 성공, true → doc별 결과 확인, 그 외(필드 없음 등) → 실패
    match value["errors"].as_bool() {
        Some(false) => return BatchResult { ok: count, failed: 0, error: None },
        Some(true) => {}
        None => {
            return BatchResult { ok: 0, failed: count, error: Some(format!("errors 필드 없음: {text}")) };
        }
    }
    // 일부 doc 실패: doc마다 결과가 하나씩 와야 하므로 개수부터 확인
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