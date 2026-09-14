 // ./loadgen.exe file_download 7100000 15000 1000 --reps 5 --cooldown 10

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(
    about = "Shire Guard 부하테스트 도구",
    version,
    after_help = "\
테스트 타입:
  file_download  파일 다운로드 이벤트
  login_failure  로그인 실패 이벤트
  dns_beacon     DNS beacon 이벤트

반복 테스트:
  --reps 5로 실행하면 같은 프로세스와 HTTP Client를 유지한 채
  Run을 5회 반복합니다. 각 Run의 시작 event_id는 count만큼 증가합니다.
  --cooldown은 Run 사이 대기 시간입니다.

예시:
  # 15,000건을 동시 1,000개 요청으로 1회 실행
  loadgen.exe file_download 7100000 15000 1000

  # 15,000건 × 5회, 동시 1,000개, Run 사이 10초 대기
  loadgen.exe file_download 7100000 15000 1000 --reps 5 --cooldown 10

  # DNS beacon 100,000건을 동시 200개로 전송
  loadgen.exe dns_beacon 1 100000 200

  # 동시성 1로 사실상 순차 처리
  loadgen.exe login_failure 1 100 1
"
)]
struct Args {
    /// 이벤트 타입: file_download, login_failure, dns_beacon
    event_type: String,

    /// 시작 event_id 번호
    start: u64,

    /// Run당 전송 건수
    count: u64,

    /// 동시성(최대 동시 요청 수)
    #[arg(default_value_t = 100)]
    parallel: usize,

    /// Vector 수신 URL
    #[arg(long, default_value = "http://localhost:8081")]
    url: String,

    /// 반복 횟수
    #[arg(long, default_value_t = 1)]
    reps: u32,

    /// 반복 사이 대기 시간(초)
    #[arg(long, default_value_t = 10)]
    cooldown: u64,
}

fn build_payload(event_type: &str, id: u64) -> Value {
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let event_id = format!("evt-{id}");

    match event_type {
        "file_download" => json!({
            "event_id": event_id,
            "event_type": "file_download",
            "timestamp": timestamp,
            "source_ip": "185.220.101.45",
            "destination_domain": "cdn-update-service.net",
            "file_name": "invoice_2026.exe",
            "file_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "file_size_bytes": 245760
        }),
        "login_failure" => json!({
            "event_id": event_id,
            "event_type": "login_failure",
            "timestamp": timestamp,
            "source_ip": "185.220.101.45",
            "username": "admin",
            "attempt_count": 12,
            "target_service": "ssh"
        }),
        "dns_beacon" => json!({
            "event_id": event_id,
            "event_type": "dns_beacon",
            "timestamp": timestamp,
            "source_ip": "198.51.100.77",
            "destination_domain": "telemetry-sync.info",
            "hostname": "WKS-1183",
            "request_interval_seconds": 60
        }),
        other => panic!("알 수 없는 event_type: {other}"),
    }
}

async fn run_once(
    client: &Client,
    semaphore: Arc<Semaphore>,
    args: &Args,
    start: u64,
) -> (usize, usize, f64) {
    let end = start + args.count - 1;

    println!(
        "── Run: evt-{} ~ evt-{} (총 {}건, 동시 {}) ──",
        start, end, args.count, args.parallel
    );

    let success = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.count as usize);

    for id in start..=end {
        let client = client.clone();
        let semaphore = semaphore.clone();
        let event_type = args.event_type.clone();
        let url = args.url.clone();
        let success = success.clone();
        let fail = fail.clone();

        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let payload = build_payload(&event_type, id);

            match client.post(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    success.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let success_count = success.load(Ordering::Relaxed);
    let fail_count = fail.load(Ordering::Relaxed);

    println!(
        "▶ 완료: 성공 {} / 실패 {} (총 {}건, {:.2}초, {:.1} req/s)",
        success_count,
        fail_count,
        args.count,
        elapsed,
        args.count as f64 / elapsed
    );

    (success_count, fail_count, elapsed)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if !["file_download", "login_failure", "dns_beacon"].contains(&args.event_type.as_str()) {
        eprintln!("알 수 없는 event_type: {}", args.event_type);
        eprintln!("사용 가능: file_download, login_failure, dns_beacon");
        std::process::exit(1);
    }

    // 중요:
    // Client와 connection pool을 전체 반복 테스트 동안 하나만 유지한다.
    let client = Client::new();
    let semaphore = Arc::new(Semaphore::new(args.parallel));

    let mut total_success = 0usize;
    let mut total_fail = 0usize;
    let mut total_time = 0.0f64;

    for run in 0..args.reps {
        let run_start = args.start + run as u64 * args.count;

        let (success, fail, elapsed) =
            run_once(&client, semaphore.clone(), &args, run_start).await;

        total_success += success;
        total_fail += fail;
        total_time += elapsed;

        if run + 1 < args.reps {
            println!("-- 쿨다운 {}초 대기 --", args.cooldown);
            tokio::time::sleep(Duration::from_secs(args.cooldown)).await;
        }
    }

    let avg_time = total_time / args.reps as f64;
    let avg_success = total_success as f64 / args.reps as f64;
    let avg_fail = total_fail as f64 / args.reps as f64;

    println!();
    println!(
        "=== 결과 요약 ({}회 반복, 동시성 {}) ===",
        args.reps, args.parallel
    );
    println!("평균 성공: {:.1} / 평균 실패: {:.1}", avg_success, avg_fail);
    println!("평균 처리시간: {:.2}초", avg_time);
}