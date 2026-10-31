// loadgen: Vector(HTTP 수신)로 테스트 이벤트를 대량 전송하는 부하 테스트 도구
// 사용법과 옵션 설명은 `loadgen --help` 참고 (HELP_EXTRA)

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
// Parser: 터미널 입력값을 파싱해 구조체의 각 필드에 담음
// ValueEnum: enum의 각 항목을 터미널에서 입력할 수 있는 선택지로 사용
use clap::{Parser, ValueEnum};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

// ── 고정 IOC 값 ─────────────────────────────────────
// 실제 AbuseIPDB / VirusTotal 조회로 결과를 확인해 둔 값만 사용 (조회 결과가 항상 같아야 판정을 예측할 수 있음)

// AbuseIPDB 위험도 100점 IP / 0점 IP
const MALICIOUS_IP: &str = "144.48.243.18";
const SAFE_IP: &str = "118.25.6.39";

// VirusTotal 악성 판정 66개(EICAR 테스트 파일) 해시 / 악성 판정 0개(빈 파일) 해시
const MALICIOUS_SHA256: &str = "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f";
const SAFE_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// dns_beacon 도메인
// 위험: 차단 도메인 플레이북에 등록된 도메인 (.example은 테스트용 예약 도메인이라 실제로 존재하지 않음)
// 안전: 실제 존재하는 일반 도메인
const MALICIOUS_DOMAIN: &str = "c2.devil.example";
const SAFE_DOMAIN: &str = "nexon.com";

// file_download의 다운로드 출처 도메인 (실제로 존재하지 않는 도메인이라 DNS 조회 결과는 항상 NotFound)
const DOWNLOAD_DOMAIN: &str = "cdn-update-service.net";

// event_id / loadgen_id 앞에 붙는 값 (실제 로그와 섞여도 테스트 이벤트를 구분하기 위함)
const ID_PREFIX: &str = "loadgen";

// 거부 이벤트에 넣을 필드: 템플릿(docker/opensearch/templates/event-logs.json)에 객체로 정의된 필드에 문자열을 넣어
// OpenSearch가 doc을 400(mapper_parsing_exception)으로 거부하게 함
// 매핑에 이미 있는 필드라 새 필드를 만들지 않으므로 매핑 갱신 부담이 없음
// (거부 이벤트는 login_failure 기반이라 도메인이 없어 Backend가 이 필드를 만들지 않으므로 보낸 값이 그대로 OpenSearch로 감)
const REJECT_FIELD: &str = "enrichment_dst_domain";

// Backend /health 포트 (Backend main.rs에 3001로 고정되어 있음)
const BACKEND_PORT: u16 = 3001;

// Vector 수신 주소 기본값 (인자 방식은 --url로 변경 가능, 대화형은 이 값 사용)
const DEFAULT_URL: &str = "http://localhost:8081";

// 실행 가능한 최대 번호 (event_id 번호 계산이 u64 범위를 넘지 않도록 제한)
const MAX_NUMBER: u64 = 1_000_000_000_000_000;

// ── 재전송 설정 ──
// Vector가 Kafka로 전달하지 못했다고 응답하는 등 일시적인 오류면 같은 이벤트(같은 event_id)를 다시 보냄
const RETRY_LIMIT: Duration = Duration::from_secs(90); // 이벤트 1건당 재전송을 시도하는 최대 시간
const RETRY_FIRST_DELAY: Duration = Duration::from_millis(500); // 첫 재전송 전 대기 시간 (이후 2배씩 증가)
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5); // 재전송 전 대기 시간의 최대값

// --help 하단에 출력할 설명과 사용 예시
const HELP_EXTRA: &str = "\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 [ 대화형 설정 / 결과 확인 ] ./loadgen.exe setup | check
   setup              질문에 답하며 이벤트 종류 / 건수 / 동시성 등을 설정하고 실행 (여러 이벤트 타입을 섞어서 전송 가능)
   check              저장소별 결과 확인 방법 출력 (웹 주소, 확인 명령), 기대값은 전송 후 출력되는 값과 비교

 [ 이벤트 타입 ]
   file_download      파일 다운로드
   login_failure      로그인 실패
   dns_beacon         DNS Beacon 이벤트

 [ 케이스 ] --case
   safe               안전 IOC: AbuseIPDB 0점 IP / VirusTotal 악성 0개 해시 / 도메인 nexon.com
   malicious (기본값) 위험 IOC: AbuseIPDB 100점 IP / VirusTotal 악성 66개 해시 / 플레이북 차단 도메인 c2.devil.example

 [ event_id ]
   loadgen-<번호>     START_ID부터 1씩 증가 (이전 테스트와 번호가 겹치지 않게 지정)

 [ 실패 / 거부 이벤트 ] 정상 이벤트 사이에 섞어서 전송
   --fail N           event_id 없음 → Backend 파싱 실패 → Postgres storage_failures 저장
                      추적용 loadgen_id 필드 포함 (loadgen-failure-<번호>)
   --reject N         객체 필드(enrichment_dst_domain)에 문자열 \"x\" → OpenSearch가 타입 불일치로 거부 → storage_failures 저장
                      event_id는 loadgen-reject-<번호>, OpenSearch / ClickHouse에는 저장 안 됨

 [ 다수결 테스트 ] --votes
   --votes D1,D2,H1   test_votes 필드 추가 → 다수결 테스트용 플레이북([TEST] D1~T2)이 자기 이름이 포함되면 판정
                      D1, D2 → DENY  /  H1, H2 → HOLD  /  T1, T2 → DETECT
                      다른 플레이북이 함께 판정하지 않도록 login_failure --case safe로 전송

 [ 반복 ] --reps, --cooldown
   --reps N           같은 설정을 N회 반복해 평균을 냄 (회차마다 event_id 번호는 이어서 증가)
   --cooldown 초      회차 사이 대기 시간

 [ 출력 ] -v, --verbose
   -v                 설정 / 예상 결과 / 서비스 상태 / 결과 요약 / 기대값까지 자세히 출력

 [ 재전송 ]
   Vector가 Kafka 전달 실패(HTTP 400 failed to deliver)나 일시적 오류(408, 429, 5xx)로 응답하거나
   Vector에 연결하지 못하면, 같은 이벤트를 최대 90초 동안 다시 전송 (0.5초부터 2배씩, 최대 5초 간격)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 [ 예시 ]
   # file_download / 위험 / 총 15000건 / 동시성 1000
   ./loadgen.exe file_download 1 15000 1000

   # login_failure / 안전 / 총 100건 / 동시성 1 (순차 전송)
   ./loadgen.exe login_failure 15001 100 1 --case safe

   # file_download / 위험 / 총 10000건 + 실패 10건 + 거부 10건 / 동시성 100 (기본값)
   ./loadgen.exe file_download 20001 10000 --fail 10 --reject 10

   # 다수결: login_failure / 안전 / 총 10건 / DENY 2 + HOLD 1 → 최종 DENY 기대
   ./loadgen.exe login_failure 40001 10 10 --case safe --votes D1,D2,H1

   # 반복: file_download / 위험 / 총 15000건 / 동시성 1000 / 5회 / 회차 사이 10초 대기
   ./loadgen.exe file_download 50001 15000 1000 --reps 5 --cooldown 10

   # 자세한 출력: file_download / 위험 / 총 15000건 / 동시성 1000
   ./loadgen.exe file_download 60001 15000 1000 -v

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 [ 참고 ]
   • 성공 건수는 Vector가 요청을 받았다는 뜻이며, 저장까지 끝났는지는 ./loadgen.exe check의 확인 방법으로 확인
   • 서비스 주소(포트)는 docker/.env 값을 사용하고, 없으면 기본 포트 사용";

// 이벤트 타입
// #[value(name)]: 터미널에서 입력하는 이름 지정 (지정하지 않으면 clap이 file-download처럼 하이픈으로 바꿈)
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum EventType {
    #[value(name = "file_download")]
    FileDownload,
    #[value(name = "login_failure")]
    LoginFailure,
    #[value(name = "dns_beacon")]
    DnsBeacon,
}

impl EventType {
    // 이벤트의 event_type 필드에 들어갈 값
    fn as_str(self) -> &'static str {
        match self {
            EventType::FileDownload => "file_download",
            EventType::LoginFailure => "login_failure",
            EventType::DnsBeacon => "dns_beacon",
        }
    }
}

// 테스트 케이스: 이벤트에 넣을 IOC(IP, 해시, 도메인)를 위험한 값으로 할지 안전한 값으로 할지 선택
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Safe,
    Malicious,
}

impl Case {
    // 화면 출력용 이름
    fn label(self) -> &'static str {
        match self {
            Case::Safe => "안전",
            Case::Malicious => "위험",
        }
    }
}

// 보낼 이벤트의 종류
// Normal: 정상적으로 처리되는 이벤트 (event_id 있음)
// Failure: Backend에서 파싱에 실패하도록 만든 이벤트 (event_id 없음)
// Reject: OpenSearch가 저장을 거부하도록 만든 이벤트 (객체 필드에 문자열)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Normal(EventType),
    Failure,
    Reject,
}

// 보낼 이벤트 묶음 1개: 어떤 이벤트를, 어떤 케이스로, 몇 건 보낼지
// 예: file_download / 위험 / 10000건
#[derive(Clone, Debug)]
struct EventSpec {
    kind: Kind,
    case: Case,
    count: u64,
    votes: Option<String>, // 다수결 테스트용 test_votes 값 (예: "D1,D2,H1"), 없으면 필드를 넣지 않음
}

impl EventSpec {
    // 화면 출력용 한 줄 설명 (예: "file_download / 위험 / 10,000건")
    fn describe(&self) -> String {
        let name = match self.kind {
            Kind::Normal(t) => t.as_str().to_string(),
            Kind::Failure => "실패 이벤트 (event_id 없음)".to_string(),
            Kind::Reject => format!("거부 이벤트 ({REJECT_FIELD}에 문자열)"),
        };
        let votes = match &self.votes {
            Some(v) => format!(" / 다수결 {v}"),
            None => String::new(),
        };
        // 실패/거부 이벤트는 저장되지 않으므로 케이스를 표시하지 않음
        match self.kind {
            Kind::Normal(_) => format!("{name} / {} / {}건{votes}", self.case.label(), with_commas(self.count)),
            Kind::Failure | Kind::Reject => format!("{name} / {}건", with_commas(self.count)),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    about = "Shire Guard 부하 테스트 도구 (Vector로 테스트 이벤트를 대량 전송)",
    after_help = HELP_EXTRA,
    arg_required_else_help = true, // 인자 없이 실행하면 help 출력
    disable_help_flag = true       // clap 기본 -h, --help(영어 설명)를 끄고 아래 help 필드로 한글 설명 사용
)]
struct Args {
    /// 이벤트 타입
    event_type: EventType,

    /// 시작 번호: 첫 이벤트의 event_id가 loadgen-<START_ID>가 되고 1씩 증가
    #[arg(value_name = "START_ID")]
    start_id: u64,

    /// 회차당 전송 건수 (--fail, --reject로 추가한 이벤트 제외)
    count: u64,

    /// 동시성: 동시에 보낼 최대 요청 수
    #[arg(default_value_t = 100)]
    parallel: usize,

    /// Vector 수신 주소
    #[arg(long, default_value = DEFAULT_URL)]
    url: String,

    /// 테스트 케이스: safe(안전) 또는 malicious(위험)
    #[arg(long, value_enum, default_value_t = Case::Malicious)]
    case: Case,

    /// 회차당 함께 보낼 실패 이벤트 수 (event_id 없음 → storage_failures 저장)
    #[arg(long, default_value_t = 0)]
    fail: u64,

    /// 회차당 함께 보낼 거부 이벤트 수 (객체 필드에 문자열 → OpenSearch 거부 → storage_failures 저장)
    #[arg(long, default_value_t = 0)]
    reject: u64,

    /// 다수결 테스트용 test_votes 값 (예: D1,D2,H1)
    #[arg(long)]
    votes: Option<String>,

    /// 반복 횟수
    #[arg(long, default_value_t = 1)]
    reps: u32,

    /// 반복 사이 대기 시간(초)
    #[arg(long, default_value_t = 10)]
    cooldown: u64,

    /// 자세히 출력
    #[arg(short, long)]
    verbose: bool,

    /// 도움말 출력
    // ArgAction::Help: 이 옵션이 입력되면 help를 출력하고 종료 (값은 사용하지 않으므로 dead_code 경고 끔)
    #[arg(short, long, action = clap::ArgAction::Help)]
    #[allow(dead_code)]
    help: Option<bool>,
}

// 전송 설정 (인자 방식, 대화형 모두 이 값으로 실행)
struct RunConfig {
    specs: Vec<EventSpec>, // 보낼 이벤트 묶음 목록
    start_id: u64,         // 첫 이벤트 번호
    parallel: usize,       // 동시성
    url: String,           // Vector 수신 주소
    reps: u32,             // 반복 횟수
    cooldown: u64,         // 반복 사이 대기 시간(초)
    verbose: bool,         // true: 자세히 출력 / false: 회차마다 결과 1줄
}

impl RunConfig {
    // 회차당 전송 건수 (모든 묶음의 합)
    fn total_per_run(&self) -> u64 {
        self.specs.iter().map(|s| s.count).sum()
    }

    // 간단 출력에서 실행을 구분하는 이름: 묶음 종류를 +로 연결 (예: file_download+fail+reject)
    // 실패 / 거부 이벤트는 인자 옵션 이름(--fail, --reject)과 같게 표시
    fn label(&self) -> String {
        self.specs
            .iter()
            .map(|s| match s.kind {
                Kind::Normal(t) => t.as_str(),
                Kind::Failure => "fail",
                Kind::Reject => "reject",
            })
            .collect::<Vec<_>>()
            .join("+")
    }
}

// 회차 1번의 결과
struct RunResult {
    success: u64,
    fail: u64,                   // 재전송까지 마친 뒤에도 실패한 이벤트 수 (최종 실패)
    retried: u64,                // 한 번 이상 재전송한 이벤트 수
    elapsed: f64,                // 소요 시간(초)
    first_error: Option<String>, // 첫 번째 실패의 원인 (실패가 없으면 None)
}

// ── 이벤트 생성 ─────────────────────────────────────

// 이벤트 타입별 기본 필드 (event_id는 넣지 않음, 호출하는 쪽에서 종류에 맞게 추가)
// 필드 이름은 보안 장비가 보내는 원본 형식 (source_ip, destination_domain은 Vector가 src_ip, dst_domain으로 바꿈)
fn base_fields(event_type: EventType, case: Case) -> Value {
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(); // 예: 2026-10-23T10:00:00Z
    let source_ip = match case {
        Case::Safe => SAFE_IP,
        Case::Malicious => MALICIOUS_IP,
    };

    match event_type {
        EventType::FileDownload => {
            let file_sha256 = match case {
                Case::Safe => SAFE_SHA256,
                Case::Malicious => MALICIOUS_SHA256,
            };
            json!({
                "event_type": "file_download",
                "timestamp": timestamp,
                "source_ip": source_ip,
                "destination_domain": DOWNLOAD_DOMAIN,
                "file_name": "invoice_2026.exe",
                "file_sha256": file_sha256,
                "file_size_bytes": 245760
            })
        }
        EventType::LoginFailure => json!({
            "event_type": "login_failure",
            "timestamp": timestamp,
            "source_ip": source_ip,
            "username": "admin",
            "attempt_count": 12,
            "target_service": "ssh"
        }),
        EventType::DnsBeacon => {
            let domain = match case {
                Case::Safe => SAFE_DOMAIN,
                Case::Malicious => MALICIOUS_DOMAIN,
            };
            json!({
                "event_type": "dns_beacon",
                "timestamp": timestamp,
                "source_ip": source_ip,
                "destination_domain": domain,
                "hostname": "WKS-1183",
                "request_interval_seconds": 60
            })
        }
    }
}

// 전송할 이벤트 1건 생성
// number: 이번 이벤트의 번호 (event_id, loadgen_id에 사용)
fn build_payload(spec: &EventSpec, number: u64) -> Value {
    match spec.kind {
        // 정상 이벤트: 기본 필드 + event_id (+ 다수결 값)
        Kind::Normal(event_type) => {
            let mut payload = with_id_first("event_id", format!("{ID_PREFIX}-{number}"), base_fields(event_type, spec.case));
            if let Some(votes) = &spec.votes {
                payload["test_votes"] = json!(votes);
            }
            payload
        }
        // 실패 이벤트: login_failure 기본 필드 + 추적용 loadgen_id (event_id가 없어 Backend에서 파싱 실패)
        Kind::Failure => with_id_first(
            "loadgen_id",
            format!("{ID_PREFIX}-failure-{number}"),
            base_fields(EventType::LoginFailure, spec.case),
        ),
        // 거부 이벤트: login_failure 기본 필드 + event_id + 객체 필드에 문자열 (OpenSearch가 타입 불일치로 거부)
        Kind::Reject => {
            let mut payload = with_id_first(
                "event_id",
                format!("{ID_PREFIX}-reject-{number}"),
                base_fields(EventType::LoginFailure, spec.case),
            );
            payload[REJECT_FIELD] = json!("x");
            payload
        }
    }
}

// ID 필드를 맨 앞에 두고 나머지 필드를 이어 붙인 이벤트 생성
// serde_json의 preserve_order 기능을 켜 두어 필드가 넣은 순서대로 유지됨 (화면에 JSON 예시를 보여줄 때 ID가 먼저 보이도록)
fn with_id_first(key: &str, id: String, fields: Value) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(key.to_string(), json!(id));
    if let Value::Object(rest) = fields {
        map.extend(rest);
    }
    Value::Object(map)
}

// ── 예상 결과 (docker/postgres/init/002_seed.sql의 플레이북 기준) ─────────────

// 플레이북 판정
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Verdict {
    Deny,
    Hold,
    Detect,
    NoDecision, // 판정을 낸 플레이북이 없음 (저장된 doc에 playbook_result 필드가 없음)
}

impl Verdict {
    // 화면 출력용 이름
    fn label(self) -> &'static str {
        match self {
            Verdict::Deny => "DENY",
            Verdict::Hold => "HOLD",
            Verdict::Detect => "DETECT",
            Verdict::NoDecision => "판정 없음",
        }
    }
}

// 이벤트 1건이 어떻게 처리될지 예상한 결과
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expected {
    Stored { verdict: Verdict, decisions: usize }, // OpenSearch / ClickHouse에 저장 (decisions: 판정을 낸 플레이북 수)
    ParseFailure,                                   // Backend 파싱 실패 → storage_failures
    OsReject,                                       // OpenSearch 거부 → storage_failures
}

// 다수결 테스트용 플레이북: test_votes에 이름이 포함되면 해당 판정을 냄
const VOTE_PLAYBOOKS: [(&str, Verdict); 6] = [
    ("D1", Verdict::Deny),
    ("D2", Verdict::Deny),
    ("H1", Verdict::Hold),
    ("H2", Verdict::Hold),
    ("T1", Verdict::Detect),
    ("T2", Verdict::Detect),
];

// 이벤트 묶음 1개의 예상 결과 계산
// seed 플레이북 중 loadgen 이벤트에 걸리는 것:
//   2번 고위험 IP 차단          : 위험 IP(AbuseIPDB 100점)면 이벤트 종류와 관계없이 DENY
//   4번 고위험 실행파일 차단    : file_download + 위험 해시(악성 66개) + .exe → DENY
//   5번 차단 도메인 비컨 차단   : dns_beacon + 차단 도메인 → DENY
//   6~11번 [TEST] D1~T2         : test_votes에 이름이 포함되면 판정
// 1번(조회 실패 보류)은 외부 조회 오류 시에만 걸리고, 3번(AI 분석)은 비활성이며 loadgen 해시(악성 0개 / 66개)는 조건(1~9개)에 해당하지 않음
fn expected(spec: &EventSpec) -> Expected {
    let event_type = match spec.kind {
        Kind::Normal(t) => t,
        Kind::Failure => return Expected::ParseFailure,
        Kind::Reject => return Expected::OsReject,
    };

    let malicious = spec.case == Case::Malicious;
    let mut decisions = Vec::new();
    if malicious {
        decisions.push(Verdict::Deny); // 2번
    }
    if malicious && event_type == EventType::FileDownload {
        decisions.push(Verdict::Deny); // 4번
    }
    if malicious && event_type == EventType::DnsBeacon {
        decisions.push(Verdict::Deny); // 5번
    }
    if let Some(votes) = &spec.votes {
        for (name, verdict) in VOTE_PLAYBOOKS {
            if votes.contains(name) {
                decisions.push(verdict);
            }
        }
    }

    Expected::Stored { verdict: majority(&decisions), decisions: decisions.len() }
}

// 다수결 집계 (Backend engine.rs의 majority_vote와 같은 규칙)
// DENY가 과반이면 DENY, DETECT가 과반이면 DETECT, 그 외(과반 없음, 동률)는 HOLD, 판정이 하나도 없으면 판정 없음
fn majority(decisions: &[Verdict]) -> Verdict {
    if decisions.is_empty() {
        return Verdict::NoDecision;
    }
    let total = decisions.len();
    let deny = decisions.iter().filter(|&&v| v == Verdict::Deny).count();
    let detect = decisions.iter().filter(|&&v| v == Verdict::Detect).count();
    if deny * 2 > total {
        Verdict::Deny
    } else if detect * 2 > total {
        Verdict::Detect
    } else {
        Verdict::Hold
    }
}

// 예상 결과를 화면 출력용 문장으로 변환
fn describe_expected(expected: Expected) -> String {
    match expected {
        Expected::Stored { verdict: Verdict::NoDecision, .. } => "저장 / 판정 없음".to_string(),
        Expected::Stored { verdict, decisions } => format!("저장 / {} (판정한 플레이북 {decisions}개)", verdict.label()),
        Expected::ParseFailure => "storage_failures 저장 (Backend 파싱 실패)".to_string(),
        Expected::OsReject => "storage_failures 저장 (OpenSearch 거부, OpenSearch / ClickHouse 저장 안 됨)".to_string(),
    }
}

// 전체 실행(반복 포함) 기준 기대 건수
struct ExpectedTotals {
    stored: u64,                        // OpenSearch / ClickHouse에 저장될 건수
    by_verdict: BTreeMap<Verdict, u64>, // 저장될 이벤트의 판정별 건수
    parse_failure: u64,                 // storage_failures 파싱 실패 건수
    os_reject: u64,                     // storage_failures OpenSearch 거부 건수
}

fn expected_totals(config: &RunConfig) -> ExpectedTotals {
    let mut totals = ExpectedTotals { stored: 0, by_verdict: BTreeMap::new(), parse_failure: 0, os_reject: 0 };
    for spec in &config.specs {
        let count = spec.count * config.reps as u64;
        match expected(spec) {
            Expected::Stored { verdict, .. } => {
                totals.stored += count;
                *totals.by_verdict.entry(verdict).or_insert(0) += count;
            }
            Expected::ParseFailure => totals.parse_failure += count,
            Expected::OsReject => totals.os_reject += count,
        }
    }
    totals
}

// ── 서비스 주소 / 상태 확인 ─────────────────────────────────────

// 서비스 포트 (docker/.env 값, 없으면 docker/.env.example의 기본값)
struct Ports {
    vector_api: u16,
    opensearch: u16,
    opensearch_dashboards: u16,
    clickhouse: u16,
    postgres: u16,
}

fn load_ports() -> Ports {
    // docker/.env를 환경변수로 읽음 (파일이 없으면 무시, 이미 설정된 환경변수가 우선)
    // CARGO_MANIFEST_DIR: 빌드할 때 scripts/loadgen의 절대경로로 고정되므로 실행 위치와 관계없이 같은 파일을 읽음
    let _ = dotenvy::from_path(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/.env"));
    let port = |name: &str, default: u16| {
        std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
    };
    Ports {
        vector_api: port("VECTOR_API_PORT", 8686),
        opensearch: port("OPENSEARCH_PORT", 9200),
        opensearch_dashboards: port("OPENSEARCH_DASHBOARDS_PORT", 5601),
        clickhouse: port("CLICKHOUSE_HTTP_PORT", 8123),
        postgres: port("POSTGRES_PORT", 5432),
    }
}

// 서비스 상태 확인 결과를 출력 (꺼진 서비스가 있어도 전송은 막지 않음)
// 꺼진 서비스가 있으면 전송은 성공으로 나와도 저장이 안 되거나 확인 명령이 실패함
async fn check_services(client: &Client, ports: &Ports) {
    const TIMEOUT: Duration = Duration::from_secs(2);
    let http_checks = [
        ("Vector", format!("http://localhost:{}/health", ports.vector_api)),
        ("Backend", format!("http://localhost:{BACKEND_PORT}/health")),
        ("OpenSearch", format!("http://localhost:{}", ports.opensearch)),
        ("ClickHouse", format!("http://localhost:{}/ping", ports.clickhouse)),
    ];

    box_top("서비스 상태");
    let mut all_ok = true;
    for (name, url) in http_checks {
        let ok = match client.get(&url).timeout(TIMEOUT).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        };
        all_ok &= ok;
        box_line(&format!("{} {name:<10} {url}", if ok { "✓" } else { "✗" }));
    }

    // Postgres는 HTTP를 제공하지 않으므로 포트에 연결되는지만 확인
    let pg_addr = format!("localhost:{}", ports.postgres);
    let pg_ok = matches!(tokio::time::timeout(TIMEOUT, TcpStream::connect(&pg_addr)).await, Ok(Ok(_)));
    all_ok &= pg_ok;
    box_line(&format!("{} {:<10} {pg_addr} (포트 연결만 확인)", if pg_ok { "✓" } else { "✗" }, "Postgres"));

    if !all_ok {
        box_line("⚠ 꺼진 서비스가 있음: 전송은 성공으로 나와도 저장되지 않거나 확인 명령이 실패할 수 있음");
    }
    box_bottom();
    println!();
}

// 기대값 요약 출력 (실행 전 확인 화면, 실행 후 결과 요약에서 사용)
// 확인 명령은 매번 같으므로 여기서는 이번 실행에서 달라지는 기대값만 보여주고, 확인 방법은 ./loadgen.exe check로 안내
fn print_expected_summary(config: &RunConfig) {
    let totals = expected_totals(config);
    let first = config.start_id;
    let last = config.start_id + config.reps as u64 * config.total_per_run() - 1;

    box_top(&format!("기대값 (이번 실행 번호 {first} ~ {last})"));
    if totals.stored > 0 {
        // 판정별 기대 건수 (예: "DENY 10,000 / 판정 없음 100")
        let verdicts = totals
            .by_verdict
            .iter()
            .map(|(v, c)| format!("{} {}", v.label(), with_commas(*c)))
            .collect::<Vec<_>>()
            .join(" / ");
        box_line(&format!("OpenSearch / ClickHouse : {}건 ({verdicts})", with_commas(totals.stored)));
    }
    if totals.parse_failure > 0 || totals.os_reject > 0 {
        let mut parts = Vec::new();
        if totals.parse_failure > 0 {
            parts.push(format!("failure {}", with_commas(totals.parse_failure)));
        }
        if totals.os_reject > 0 {
            parts.push(format!("reject {}", with_commas(totals.os_reject)));
        }
        box_line(&format!("storage_failures        : {}", parts.join(" / ")));
    }
    box_line("");
    box_line("※ 이전 실행과 번호가 겹치면 이전 실행분까지 합산되어 기대값보다 크게 나옴");
    box_line("※ 최종 실패가 있으면 그 건수만큼 기대값에서 빼고 비교 (재전송은 같은 event_id로 보내므로 중복 저장 여부는 고유 event_id 수로 확인)");
    box_line("※ seed 플레이북 기준이며, 외부 조회(AbuseIPDB, VirusTotal, DNS) 오류 시 HOLD가 추가되어 달라질 수 있음");
    box_bottom();
}

// 결과 확인 방법 출력 (./loadgen.exe check)
// 웹 화면 주소와 확인 명령을 안내 (명령은 복사해서 그대로 실행할 수 있도록 들여쓰기 없이 출력)
// 모든 loadgen 이벤트(event_id가 loadgen-으로 시작)를 대상으로 세므로, 이전 실행분까지 합산된 값이 나옴
fn print_check_guide(ports: &Ports) {
    print_banner("결과 확인 방법");
    println!("• Backend 처리가 끝난 뒤 확인 (건수가 기대값보다 적으면 잠시 후 다시 확인)");
    println!("• 기대값은 전송 후 출력된 기대값과 비교 (이전 실행분까지 합산된 값이 나옴)");
    println!("• ClickHouse / Postgres 명령은 마지막 SQL 줄까지 함께 복사해서 실행");

    print_step("[웹에서 확인]");
    println!(
        "OpenSearch Dashboards : http://localhost:{}  (Discover → 인덱스 패턴 event-logs-* → 검색창에 event_id:{ID_PREFIX}-*)",
        ports.opensearch_dashboards
    );
    println!(
        "ClickHouse            : http://localhost:{}/play  (docker/.env의 CLICKHOUSE_USER / CLICKHOUSE_PASSWORD로 로그인 후 아래 SQL 실행)",
        ports.clickhouse
    );

    print_step("[OpenSearch] 저장된 이벤트 수와 판정별 건수 (판정 없음은 NONE)");
    println!("curl -s -X POST \"http://localhost:{}/event-logs-*/_refresh\" > /dev/null", ports.opensearch);
    println!(
        "curl -s \"http://localhost:{}/event-logs-*/_search?size=0&filter_path=hits.total.value,aggregations.action.buckets\" -H \"Content-Type: application/json\" -d '{{\"track_total_hits\":true,\"query\":{{\"prefix\":{{\"event_id\":\"{ID_PREFIX}-\"}}}},\"aggs\":{{\"action\":{{\"terms\":{{\"field\":\"playbook_result.action\",\"missing\":\"NONE\"}}}}}}}}'",
        ports.opensearch
    );

    print_step("[ClickHouse] 판정별 건수 (판정 없음은 \\N, FINAL: 중복 저장분을 하나로 합쳐서 계산)");
    println!("docker exec -i clickhouse sh -c 'clickhouse-client -u \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\"' <<'SQL'");
    println!("SELECT payload.playbook_result.action AS action, count() AS count");
    println!("FROM shire.event_logs FINAL");
    println!("WHERE startsWith(payload.event_id, '{ID_PREFIX}-')");
    println!("GROUP BY action ORDER BY action");
    println!("SQL");

    print_step("[Postgres] storage_failures 건수 (failure: Backend 파싱 실패 / reject: OpenSearch 거부)");
    println!("docker exec -i postgres sh -c 'psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\"' <<'SQL'");
    println!("SELECT CASE WHEN t LIKE '%{ID_PREFIX}-failure-%' THEN 'failure' ELSE 'reject' END AS kind, count(*)");
    println!("FROM (SELECT bytea_to_text_safe(payload) AS t FROM storage_failures) s");
    println!("WHERE t LIKE '%{ID_PREFIX}-failure-%' OR t LIKE '%{ID_PREFIX}-reject-%'");
    println!("GROUP BY kind ORDER BY kind;");
    println!("SQL");
}

// 여러 이벤트 묶음을 고르게 섞는 순서 생성기 (Smooth Weighted Round Robin)
// 건수가 많은 묶음은 자주, 적은 묶음은 드물게 나오도록 섞되, 한 바퀴(총 건수)를 돌면 각 묶음이 정확히 자기 건수만큼 나옴
// 예: A 3건, B 1건 → A A B A 처럼 한쪽에 몰리지 않게 섞임
// 순서를 미리 만들어 두지 않고 1건씩 계산하므로 100만 건이어도 메모리를 거의 쓰지 않음
struct Interleaver {
    weights: Vec<i128>, // 묶음별 건수
    current: Vec<i128>, // 묶음별 누적 점수 (점수가 가장 높은 묶음이 다음 순서)
    total: i128,        // 전체 건수
}

impl Interleaver {
    fn new(counts: &[u64]) -> Self {
        let weights: Vec<i128> = counts.iter().map(|&c| c as i128).collect();
        let total = weights.iter().sum();
        Self { current: vec![0; weights.len()], weights, total }
    }

    // 다음에 보낼 묶음의 위치(specs의 index) 반환
    // 1. 모든 묶음의 점수에 자기 건수를 더함
    // 2. 점수가 가장 높은 묶음을 선택 (같으면 앞쪽 묶음)
    // 3. 선택한 묶음의 점수에서 전체 건수를 뺌 → 방금 나온 묶음은 한동안 뒤로 밀림
    fn next(&mut self) -> usize {
        let mut picked = 0;
        for i in 0..self.weights.len() {
            self.current[i] += self.weights[i];
            if self.current[i] > self.current[picked] {
                picked = i;
            }
        }
        self.current[picked] -= self.total;
        picked
    }
}

// ── 전송 ─────────────────────────────────────

// 요청 1번의 실패 종류
enum SendError {
    Retry(String), // 다시 보내면 성공할 수 있는 일시적 오류
    Fail(String),  // 다시 보내도 같은 결과이거나, 다시 보내면 중복될 수 있는 오류 → 바로 최종 실패
}

// 다시 보낼 HTTP 오류인지 판단
// - 400 + "failed to deliver": Vector가 Kafka로 전달하지 못함 (Kafka 장애 중 실제로 받은 응답: Contents failed to deliver to sink)
// - 408 / 429 / 5xx: HTTP 표준에서 일시적인 오류를 뜻하는 상태 코드
// 그 외 4xx(잘못된 JSON 등)는 다시 보내도 같은 결과이므로 제외
fn is_retryable_status(code: u16, body: &str) -> bool {
    (code == 400 && body.contains("failed to deliver")) || code == 408 || code == 429 || (500..600).contains(&code)
}

// 요청 1번 전송
async fn send_once(client: &Client, url: &str, payload: &Value) -> Result<(), SendError> {
    match client.post(url).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        // HTTP 응답은 받았지만 오류 상태
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let msg = format!("HTTP {status}: {body}");
            if is_retryable_status(status.as_u16(), &body) { Err(SendError::Retry(msg)) } else { Err(SendError::Fail(msg)) }
        }
        // 연결 단계에서 실패 (예: Vector 재시작 중 연결 거부): 요청이 Vector에 도착하지 않았으므로 다시 보내도 중복되지 않음
        Err(e) if e.is_connect() => Err(SendError::Retry(format!("연결 실패: {e}"))),
        // 요청을 보낸 뒤 실패 (예: 응답 전에 연결이 끊김): Vector가 이미 받았을 수 있어 다시 보내면 중복될 수 있으므로 재전송하지 않음
        Err(e) => Err(SendError::Fail(format!("전송 실패: {e}"))),
    }
}

// 이벤트 1건 전송: 일시적 오류면 같은 payload(같은 event_id)로 다시 보냄
// 대기 시간: 0.5초 → 1초 → 2초 → 4초 → 5초 → 5초 ... (RETRY_LIMIT이 지나면 포기)
// 대기하는 동안에도 동시성 자리(permit)를 차지하므로, 재전송 요청이 몰려도 동시 요청 수는 설정한 동시성을 넘지 않음
// 반환: (재전송 횟수, 최종 결과)
async fn send_event(client: &Client, url: &str, payload: &Value) -> (u32, Result<(), String>) {
    let started = Instant::now();
    let mut delay = RETRY_FIRST_DELAY;
    let mut retries = 0;
    loop {
        match send_once(client, url, payload).await {
            Ok(()) => return (retries, Ok(())),
            Err(SendError::Fail(e)) => return (retries, Err(e)),
            Err(SendError::Retry(e)) => {
                if started.elapsed() >= RETRY_LIMIT {
                    return (retries, Err(format!("{}초 동안 재전송 {retries}회 후 포기: {e}", RETRY_LIMIT.as_secs())));
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX_DELAY);
                retries += 1;
            }
        }
    }
}

// 회차 1번 실행: 모든 묶음을 섞어서 전송하고 결과를 집계
// rep: 이번 회차 순서 (0부터), first_number: 이번 회차 첫 이벤트의 번호
async fn run_once(client: &Client, config: &RunConfig, rep: u32, first_number: u64) -> RunResult {
    let total = config.total_per_run();
    let last_number = first_number + total - 1;
    if config.verbose {
        println!(
            "── [ 회차 시작 ] 번호 {} ~ {} (총 {}건, 동시성 {}) ──",
            first_number,
            last_number,
            with_commas(total),
            config.parallel
        );
    }

    let counts: Vec<u64> = config.specs.iter().map(|s| s.count).collect();
    let mut interleaver = Interleaver::new(&counts);
    let semaphore = Arc::new(Semaphore::new(config.parallel)); // 동시에 실행 중인 요청 수 제한
    let url: Arc<str> = Arc::from(config.url.as_str()); // 모든 Task가 같은 주소를 복사 없이 공유
    let mut set = JoinSet::new();

    let mut result = RunResult { success: 0, fail: 0, retried: 0, elapsed: 0.0, first_error: None };
    let started = Instant::now();

    for offset in 0..total {
        let spec = &config.specs[interleaver.next()];
        let payload = build_payload(spec, first_number + offset);

        // 자리(permit)를 먼저 받은 뒤 Task를 만듦
        // Task를 먼저 만들고 안에서 기다리면, 100만 건일 때 대기 중인 Task가 100만 개 쌓여 메모리를 크게 차지함
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let url = Arc::clone(&url);
        set.spawn(async move {
            let _permit = permit; // Task가 끝나면 자리 반환
            send_event(&client, &url, &payload).await
        });

        // 이미 끝난 Task의 결과는 바로 집계 (끝난 결과가 JoinSet에 계속 쌓이지 않도록)
        while let Some(joined) = set.try_join_next() {
            tally(&mut result, joined);
        }
    }

    // 남은 Task가 모두 끝날 때까지 기다리며 집계
    while let Some(joined) = set.join_next().await {
        tally(&mut result, joined);
    }
    result.elapsed = started.elapsed().as_secs_f64();

    if config.verbose {
        println!(
            "▶ 완료: 성공 {} / 실패 {} / 재전송 {} ({:.2}초, {:.1} req/s)",
            with_commas(result.success),
            with_commas(result.fail),
            with_commas(result.retried),
            result.elapsed,
            total as f64 / result.elapsed
        );
    } else {
        // 간단 출력: 1줄 (여러 loadgen을 동시에 실행해도 구분되도록 이벤트 종류와 번호 범위를 앞에 표시)
        // 예: file_download+fail+reject 1~1200000 | 1,200,000건 | 성공 1,200,000 / 실패 0 / 재전송 0 | 49.14초 (24,420 req/s)
        let rep_prefix = if config.reps > 1 { format!("[{}/{}] ", rep + 1, config.reps) } else { String::new() };
        println!(
            "{rep_prefix}{} {}~{} | {}건 | 성공 {} / 실패 {} / 재전송 {} | {:.2}초 ({} req/s)",
            config.label(),
            first_number,
            last_number,
            with_commas(total),
            with_commas(result.success),
            with_commas(result.fail),
            with_commas(result.retried),
            result.elapsed,
            with_commas((total as f64 / result.elapsed) as u64)
        );
    }
    if let Some(e) = &result.first_error {
        println!("  ⚠ 첫 실패 원인: {e}");
    }
    result
}

// Task 1개의 결과를 집계에 반영
// JoinError: Task가 panic 등으로 비정상 종료된 경우 (전송 여부를 알 수 없으므로 실패로 집계)
fn tally(result: &mut RunResult, joined: Result<(u32, Result<(), String>), tokio::task::JoinError>) {
    let (retries, outcome) = match joined {
        Ok(sent) => sent,
        Err(e) => (0, Err(format!("전송 Task 비정상 종료: {e}"))),
    };
    if retries > 0 {
        result.retried += 1;
    }
    match outcome {
        Ok(()) => result.success += 1,
        Err(e) => {
            result.fail += 1;
            if result.first_error.is_none() {
                result.first_error = Some(e);
            }
        }
    }
}

// 설정과 예상 결과 출력
fn print_plan(config: &RunConfig) {
    box_top("설정");
    for (i, spec) in config.specs.iter().enumerate() {
        box_line(&format!("[{}] {}", i + 1, spec.describe()));
    }
    box_line("");
    box_line(&format!(
        "회차당 {}건 / 동시성 {} / 반복 {}회 / 시작 번호 {}",
        with_commas(config.total_per_run()),
        config.parallel,
        config.reps,
        config.start_id
    ));
    box_bottom();
    println!();

    box_top("예상 결과 (seed 플레이북 기준)");
    for (i, spec) in config.specs.iter().enumerate() {
        box_line(&format!("[{}] {}", i + 1, describe_expected(expected(spec))));
    }
    box_bottom();
    println!();
}

// 전송 실행: 회차 반복 → 결과 요약 → 기대값 요약 + 결과 확인 방법 안내
async fn execute(config: &RunConfig, client: &Client) {
    let total = config.total_per_run();
    let mut runs = Vec::new();

    for rep in 0..config.reps {
        // 회차마다 번호를 이어서 사용 (1회차가 1~100이면 2회차는 101~200)
        let first_number = config.start_id + rep as u64 * total;
        runs.push(run_once(client, config, rep, first_number).await);

        if rep + 1 < config.reps {
            if config.verbose {
                println!("-- 쿨다운 {}초 대기 --", config.cooldown);
            }
            tokio::time::sleep(Duration::from_secs(config.cooldown)).await;
        }
    }

    let reps = runs.len() as f64;
    let avg_success = runs.iter().map(|r| r.success).sum::<u64>() as f64 / reps;
    let avg_fail = runs.iter().map(|r| r.fail).sum::<u64>() as f64 / reps;
    let avg_time = runs.iter().map(|r| r.elapsed).sum::<f64>() / reps;
    let last_number = config.start_id + config.reps as u64 * total - 1;

    // 간단 출력: 반복할 때만 평균 1줄 추가 (회차별 결과는 run_once에서 출력)
    if !config.verbose {
        if config.reps > 1 {
            println!(
                "[평균 {}회] {} {}~{} | 성공 {avg_success:.1} / 실패 {avg_fail:.1} | {avg_time:.2}초 ({} req/s)",
                config.reps,
                config.label(),
                config.start_id,
                last_number,
                with_commas((total as f64 / avg_time) as u64)
            );
        }
        return;
    }

    let retried: u64 = runs.iter().map(|r| r.retried).sum();

    println!();
    box_top(&format!("결과 요약 ({}회 반복, 동시성 {})", config.reps, config.parallel));
    box_line(&format!("사용한 번호: {} ~ {}", config.start_id, last_number));
    box_line(&format!("평균 성공: {avg_success:.1} / 평균 실패: {avg_fail:.1}"));
    if retried > 0 {
        box_line(&format!("재전송한 이벤트 합계: {}", with_commas(retried)));
    }
    box_line(&format!("평균 처리시간: {avg_time:.2}초 ({:.1} req/s)", total as f64 / avg_time));
    box_bottom();
    println!();

    print_expected_summary(config);
    println!(" ▶ 결과 확인 방법: ./loadgen.exe check");
}

// 인자 방식 실행: (-v일 때만 설정 / 예상 결과 / 서비스 상태) → 전송
async fn run(config: RunConfig) {
    let client = Client::new();
    if config.verbose {
        print_plan(&config);
        check_services(&client, &load_ports()).await;
    }
    execute(&config, &client).await;
}

// 실행할 수 없는 설정인지 검사 (문제가 있으면 이유를 담은 Err)
// - 동시성 0: 요청을 영원히 보내지 못함
// - 반복 0: 평균을 낼 수 없음
// - 총 건수 0: 보낼 이벤트가 없음
// - 마지막 번호가 MAX_NUMBER 초과: 번호 계산이 숫자 범위를 넘을 수 있음
fn validate(config: &RunConfig) -> Result<(), String> {
    if config.parallel == 0 || config.reps == 0 {
        return Err("동시성과 반복 횟수는 1 이상이어야 합니다.".to_string());
    }
    let total = config.total_per_run();
    if total == 0 {
        return Err("보낼 이벤트가 없습니다. (전송 건수, 실패 이벤트 수, 거부 이벤트 수가 모두 0)".to_string());
    }
    // checked_*: 계산 결과가 u64 범위를 넘으면 None
    let last = total
        .checked_mul(config.reps as u64)
        .and_then(|n| n.checked_add(config.start_id))
        .filter(|&n| n <= MAX_NUMBER);
    if last.is_none() {
        return Err(format!("번호 범위가 너무 큽니다. (시작 번호 + 총 건수 × 반복 횟수 ≤ {})", with_commas(MAX_NUMBER)));
    }
    Ok(())
}

// 눈에 띄는 제목 출력 (예: 결과 확인 방법)
fn print_banner(title: &str) {
    let line = "━".repeat(60);
    println!();
    println!("{line}");
    println!("  ▶ {title}");
    println!("{line}");
}

// 결과 확인 방법 안의 항목 제목 (예: ── [OpenSearch] ... ──)
fn print_step(title: &str) {
    println!();
    println!("── {title} ──");
}

// 상자 형태 출력: 윗줄 (제목 포함) / 내용 한 줄 / 아랫줄
// ┌─ 설정 ─────────
// │ 내용
// └────────────────
const BOX_WIDTH: usize = 60;

fn box_top(title: &str) {
    let used = display_width(title) + 4; // "┌─ " + 제목 + " " 이 차지하는 칸 수
    println!("┌─ {title} {}", "─".repeat(BOX_WIDTH.saturating_sub(used)));
}

fn box_line(text: &str) {
    println!("│ {text}");
}

fn box_bottom() {
    println!("└{}", "─".repeat(BOX_WIDTH - 1));
}

// 터미널에서 문자열이 차지하는 칸 수 (한글 등 전각 문자는 2칸, 그 외는 1칸)
// 줄 끝 주석의 시작 위치를 맞출 때 사용 (한글을 1칸으로 세면 주석 위치가 어긋남)
fn display_width(text: &str) -> usize {
    text.chars().map(|c| if is_wide(c) { 2 } else { 1 }).sum()
}

// 전각 문자 판별: 한글 자모 / 한글 음절 / 한중일 문자 / 전각 기호 범위
fn is_wide(c: char) -> bool {
    matches!(c as u32,
        0x1100..=0x115F       // 한글 자모
        | 0x2E80..=0x303E     // 한중일 부수, 기호
        | 0x3041..=0x33FF     // 일본어 가나, 한중일 호환 문자
        | 0x3400..=0x4DBF     // 한자 확장 A
        | 0x4E00..=0x9FFF     // 한자
        | 0xAC00..=0xD7A3     // 한글 음절
        | 0xF900..=0xFAFF     // 한자 호환
        | 0xFF00..=0xFF60     // 전각 영문, 기호
        | 0xFFE0..=0xFFE6)
}

// 숫자에 천 단위 쉼표 추가 (예: 1000000 → 1,000,000)
fn with_commas(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

// ── 대화형 설정 (loadgen setup) ─────────────────────────────────────

// 대화형 입력 범위
const MAX_SPEC_COUNT: u64 = 100_000_000; // 이벤트 묶음 1개의 최대 건수
const MAX_PARALLEL: u64 = 100_000; // 최대 동시성
const MAX_REPS: u64 = 1_000; // 최대 반복 횟수
const MAX_COOLDOWN: u64 = 3_600; // 최대 쿨다운(초)
const MAX_VOTES_PER_ACTION: u64 = 2; // 판정별 최대 표 수 (다수결 테스트용 플레이북이 판정마다 2개씩 있음)

// 이벤트 종류 메뉴 항목 (이름, 종류, 케이스 선택 여부, 다수결 여부)
// 케이스를 고르지 않는 항목은 안전 케이스로 고정 (실패/거부 이벤트는 저장되지 않고, 다수결은 다른 플레이북이 함께 판정하지 않도록 안전 IP 사용)
struct MenuItem {
    title: &'static str,
    kind: Kind,
    choose_case: bool,
    votes: bool,
}

static MENU: [MenuItem; 6] = [
    MenuItem { title: "file_download (파일 다운로드)", kind: Kind::Normal(EventType::FileDownload), choose_case: true, votes: false },
    MenuItem { title: "login_failure (로그인 실패)", kind: Kind::Normal(EventType::LoginFailure), choose_case: true, votes: false },
    MenuItem { title: "dns_beacon (외부 도메인과 주기적 통신)", kind: Kind::Normal(EventType::DnsBeacon), choose_case: true, votes: false },
    MenuItem {
        title: "실패 이벤트 (event_id 없음 → Backend 파싱 실패 → storage_failures 저장)",
        kind: Kind::Failure,
        choose_case: false,
        votes: false,
    },
    MenuItem {
        title: "거부 이벤트 (객체 필드에 문자열 → OpenSearch 거부 → storage_failures 저장)",
        kind: Kind::Reject,
        choose_case: false,
        votes: false,
    },
    MenuItem {
        title: "다수결 테스트 (login_failure 안전 케이스 + test_votes → 입력한 표 수로 최종 판정)",
        kind: Kind::Normal(EventType::LoginFailure),
        choose_case: false,
        votes: true,
    },
];

// JSON 예시의 필드별 설명 (메뉴에서 각 줄 끝에 주석으로 표시)
// 여기에 없는 필드는 설명 없이 표시됨
const FIELD_HELP: [(&str, &str); 16] = [
    ("event_id", "이벤트 ID (실행할 때 지정한 시작 번호부터 1씩 증가)"),
    ("loadgen_id", "추적용 ID (event_id가 없어 Backend 파싱 실패, storage_failures에서 찾을 때 사용)"),
    ("event_type", "이벤트 종류"),
    ("timestamp", "이벤트 발생 시각 (UTC, 전송 시각으로 채움)"),
    ("source_ip", "출발지 IP (AbuseIPDB 위험도 조회 대상)"),
    ("destination_domain", "접속 도메인 (DNS 조회 대상)"),
    ("file_name", "다운로드한 파일 이름"),
    ("file_sha256", "파일 해시 (VirusTotal 악성 판정 조회 대상)"),
    ("file_size_bytes", "파일 크기 (바이트)"),
    ("username", "로그인을 시도한 계정"),
    ("attempt_count", "로그인 시도 횟수"),
    ("target_service", "로그인 대상 서비스"),
    ("hostname", "외부와 통신한 내부 PC 이름"),
    ("request_interval_seconds", "통신 주기 (초)"),
    ("test_votes", "판정을 낼 다수결 테스트용 플레이북 이름 목록"),
    ("enrichment_dst_domain", "OpenSearch 거부용 (객체로 정의된 필드에 문자열을 넣어 타입 불일치 발생)"),
];

// 대화형 실행: 설정 → 확인 → 실행 / 처음부터 다시 / 종료
async fn setup() {
    let client = Client::new();
    let ports = load_ports();

    loop {
        // 1. 이벤트 묶음 설정 (종료를 고르면 None)
        let Some(specs) = ask_specs() else {
            return;
        };

        // 2. 동시성 / 시작 번호 / 반복 / 쿨다운 설정
        let config = ask_options(specs);
        if let Err(e) = validate(&config) {
            println!(" ⚠ {e}");
            read_input(" ▶ Enter를 누르면 처음부터 다시 설정합니다.");
            continue;
        }

        // 3. 설정 / 예상 결과 / 서비스 상태 / 결과 확인 방법을 보여주고 실행 여부 확인
        clear_screen();
        print_banner("loadgen setup : 최종 확인");
        println!();
        print_plan(&config);
        check_services(&client, &ports).await;
        print_expected_summary(&config);
        println!();
        match ask_number(" [1] 실행   [2] 처음부터 다시   [3] 종료\n ▶ 선택: ", None, 1, 3) {
            1 => {
                println!();
                execute(&config, &client).await;
                return;
            }
            2 => continue,
            _ => return,
        }
    }
}

// 이벤트 묶음 설정 화면: 현재 설정을 보여주고 추가 / 처음부터 다시 / 완료를 반복
// 반환: 설정한 묶음 목록, 종료를 고르면 None
fn ask_specs() -> Option<Vec<EventSpec>> {
    let mut specs: Vec<EventSpec> = Vec::new();
    loop {
        clear_screen();
        print_banner("loadgen setup : 이벤트 설정");
        println!();
        print_current(&specs);

        // 아직 추가한 이벤트가 없으면 "처음부터 다시"와 "설정 완료"는 의미가 없으므로 보여주지 않음
        if specs.is_empty() {
            match ask_number(" [1] 이벤트 추가   [2] 종료\n ▶ 선택: ", None, 1, 2) {
                1 => specs.push(ask_spec()),
                _ => return None,
            }
        } else {
            match ask_number(" [1] 이벤트 추가   [2] 처음부터 다시   [3] 설정 완료\n ▶ 선택: ", None, 1, 3) {
                1 => specs.push(ask_spec()),
                2 => specs.clear(),
                _ => return Some(specs),
            }
        }
    }
}

// 이벤트 묶음 1개 설정: 종류 → (케이스 또는 다수결 표 수) → 건수
fn ask_spec() -> EventSpec {
    clear_screen();
    print_event_menu();
    let item = &MENU[(ask_number(" ▶ 보낼 이벤트 번호: ", None, 1, MENU.len() as u64) - 1) as usize];
    println!();
    println!(" 선택: {}", item.title);

    let case = if item.choose_case {
        match ask_number(" 케이스   [1] 안전(safe)   [2] 위험(malicious)\n ▶ 선택: ", None, 1, 2) {
            1 => Case::Safe,
            _ => Case::Malicious,
        }
    } else {
        Case::Safe
    };

    let votes = if item.votes { Some(ask_votes()) } else { None };
    let count = ask_number(" ▶ 건수: ", None, 1, MAX_SPEC_COUNT);

    EventSpec { kind: item.kind, case, count, votes }
}

// 다수결 표 수 입력 → test_votes 값 생성 (예: DENY 2, HOLD 1 → "D1,D2,H1")
// 표가 하나도 없으면 판정이 나오지 않아 테스트 의미가 없으므로 다시 입력
fn ask_votes() -> String {
    println!(" 판정별 표 수 (0 ~ {MAX_VOTES_PER_ACTION}, Enter = 0)");
    println!(" 집계 규칙: DENY 과반 → DENY / DETECT 과반 → DETECT / 그 외 → HOLD");
    loop {
        let deny = ask_number("   ▶ DENY 표 수: ", Some(0), 0, MAX_VOTES_PER_ACTION);
        let hold = ask_number("   ▶ HOLD 표 수: ", Some(0), 0, MAX_VOTES_PER_ACTION);
        let detect = ask_number("   ▶ DETECT 표 수: ", Some(0), 0, MAX_VOTES_PER_ACTION);
        if deny + hold + detect > 0 {
            return build_votes(deny, hold, detect);
        }
        println!("   ⚠ 표가 하나 이상 있어야 합니다. 다시 입력하세요.");
    }
}

// 판정별 표 수를 test_votes 값으로 변환 (D = DENY, H = HOLD, T = DETECT, 번호는 1부터)
fn build_votes(deny: u64, hold: u64, detect: u64) -> String {
    let mut codes = Vec::new();
    for (prefix, n) in [("D", deny), ("H", hold), ("T", detect)] {
        for i in 1..=n {
            codes.push(format!("{prefix}{i}"));
        }
    }
    codes.join(",")
}

// 동시성 / 시작 번호 / 반복 / 쿨다운 입력 (Enter만 누르면 기본값)
fn ask_options(specs: Vec<EventSpec>) -> RunConfig {
    clear_screen();
    print_banner("loadgen setup : 전송 옵션");
    println!();
    print_current(&specs);
    println!(" Enter만 누르면 기본값 사용");
    let parallel = ask_number(" ▶ 동시성 [기본 100]: ", Some(100), 1, MAX_PARALLEL) as usize;
    let start_id = ask_number(" ▶ 시작 번호 [기본 1] (이전 테스트와 겹치지 않게): ", Some(1), 0, MAX_NUMBER);
    let reps = ask_number(" ▶ 반복 횟수 [기본 1]: ", Some(1), 1, MAX_REPS) as u32;
    // 쿨다운은 회차 사이 대기 시간이므로 2회 이상 반복할 때만 질문
    let cooldown = if reps > 1 { ask_number(" ▶ 쿨다운 초 [기본 10]: ", Some(10), 0, MAX_COOLDOWN) } else { 0 };

    // 대화형은 최종 확인 화면이 필요하므로 항상 자세히 출력
    RunConfig { specs, start_id, parallel, url: DEFAULT_URL.to_string(), reps, cooldown, verbose: true }
}

// 현재 설정한 이벤트 묶음 목록 출력
fn print_current(specs: &[EventSpec]) {
    box_top("현재 설정");
    if specs.is_empty() {
        box_line("(없음)");
    }
    for (i, spec) in specs.iter().enumerate() {
        box_line(&format!("[{}] {}", i + 1, spec.describe()));
        box_line(&format!("    → {}", describe_expected(expected(spec))));
    }
    box_bottom();
}

// 이벤트 종류 메뉴 출력 (번호마다 실제로 보낼 JSON 예시와 필드 설명 포함)
fn print_event_menu() {
    print_banner("loadgen setup : 보낼 이벤트 종류");
    for (i, item) in MENU.iter().enumerate() {
        println!();
        box_top(&format!("[{}] {}", i + 1, item.title));
        for line in render_example(item).lines() {
            box_line(line);
        }
        box_bottom();
    }
    println!();
}

// 메뉴에 보여줄 JSON 예시 값 생성
// 실제 전송에 쓰는 build_payload로 만들기 때문에, 이벤트 필드를 바꾸면 예시도 자동으로 바뀜
// 매번 달라지는 값은 설명 문구로 바꿔서 보여줌
// - 번호: loadgen-<번호>
// - 시각: <전송 시각>
// - 안전 / 위험 케이스에 따라 달라지는 값: <케이스 설정 시 세팅>
// - 다수결 값: <다수결 설정 시 세팅>
fn example_value(item: &MenuItem) -> Value {
    let make = |case| {
        let spec = EventSpec { kind: item.kind, case, count: 1, votes: item.votes.then(|| "D1".to_string()) };
        build_payload(&spec, 0)
    };
    let malicious = make(Case::Malicious);
    let mut example = make(Case::Safe);

    if let Some(fields) = example.as_object_mut() {
        for (key, value) in fields.iter_mut() {
            let text = value.as_str().unwrap_or_default().to_string();
            if key == "event_id" || key == "loadgen_id" {
                // 번호 0으로 만든 값의 끝 "-0"을 "-<번호>"로 변경 (예: loadgen-reject-0 → loadgen-reject-<번호>)
                if let Some(prefix) = text.strip_suffix("-0") {
                    *value = json!(format!("{prefix}-<번호>"));
                }
            } else if key == "timestamp" {
                *value = json!("<전송 시각>");
            } else if key == "test_votes" {
                *value = json!("<다수결 설정 시 세팅>");
            } else if item.choose_case && malicious.get(key) != Some(&*value) {
                *value = json!("<케이스 설정 시 세팅>");
            }
        }
    }
    example
}

// JSON 예시를 화면 출력용 문자열로 변환: 각 필드 줄 끝에 // 설명 주석 추가 (주석 시작 위치는 한 열로 맞춤)
// 예:   "event_id": "loadgen-<번호>",      // 이벤트 ID (...)
fn render_example(item: &MenuItem) -> String {
    let pretty = serde_json::to_string_pretty(&example_value(item)).unwrap_or_default();
    let lines: Vec<&str> = pretty.lines().collect();
    let width = lines.iter().map(|line| display_width(line)).max().unwrap_or(0);

    let mut out = Vec::new();
    for line in &lines {
        // 필드 줄은 `  "필드명": 값` 형태이므로 첫 번째 따옴표 사이의 문자열이 필드명
        let key = line.trim_start().strip_prefix('"').and_then(|rest| rest.split('"').next());
        let help = key.and_then(|k| FIELD_HELP.iter().find(|(name, _)| *name == k)).map(|(_, help)| *help);
        match help {
            Some(help) => {
                let pad = " ".repeat(width - display_width(line));
                out.push(format!("{line}{pad}  // {help}"));
            }
            None => out.push(line.to_string()),
        }
    }
    out.join("\n")
}

// 화면 지우기 (ANSI 제어 문자: 화면 전체 지우기 + 커서를 맨 위로 이동)
fn clear_screen() {
    print!("\x1B[2J\x1B[H");
    let _ = std::io::stdout().flush();
}

// 한 줄 입력 (앞뒤 공백 제거)
// 입력이 끝난 경우(Ctrl+D, 파이프 입력 종료)나 읽기 오류면 같은 질문이 무한 반복되지 않도록 프로그램 종료
fn read_input(prompt: &str) -> String {
    print!("{prompt}");
    let _ = std::io::stdout().flush(); // print!는 줄바꿈 전까지 화면에 바로 나오지 않을 수 있어 즉시 출력
    let mut line = String::new();
    match std::io::stdin().read_line(&mut line) {
        Ok(0) | Err(_) => {
            println!();
            println!("입력이 끝나 종료합니다.");
            std::process::exit(0);
        }
        Ok(_) => line.trim().to_string(),
    }
}

// 숫자 입력: 범위(min ~ max) 안의 값이 들어올 때까지 다시 질문
// default가 있으면 Enter만 눌렀을 때 기본값 사용
fn ask_number(prompt: &str, default: Option<u64>, min: u64, max: u64) -> u64 {
    loop {
        let input = read_input(prompt);
        if input.is_empty() {
            if let Some(value) = default {
                return value;
            }
            println!("   ⚠ 값을 입력하세요.");
            continue;
        }
        match parse_number(&input) {
            Some(n) if (min..=max).contains(&n) => return n,
            _ => println!("   ⚠ {} ~ {} 사이의 숫자를 입력하세요.", with_commas(min), with_commas(max)),
        }
    }
}

// 숫자 변환: 쉼표(,)와 밑줄(_)은 무시 (예: "1,000,000" → 1000000)
fn parse_number(input: &str) -> Option<u64> {
    input.replace([',', '_'], "").parse().ok()
}

#[tokio::main]
async fn main() {
    // `loadgen setup`이면 대화형으로 설정 (setup 하나만 입력한 경우)
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() == 2 && raw[1] == "setup" {
        setup().await;
        return;
    }
    // `loadgen check`이면 결과 확인 방법만 출력 (전송하지 않음)
    if raw.len() == 2 && raw[1] == "check" {
        print_check_guide(&load_ports());
        return;
    }

    let args = Args::parse();

    // 인자 방식: 지정한 이벤트 묶음 1개 + 실패 이벤트 묶음(--fail) + 거부 이벤트 묶음(--reject)
    let mut specs = Vec::new();
    if args.count > 0 {
        specs.push(EventSpec {
            kind: Kind::Normal(args.event_type),
            case: args.case,
            count: args.count,
            votes: args.votes.clone(),
        });
    }
    if args.fail > 0 {
        specs.push(EventSpec { kind: Kind::Failure, case: args.case, count: args.fail, votes: None });
    }
    if args.reject > 0 {
        specs.push(EventSpec { kind: Kind::Reject, case: args.case, count: args.reject, votes: None });
    }

    let config = RunConfig {
        specs,
        start_id: args.start_id,
        parallel: args.parallel,
        url: args.url,
        reps: args.reps,
        cooldown: args.cooldown,
        verbose: args.verbose,
    };
    if let Err(e) = validate(&config) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    run(config).await;
}

// cargo test
#[cfg(test)]
mod tests {
    use super::*;

    fn spec(kind: Kind, case: Case, count: u64, votes: Option<&str>) -> EventSpec {
        EventSpec { kind, case, count, votes: votes.map(String::from) }
    }

    // 시나리오 1: 한 바퀴(총 건수)를 돌면 각 묶음이 정확히 자기 건수만큼 나오는지
    #[test]
    fn interleaver_exact_counts() {
        for counts in [vec![3, 1], vec![1_000_000, 10], vec![5, 0, 2], vec![7], vec![1, 1, 1]] {
            let mut it = Interleaver::new(&counts);
            let total: u64 = counts.iter().sum();
            let mut seen = vec![0u64; counts.len()];
            for _ in 0..total {
                seen[it.next()] += 1;
            }
            assert_eq!(seen, counts);
        }
    }

    // 시나리오 2: 한쪽에 몰리지 않고 고르게 섞이는지 (A 3건, B 1건)
    #[test]
    fn interleaver_spreads_evenly() {
        let mut it = Interleaver::new(&[3, 1]);
        let order: Vec<usize> = (0..4).map(|_| it.next()).collect();
        assert_eq!(order, vec![0, 0, 1, 0]);
    }

    // 시나리오 3: 정상 이벤트는 event_id가 있고, 실패 이벤트는 event_id 없이 loadgen_id만 있는지
    #[test]
    fn payload_ids() {
        let normal = build_payload(&spec(Kind::Normal(EventType::LoginFailure), Case::Safe, 1, None), 7);
        assert_eq!(normal["event_id"], "loadgen-7");
        assert!(normal.get("loadgen_id").is_none());
        assert!(normal.get("test_votes").is_none());

        let failure = build_payload(&spec(Kind::Failure, Case::Safe, 1, None), 8);
        assert!(failure.get("event_id").is_none());
        assert_eq!(failure["loadgen_id"], "loadgen-failure-8");
    }

    // 시나리오 4: 케이스에 따라 IOC 값이 바뀌는지
    #[test]
    fn payload_case_values() {
        let fd = build_payload(&spec(Kind::Normal(EventType::FileDownload), Case::Malicious, 1, None), 1);
        assert_eq!(fd["source_ip"], MALICIOUS_IP);
        assert_eq!(fd["file_sha256"], MALICIOUS_SHA256);

        let dns_bad = build_payload(&spec(Kind::Normal(EventType::DnsBeacon), Case::Malicious, 1, None), 1);
        let dns_ok = build_payload(&spec(Kind::Normal(EventType::DnsBeacon), Case::Safe, 1, None), 1);
        assert_eq!(dns_bad["destination_domain"], MALICIOUS_DOMAIN);
        assert_eq!(dns_ok["destination_domain"], SAFE_DOMAIN);
        assert_eq!(dns_ok["source_ip"], SAFE_IP);
    }

    // 시나리오 5: 다수결 값이 test_votes 필드로 들어가는지
    #[test]
    fn payload_votes() {
        let p = build_payload(&spec(Kind::Normal(EventType::LoginFailure), Case::Safe, 1, Some("D1,D2,H1")), 1);
        assert_eq!(p["test_votes"], "D1,D2,H1");
    }

   // 시나리오 6: 거부 이벤트는 event_id가 있고, 객체 필드에 문자열이 들어가는지
    #[test]
    fn payload_reject() {
        let p = build_payload(&spec(Kind::Reject, Case::Safe, 1, None), 9);
        assert_eq!(p["event_id"], "loadgen-reject-9");
        assert_eq!(p[REJECT_FIELD], "x");
    }

    // 시나리오 7: 예상 결과가 seed 플레이북 기준과 같은지
    #[test]
    fn expected_results() {
        let stored = |verdict, decisions| Expected::Stored { verdict, decisions };
        let normal = |t, case, votes| expected(&spec(Kind::Normal(t), case, 1, votes));

        assert_eq!(normal(EventType::FileDownload, Case::Malicious, None), stored(Verdict::Deny, 2));
        assert_eq!(normal(EventType::FileDownload, Case::Safe, None), stored(Verdict::NoDecision, 0));
        assert_eq!(normal(EventType::LoginFailure, Case::Malicious, None), stored(Verdict::Deny, 1));
        assert_eq!(normal(EventType::LoginFailure, Case::Safe, None), stored(Verdict::NoDecision, 0));
        assert_eq!(normal(EventType::DnsBeacon, Case::Malicious, None), stored(Verdict::Deny, 2));
        assert_eq!(normal(EventType::DnsBeacon, Case::Safe, None), stored(Verdict::NoDecision, 0));

        // 다수결 (seed 주석의 기대 결과)
        let vote = |v| normal(EventType::LoginFailure, Case::Safe, Some(v));
        assert_eq!(vote("D1,D2,H1"), stored(Verdict::Deny, 3));
        assert_eq!(vote("H1,H2,D1"), stored(Verdict::Hold, 3));
        assert_eq!(vote("T1,T2,D1"), stored(Verdict::Detect, 3));
        assert_eq!(vote("D1,H1,T1"), stored(Verdict::Hold, 3));
        assert_eq!(vote("D1,D2,T1,T2"), stored(Verdict::Hold, 4));

        assert_eq!(expected(&spec(Kind::Failure, Case::Safe, 1, None)), Expected::ParseFailure);
        assert_eq!(expected(&spec(Kind::Reject, Case::Safe, 1, None)), Expected::OsReject);
    }

    // 시나리오 8: 다수결 표 수 → test_votes 값
    #[test]
    fn votes_string() {
        assert_eq!(build_votes(2, 1, 0), "D1,D2,H1");
        assert_eq!(build_votes(0, 0, 2), "T1,T2");
        assert_eq!(build_votes(1, 1, 1), "D1,H1,T1");
        assert_eq!(build_votes(2, 2, 2), "D1,D2,H1,H2,T1,T2");
    }

    // 시나리오 9: 숫자 입력 변환
    #[test]
    fn number_input() {
        assert_eq!(parse_number("1000000"), Some(1_000_000));
        assert_eq!(parse_number("1,000,000"), Some(1_000_000));
        assert_eq!(parse_number("1_000"), Some(1_000));
        assert_eq!(parse_number("-1"), None);
        assert_eq!(parse_number("abc"), None);
        assert_eq!(parse_number("1.5"), None);
    }

    // 시나리오 10: 실행할 수 없는 설정 검사
    #[test]
    fn validate_config() {
        let config = |count, start_id, parallel, reps| RunConfig {
            specs: vec![spec(Kind::Normal(EventType::LoginFailure), Case::Safe, count, None)],
            start_id,
            parallel,
            url: DEFAULT_URL.to_string(),
            reps,
            cooldown: 0,
            verbose: false,
        };
        assert!(validate(&config(10, 1, 5, 1)).is_ok());
        assert!(validate(&config(10, 1, 0, 1)).is_err()); // 동시성 0
        assert!(validate(&config(10, 1, 5, 0)).is_err()); // 반복 0
        assert!(validate(&config(0, 1, 5, 1)).is_err()); // 총 건수 0
        assert!(validate(&config(u64::MAX, 1, 5, 2)).is_err()); // 번호 계산 범위 초과
    }

    // 시나리오 11: 메뉴 JSON 예시의 설명 문구 치환
    #[test]
    fn menu_examples() {
        let file_download = example_value(&MENU[0]);
        assert_eq!(file_download["event_id"], "loadgen-<번호>");
        assert_eq!(file_download["timestamp"], "<전송 시각>");
        assert_eq!(file_download["source_ip"], "<케이스 설정 시 세팅>");
        assert_eq!(file_download["file_sha256"], "<케이스 설정 시 세팅>");
        assert_eq!(file_download["destination_domain"], DOWNLOAD_DOMAIN); // 케이스와 무관한 값은 그대로
        // ID가 맨 앞 필드인지 (preserve_order)
        assert_eq!(file_download.as_object().unwrap().keys().next().unwrap(), "event_id");

        let dns = example_value(&MENU[2]);
        assert_eq!(dns["destination_domain"], "<케이스 설정 시 세팅>");

        let failure = example_value(&MENU[3]);
        assert_eq!(failure["loadgen_id"], "loadgen-failure-<번호>");
        assert!(failure.get("event_id").is_none());
        assert_eq!(failure["source_ip"], SAFE_IP); // 케이스를 고르지 않는 항목은 안전 값 그대로
        assert_eq!(failure.as_object().unwrap().keys().next().unwrap(), "loadgen_id");

        let reject = example_value(&MENU[4]);
        assert_eq!(reject["event_id"], "loadgen-reject-<번호>");
        assert_eq!(reject[REJECT_FIELD], "x");

        let votes = example_value(&MENU[5]);
        assert_eq!(votes["test_votes"], "<다수결 설정 시 세팅>");
        assert_eq!(votes["source_ip"], SAFE_IP);
    }

    // 시나리오 12: 화면용 JSON 예시에 필드 설명 주석이 붙고, 모든 필드에 설명이 있는지
    #[test]
    fn menu_example_comments() {
        for item in &MENU {
            let text = render_example(item);
            for (key, _) in example_value(item).as_object().unwrap() {
                let line = text.lines().find(|l| l.contains(&format!("\"{key}\""))).unwrap();
                assert!(line.contains("  // "), "설명 없는 필드: {key}");
            }
        }
    }

    // 시나리오 13: 한글은 2칸, 영문/숫자는 1칸
    #[test]
    fn width() {
        assert_eq!(display_width("abc"), 3);
        assert_eq!(display_width("한글"), 4);
        assert_eq!(display_width("a한"), 3);
    }

    // 시나리오 14: 천 단위 쉼표
    #[test]
    fn commas() {
        assert_eq!(with_commas(0), "0");
        assert_eq!(with_commas(999), "999");
        assert_eq!(with_commas(1000), "1,000");
        assert_eq!(with_commas(1_000_010), "1,000,010");
    }

    // 시나리오 15: 재전송할 HTTP 응답인지 판단
    #[test]
    fn retryable_status() {
        assert!(is_retryable_status(400, r#"{"code":400,"message":"Contents failed to deliver to sink"}"#));
        assert!(!is_retryable_status(400, r#"{"code":400,"message":"Failed decoding body"}"#));
        assert!(is_retryable_status(408, "") && is_retryable_status(429, "") && is_retryable_status(503, ""));
        assert!(!is_retryable_status(404, "") && !is_retryable_status(422, ""));
    }
}