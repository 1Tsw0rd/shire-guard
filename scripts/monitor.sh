#!/bin/bash
# Shire Guard 모니터링 CLI (데모 / 성능 테스트용)
# 사용법: bash scripts/monitor.sh <번호>   (번호 없이 실행하면 메뉴 출력)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../docker/.env"

# Rust Backend /metrics 주소 (Backend main.rs에 3001로 고정)
BACKEND_URL="http://127.0.0.1:3001/metrics"

# 옵션별 출력 주기(초): 조회에 걸린 시간을 포함해 이 주기를 맞춤
INTERVAL_STORAGE=1
INTERVAL_CPU=2
INTERVAL_KAFKA=5
INTERVAL_DB=5
INTERVAL_DOCKER=3

# 모니터링할 컨테이너 이름 (추가하려면 공백으로 구분해 이름 추가)
CONTAINERS="opensearch clickhouse kafka"

LINE="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SUB_LINE="────────────────────────────────────────────────────────────"

trap 'echo; echo "모니터링 종료"; exit 0' INT

# ── 공통 함수 ─────────────────────────────────────

# docker/.env에서 필요한 키 하나만 읽음 (따옴표, Windows 줄바꿈(\r) 제거)
read_env() {
  [ -f "$ENV_FILE" ] || return
  grep -E "^$1=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d '\r"'"'"
}

# 천 단위 쉼표 (예: 1000000 -> 1,000,000)
commas() {
  local n="$1" sign="" out=""
  [[ $n == -* ]] && sign="-" && n="${n#-}"
  while [ ${#n} -gt 3 ]; do
    out=",${n: -3}$out"
    n="${n:0:${#n}-3}"
  done
  echo "$sign$n$out"
}

now_ns() { date +%s%N; }

# 주기 맞추기: 이번 회차 시작 시각(ns) 기준으로 남은 시간만큼 대기 (조회가 주기보다 오래 걸리면 바로 다음 회차)
wait_next() {
  local started=$1 interval=$2
  local remain
  remain=$(awk -v s="$started" -v n="$(now_ns)" -v i="$interval" 'BEGIN{r=i-(n-s)/1e9; if(r>0) printf "%.3f", r}')
  [ -n "$remain" ] && sleep "$remain"
}

# 초당 속도: (현재 - 이전) / 경과 초
rate() {
  awk -v a="$1" -v b="$2" -v d="$3" 'BEGIN{printf "%.0f", (a-b)/(d/1e9)}'
}

# 제목 출력: header "<제목>" "<설명 줄>"...
header() {
  echo "$LINE"
  echo "  Shire Guard Monitor ▶ $1"
  shift
  for l in "$@"; do
    if [ "$l" = "---" ]; then echo "$SUB_LINE"; else echo "  $l"; fi
  done
  echo "$LINE"
}

fetch_metrics() { curl -s --max-time 2 "$BACKEND_URL"; }

# ── [1] Kafka 이벤트 소비 / DB 로그 저장 / 처리 속도 ─────────────

mode_storage() {
  header "[1] Kafka 이벤트 소비 / DB 로그 저장 / 처리 속도" \
    "주기: ${INTERVAL_STORAGE}초 | 대상: $BACKEND_URL | 종료: Ctrl+C" \
    "---" \
    "소비     : Kafka/RedPanda에서 Rust가 수신한 이벤트 수" \
    "저장     : OpenSearch → ClickHouse 순서로 둘 다 저장 완료된 로그 수" \
    "실패보관 : 파싱 실패 / OpenSearch 거절로 PostgreSQL(storage_failures)에 보관된 로그 수" \
    "재시도   : DB 저장에 실패해 다시 보낸 요청 횟수 (로그 건수 아님)"

  local has_prev=0 prev_r=0 prev_s=0 prev_t=0
  while true; do
    local started m
    started=$(now_ns)
    m=$(fetch_metrics)

    if ! echo "$m" | grep -q '^shireguard_consumer_messages_received_total'; then
      echo "$(date +%T)  Backend 응답 없음"
      has_prev=0
      wait_next "$started" "$INTERVAL_STORAGE"
      continue
    fi

    local recv sent failed retry
    read -r recv sent failed retry < <(echo "$m" | awk '
      /^shireguard_consumer_messages_received_total / { r = $2 }
      /^shireguard_storage_events_sent_total /        { s = $2 }
      /^shireguard_consumer_events_failed_total /     { f += $2 }
      /^shireguard_storage_rejected_total /           { f += $2 }
      /^shireguard_storage_retries_total\{/           { t += $2 }
      END { printf "%.0f %.0f %.0f %.0f\n", r, s, f, t }')

    local now r_rate s_rate
    now=$(now_ns)
    if [ "$has_prev" -eq 1 ] && { [ "$recv" -lt "$prev_r" ] || [ "$sent" -lt "$prev_s" ]; }; then
      echo "$(date +%T)  Rust 재시작 감지, 기준값 초기화"
      has_prev=0
    fi
    if [ "$has_prev" -eq 1 ]; then
      r_rate="$(commas "$(rate "$recv" "$prev_r" $((now - prev_t)))")/s"
      s_rate="$(commas "$(rate "$sent" "$prev_s" $((now - prev_t)))")/s"
    else
      r_rate="-"
      s_rate="-"
    fi

    printf "%s  소비=%-12s 소비속도=%-12s 저장=%-12s 저장속도=%-12s 실패보관=%-8s 재시도=%s\n" \
      "$(date +%T)" "$(commas "$recv")" "$r_rate" "$(commas "$sent")" "$s_rate" "$(commas "$failed")" "$(commas "$retry")"

    has_prev=1; prev_r=$recv; prev_s=$sent; prev_t=$now
    wait_next "$started" "$INTERVAL_STORAGE"
  done
}

# ── [2] Rust 서버 CPU ─────────────────────────────

# Windows: typeperf로 1초 간격 2번 측정 후 마지막 값 사용 (첫 값은 비교 대상이 없어 부정확)
# 매 회차 새로 실행하므로 Rust가 재시작되어도 영향 없음
cpu_windows() {
  typeperf "\\Process(backend)\\% Processor Time" -si 1 -sc 2 2>/dev/null \
    | tr -d '\r' | grep -E '^"[0-9]' | tail -n 1 | awk -F'"' '{ if ($4 != "") printf "%.1f", $4 }'
}

# Linux: /proc/<pid>/stat의 CPU 사용 시간(utime + stime) 증가분 / 경과 시간
cpu_ticks() {
  awk '{ sub(/.*\) /, ""); print $12 + $13 }' "/proc/$1/stat" 2>/dev/null
}

mode_cpu() {
  local os cores
  case "$(uname -s)" in
    Linux*) os="Linux (/proc)"; cores=$(nproc) ;;
    MINGW* | MSYS* | CYGWIN*) os="Windows (typeperf)"; cores=${NUMBER_OF_PROCESSORS:-?} ;;
    *) echo "지원하지 않는 OS입니다: $(uname -s) (Linux, Windows만 지원)"; exit 1 ;;
  esac

  header "[2] Rust 서버 CPU" \
    "주기: ${INTERVAL_CPU}초 | OS: $os | 단위: 코어 합계 % (${cores}코어 = 최대 ${cores}00%) | 종료: Ctrl+C"

  local prev_pid="" prev_ticks=0 prev_t=0 hz
  [ "${os%% *}" = "Linux" ] && hz=$(getconf CLK_TCK)

  while true; do
    local started value=""
    started=$(now_ns)

    if [ "${os%% *}" = "Windows" ]; then
      value=$(cpu_windows)
    else
      local pid ticks now
      pid=$(pgrep -x backend | head -n 1)
      if [ -n "$pid" ]; then
        ticks=$(cpu_ticks "$pid")
        now=$(now_ns)
        if [ -n "$ticks" ] && [ "$pid" = "$prev_pid" ]; then
          value=$(awk -v a="$ticks" -v b="$prev_ticks" -v hz="$hz" -v d=$((now - prev_t)) \
            'BEGIN{printf "%.1f", (a-b)/hz/(d/1e9)*100}')
        elif [ -n "$ticks" ]; then
          value="측정 준비"   # 처음 찾았거나 재시작으로 PID가 바뀜: 다음 회차부터 계산
        fi
        prev_pid=$pid; prev_ticks=$ticks; prev_t=$now
      else
        prev_pid=""
      fi
    fi

    case "$value" in
      "") echo "$(date +%T)  backend 프로세스 없음" ;;
      "측정 준비") echo "$(date +%T)  backend 발견, 다음 주기부터 측정" ;;
      *) echo "$(date +%T)  backend CPU=${value}%" ;;
    esac
    wait_next "$started" "$INTERVAL_CPU"
  done
}

# ── [3] Kafka 토픽누적 / commit / LAG ─────────────────

mode_kafka() {
  local topic group port
  topic=$(read_env KAFKA_TOPIC)
  group=$(read_env KAFKA_GROUP_ID)
  port=$(read_env KAFKA_PORT)
  port=${port:-9092}
  if [ -z "$topic" ] || [ -z "$group" ]; then
    echo "docker/.env에서 KAFKA_TOPIC 또는 KAFKA_GROUP_ID를 찾을 수 없습니다: $ENV_FILE"
    exit 1
  fi

  header "[3] Kafka 토픽누적 / commit / LAG" \
    "주기: ${INTERVAL_KAFKA}초 | 토픽: $topic | 그룹: $group (docker/.env) | 종료: Ctrl+C" \
    "※ commit은 Rust가 5초마다 반영하므로 처리가 끝난 뒤에도 LAG가 잠시 남을 수 있음"

  while true; do
    local started out line
    started=$(now_ns)
    out=$(MSYS_NO_PATHCONV=1 docker exec -e KAFKA_OPTS= kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server "localhost:$port" --describe --group "$group" 2>/dev/null)
    line=$(echo "$out" | awk -v t="$topic" '
      $2 == t { n++; e += $5; if ($4 == "-") nc = 1; else c += $4; if ($6 != "-") l += $6 }
      END { if (n) printf "%.0f %s %s", e, (nc ? "-" : sprintf("%.0f", c)), (nc ? "-" : sprintf("%.0f", l)) }')

    if [ -z "$line" ]; then
      echo "$(date +%T)  조회불가(Kafka 중단 또는 그룹/토픽 없음)"
    else
      local e c l
      read -r e c l <<< "$line"
      if [ "$c" = "-" ]; then
        c="없음        "   # 한글 2글자 = 4칸, 숫자 칸(12)에 맞춰 공백 직접 추가
      else
        c=$(printf "%-12s" "$(commas "$c")")
      fi
      [ "$l" != "-" ] && l=$(commas "$l")
      printf "%s  토픽누적=%-12s commit=%s LAG=%s\n" "$(date +%T)" "$(commas "$e")" "$c" "$l"
    fi
    wait_next "$started" "$INTERVAL_KAFKA"
  done
}

# ── [4] DB별 평균 응답 시간 ───────────────────────

mode_db() {
  header "[4] DB별 평균 응답 시간" \
    "주기: ${INTERVAL_DB}초 | 구간: 직전 ${INTERVAL_DB}초에 끝난 요청 | 누적: Rust 기동 이후 | 종료: Ctrl+C"

  local has_prev=0 p_os_s=0 p_os_c=0 p_ch_s=0 p_ch_c=0
  local first=1
  while true; do
    local started m

    # 회차 구분선: 한 회차가 여러 줄(DB별 1줄)이라 경계가 잘 보이도록 회차마다 위에 구분선 출력
    # 첫 회차는 바로 위에 제목 실선이 있으므로 생략
    if [ "$first" -eq 1 ]; then
      first=0
    else
      echo "$SUB_LINE"
    fi

    started=$(now_ns)
    m=$(fetch_metrics)

    if ! echo "$m" | grep -q '^shireguard_consumer_messages_received_total'; then
      echo "$(date +%T)  Backend 응답 없음"
      has_prev=0
      wait_next "$started" "$INTERVAL_DB"
      continue
    fi

    # 요청이 한 번도 없던 DB는 지표가 없으므로 0
    local os_s os_c ch_s ch_c
    read -r os_s os_c ch_s ch_c < <(echo "$m" | awk '
      /^shireguard_storage_request_seconds_sum\{db="opensearch"\}/   { a = $2 }
      /^shireguard_storage_request_seconds_count\{db="opensearch"\}/ { b = $2 }
      /^shireguard_storage_request_seconds_sum\{db="clickhouse"\}/   { c = $2 }
      /^shireguard_storage_request_seconds_count\{db="clickhouse"\}/ { d = $2 }
      END { printf "%s %.0f %s %.0f\n", a + 0, b, c + 0, d }')

    if [ "$has_prev" -eq 1 ] && { [ "$os_c" -lt "$p_os_c" ] || [ "$ch_c" -lt "$p_ch_c" ]; }; then
      echo "$(date +%T)  Rust 재시작 감지, 기준값 초기화"
      has_prev=0
    fi

    local ts
    ts="$(date +%T)"
    db_line "$ts"        "opensearch" "$os_s" "$os_c" "$p_os_s" "$p_os_c" "$has_prev"
    db_line "        "   "clickhouse" "$ch_s" "$ch_c" "$p_ch_s" "$p_ch_c" "$has_prev"

    has_prev=1; p_os_s=$os_s; p_os_c=$os_c; p_ch_s=$ch_s; p_ch_c=$ch_c
    wait_next "$started" "$INTERVAL_DB"
  done
}

# DB 1개 줄 출력: 구간 평균 = (sum 증가분 / count 증가분), 누적 평균 = sum / count
db_line() {
  local ts=$1 name=$2 s=$3 c=$4 ps=$5 pc=$6 has_prev=$7 interval total
  # 세 경우 모두 터미널 표시 폭이 같도록 공백을 맞춤 (한글은 printf 폭 지정으로 맞출 수 없음)
  if [ "$has_prev" -eq 0 ]; then
    interval="구간 -                  "
  elif [ "$c" -gt "$pc" ]; then
    interval=$(awk -v s="$s" -v ps="$ps" -v n=$((c - pc)) 'BEGIN{printf "구간 %4d회 평균 %5.2f초", n, (s-ps)/n}')
  else
    interval="구간 요청 없음          "
  fi
  if [ "$c" -gt 0 ]; then
    total="누적 $(commas "$c")회 평균 $(awk -v s="$s" -v c="$c" 'BEGIN{printf "%.2f", s/c}')초"
  else
    total="누적 요청 없음"
  fi
  printf "%s  %-10s  %s | %s\n" "$ts" "$name" "$interval" "$total"
}

# ── [5] 컨테이너 CPU / 메모리 ─────────────────────

mode_docker() {
  header "[5] 컨테이너 CPU / 메모리" \
    "주기: ${INTERVAL_DOCKER}초 (docker stats 수집 시간 포함) | 대상: $CONTAINERS | 종료: Ctrl+C"

  while true; do
    local started running targets="" stats
    started=$(now_ns)

    if ! running=$(docker ps --format '{{.Names}}' 2>/dev/null); then
      echo "$(date +%T)  Docker 응답 없음"
      wait_next "$started" "$INTERVAL_DOCKER"
      continue
    fi

    # 실행 중인 컨테이너만 조회 (없는 이름이 섞이면 docker stats 전체가 실패하므로)
    for c in $CONTAINERS; do
      grep -qx "$c" <<< "$running" && targets="$targets $c"
    done
    stats=""
    [ -n "$targets" ] && stats=$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' $targets 2>/dev/null)

    echo "$(date +%T)"
    for c in $CONTAINERS; do
      local row
      row=$(grep "^$c|" <<< "$stats")
      if [ -n "$row" ]; then
        IFS='|' read -r _ cpu mem <<< "$row"
        printf "  %-12s CPU %8s  MEM %s\n" "$c" "$cpu" "$mem"
      else
        printf "  %-12s 중지됨\n" "$c"
      fi
    done
    wait_next "$started" "$INTERVAL_DOCKER"
  done
}

# ── 메뉴 ─────────────────────────────────────────

print_help() {
  echo "$LINE"
  echo "  Shire Guard Monitor"
  echo "$LINE"
  echo "사용법: bash scripts/monitor.sh <번호>"
  echo ""
  # 한글은 터미널에서 2칸을 차지해 printf 폭 지정으로 맞출 수 없으므로 공백을 직접 맞춤
  echo "  1  Kafka 이벤트 소비 / DB 로그 저장 / 처리 속도   ${INTERVAL_STORAGE}초 주기"
  echo "  2  Rust 서버 CPU                                  ${INTERVAL_CPU}초 주기"
  echo "  3  Kafka 토픽누적 / commit / LAG                  ${INTERVAL_KAFKA}초 주기"
  echo "  4  DB별 평균 응답 시간                            ${INTERVAL_DB}초 주기"
  echo "  5  컨테이너 CPU / 메모리                          ${INTERVAL_DOCKER}초 주기"
}

case "$1" in
  1) mode_storage ;;
  2) mode_cpu ;;
  3) mode_kafka ;;
  4) mode_db ;;
  5) mode_docker ;;
  "" | -h | --help) print_help ;;
  *)
    echo "알 수 없는 번호: $1"
    echo ""
    print_help
    exit 1
    ;;
esac