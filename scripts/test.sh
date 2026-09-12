#!/bin/bash
set -e

# ── 설정 ──────────────────────────────────────────
VECTOR_URL="http://localhost:8081"
VECTOR_CONTAINER="vector"
OLLAMA_URL="http://localhost:11434/api/chat"

MODEL_1="qwen2.5:14b-instruct"
MODEL_2="hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF"
TRANSLATE_MODEL="translategemma:12b"

SYSTEM_PROMPT="You are a security analysis assistant. Based on the provided evidence, assess the risk as HIGH, MEDIUM, or LOW, and explain your reasoning concisely."
PROMPT="Review the following file analysis evidence: filename invoice_2026.exe, UPX packing detected, VirusTotal 41/72 detections."


# ── 표준 RAW EVENT 정의 (event_id는 인자로 받음) ──────────────────
event_file_download() {
  local eid=$1
  cat <<EOF
{"event_id":"$eid","event_type":"file_download","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","source_ip":"185.220.101.45","destination_domain":"cdn-update-service.net","file_name":"invoice_2026.exe","file_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","file_size_bytes":245760}
EOF
}

event_login_failure() {
  local eid=$1
  cat <<EOF
{"event_id":"$eid","event_type":"login_failure","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","source_ip":"185.220.101.45","username":"admin","attempt_count":12,"target_service":"ssh"}
EOF
}

event_dns_beacon() {
  local eid=$1
  cat <<EOF
{"event_id":"$eid","event_type":"dns_beacon","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","source_ip":"198.51.100.77","destination_domain":"telemetry-sync.info","hostname":"WKS-1183","request_interval_seconds":60}
EOF
}

list_events() {
  echo "사용 가능한 event_type:"
  echo "  file_download  - AbuseIPDB + DNS + VirusTotal 전부 검증"
  echo "  login_failure  - AbuseIPDB만 검증"
  echo "  dns_beacon     - AbuseIPDB + DNS 검증 (파일 없음)"
}


# ── 이벤트(Vector→Kafka/RedPanda/OpenSearch/ClickHouse) 테스트 ──
test_event() {
  local event_type=$1
  local start=${2:-1}
  local count=${3:-1}

  if [ "$event_type" == "list" ] || [ "$event_type" == "ls" ]; then
    list_events
    return
  fi

  local fn
  case "$event_type" in
    file_download) fn=event_file_download ;;
    login_failure) fn=event_login_failure ;;
    dns_beacon)    fn=event_dns_beacon ;;
    "")            fn=event_file_download ;;
    *)
      echo "알 수 없는 event_type: $event_type"
      list_events
      exit 1
      ;;
  esac

  local end=$((start + count - 1))
  echo "▶ evt-$start ~ evt-$end 전송 시작 ($event_type, 총 $count 건)"

  local n=$start

  for i in $(seq 1 "$count"); do
    local payload
    payload=$("$fn" "evt-$n")

    curl -sS \
      --connect-timeout 1 \
      --max-time 5 \
      -H "Content-Type: application/json" \
      --data "$payload" \
      "$VECTOR_URL" > /dev/null

    n=$((n + 1))
  done

  echo "▶ $count 건 전송 완료 (evt-$start ~ evt-$end)"
  echo "▶ 변환 결과 (최근 로그 1줄):"

  for _ in {1..5}; do
    local log_line
    log_line=$(
      docker logs --tail 5 "$VECTOR_CONTAINER" 2>/dev/null \
        | grep -E '^\{' \
        | tail -n 1
    )

    if [ -n "$log_line" ]; then
      echo "$log_line" | sed 's/,/,\n  /g'
      break
    fi

    sleep 0.1
  done
}


# ── 고속 대량 전송 (병렬) ──────────────────────────
# ./test.sh fast dns_beacon 1 1000000 200
#   → evt-1부터 100만 건, 동시 200개 커넥션으로 전송
test_event_fast() {
  local event_type=$1
  local start=${2:-1}
  local count=${3:-1000}
  local parallel=${4:-100}

  if [ "$event_type" == "list" ] || [ "$event_type" == "ls" ]; then
    list_events
    return
  fi

  local fn
  case "$event_type" in
    file_download) fn=event_file_download ;;
    login_failure) fn=event_login_failure ;;
    dns_beacon)    fn=event_dns_beacon ;;
    "")            fn=event_file_download ;;
    *)
      echo "알 수 없는 event_type: $event_type"
      list_events
      exit 1
      ;;
  esac

  local end=$((start + count - 1))
  echo "▶ evt-$start ~ evt-$end 고속 전송 시작 ($event_type, 총 $count 건, 동시 $parallel)"

  export -f event_file_download event_login_failure event_dns_beacon
  export VECTOR_URL

  seq "$start" "$end" | xargs -P "$parallel" -I {} bash -c "
    payload=\$($fn \"evt-{}\")
    curl -sS --connect-timeout 1 --max-time 5 \
      -H 'Content-Type: application/json' \
      --data \"\$payload\" \
      \"\$VECTOR_URL\" > /dev/null
  "

  echo "▶ $count 건 전송 완료 (evt-$start ~ evt-$end)"
}


json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

translate_to_korean() {
  local text=$1
  local escaped_text
  escaped_text=$(json_escape "$text")

  local payload="{\"model\":\"$TRANSLATE_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"Translate to Korean: $escaped_text\"}],\"stream\":false,\"keep_alive\":\"30m\",\"options\":{\"num_predict\":4096}}"

  curl -s -X POST "$OLLAMA_URL" \
    -H "Content-Type: application/json" \
    -d "$payload"
}


resolve_model() {
  case "$1" in
    1) echo "$MODEL_1" ;;
    2) echo "$MODEL_2" ;;
    "") echo "$MODEL_1" ;;
    *) echo "$1" ;;
  esac
}

list_models() {
  echo "번호로 지정 가능한 모델:"
  echo "  1) $MODEL_1"
  echo "  2) $MODEL_2"
}

test_ollama() {
  local model_input=$1
  local translate_flag=$2

  if [ "$model_input" == "list" ] || [ "$model_input" == "ls" ]; then
    list_models
    return
  fi

  local model
  model=$(resolve_model "$model_input")

  echo "▶ Ollama 모델 [$model] 호출 중..."

  local payload="{\"model\":\"$model\",\"messages\":[{\"role\":\"system\",\"content\":\"$SYSTEM_PROMPT\"},{\"role\":\"user\",\"content\":\"$PROMPT\"}],\"stream\":false,\"keep_alive\":\"30m\"}"

  local response
  response=$(curl -s -X POST "$OLLAMA_URL" \
    -H "Content-Type: application/json" \
    -d "$payload")

  echo "$response" | sed 's/,/,\n  /g'

  if [ "$translate_flag" == "--ko" ]; then
    echo ""
    echo "▶ 한국어 번역 중... (translategemma:12b)"
    local content
    content=$(echo "$response" | grep -oP '"content":"\K[^"]*(?=")')
    translate_to_korean "$content" | sed 's/,/,\n  /g'
  fi
}

print_help() {
  echo "Shire Guard 테스트 스크립트"
  echo ""
  echo "  ./test.sh event                                       file_download 이벤트 1건 전송 (evt-1)"
  echo "  ./test.sh event list                                  사용 가능한 event_type 목록 확인"
  echo "  ./test.sh event <event_type> [시작번호] [반복횟수]     event_id를 evt-{시작번호}부터 순차 증가시키며 N건 전송"
  echo "                                                         예: ./test.sh event login_failure 2000 100 → evt-2000~evt-2099"
  echo "  ./test.sh fast <event_type> [시작번호] [반복횟수] [동시성]  병렬(xargs)로 대량 전송, 기본 동시성 100"
  echo "                                                         예: ./test.sh fast dns_beacon 1 1000000 200 → evt-1~evt-1000000, 동시 200개"
  echo "  ./test.sh ollama list                번호별 실제 모델명 확인"
  echo "  ./test.sh ollama [번호|모델명] [--ko] AI 위험도 분석 테스트 (1:Qwen 2:Foundation-Sec, --ko: 한국어 번역)"
}

# ── 진입점 ────────────────────────────────────────
case "$1" in
  event)
    test_event "$2" "$3" "$4"
    ;;
  fast)
    test_event_fast "$2" "$3" "$4" "$5"
    ;;
  ollama)
    test_ollama "$2" "$3"
    ;;
  --help|-h|"")
    print_help
    ;;
  *)
    echo "알 수 없는 명령어: $1"
    echo "도움말은 './test.sh --help' 참고"
    exit 1
    ;;
esac