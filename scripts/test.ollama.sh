#!/bin/bash
set -e

# ── 설정 ──────────────────────────────────────────
OLLAMA_URL="http://localhost:11434/api/chat"

MODEL_1="qwen2.5:14b-instruct"
MODEL_2="hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF"
TRANSLATE_MODEL="translategemma:12b"

SYSTEM_PROMPT="You are a security analysis assistant. Based on the provided evidence, assess the risk as HIGH, MEDIUM, or LOW, and explain your reasoning concisely."
PROMPT="Review the following file analysis evidence: filename invoice_2026.exe, UPX packing detected, VirusTotal 41/72 detections."

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

translate_to_korean() {
  local text="$1"  
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
  local model_input="$1"
  local translate_flag="$2"

  if [ "$model_input" == "list" ] || [ "$model_input" == "ls" ]; then
    list_models
    return
  fi

  local model
  model=$(resolve_model "$model_input")

  echo "▶ Ollama 모델 [$model] 호출 중..."

  local payload="{\"model\":\"$model\",\"messages\":[{\"role\":\"system\",\"content\":\"$SYSTEM_PROMPT\"},{\"role\":\"user\",\"content\":\"$PROMPT\"}],\"stream\":false,\"keep_alive\":\"30m\"}"

  local response
  response=$(curl -sS --fail -X POST "$OLLAMA_URL" \
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
  echo "Shire Guard 테스트 스크립트(Ollama 전용)"
  echo "  ./test.ollama.sh ollama list                번호별 실제 모델명 확인"
  echo "  ./test.ollama.sh ollama [번호|모델명] [--ko] AI 위험도 분석 테스트 (1:Qwen 2:Foundation-Sec, --ko: 한국어 번역)"
}

# ── 진입점 ────────────────────────────────────────
case "$1" in
  ollama)
    test_ollama "$2" "$3"
    ;;
  --help|-h|"")
    print_help
    ;;
  *)
    echo "알 수 없는 명령어: $1"
    echo "도움말은 './test.ollama.sh --help' 참고"
    exit 1
    ;;
esac