#!/bin/bash
set -e

# ── 설정 ──────────────────────────────────────────
# Ollama 채팅 API 주소
OLLAMA_URL="http://localhost:11434/api/chat"

# 번호로 호출할 모델
MODEL_1="qwen2.5:14b-instruct"
MODEL_2="hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF"

# --ko 옵션 사용 시 응답 번역에 쓰는 모델
TRANSLATE_MODEL="translategemma:12b"

# AI 지침: 모델의 역할과 응답 지침을 지정함, 현재는 보안 분석 및 위험도 평가를 위한 지침이 설정되어 있음
SYSTEM_PROMPT="You are a security analysis assistant. Based on the provided evidence, assess the risk as HIGH, MEDIUM, or LOW, and explain your reasoning concisely."

# AI 분석 내용: 일반 질문이나 보안 이벤트·분석 증거 등 테스트할 내용을 지정
PROMPT="Review the following file analysis evidence: filename invoice_2026.exe, UPX packing detected, VirusTotal 41/72 detections."

translate_to_korean() {
  # 응답에서 추출한 content는 이미 JSON 문자열로 이스케이프된 상태라 그대로 사용
  local escaped_text="$1"
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
    echo "▶ 한국어 번역 중... ($TRANSLATE_MODEL)"

    local content
    content=$(echo "$response" | grep -oP '"content":"\K(?:[^"\\]|\\.)*')
    translate_to_korean "$content" | sed 's/,/,\n  /g'
    echo ""
  fi
}

print_help() {
  echo "Ollama 모델을 직접 호출해 응답을 확인하는 수동 테스트 도구"
  echo ""
  echo "사용법 (scripts 폴더에서 실행)"
  echo ""
  echo "1. 테스트 목적에 맞게 스크립트 상단의 SYSTEM_PROMPT와 PROMPT 상수를 수정"
  echo ""
  echo "2. 실행"
  echo "  ./test.ollama.sh ollama list          번호별 모델명 확인"
  echo "  ./test.ollama.sh ollama [번호|모델명] [--ko] AI 위험도 분석 테스트 (1:Qwen 2:Foundation-Sec, --ko: 한국어 번역)"
  echo ""
  echo "   사용 예시: "
  echo "  ./test.ollama.sh ollama 1             Qwen2.5 14B 호출"
  echo "  ./test.ollama.sh ollama 2             Foundation-Sec 8B 호출"
  echo "  ./test.ollama.sh ollama <모델명>       설치된 다른 모델이 있는 경우 직접 지정"
  echo "  ./test.ollama.sh ollama 1 --ko        응답을 한국어로 번역"
  echo "                                        설치: docker exec -it ollama ollama pull $TRANSLATE_MODEL"
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