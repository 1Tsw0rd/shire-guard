#!/bin/bash
set -e

COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../docker" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$COMPOSE_DIR/.env" ]; then
  set -a
  source "$COMPOSE_DIR/.env"
  set +a
fi

# ── 공통 명령 (모든 서비스 공용) ──────────────────
common_cmd() {
  local container=$1
  local action=$2
  shift 2

  case "$action" in
    logs)
      docker logs -f "$container"
      ;;
    inspect)
      docker inspect "$container"
      ;;
    exec)
      if [ -z "$1" ]; then
        docker exec -it "$container" sh
      else
        docker exec -it "$container" "$@"
      fi
      ;;
    start)
      docker start "$container"
      ;;
    stop)
      docker stop "$container"
      ;;
    restart)
      docker restart "$container"
      ;;
    *)
      return 1
      ;;
  esac
}

# ── Ollama 관련 ──────────────────────────────────
cmd_ollama() {
  common_cmd ollama "$1" "${@:2}" && return

  case "$1" in
    list|ls)
      docker exec -it ollama ollama list
      ;;
    ps)
      docker exec -it ollama ollama ps
      ;;
    pull)
      if [ -z "$2" ]; then
        echo "사용법: shire ollama pull <모델명>"
        exit 1
      fi
      docker exec -it ollama ollama pull "$2"
      ;;
    run)
      if [ -z "$2" ]; then
        echo "사용법: shire ollama run <모델명>"
        exit 1
      fi
      docker exec -it ollama ollama run "$2"
      ;;
    stop)
      if [ -z "$2" ]; then
        echo "사용법: shire ollama stop <모델명>"
        exit 1
      fi
      docker exec -it ollama ollama stop "$2"
      ;;
    *)
      echo "사용법: shire ollama [logs|inspect|exec|start|stop|restart|list|ps|pull <모델명>|run <모델명>|stop <모델명>]"
      exit 1
      ;;
  esac
}

# ── Vector 관련 ──────────────────────────────────
cmd_vector() {
  common_cmd vector "$1" "${@:2}" && return

  case "$1" in
    validate)
      docker exec -it vector vector validate //etc/vector/vector.toml
      ;;
    *)
      echo "사용법: shire vector [logs|inspect|exec|start|stop|restart|validate]"
      exit 1
      ;;
  esac
}

# ── Kafka 관련 ───────────────────────────────────
cmd_kafka() {
  common_cmd kafka "$1" "${@:2}" && return

  case "$1" in
    topics)
      docker exec -it kafka //opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
      ;;
    consume)
      # 임시 디버깅용 — Rust Consumer 구현 전까지 사용
      if [ -z "$2" ]; then
        echo "사용법: shire kafka consume <토픽명>"
        exit 1
      fi
      docker exec -it kafka //opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$2" \
        --from-beginning
      ;;
    *)
      echo "사용법: shire kafka [logs|inspect|exec|start|stop|restart|topics|consume <토픽명>]"
      exit 1
      ;;
  esac
}

# ── OpenSearch 관련 ──────────────────────────────
cmd_opensearch() {
  common_cmd opensearch "$1" "${@:2}" && return

  case "$1" in
    ping)
      curl -s http://localhost:9200
      echo ""
      ;;
    *)
      echo "사용법: shire opensearch [logs|inspect|exec|start|stop|restart|ping]"
      exit 1
      ;;
  esac
}

# ── ClickHouse 관련 ──────────────────────────────
cmd_clickhouse() {
  common_cmd clickhouse "$1" "${@:2}" && return

  case "$1" in
    cli)
      docker exec -it clickhouse clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}"
      ;;
    *)
      echo "사용법: shire clickhouse [logs|inspect|exec|start|stop|restart|cli]"
      exit 1
      ;;
  esac
}

# ── Grafana 관련 ──────────────────────────────────
cmd_grafana() {
  common_cmd grafana "$1" "${@:2}" && return

  case "$1" in
    *)
      echo "사용법: shire grafana [logs|inspect|exec|start|stop|restart]"
      exit 1
      ;;
  esac
}

# ── 전체 서비스 관리 ──────────────────────────────
cmd_up() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" up -d
}

cmd_down() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" down
}

cmd_start() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" start
}

cmd_stop() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" stop
}

cmd_restart() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" restart
}

cmd_ps() {
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" ps
}

cmd_logs() {
  if [ -z "$1" ]; then
    docker compose -f "$COMPOSE_DIR/docker-compose.yml" logs -f
  else
    docker compose -f "$COMPOSE_DIR/docker-compose.yml" logs -f "$1"
  fi
}

cmd_stats() {
  docker stats
}

print_help() {
  echo "Shire Guard CLI"
  echo ""
  echo "전체 서비스 관리:"
  echo "  shire up                              전체 서비스 생성 및 실행"
  echo "  shire down                            전체 서비스 종료 및 컨테이너 제거"
  echo "  shire start                           전체 서비스 시작 (컨테이너 유지, 중지 상태에서)"
  echo "  shire stop                            전체 서비스 중지 (컨테이너 유지)"
  echo "  shire restart                         전체 서비스 재시작"
  echo "  shire ps                              전체 서비스 상태 확인"
  echo "  shire logs [서비스명]                  로그 확인 (생략 시 전체)"
  echo "  shire stats                           전체 컨테이너 리소스 사용량 확인"
  echo ""
  echo "개별 서비스 공통 명령 (ollama, vector, kafka, opensearch, clickhouse, grafana):"
  echo "  shire <서비스명> logs                  해당 서비스 로그 확인"
  echo "  shire <서비스명> inspect               해당 서비스 상세 정보 확인"
  echo "  shire <서비스명> exec [명령어]          컨테이너 내부 접속 또는 명령 실행"
  echo "  shire <서비스명> start                 해당 서비스만 시작"
  echo "  shire <서비스명> stop                  해당 서비스만 중지"
  echo "  shire <서비스명> restart               해당 서비스만 재시작"
  echo ""
  echo "Ollama 전용 명령:"
  echo "  shire ollama list                     설치된 모델 목록"
  echo "  shire ollama ps                       현재 로드된 모델 확인"
  echo "  shire ollama pull <모델명>             모델 설치"
  echo "  shire ollama run <모델명>              대화형 실행"
  echo "  shire ollama stop <모델명>             모델 강제 언로드"
  echo ""
  echo "Vector 전용 명령:"
  echo "  shire vector validate                 설정 파일 문법 검증"
  echo ""
  echo "Kafka 전용 명령:"
  echo "  shire kafka topics                    토픽 목록 확인"
  echo "  shire kafka consume <토픽명>           토픽 메시지 확인 (임시 디버깅용)"
  echo ""
  echo "OpenSearch 전용 명령:"
  echo "  shire opensearch ping                 상태 확인"
  echo ""
  echo "ClickHouse 전용 명령:"
  echo "  shire clickhouse cli                  대화형 클라이언트 접속"
  echo ""
  echo "Grafana 전용 명령:"
  echo "  (공통 명령만 사용)"
}

# ── 진입점 ───────────────────────────────────────
case "$1" in
  ollama|vector|kafka|opensearch|clickhouse|grafana)
    service="$1"
    shift
    "cmd_${service}" "$@"
    ;;
  up)
    cmd_up
    ;;
  down)
    cmd_down
    ;;
  start)
    cmd_start
    ;;
  stop)
    cmd_stop
    ;;
  restart)
    cmd_restart
    ;;
  ps)
    cmd_ps
    ;;
  logs)
    shift
    cmd_logs "$@"
    ;;
  stats)
    cmd_stats
    ;;
  --help|-h|"")
    print_help
    ;;
  *)
    echo "알 수 없는 명령어: $1"
    echo "도움말은 'shire --help' 또는 'shire -h' 참고"
    exit 1
    ;;
esac