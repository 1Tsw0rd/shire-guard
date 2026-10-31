#!/bin/bash
set -e

COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../docker" && pwd)"
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

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

  # docker 명령이 실패하면 그 상태 코드로 바로 종료 (아래 서비스별 사용법 안내로 넘어가지 않게)
  case "$action" in
    logs)
      docker logs -f "$container" || exit $?
      ;;
    inspect)
      docker inspect "$container" || exit $?
      ;;
    exec)
      if [ -z "$1" ]; then
        docker exec -it "$container" sh || exit $?
      else
        MSYS_NO_PATHCONV=1 docker exec -it "$container" "$@" || exit $?
      fi
      ;;
    start)
      docker start "$container" || exit $?
      ;;
    stop)
      docker stop "$container" || exit $?
      ;;
    restart)
      docker restart "$container" || exit $?
      ;;
    *)
      return 1   # 공통 명령이 아님 → 서비스별 전용 명령으로 넘어감
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
    unload)
      if [ -z "$2" ]; then
        echo "사용법: shire ollama unload <모델명>"
        exit 1
      fi
      docker exec -it ollama ollama stop "$2"
      ;;
    *)
      echo "사용법: shire ollama [logs|inspect|exec|start|stop|restart|list|ps|pull <모델명>|run <모델명>|unload <모델명>]"
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
      docker exec -it -e KAFKA_OPTS= kafka //opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
      ;;
    consume)
      if [ -z "$2" ]; then
        echo "사용법: shire kafka consume <토픽명>"
        exit 1
      fi
      docker exec -it -e KAFKA_OPTS= kafka //opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$2" \
        --from-beginning
      ;;
    groups)
      # Consumer Group 목록
      docker exec -it -e KAFKA_OPTS= kafka //opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "localhost:${KAFKA_PORT:-9092}" \
        --list
      ;;
    lag)
      # Consumer Group의 partition별 commit offset, 토픽 끝 offset, LAG 확인
      # 그룹명을 생략하면 docker/.env의 KAFKA_GROUP_ID 사용
      docker exec -it -e KAFKA_OPTS= kafka //opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "localhost:${KAFKA_PORT:-9092}" \
        --describe --group "${2:-$KAFKA_GROUP_ID}"
      ;;
    reset)
      # 테스트용 offset 초기화 (docker/.env의 KAFKA_GROUP_ID, KAFKA_TOPIC 대상)
      #   latest  : commit 위치를 토픽 끝으로 옮김 -> 남아 있던 이벤트는 처리하지 않고 건너뜀 (LAG 0)
      #   earliest: commit 위치를 토픽 맨 앞으로 옮김 -> 보관 중인 이벤트를 전부 다시 처리
      # --execute 없이 실행하면 옮길 위치만 보여주고 변경하지 않음 (--dry-run)
      # Consumer(Rust)가 실행 중이면 Kafka가 거부함 (그룹에 연결된 Consumer가 없어야 변경 가능)
      if [ "$2" != "latest" ] && [ "$2" != "earliest" ]; then
        echo "사용법: shire kafka reset <latest|earliest> [--execute]"
        exit 1
      fi
      local mode="--dry-run"
      [ "${3:-}" = "--execute" ] && mode="--execute"
      docker exec -it -e KAFKA_OPTS= kafka //opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "localhost:${KAFKA_PORT:-9092}" \
        --group "$KAFKA_GROUP_ID" --topic "$KAFKA_TOPIC" \
        --reset-offsets "--to-$2" "$mode"
      if [ "$mode" = "--dry-run" ]; then
        echo ""
        echo "미리보기만 실행됨 (변경 안 함). 적용하려면: shire kafka reset $2 --execute"
      fi
      ;;
    purge)
      # (테스트용) 토픽 완전 초기화: 메시지를 모두 지우고 offset을 0부터 다시 시작 (docker/.env의 KAFKA_TOPIC, KAFKA_GROUP_ID 대상)
      # 순서: Consumer Group 삭제 -> 토픽 삭제 -> 삭제 완료 대기 -> 토픽 재생성
      local server="localhost:${KAFKA_PORT:-9092}"
      local bin="//opt/kafka/bin"

      if [ "${2:-}" != "--execute" ]; then
        echo "[미리보기] 아래 토픽과 Consumer Group을 삭제하고 토픽을 새로 만듦 (변경 안 함)"
        echo "  토픽: $KAFKA_TOPIC / Consumer Group: $KAFKA_GROUP_ID"
        echo ""
        docker exec -e KAFKA_OPTS= kafka "$bin/kafka-consumer-groups.sh" \
          --bootstrap-server "$server" --describe --group "$KAFKA_GROUP_ID" || true
        echo ""
        echo "적용하려면: shire kafka purge --execute"
        exit 0
      fi

      # 1. Consumer Group 삭제 (있을 때만, 연결된 Consumer가 없어야 함)
      if docker exec -e KAFKA_OPTS= kafka "$bin/kafka-consumer-groups.sh" \
          --bootstrap-server "$server" --list | tr -d '\r' | grep -qx "$KAFKA_GROUP_ID"; then
        local group_info
        group_info=$(docker exec -e KAFKA_OPTS= kafka "$bin/kafka-consumer-groups.sh" \
          --bootstrap-server "$server" --describe --group "$KAFKA_GROUP_ID" 2>&1)
        if ! grep -q "has no active members" <<< "$group_info"; then
          echo "$group_info"
          echo ""
          echo "중단: Consumer Group에 연결된 Consumer가 있음 (Rust 종료 후 다시 실행). 아무것도 삭제하지 않음"
          exit 1
        fi
        docker exec -e KAFKA_OPTS= kafka "$bin/kafka-consumer-groups.sh" \
          --bootstrap-server "$server" --delete --group "$KAFKA_GROUP_ID"
      else
        echo "Consumer Group 없음: $KAFKA_GROUP_ID (삭제 생략)"
      fi

      # 2. 토픽 삭제 (있을 때만)
      if docker exec -e KAFKA_OPTS= kafka "$bin/kafka-topics.sh" \
          --bootstrap-server "$server" --list | tr -d '\r' | grep -qx "$KAFKA_TOPIC"; then
        docker exec -e KAFKA_OPTS= kafka "$bin/kafka-topics.sh" \
          --bootstrap-server "$server" --delete --topic "$KAFKA_TOPIC"

        # 3. 삭제 완료 대기 (최대 약 60초)
        local i
        for i in $(seq 1 20); do
          if ! docker exec -e KAFKA_OPTS= kafka "$bin/kafka-topics.sh" \
              --bootstrap-server "$server" --list | tr -d '\r' | grep -qx "$KAFKA_TOPIC"; then
            break
          fi
          if [ "$i" -eq 20 ]; then
            echo "중단: 토픽 삭제가 끝나지 않음. 잠시 후 shire kafka purge --execute 를 다시 실행"
            exit 1
          fi
          sleep 1
        done
      else
        echo "토픽 없음: $KAFKA_TOPIC (삭제 생략)"
      fi

      # 4. 토픽 재생성
      docker exec -e KAFKA_OPTS= kafka "$bin/kafka-topics.sh" \
        --bootstrap-server "$server" --create --topic "$KAFKA_TOPIC" \
        --partitions 1 --replication-factor 1

      # 5. 결과: partition별 끝 offset (모두 0이어야 함, 형식: 토픽:partition:offset)
      echo ""
      echo "완료. 현재 offset:"
      docker exec -e KAFKA_OPTS= kafka "$bin/kafka-get-offsets.sh" \
        --bootstrap-server "$server" --topic "$KAFKA_TOPIC"
      ;;
    *)
      echo "사용법: shire kafka [logs|inspect|exec|start|stop|restart|topics|consume <토픽명>|groups|lag [그룹명]|reset <latest|earliest> [--execute]|purge [--execute]]"
      exit 1
      ;;
  esac
}

# ── RedPanda 관련 ─────────────────────────────────
cmd_redpanda() {
  common_cmd redpanda "$1" "${@:2}" && return

  case "$1" in
    health)
      docker exec -it redpanda rpk cluster health
      ;;
    info)
      docker exec -it redpanda rpk cluster info
      ;;
    topics)
      docker exec -it redpanda rpk topic list
      ;;
    consume)
      if [ -z "$2" ]; then
        echo "사용법: shire redpanda consume <토픽명>"
        exit 1
      fi
      docker exec -it redpanda rpk topic consume "$2"
      ;;
    groups)
      # Consumer Group 목록
      docker exec -it redpanda rpk group list
      ;;
    lag)
      # Consumer Group의 partition별 commit offset, 토픽 끝 offset, LAG 확인
      # 그룹명을 생략하면 docker/.env의 KAFKA_GROUP_ID 사용
      docker exec -it redpanda rpk group describe "${2:-$KAFKA_GROUP_ID}"
      ;;
    reset)
      # 테스트용 offset 초기화 (docker/.env의 KAFKA_GROUP_ID, KAFKA_TOPIC 대상)
      #   latest  : commit 위치를 토픽 끝으로 옮김 -> 남아 있던 이벤트는 처리하지 않고 건너뜀 (LAG 0)
      #   earliest: commit 위치를 토픽 맨 앞으로 옮김 -> 보관 중인 이벤트를 전부 다시 처리
      # rpk group seek에는 미리보기 옵션이 없어서, --execute 없이 실행하면 현재 offset 상태만 보여주고 변경하지 않음
      local to
      case "$2" in
        latest) to="end" ;;
        earliest) to="start" ;;
        *)
          echo "사용법: shire redpanda reset <latest|earliest> [--execute]"
          exit 1
          ;;
      esac
      if [ "${3:-}" != "--execute" ]; then
        docker exec -it redpanda rpk group describe "$KAFKA_GROUP_ID"
        echo ""
        echo "현재 상태만 출력됨 (변경 안 함). 적용하려면: shire redpanda reset $2 --execute"
        exit 0
      fi
      docker exec -it redpanda rpk group seek "$KAFKA_GROUP_ID" --to "$to" --topics "$KAFKA_TOPIC"
      ;;
    purge)
      # 테스트용 토픽 완전 초기화: 메시지를 모두 지우고 offset을 0부터 다시 시작 (docker/.env의 KAFKA_TOPIC, KAFKA_GROUP_ID 대상)
      # 순서: Consumer Group 삭제 -> 토픽 삭제 -> 토픽 재생성
      if [ "${2:-}" != "--execute" ]; then
        echo "[미리보기] 아래 토픽과 Consumer Group을 삭제하고 토픽을 새로 만듦 (변경 안 함)"
        echo "  토픽: $KAFKA_TOPIC / Consumer Group: $KAFKA_GROUP_ID"
        echo ""
        docker exec redpanda rpk group describe "$KAFKA_GROUP_ID" || true
        echo ""
        echo "적용하려면: shire redpanda purge --execute"
        exit 0
      fi

      # 1. Consumer Group 삭제 (있을 때만, 연결된 Consumer가 없어야 함)
      if docker exec redpanda rpk group list | awk -v g="$KAFKA_GROUP_ID" '$2 == g { found = 1 } END { exit !found }'; then
        local members
        members=$(docker exec redpanda rpk group describe "$KAFKA_GROUP_ID" | awk '$1 == "MEMBERS" { print $2 }')
        if [ "$members" != "0" ]; then
          docker exec redpanda rpk group describe "$KAFKA_GROUP_ID"
          echo ""
          echo "중단: Consumer Group에 연결된 Consumer가 있음 (Rust 종료 후 다시 실행). 아무것도 삭제하지 않음"
          exit 1
        fi
        docker exec redpanda rpk group delete "$KAFKA_GROUP_ID"
      else
        echo "Consumer Group 없음: $KAFKA_GROUP_ID (삭제 생략)"
      fi

      # 2. 토픽 삭제 (있을 때만)
      if docker exec redpanda rpk topic list | awk -v t="$KAFKA_TOPIC" '$1 == t { found = 1 } END { exit !found }'; then
        docker exec redpanda rpk topic delete "$KAFKA_TOPIC"
      else
        echo "토픽 없음: $KAFKA_TOPIC (삭제 생략)"
      fi

      # 3. 토픽 재생성
      docker exec redpanda rpk topic create "$KAFKA_TOPIC" --partitions 1 --replicas 1

      echo ""
      echo "완료. 현재 토픽 목록:"
      docker exec redpanda rpk topic list
      ;;
    *)
      echo "사용법: shire redpanda [logs|inspect|exec|start|stop|restart|health|info|topics|consume <토픽명>|groups|lag [그룹명]|reset <latest|earliest> [--execute]|purge [--execute]]"
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

# ── OpenSearch Dashboards 관련 ────────────────────
cmd_opensearch_dashboards() {
  common_cmd opensearch-dashboards "$1" "${@:2}" && return

  case "$1" in
    *)
      echo "사용법: shire opensearch-dashboards [logs|inspect|exec|start|stop|restart]"
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

# ── PostgreSQL 관련 ───────────────────────────────
cmd_postgres() {
  common_cmd postgres "$1" "${@:2}" && return

  case "$1" in
    cli)
      docker exec -it postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"
      ;;
    *)
      echo "사용법: shire postgres [logs|inspect|exec|start|stop|restart|cli]"
      exit 1
      ;;
  esac
}

# ── Redis 관련 ────────────────────────────────────
cmd_redis() {
  common_cmd redis "$1" "${@:2}" && return

  case "$1" in
    cli)
      docker exec -it redis redis-cli -a "${REDIS_PASSWORD}"
      ;;
    *)
      echo "사용법: shire redis [logs|inspect|exec|start|stop|restart|cli]"
      exit 1
      ;;
  esac
}

# ── Dragonfly 관련 ────────────────────────────────
cmd_dragonfly() {
  common_cmd dragonfly "$1" "${@:2}" && return

  case "$1" in
    cli)
      docker exec -it dragonfly redis-cli -a "${DRAGONFLY_PASSWORD}"
      ;;
    *)
      echo "사용법: shire dragonfly [logs|inspect|exec|start|stop|restart|cli]"
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

# ── Loki 관련 ─────────────────────────────────────
cmd_loki() {
  # Loki 이미지에는 셸(sh/bash)이 없어 인자 없는 exec(대화형 접속)는 불가 → 대신 확인 방법 안내
  if [ "$1" = "exec" ] && [ "$#" -eq 1 ]; then
    echo "Loki 이미지에는 sh/bash가 없어 대화형 셸에 접속할 수 없습니다." >&2
    echo "상태 확인: curl http://localhost:${LOKI_PORT:-3100}/ready" >&2
    echo "로그 확인: shire loki logs" >&2
    echo "설정 파일: $COMPOSE_DIR/loki/config/local-config.yaml" >&2
    exit 1
  fi

  common_cmd loki "$1" "${@:2}" && return

  case "$1" in
    *)
      echo "사용법: shire loki [logs|inspect|exec|start|stop|restart]"
      exit 1
      ;;
  esac
}

# ── Prometheus 관련 ───────────────────────────────
cmd_prometheus() {
  common_cmd prometheus "$1" "${@:2}" && return

  case "$1" in
    *)
      echo "사용법: shire prometheus [logs|inspect|exec|start|stop|restart]"
      exit 1
      ;;
  esac
}

# ── cAdvisor 관련 ─────────────────────────────────
cmd_cadvisor() {
  common_cmd cadvisor "$1" "${@:2}" && return

  case "$1" in
    *)
      echo "사용법: shire cadvisor [logs|inspect|exec|start|stop|restart]"
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
  echo "Shire Guard Docker 서비스 관리 CLI"
  echo ""
  echo "실행 방법:"
  echo "  bash scripts/shire.sh <명령>          저장소 최상위 폴더에서 실행"
  echo "  ./shire.sh <명령>                     scripts 폴더에서 실행"
  echo ""
  echo "shire로 짧게 쓰려면 alias 등록 (현재 터미널에서만 유지, 계속 쓰려면 ~/.bashrc에 추가):"
  echo "  alias shire=\"bash $SCRIPT_PATH\""
  echo ""
  echo "[Shire Guard CLI]"
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
  echo "개별 서비스 공통 명령 (ollama, vector, kafka, redpanda, opensearch, opensearch-dashboards, clickhouse, postgres, redis, dragonfly, grafana, loki, prometheus, cadvisor):"
  echo "  shire <서비스명> logs                  해당 서비스 로그 확인"
  echo "  shire <서비스명> inspect               해당 서비스 상세 정보 확인"
  echo "  shire <서비스명> exec [명령어]          컨테이너 내부 접속 또는 명령 실행 (loki는 셸이 없어 명령 지정만 가능)"
  echo "  shire <서비스명> start                 해당 서비스만 시작"
  echo "  shire <서비스명> stop                  해당 서비스만 중지"
  echo "  shire <서비스명> restart               해당 서비스만 재시작"
  echo ""
  echo "Ollama 전용 명령:"
  echo "  shire ollama list                     설치된 모델 목록"
  echo "  shire ollama ps                       현재 로드된 모델 확인"
  echo "  shire ollama pull <모델명>             모델 설치"
  echo "  shire ollama run <모델명>              대화형 실행"
  echo "  shire ollama unload <모델명>           모델 강제 언로드"
  echo ""
  echo "Vector 전용 명령:"
  echo "  shire vector validate                 설정 파일 문법 검증"
  echo ""
  echo "Kafka 전용 명령:"
  echo "  shire kafka topics                    토픽 목록 확인"
  echo "  shire kafka consume <토픽명>           토픽 메시지 확인"
  echo "  shire kafka groups                    Consumer Group 목록 확인"
  echo "  shire kafka lag [그룹명]               Consumer Group offset / LAG 확인 (생략 시 docker/.env의 KAFKA_GROUP_ID)"
  echo "  shire kafka reset <latest|earliest>   offset 초기화 미리보기"
  echo "  shire kafka reset <...> --execute     offset 초기화 적용 (테스트 전용, Rust 종료 상태에서만 가능)"
  echo "                                        latest: 남은 이벤트를 처리하지 않고 건너뜀 / earliest: 토픽 맨 앞부터 다시 처리"
  echo "  shire kafka purge                     토픽 완전 초기화 미리보기"
  echo "  shire kafka purge --execute           토픽·Consumer Group 삭제 후 토픽 재생성 → offset 0부터 시작"
  echo ""
  echo "RedPanda 전용 명령:"
  echo "  shire redpanda health                 클러스터 헬스체크"
  echo "  shire redpanda info                   클러스터/브로커 정보 확인"
  echo "  shire redpanda topics                 토픽 목록 확인"
  echo "  shire redpanda consume <토픽명>        토픽 메시지 확인"
  echo "  shire redpanda groups                 Consumer Group 목록 확인"
  echo "  shire redpanda lag [그룹명]            Consumer Group offset / LAG 확인 (생략 시 docker/.env의 KAFKA_GROUP_ID)"
  echo "  shire redpanda reset <latest|earliest>  현재 offset 상태 출력"
  echo "  shire redpanda reset <...> --execute  offset 초기화 적용 (테스트 전용, Rust 종료 상태에서 실행)"
  echo "                                        latest: 남은 이벤트를 처리하지 않고 건너뜀 / earliest: 토픽 맨 앞부터 다시 처리"
  echo "  shire redpanda purge                  토픽 완전 초기화 미리보기"
  echo "  shire redpanda purge --execute        토픽·Consumer Group 삭제 후 토픽 재생성 → offset 0부터 시작"
  echo ""
  echo "OpenSearch 전용 명령:"
  echo "  shire opensearch ping                 상태 확인"
  echo ""
  echo "ClickHouse 전용 명령:"
  echo "  shire clickhouse cli                  대화형 클라이언트 접속"
  echo ""
  echo "PostgreSQL 전용 명령:"
  echo "  shire postgres cli                    대화형 클라이언트 접속"
  echo ""
  echo "Redis 전용 명령:"
  echo "  shire redis cli                       대화형 클라이언트 접속"
  echo ""
  echo "Dragonfly 전용 명령:"
  echo "  shire dragonfly cli                   대화형 클라이언트 접속 (redis-cli 호환)"
  echo ""
  echo "OpenSearch Dashboards · Grafana · Loki · Prometheus · cAdvisor 전용 명령:"
  echo "  (공통 명령만 사용)"
}

# ── 진입점 ───────────────────────────────────────
case "$1" in
  ollama|vector|kafka|redpanda|opensearch|opensearch-dashboards|clickhouse|postgres|redis|dragonfly|grafana|loki|prometheus|cadvisor)
    service="$1"
    shift
    "cmd_${service//-/_}" "$@"
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