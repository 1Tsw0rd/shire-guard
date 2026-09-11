// Shire Guard 애플리케이션에서 사용할 Prometheus metric을 정의하고 관리하는 모듈
//
// 이 파일에서는 두 가지 종류의 정보를 관리한다.
//
// 1. Consumer 처리 metric
//    - Message Broker에서 메시지를 몇 개 받았는지
//    - RawEvent 역직렬화에 몇 개 성공했는지
//    - 역직렬화에 몇 개 실패했는지
//
// 2. 현재 설정된 Message Broker 정보
//    - docker/.env의 MESSAGE_BROKER 값을 기준으로 현재 어떤 Broker(Kafka/RedPanda)를 사용하는지 표시
//
// 생성한 metric은 Registry에 등록하고,
// Rust Axum의 /metrics API에서 Prometheus가 읽을 수 있는 텍스트 형식으로 변환해서 반환한다.

use prometheus::{
    Encoder, GaugeVec, IntCounter, IntGauge, Opts, Registry, TextEncoder
};

use crate::common::error::AppError;
use crate::config::{BrokerConfig, MessageBroker};

pub struct Metrics {
    registry: Registry,
    pub consumer_messages_received: IntCounter,
    pub consumer_events_parsed: IntCounter,
    pub consumer_events_failed: IntCounter,
    pub consumer_broker_connected: IntGauge,
}

impl Metrics {
    pub fn new(broker_config: &BrokerConfig) -> Result<Self, AppError> {
        // 빈 Registry 생성: 아래에서 만드는 metric들을 여기 등록해야 나중에 /metrics에서 노출됨
        let registry = Registry::new();

        // IntCounter::new(name, help)
        // name은 Prometheus에 실제로 노출되는 metric 이름
        // help는 `# HELP` 줄로 출력되는 설명
        // 이 시점엔 아직 Registry에 등록 전이라 /metrics엔 안 보임
        let consumer_messages_received = IntCounter::new(
            "shireguard_consumer_messages_received_total",
            "Consumer가 브로커(Kafka/RedPanda)로부터 수신한 메시지 수",
        )
        .map_err(|e| {
            AppError::Internal(format!(
                "shireguard_consumer_messages_received_total 생성 실패: {e}"
            ))
        })?;

        let consumer_events_parsed = IntCounter::new(
            "shireguard_consumer_events_parsed_total",
            "RawEvent 역직렬화에 성공한 이벤트 수",
        )
        .map_err(|e| {
            AppError::Internal(format!(
                "shireguard_consumer_events_parsed_total 생성 실패: {e}"
            ))
        })?;

        let consumer_events_failed = IntCounter::new(
            "shireguard_consumer_events_failed_total",
            "RawEvent 역직렬화에 실패한 이벤트 수 (빈 메시지 포함)",
        )
        .map_err(|e| {
            AppError::Internal(format!(
                "shireguard_consumer_events_failed_total 생성 실패: {e}"
            ))
        })?;

        let consumer_broker_connected = IntGauge::new(
            "shireguard_consumer_broker_connected",
            "Consumer와 Broker 현재 연결 상태 (0=끊김, 1=연결됨)",
        )
        .map_err(|e| {
            AppError::Internal(format!("shireguard_consumer_broker_connected 생성 실패: {e}"))
        })?;

        // 현재 docker/.env 안에 MESSAGE_BROKER 설정값을 노출하는 게이지로 Grafana에서 표시
        // Opts::new(name, help)
        // &["broker"]는 이 게이지를 "broker"라는 label로 나눠서 관리하겠다는 선언 (아직 실제 값은 안 정해짐)
        let config_active_broker = GaugeVec::new(
            Opts::new(
                "shireguard_config_active_broker",
                "현재 MESSAGE_BROKER 설정값 (해당 broker label만 1로 노출)",
            ),
            &["broker"],
        )
        .map_err(|e| {
            AppError::Internal(format!("shireguard_config_active_broker 생성 실패: {e}"))
        })?;

        let broker_label = match broker_config.broker {
            MessageBroker::Kafka => "kafka",
            MessageBroker::RedPanda => "redpanda"
        };

        // 하나를 찾거나 새로 만들고, set(1.0)이 그 값을 1로 세팅함
        // 반대쪽 label(kafka)은 이 함수를 안 부르니까 시리즈 자체가 안 생김
        //
        // 여기서 "시리즈(series)"는 Prometheus에서 metric 이름 + label 조합 하나를 의미
        // 예: shireguard_config_active_broker{broker="redpanda"} 는 하나의 시리즈
        config_active_broker
            .with_label_values(&[broker_label])
            .set(1.0);

        // clone(): Metrics 구조체에서도 이 Counter를 계속 사용해야 하므로 같은 metric을 가리키는 핸들을 하나 더 만든다.
        //
        // Box::new(): metric을 Heap에 저장하고, Heap에 저장된 metric을 가리키는 Box를 만든다.
        // Registry는 서로 다른 종류의 metric(IntCounter, GaugeVec 등)을 Collector라는 공통 trait으로 관리하기 때문에, metric을 Box로 감싸서 전달한다.
        //
        // register(): Box로 감싼 metric을 Registry에 등록한다.
        // 등록된 metric은 나중에 gather()를 호출하면 Prometheus 출력 대상에 포함된다.
        registry
            .register(Box::new(consumer_messages_received.clone()))
            .map_err(|e| AppError::Internal(format!("metric 등록 실패: {e}")))?;
        registry
            .register(Box::new(consumer_events_parsed.clone()))
            .map_err(|e| AppError::Internal(format!("metric 등록 실패: {e}")))?;
        registry
            .register(Box::new(consumer_events_failed.clone()))
            .map_err(|e| AppError::Internal(format!("metric 등록 실패: {e}")))?;
        // config_active_broker는 이후에 값을 다시 바꿀 일이 없어서(시작 시 1회 세팅) .clone() 없이 통째로 넘김
        registry
            .register(Box::new(config_active_broker))
            .map_err(|e| AppError::Internal(format!("metric 등록 실패: {e}")))?;
        registry
            .register(Box::new(consumer_broker_connected.clone()))
            .map_err(|e| AppError::Internal(format!("metric 등록 실패: {e}")))?;

        Ok(Self {
            registry,
            consumer_messages_received,
            consumer_events_parsed,
            consumer_events_failed,
            consumer_broker_connected,
        })
    }

    // Rust Axum Api /metrics 핸들러에서 사용: registry에 등록된 모든 metric을 Prometheus 텍스트 포맷으로 인코딩
    // (로컬 확인: curl http://localhost:3001/metrics)
    pub fn encode(&self) -> Result<String, AppError> {
        // gather() — 등록된 모든 metric을 지금 이 순간 값으로 전부 읽어서 리스트로 반환
        // "스냅샷(snapshot)"은 metric의 값을 지금 이 순간 한 번 읽어서 모아놓은 결과를 의미한다.
        //
        // 예를 들어 현재 값이:
        // consumer_messages_received = 10
        // consumer_events_parsed     = 8
        // consumer_events_failed     = 2
        //
        // 이 상태에서 gather()를 호출하면
        // "현재 시점의 값 10, 8, 2"를 담은 스냅샷이 만들어진다.
        //
        // 이후 Counter 값이 11, 9, 2로 변하더라도 이미 만들어진 스냅샷 자체가 자동으로 변경되는 것은 아니다.
        // /metrics 요청이 다시 들어와 gather()를 호출하면 그때의 최신 값을 다시 읽어서 새로운 스냅샷을 만든다.
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        // 스냅샷을 실제 Prometheus 텍스트 포맷(# HELP ...` 형식)으로 변환해서 buffer(바이트 배열)에 써넣음
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .map_err(|e| AppError::Internal(format!("metric 인코딩 실패: {e}")))?;

        String::from_utf8(buffer)
            .map_err(|e| AppError::Internal(format!("metric 인코딩 실패: {e}")))
    }
}