// 이 파일은 common/ 안에 두지 않고 src/ 최상위에 둠
//
// common/은 "여러 도메인이 동등하게 재사용하는 유틸"을 모으는 곳인데,
// 이 파일은 그런 성격이 아니라 main.rs가 애플리케이션을 조립하기 위해
// 직접 소비하는 "진입점의 부속품"에 가까움
//
// 규칙: 환경변수(.env)는 오직 이 파일에서만 읽는다.
// 다른 모든 모듈(core/, domains/, common/clients/ 등)은 여기서 만들어진
// 설정 구조체를 인자로 전달받기만 하고, std::env::var()를 직접 호출하지 않는다.
//
// MESSAGE_BROKER, KAFKA_*, REDPANDA_* 값은 전부 docker/.env가 원본이다.
// (docker/.env가 인프라 접속 정보의 단일 원본, backend/.env는 DATABASE_URL/RUST_LOG 등
// Rust 애플리케이션 전용 값만 소유)

use crate::common::error::AppError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBroker {
    Kafka,
    RedPanda,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub broker: MessageBroker,
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub rust_log: String,
    pub broker: BrokerConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, AppError> {
        Ok(Self {
            // Application
            database_url: std::env::var("DATABASE_URL").map_err(|_| {
                AppError::Internal("DATABASE_URL 환경변수가 설정되지 않았습니다.".into())
            })?,
            rust_log: std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,tower_http=debug".into()),

            // Message Broker (Kafka / RedPanda)
            broker: BrokerConfig::from_env()?,
        })
    }
}

impl BrokerConfig {
    pub fn from_env() -> Result<Self, AppError> {
        // MESSAGE_BROKER 환경변수로 Kafka 또는 RedPanda 선택
        let broker_type = std::env::var("MESSAGE_BROKER").map_err(|_| {
            AppError::Internal("MESSAGE_BROKER 환경변수가 설정되지 않았습니다.".into())
        })?;

        // Broker 접속 정보
        // .ok()로 Option<String> 유지 — "없음"과 "빈 문자열"을 구분 가능한 상태로 넘김
        let kafka_host = std::env::var("KAFKA_HOST").ok();
        let kafka_external_port = std::env::var("KAFKA_EXTERNAL_PORT").ok();
        let redpanda_host = std::env::var("REDPANDA_HOST").ok();
        let redpanda_port = std::env::var("REDPANDA_PORT").ok();

        let (broker, brokers) = Self::resolve(
            &broker_type,
            kafka_host.as_deref(), // Option<String> → Option<&str>
            kafka_external_port.as_deref(),
            redpanda_host.as_deref(),
            redpanda_port.as_deref(),
        )?;

        // Consumer 설정
        let topic = std::env::var("KAFKA_TOPIC")
            .map_err(|_| AppError::Internal("KAFKA_TOPIC 환경변수가 설정되지 않았습니다.".into()))?;
        let group_id = std::env::var("KAFKA_GROUP_ID").map_err(|_| {
            AppError::Internal("KAFKA_GROUP_ID 환경변수가 설정되지 않았습니다.".into())
        })?;

        Ok(Self {
            broker,
            brokers,
            topic,
            group_id,
        })
    }

    // MESSAGE_BROKER 값에 따라 사용할 브로커와 접속 주소(host:port)를 결정
    // Rust는 호스트에서 cargo run으로 실행되므로, Kafka는 컨테이너 내부용 포트(KAFKA_PORT)가 아니라
    // 호스트에서 직접 접속 가능한 EXTERNAL 리스너 포트(KAFKA_EXTERNAL_PORT)를 사용해야 함
    fn resolve(
        broker_type: &str,
        kafka_host: Option<&str>,
        kafka_external_port: Option<&str>,
        redpanda_host: Option<&str>,
        redpanda_port: Option<&str>,
    ) -> Result<(MessageBroker, String), AppError> {
        match broker_type {
            "kafka" => {
                let host = kafka_host.ok_or_else(|| {
                    AppError::Internal(
                        "MESSAGE_BROKER=kafka인데 KAFKA_HOST 환경변수가 설정되지 않았습니다.".into(),
                    )
                })?;
                let port = kafka_external_port.ok_or_else(|| {
                    AppError::Internal(
                        "MESSAGE_BROKER=kafka인데 KAFKA_EXTERNAL_PORT 환경변수가 설정되지 않았습니다.".into(),
                    )
                })?;
                Ok((MessageBroker::Kafka, format!("{host}:{port}")))
            }
            "redpanda" => {
                let host = redpanda_host.ok_or_else(|| {
                    AppError::Internal(
                        "MESSAGE_BROKER=redpanda인데 REDPANDA_HOST 환경변수가 설정되지 않았습니다.".into(),
                    )
                })?;
                let port = redpanda_port.ok_or_else(|| {
                    AppError::Internal(
                        "MESSAGE_BROKER=redpanda인데 REDPANDA_PORT 환경변수가 설정되지 않았습니다.".into(),
                    )
                })?;
                Ok((MessageBroker::RedPanda, format!("{host}:{port}")))
            }
            other => Err(AppError::Internal(format!(
                "지원하지 않는 MESSAGE_BROKER입니다: {other}"
            ))),
        }
    }
}

// cargo test --lib config
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: Kafka를 선택하면 Kafka와 localhost:19092를 반환
    #[test]
    fn resolve_kafka_broker() {
        let (broker, brokers) =
            BrokerConfig::resolve("kafka", Some("localhost"), Some("19092"), None, None)
                .unwrap();

        assert_eq!(broker, MessageBroker::Kafka);
        assert_eq!(brokers, "localhost:19092");
    }

    // 시나리오 2: RedPanda를 선택하면 RedPanda와 localhost:29092를 반환
    #[test]
    fn resolve_redpanda_broker() {
        let (broker, brokers) =
            BrokerConfig::resolve("redpanda", None, None, Some("localhost"), Some("29092"))
                .unwrap();

        assert_eq!(broker, MessageBroker::RedPanda);
        assert_eq!(brokers, "localhost:29092");
    }

    // 시나리오 3: 지원하지 않는 브로커를 지정하면 에러를 반환
    #[test]
    fn resolve_unknown_broker_fails() {
        let result = BrokerConfig::resolve("unknown", None, None, None, None);

        assert!(result.is_err());
    }

    // 시나리오 4: Kafka인데 host가 없으면 에러를 반환
    #[test]
    fn resolve_kafka_missing_host_fails() {
        let result = BrokerConfig::resolve("kafka", None, Some("29092"), None, None);

        assert!(result.is_err());
    }

    // 시나리오 5: Kafka인데 external port가 없으면 에러를 반환
    #[test]
    fn resolve_kafka_missing_port_fails() {
        let result = BrokerConfig::resolve("kafka", Some("localhost"), None, None, None);

        assert!(result.is_err());
    }

    // 시나리오 6: RedPanda인데 host가 없으면 에러를 반환
    #[test]
    fn resolve_redpanda_missing_host_fails() {
        let result = BrokerConfig::resolve("redpanda", None, None, None, Some("29092"));

        assert!(result.is_err());
    }

    // 시나리오 7: RedPanda인데 port가 없으면 에러를 반환
    #[test]
    fn resolve_redpanda_missing_port_fails() {
        let result = BrokerConfig::resolve("redpanda", None, None, Some("localhost"), None);

        assert!(result.is_err());
    }
}