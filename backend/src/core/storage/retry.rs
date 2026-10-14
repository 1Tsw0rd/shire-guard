/*
DB 저장 재시도 기능
- 대기 간격은 RETRY_INITIAL_DELAY(0.1초)부터 2배씩 늘어나 RETRY_MAX_DELAY(30초)에서 멈추고, 이후 30초 간격으로 성공할 때까지 계속 재시도 (재시도 횟수 제한 없음)
- forwarder.rs(OpenSearch와 ClickHouse 저장)와 failures.rs(PostgreSQL 실패 로그 저장)가 함께 사용
*/

use std::time::Duration;

use tokio::time::sleep;

const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100); // 재시도 간격 시간 (0.1초)
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30); // 재시도 대기 간격의 최대값 (30초)

// pub(crate): src/lib.rs에 선언된 모듈(common, config, core, state 등) 안에서만 사용할 수 있도록 공개
//             lib.rs 밖(main.rs, 외부 프로젝트)에서는 사용 불가
// 예: forwarder.rs, failures.rs에서 RetryDelay::new(), delay.current(), delay.wait().await 호출 가능
pub(crate) struct RetryDelay {
    delay: Duration,
}

impl RetryDelay {
    pub(crate) fn new() -> Self {
        Self {
            delay: RETRY_INITIAL_DELAY,
        }
    }

    pub(crate) fn current(&self) -> Duration {
        self.delay
    }

    pub(crate) async fn wait(&mut self) {
        sleep(self.delay).await;
        self.delay = (self.delay * 2).min(RETRY_MAX_DELAY);
    }
}

// cargo test --lib storage::retry
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: 재시도 대기 시간이 100ms부터 2배씩 증가하고 최대 30초로 제한되는지 확인
    // start_paused = true: Tokio의 가상 시간을 제어해 sleep()의 대기를 실제 시간만큼 기다리지 않고 테스트(예: 30초 설정했다고 30초 다 기다리지 않고, 기다렸다 치고 처리됨)
    #[tokio::test(start_paused = true)]
    async fn retry_delay_doubles_up_to_max() {
        let mut delay = RetryDelay::new();
        // 최초 대기 시간이 100ms인지 확인
        assert_eq!(delay.current(), Duration::from_millis(100));
        // wait(): 현재 대기 시간(100ms)만큼 비동기로 기다린 뒤 다음 대기 시간을 2배로 변경
        delay.wait().await;
        // 100 * 2 = 200ms
        assert_eq!(delay.current(), Duration::from_millis(200));

        // 20번 반복해서 대기 시간이 최대값에 도달하도록 함
        for _ in 0..20 {
            delay.wait().await;
        }
        // 반복 실행 후 대기 시간이 최대값인 30초에 도달했는지 확인
        assert_eq!(delay.current(), RETRY_MAX_DELAY);
    }
}
