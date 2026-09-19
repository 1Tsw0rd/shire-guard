/*
서킷 브레이커 장치 작동 설명
- provider마다 독립적으로 1개씩 존재

            연속 3회 실패
        ┌──────────────────────┐
        │                      ▼
┌─────────────┐         ┌─────────────┐
│   CLOSED    │         │     OPEN    │ ◀──────────────┐
│  정상 호출   │          │  호출 차단   │                │
└─────────────┘         └─────────────┘                │
        ▲                      │                       │
        │                      │ cooldown 30초 경과     │
        │                      │                       │
        │                      ▼                       │
        │                ┌─────────────┐               │
        │                │  HALF-OPEN  │               │
        │                │  시험 요청  │                │
        │                │ 동시 1개만  │                │
        │                │   허용      │               │
        │                └─────────────┘              │
        │                   │       │                 │
        │                성공│       │실패             │
        │                   │       │                 │
        └───────────────────┘       └─────────────────┘
*/

// 멀티스레드 동시성 제어용 원자적(Atomic) 타입 및 메모리 오더링
// 개념 보충설명:
// Lock     → 한 번에 하나의 스레드만 특정 코드 영역에 접근하도록 제한
//
// Atomic   → 여러 스레드가 공유 상태를 동시에 읽거나 변경해도,
//            값이 중간에 깨지지 않도록 원자적으로 처리
//
// Ordering → 다른 스레드들이 "사용중/사용안함" 같은 상태 변경을
//            일관된 순서와 가시성으로 인식하도록 하는 메모리 접근 규칙
use std::sync::atomic::{
    AtomicU8, // 1바이트

    AtomicU32, // 4바이트
    // CPU 수준에서 데이터 경합(Data Race)을 막아주어,
    // 여러 스레드가 동시에 접근해도 원자적으로 값을 변경할 수 있는 특수 정수 타입
    AtomicU64, // 8바이트
    // 성능 최적화를 위해 컴파일러나 CPU가 코드 실행 순서를 임의로 재배치(Reordering)하는 것을 막아주고,
    // 스레드 간의 메모리 가시성(Visibility)과 실행 순서를 강제하는 규칙
    Ordering,
};

use std::time::{Duration, Instant};

// 아래 CircuitBreaker의 state 필드가 AtomicU8이라
// enum 값을 as u8로 변환해서 그 안에 원자적으로 저장/비교하기 위함
// 컴파일러가 enum 메모리 크기를 함부로 바꾸지 않고 u8만큼 크기 고정하게 함
#[repr(u8)] // 데이터 크기를 u8로 고정(1바이트)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CircuitState {
    // (이해를 위해)차단기 비활성화: 정상 상태
    // 모든 요청이 정상적으로 통과하며, 최근 실패율이 기준치를 넘으면 Open으로 전환
    Closed = 0,

    // 차단기 활성화: 차단 상태
    // 외부 시스템 장애로 판단하여 모든 요청을 즉시 거절(Fast-Fail)하고, 설정된 시간이 지나면 HalfOpen으로 전환
    Open = 1,

    // 차단기 반활성화: 간보기(테스트) 상태
    // 소수의 테스트 요청만 통과시켜 보고, 성공하면 Closed(정상)로 복귀하고 실패하면 다시 Open(차단)으로 후퇴
    HalfOpen = 2,
}

pub struct CircuitBreaker {
    state: AtomicU8,                 // 현재 차단기 상태 (0:Closed, 1:Open, 2:HalfOpen)
    consecutive_failures: AtomicU32, // 현재 연속 실패 횟수 카운트
    opened_at_nanos: AtomicU64, // 차단기가 켜진(Open) 시점의 타임스탬프로 Instant::now()를 기준점 대비 나노초(예: 1.000075217s)로 환산해 저장
    epoch: Instant,             // 서킷브레이커가 생성 시점의 시간 기준점
    failure_threshold: u32,     // Open 기준이 되는 연속 실패 횟수(3회 세팅 예정)
    cooldown: Duration,         // Open 유지 시간 (지나면 Half-Open으로 전환 시도)
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            consecutive_failures: AtomicU32::new(0),
            opened_at_nanos: AtomicU64::new(0),
            epoch: Instant::now(),
            failure_threshold,
            cooldown,
        }
    }

    fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64 // elapsed()는 Instant::now()로부터 얼마만큼 시간이 흘렀는지 확인
    }

    // 호출 직전 확인: 지금 이 provider를 불러도 되는지
    pub fn allow_request(&self) -> bool {
        match self.state.load(Ordering::SeqCst) {
            // 패턴 바인딩을 사용하여 self.state.load(Ordering::SeqCst) 값을 s라는 변수로 지정
            // Ordering::SeqCst: 메모리 접근 순서를 바꾸지 않고 스레드들이 순서대로 처리되도록 적용
            // state.load: 현재 state 값 추출 (추출한 값을 패턴 바인딩으로 s에 지정)
            s if s == CircuitState::Closed as u8 => true,
            s if s == CircuitState::Open as u8 => {
                let opened_at = self.opened_at_nanos.load(Ordering::SeqCst);
                // 경과시간(elapsed) = 현재 nanos - opend_at
                let elapsed = Duration::from_nanos(self.now_nanos() - opened_at);
                // Open 유지 시간을 지나지 않았다면 Open(false) 상태임을 알려줌
                if elapsed < self.cooldown {
                    return false;
                }
                // Open 유지 시간이 지났다면 Half-Open 전환 시도
                // CAS(Compare-And-Swap) 연산: 현재 값이 Open인 경우 원자적으로 HalfOpen으로 교체 후 성공 시 true 반환
                // compare_exchange: 현재 값이 기대하는 값(Open)과 동일한 경우 새로운 값(HalfOpen)으로 변경
                self.state
                    .compare_exchange(
                        CircuitState::Open as u8,     // 기대하는 현재 상태
                        CircuitState::HalfOpen as u8, // 새로운 목표 상태
                        Ordering::SeqCst,             // 성공 시 적용할 규칙
                        Ordering::SeqCst,             // 실패 시 적용할 규칙
                    )
                    .is_ok()
            }
            _ => false, // Half-Open 상태라면 이미 provider request가 진행 중이므로 나머지는 호출 차단
        }
    }

    pub fn record_success(&self) {
        let current = self.state.load(Ordering::SeqCst);
        if current == CircuitState::HalfOpen as u8 {
            // Half-Open 상태에서 provider request 성공할 경우 Closed 상태로 전환
            if self
                .state
                .compare_exchange(
                    CircuitState::HalfOpen as u8,
                    CircuitState::Closed as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                self.consecutive_failures.store(0, Ordering::SeqCst); // 실패 카운트 0으로 세팅
            }
        } else if current == CircuitState::Closed as u8 {
            // consecutive_failures = 2인 상태에서(아직 임계치 3에 도달 안 해서 여전히 Closed)
            // 세 번째 요청이 성공한 경우, 실패 카운트 0으로 세팅 필요
            self.consecutive_failures.store(0, Ordering::SeqCst);
        }
        // Open 상태에서 뒤늦게 도착한 성공은 무시함
        // Open 탈출은 오직 Half-Open 시험 요청 결과로만 결정되어야 함
    }

    pub fn record_failure(&self) {
        let current = self.state.load(Ordering::SeqCst);

        if current == CircuitState::HalfOpen as u8 {
            // Half-Open 상태에서 provider request 실패할 경우 Open 상태로 전환
            if self
                .state
                .compare_exchange(
                    CircuitState::HalfOpen as u8,
                    CircuitState::Open as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                self.opened_at_nanos
                    .store(self.now_nanos(), Ordering::SeqCst); // Open된 시간 기록
            }
            return;
        }

        if current == CircuitState::Open as u8 {
            return; // 이미 Open — 뒤늦게 도착한 실패 중복 카운트 방지
        }

        // Closed 상태의 정상 카운트
        // fetch_add 연산은 원자적으로 값을 증가시키되, 증가 전의 '이전 값(Previous Value)'을 반환(Fetch)하는데,
        // 현재 쓰기 연산이 완료된 메모리 상의 '최신 상태 값'과 별개로
        // fetch_add가 반환하는 "이전 값"만이 "내 이 호출이 정확히 몇 번째 실패를 만들었는지"를 보장해주는 유일한 값에 + 1을 함
        // 이렇게 하는 이유는 예를 들어
        // 스레드 A가 fetch_add까지 실행한 직후, load()를 하기 바로 그 찰나에
        // 다른 스레드가 record_success()를 호출해서 카운터를 0으로 리셋한 경우
        //
        // 스레드A의 load()는 A 자신이 방금 실패를 기록했다는 사실과 무관하게 0 읽어버림
        // A는 "실패가 하나도 없다"고 착각하고 아래 if failures >= self.failure_threshold 검사를 건너뛰게 됨
        // (check-then-act 레이스 버그)
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;

        if failures >= self.failure_threshold {
            // Closed 상태에서 provider request가 failure_threshold만큼 실패한 경우 Open 상태로 전환 시도
            // 여러 스레드가 동시에 threshold를 넘겨도 Closed -> Open 전환은 딱 한 번만 성공
            if self
                .state
                .compare_exchange(
                    CircuitState::Closed as u8,
                    CircuitState::Open as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                self.opened_at_nanos
                    .store(self.now_nanos(), Ordering::SeqCst); // Open된 시간 기록
            }
        }
    }

    #[cfg(test)]
    fn state(&self) -> CircuitState {
        match self.state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            _ => CircuitState::HalfOpen,
        }
    }

    #[cfg(test)]
    fn failure_count(&self) -> u32 {
        self.consecutive_failures.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn opened_at_nanos(&self) -> u64 {
        self.opened_at_nanos.load(Ordering::SeqCst)
    }
}

// cargo test --lib enrichment::circuit_breaker
#[cfg(test)]
mod tests {
    use super::*;

    // 시나리오 1: Closed 상태에서는 임계치 미만 실패로는 열리지 않는다
    #[test]
    fn stays_closed_below_threshold() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        breaker.record_failure(); // 1번 실패
        breaker.record_failure(); // 2번 실패
        assert_eq!(breaker.state(), CircuitState::Closed); // 아직까진 Closed 상태
        assert!(breaker.allow_request());
    }

    // 시나리오 2: 연속 실패가 임계치에 도달하면 Open으로 전환되고 호출이 차단된다
    #[test]
    fn opens_after_threshold_failures() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        breaker.record_failure(); // 1번 실패
        breaker.record_failure(); // 2번 실패
        breaker.record_failure(); // 3번 실패
        assert_eq!(breaker.state(), CircuitState::Open); // Opend 상태
        assert!(!breaker.allow_request());
    }

    // 시나리오 3: 성공이 한 번이라도 있으면 실패 카운터가 리셋된다
    #[test]
    fn success_resets_failure_count() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        breaker.record_failure(); // 1번 실패
        breaker.record_failure(); // 2번 실패
        breaker.record_success(); // 성공으로 실패 초기화
        breaker.record_failure(); // 1번 실패
        breaker.record_failure(); // 2번 실패
        // 리셋 안 됐으면 여기서 4회째라 Open이어야 하지만, 리셋됐으므로 아직 Closed
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    // 시나리오 4: Open 상태에서 쿨다운 전에는 여전히 차단된다
    #[test]
    fn stays_open_during_cooldown() {
        let breaker = CircuitBreaker::new(1, Duration::from_secs(30));
        breaker.record_failure(); // 1번 실패
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request()); // 쿨다운(30초) 안 지나고 provider request함, false 반환되어야 함
    }

    // 시나리오 5: 쿨다운이 지나면 Half-Open으로 전환되어 provider request 1개만 허용된다
    #[test]
    fn transitions_to_half_open_after_cooldown() {
        let breaker = CircuitBreaker::new(1, Duration::ZERO); // 쿨다운 0초로 즉시 테스트
        breaker.record_failure(); // 1번 실패
        assert!(breaker.allow_request()); // Open 상태 유지시간 0초 지나서 Half-Open 상태로 전환되고, provider request 허용된 상태
        assert_eq!(breaker.state(), CircuitState::HalfOpen); // Half-Open 상태인가? yes~
        // 같은 순간 또 다른 요청이 오면 이미 Half-Open이라 차단
        assert!(!breaker.allow_request());
    }

    // 시나리오 6: Half-Open 상태에서 provider request이 성공하면 Closed로 복귀한다
    #[test]
    fn half_open_success_closes_circuit() {
        let breaker = CircuitBreaker::new(1, Duration::ZERO);
        breaker.record_failure(); // 1번 실패
        breaker.allow_request(); // Half-Open 진입하고 provider request 함
        breaker.record_success(); // 요청 성공으로 Half-Open -> Closed 상태로 변환
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request()); // Closed 상태에서 provider request 가능
    }

    // 시나리오 7: Half-Open 상태에서 provider request 요청이 실패하면 다시 Open으로 돌아간다
    #[test]
    fn half_open_failure_reopens_circuit() {
        let breaker = CircuitBreaker::new(1, Duration::ZERO);
        breaker.record_failure(); // 1번 실패
        breaker.allow_request(); // Half-Open 진입
        breaker.record_failure(); // 2번 실패
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    // 시나리오 8 (레이스 컨디션 회귀 테스트):
    // Open 상태에서 뒤늦게 도착한 success가 Closed로 잘못 되돌리지 않아야 한다
    #[test]
    fn late_success_while_open_is_ignored() {
        let breaker = CircuitBreaker::new(1, Duration::from_secs(30));
        breaker.record_failure(); // Open 전환
        assert_eq!(breaker.state(), CircuitState::Open);

        breaker.record_success(); // 뒤늦게 도착한 성공 응답 (버그였다면 여기서 Closed로 돌아감)

        assert_eq!(breaker.state(), CircuitState::Open); // 여전히 Open이어야 정상
        assert!(!breaker.allow_request());
    }

    // 시나리오 9: 여러 스레드가 동시에 실패를 기록해도 threshold를 딱 한 번만 넘겨 Open이 되고
    // opened_at이 여러 번 재설정되지 않는다 (Closed→Open CAS 검증)
    #[test]
    fn concurrent_failures_open_circuit_exactly_once() {
        use std::sync::Arc;
        use std::thread;

        let breaker = Arc::new(CircuitBreaker::new(3, Duration::from_secs(30)));
        let mut handles = vec![];

        for _ in 0..10 {
            let breaker = Arc::clone(&breaker);
            handles.push(thread::spawn(move || {
                breaker.record_failure(); // 10번 실패
            }));
        }

        for handle in handles {
            handle.join().unwrap(); // 모든 스레드가 작업 종료될 때까지 대기
        }

        // 10개 스레드가 동시에 실패해도 최종 상태는 Open 하나로 수렴해야 함
        assert_eq!(breaker.state(), CircuitState::Open);

        // 카운터 값은 스레드 스케줄링 타이밍에 따라 달라질 수 있음
        // Open 전환 이후 도착한 스레드는 early return으로 카운트되지 않으므로 — 최소 threshold(3), 최대 스레드 수(10) 사이여야 함
        // 정확히 10이라고 단정할 수 없음(비결정적 동시성 테스트의 특성)
        let count = breaker.failure_count();
        assert!(
            (3..=10).contains(&count),
            "failure_count는 3~10 사이여야 하는데 {count}였음"
        );
    }

    // 시나리오 10: 이미 Open인 상태에서 추가 실패가 들어와도
    // opened_at이 재설정되지 않는다 (Closed→Open CAS가 상태 전환을 딱 한 번으로 제한하는지 검증)
    #[test]
    fn opened_at_is_not_reset_after_already_open() {
        let breaker = CircuitBreaker::new(1, Duration::from_secs(30));

        breaker.record_failure(); // Closed → Open, opened_at 최초 기록
        assert_eq!(breaker.state(), CircuitState::Open);
        let first_opened_at = breaker.opened_at_nanos();
        assert_ne!(first_opened_at, 0); // 최초 기록은 0보다 커야 함

        // 이미 Open인 상태에서 추가 실패가 들어옴
        breaker.record_failure();
        breaker.record_failure();

        let second_opened_at = breaker.opened_at_nanos();
        assert_eq!(first_opened_at, second_opened_at); // 재설정되지 않아야 함
    }
}
