/*
Kafka/RedPanda offset 추적기: OpenSearch·ClickHouse 저장이 끝난 로그까지만 kafka/redpanda commit 위치를 앞으로 옮김

이벤트 처리 Task(kafka.rs)는 병렬로 돌고 Batch도 동시에 저장되므로, 저장 완료 순서가 수신 순서와 다름
뒤쪽 offset이 먼저 끝나도 앞쪽 offset이 남아 있으면 commit 위치를 옮기지 않음
(마치 TCP의 윈도우 슬라이딩과 유사)

예시: partition 0에서 offset 5, 6, 7 수신
  7 완료 -> [5 처리 중, 6 처리 중, 7 완료]  commit 위치 5 유지
  5 완료 -> [6 처리 중, 7 완료]            commit 위치 6
  6 완료 -> []                            commit 위치 8 (마지막 수신 offset + 1)

- commit 위치는 "다음에 읽을 offset"이며, Consumer가 재시작하면 이 위치부터 다시 읽음
- 이 추적기가 계산한 위치만 Kafka offset으로 저장해야 함 (Consumer 설정 enable.auto.offset.store=false)
  그래야 저장이 끝나지 않은 로그는 commit되지 않고, 프로세스가 죽어도 재시작 후 다시 처리됨
  중복은 log_id(record.rs > build_log_id()에서 event_id와 Vector가 기록한 ingested_at으로 생성)로 제거됨
*/

use std::collections::{HashMap, VecDeque};
// Vec: 크기를 늘릴 수 있는 배열처럼 데이터를 저장하는 자료구조
// Deque: 앞/뒤 양쪽에서 데이터를 추가/제거할 수 있는 Queue

/*
partition 하나의 수신 기록

예시: partition 0에서 offset 5, 6, 7 수신

                 pending                    next_offset  commit 위치  completed_below
  5, 6, 7 수신 -> [5 처리 중, 6 처리 중, 7 처리 중]     8          -            0   (finish 전이라 아직 계산 안 함)
  7 완료       -> [5 처리 중, 6 처리 중, 7 완료]        8          5            5   (맨 앞 5가 미완료 -> 5 유지)
  5 완료       -> [6 처리 중, 7 완료]                  8          6            6   (5 제거, 맨 앞이 6)
  6 완료       -> []                                 8          8            8   (6, 7 제거, 비었으니 next_offset)
*/
#[derive(Default)]
struct PartitionWindow {
    pending: VecDeque<(i64, bool)>, // offset 관리용 자료구조
    next_offset: i64, // 다음 commit 할 offset 후보: 지금까지 수신한 가장 큰 offset + 1
    completed_below: i64, // 마지막으로 계산한 commit 위치: 이 값보다 작은 offset은 모두 저장 완료됨을 의미
}

impl PartitionWindow {
    // 지금 commit할 수 있는 위치: 처리 중인 가장 앞 offset, 없으면 수신한 가장 큰 offset + 1
    fn commit_position(&self) -> i64 {
        self.pending
            .front()
            .map_or(self.next_offset, |&(offset, _)| offset)
    }
}

#[derive(Default)]
pub struct OffsetTracker {
    partitions: HashMap<i32, PartitionWindow>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self::default() // #[derive(Default)]가 만들어주는 기본값으로 채운 객체를 반환
        // OffsetTracker::new()  ==  OffsetTracker { partitions: HashMap::new() } <- 이것은 빈 HashMap
    }

    /*
    메시지 수신 시 offset을 처리 중으로 등록함, Consumer가 메시지를 받은 순서대로 호출해야 함 (처리 Task 실행 순서가 아님)

    동작 예시: 현재 상태가 아래와 같을 때 offset이 들어오는 경우
      completed_below = 5   (5보다 작은 offset은 모두 저장 완료)
      pending         = [(5,false), (7,false)]
      next_offset     = 8

    1. begin(8): 일반적인 경우 (가장 큰 offset보다 큼)
       8 < 5 아님 -> 통과
       next_offset = max(8, 8+1) = 9
       맨 뒤 7보다 크므로, 맨 뒤에 새로 추가
       pending = [(5,false), (7,false), (8,false)]

    2. begin(3): 이미 저장이 끝난 offset 재전송
       3 < completed_below(5) 이므로 무시 (아무것도 바뀌지 않음)

    3. begin(7): 처리 중인 offset 재전송
       7 < 5 아님 -> 통과, next_offset = max(9, 8) = 9 (그대로)
       맨 뒤 8보다 크지 않음 -> 이진 탐색 -> Ok(1) (이미 있음) -> 추가하지 않음 (만약 없었다면 해당 인덱스 위치에 추가됨, 아래 4. begin(6)에서 설명)
       pending = [(5,false), (7,false), (8,false)] (그대로, 기존 미완료 기록 유지)

    4. begin(6): 중간에 빠져 있던 offset이 늦게 들어옴
       6 < 5 아님 -> 통과, next_offset = 9 (그대로)
       맨 뒤 8보다 크지 않음 -> 이진 탐색 -> Err(1) (없음, 넣을 자리는 인덱스 1)
       인덱스 1에 삽입 -> pending = [(5,false), (6,false), (7,false), (8,false)]
       (6이 저장되기 전에는 commit 위치가 6을 넘지 않음)
    */
    pub fn begin(&mut self, partition: i32, offset: i64) {
        let window = self.partitions.entry(partition).or_default(); // entry()로 key가 있으면 그 값을, 없으면 빈 window를 새로 넣고 그 값을 꺼냄

        // 이미 저장이 끝난 범위의 offset이 다시 들어오면(재전송) 기록하지 않음
        // 예: 5, 6 저장 완료로 commit 위치가 7인데 6이 다시 들어오면 6 < 7이므로 무시
        if offset < window.completed_below {
            return;
        }

        window.next_offset = window.next_offset.max(offset + 1);

        // 일반적인 경우: 같은 partition의 offset은 증가하는 순서로 수신되므로 뒤에 추가
        if window.pending.back().is_none_or(|&(last, _)| offset > last) {
            // pending 비어있거나, offset이 last보다 크다면
            window.pending.push_back((offset, false)); // pending 맨 뒤에 추가
            return;
        }

        // offset을 이진 탐색하여 기존 항목이 없을 때만 정렬된 위치에 삽입
        // binary_search_by_key(): 이진 탐색
        if let Err(index) = window.pending.binary_search_by_key(&offset, |&(o, _)| o) {
            window.pending.insert(index, (offset, false));
        }
    }

    /*
    저장이 완료된 로그를 반영하고, 각 partition에서 안전하게 commit할 수 있는 다음 offset을 반환함
    예: partition 0의 offset 5, 6이 모두 완료되고 앞선 로그도 완료됐다면 [(0, 7)] 반환

    OpenSearch와 ClickHouse, (거절 로그가 있으면) PostgreSQL 저장이 모두 끝난 뒤에만 호출해야 함

    done: 저장이 완료된 로그들의 목록, 각 항목은 (partition, 저장 완료한 offset)
    IntoIterator: for로 하나씩 꺼낼 수 있는 것이면 배열/Vec/iterator 등 무엇이든 받는 trait
    (i32, i64)는 (partition, offset)을 의미
    IntoIterator 사용 이유: 호출하는 곳마다 데이터 형태가 달라서 (kafka.rs는 1건짜리 배열, forwarder.rs는 Batch에서 뽑은 iterator) 별도 Vec 복사 없이 그대로 받기 위함
    */
    pub fn finish(&mut self, done: impl IntoIterator<Item = (i32, i64)>) -> Vec<(i32, i64)> {
        let mut touched = Vec::new(); // 이번 완료로 상태가 바뀐 partition 목록

        /*
        완료된 offset마다 true 표시를 하고, 어느 partition이 바뀌었는지 기록

        동작 예시:
        partitions = {
            0: pending [(5,false), (6,false), (7,false)]
            1: pending [(100,false)]
        }
        done = [(0,7), (0,5), (1,100)]  <- 이번에 저장 완료된 로그들
        touched = []

        1. (0, 7)
        partitions.get_mut(&0) <- partition 0 찾음
        binary_search(7) -> Ok(2) <- 인덱스 2(세 번째 자리)에 있음
        pending[2].1 = true -> [(5,false), (6,false), (7,true)] <- pending[2](세 번째 항목)의 .1(두 번째 칸, 완료 여부)을 true로 수정
        touched에 0 없음 -> 추가 -> touched = [0]

        2. (0, 5)
        partitions.get_mut(&0) -> 찾음
        binary_search(5) -> Ok(0)
        pending[0].1 = true -> [(5,true), (6,false), (7,true)]
        touched에 0 이미 있음 -> 추가 안 함 -> touched = [0]

        3. (1, 100)
        partitions.get_mut(&1) -> 찾음
        binary_search(100) -> Ok(0) -> [(100,true)]
        touched = [0, 1]
        */
        for (partition, offset) in done {
            let Some(window) = self.partitions.get_mut(&partition) else {
                // get_mut(): key로 찾은 값은 가변 참조로 꺼냄
                continue;
            };

            // pending은 offset 오름차순이므로 이진 탐색으로 위치를 찾아 완료 표시
            if let Ok(index) = window.pending.binary_search_by_key(&offset, |&(o, _)| o) {
                window.pending[index].1 = true; // 1은 tuple의 2번째 칸을 의미, 예: (5, false)인 경우 false
                if !touched.contains(&partition) {
                    touched.push(partition);
                }
            }
        }

        /*
        바뀐 partition마다 commit 위치를 계산해서 반환 목록에 담음

        동작 예시:
        partition 0: pending [(5,true), (6,false), (7,true)] -> next_offset 8
        partition 1: pending [(100,true)] -> next_offset 101
        touched = [0, 1]
        positions = [] <- with_capacity(2): 바뀐 partition 수(touched.len())만큼 공간을 미리 확보

        1. partition 0
        while 반복:
        front() -> (5,true) -> 완료됨 -> pop_front() -> [(6,false), (7,true)]
        front() -> (6,false) -> 미완료 -> while 종료

        commit_position() -> 맨 앞 6 반환
        completed_below = 6
        positions = [(0, 6)]

        2. partition 1
        while 반복:
        front() -> (100,true) -> 완료됨 -> pop_front() -> []
        front() -> None (비었음) -> while 종료

        commit_position() -> 비었으니 next_offset 101
        completed_below = 101
        positions = [(0, 6), (1, 101)]
         */
        let mut positions = Vec::with_capacity(touched.len());
        for partition in touched {
            let Some(window) = self.partitions.get_mut(&partition) else {
                continue;
            };

            // 앞에서부터 연속으로 완료된 offset 제거
            while window
                .pending
                .front()
                .is_some_and(|&(_, completed)| completed)
            // 값이 있고 true 라면 pop_front() 수행
            {
                window.pending.pop_front();
            }

            let position = window.commit_position();
            window.completed_below = position;
            positions.push((partition, position));
        }

        positions
    }

    // rebalance로 partition 할당이 해제되면 해당 partition의 기록을 삭제
    // 추적기의 기록(completed_below 등)은 담당하는 동안에만 유효하므로, 다시 할당되면 새로 기록해야 함
    // 예: 토픽 재생성으로 offset이 0부터 다시 시작될 때 예전 기록 completed_below=11 이 남아 있는 경우
    // 새 offset 0~10을 "이미 저장됨"으로 오판해 건너뛰고 commit -> 유실로 이어짐
    pub fn revoke(&mut self, partition: i32) {
        self.partitions.remove(&partition);
    }
}

// cargo test --lib consumer::offset
#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_with(partition: i32, offsets: &[i64]) -> OffsetTracker {
        let mut tracker = OffsetTracker::new();
        for &offset in offsets {
            tracker.begin(partition, offset);
        }
        tracker
    }

    // 시나리오 1: 수신 순서대로 완료되면 commit 위치가 마지막 offset + 1까지 이동
    #[test]
    fn advances_when_finished_in_order() {
        let mut tracker = tracker_with(0, &[5, 6, 7]);

        assert_eq!(tracker.finish([(0, 5)]), vec![(0, 6)]); // 같은 partition 0에서 offset 5가 완료 되었다면 vec![(0, 6)]이 반환되어야 함
        assert_eq!(tracker.finish([(0, 6), (0, 7)]), vec![(0, 8)]);
    }

    // 시나리오 2: 뒤쪽 offset이 먼저 완료되면 앞쪽이 끝날 때까지 commit 위치를 유지
    #[test]
    fn holds_position_until_earlier_offset_finishes() {
        let mut tracker = tracker_with(0, &[5, 6, 7]);

        assert_eq!(tracker.finish([(0, 7)]), vec![(0, 5)]);
        assert_eq!(tracker.finish([(0, 6)]), vec![(0, 5)]);
        assert_eq!(tracker.finish([(0, 5)]), vec![(0, 8)]);
    }

    // 시나리오 3: offset 사이에 빈 번호가 있어도 다음 offset을 기준으로 이동
    // 공백이 발생하는 환경에서도 offset 추적기가 올바르게 동작하는지 확인하는 방어용 테스트
    #[test]
    fn handles_offset_gaps() {
        let mut tracker = tracker_with(0, &[5, 9, 12]);

        assert_eq!(tracker.finish([(0, 5)]), vec![(0, 9)]);
        assert_eq!(tracker.finish([(0, 9), (0, 12)]), vec![(0, 13)]);
    }

    // 시나리오 4: partition마다 commit 위치를 따로 계산
    #[test]
    fn tracks_partitions_independently() {
        let mut tracker = OffsetTracker::new();
        tracker.begin(0, 10);
        tracker.begin(1, 100);
        tracker.begin(0, 11);

        let mut positions = tracker.finish([(1, 100), (0, 11)]);
        positions.sort_unstable(); // 오름차순 정렬

        assert_eq!(positions, vec![(0, 10), (1, 101)]);
    }

    // 시나리오 5: revoke된 partition의 완료는 무시하고, 다시 할당되면 새로 기록
    #[test]
    fn ignores_finish_after_revoke() {
        let mut tracker = tracker_with(0, &[5, 6]);
        tracker.revoke(0);

        assert!(tracker.finish([(0, 5), (0, 6)]).is_empty());

        tracker.begin(0, 5); // 재할당 후 commit 위치부터 다시 수신
        assert_eq!(tracker.finish([(0, 5)]), vec![(0, 6)]);
    }

    // 시나리오 6: 재할당 후 이전 할당의 Task가 같은 offset 완료를 늦게 보내도 유실은 없음
    // finish()는 저장이 모두 끝난 뒤에만 호출되므로, 이전 Task의 완료도 해당 로그가 이미 저장됐다는 뜻
    // 새 Task가 같은 로그를 다시 저장해도 log_id(OpenSearch _id·ClickHouse log_id 컬럼)로 중복이 제거됨
    #[test]
    fn late_finish_from_previous_assignment_is_safe() {
        let mut tracker = tracker_with(0, &[5, 6]);
        tracker.revoke(0);
        tracker.begin(0, 5); // 재할당 후 새 Task가 처리 시작
        tracker.begin(0, 6);

        assert_eq!(tracker.finish([(0, 5)]), vec![(0, 6)]); // 이전 Task의 5 완료가 늦게 도착
        assert!(tracker.finish([(0, 5)]).is_empty());
        assert_eq!(tracker.finish([(0, 6)]), vec![(0, 7)]);
    }

    // 시나리오 7: 등록되지 않은 offset의 완료는 무시
    #[test]
    fn ignores_unknown_offset() {
        let mut tracker = tracker_with(0, &[5]);

        assert!(tracker.finish([(0, 4), (3, 5)]).is_empty());
    }

    // 시나리오 8: 처리 중인 offset이 다시 들어와도 기존 미완료 기록을 지우지 않음
    #[test]
    fn keeps_pending_on_duplicate_begin() {
        let mut tracker = tracker_with(0, &[5, 6, 7]);
        tracker.begin(0, 6); // 6 재전송

        // 6만 완료돼도 5가 남아 있으므로 위치 5 유지
        assert_eq!(tracker.finish([(0, 6)]), vec![(0, 5)]); // [(5,false),(6,true),(7,false)]
        assert_eq!(tracker.finish([(0, 5), (0, 7)]), vec![(0, 8)]); // [(5,true),(6,true),(7,true)]
    }

    // 시나리오 9: 이미 저장이 끝난 offset이 다시 들어오면 무시하여 commit 위치가 뒤로 돌아가지 않음
    #[test]
    fn ignores_redelivered_completed_offset() {
        let mut tracker = tracker_with(0, &[5, 6]);
        assert_eq!(tracker.finish([(0, 5), (0, 6)]), vec![(0, 7)]);

        tracker.begin(0, 8);
        tracker.begin(0, 6); // 이미 저장된 6 재전송 -> 무시

        assert_eq!(tracker.finish([(0, 8)]), vec![(0, 9)]);
        assert!(tracker.finish([(0, 6)]).is_empty());
    }
}
