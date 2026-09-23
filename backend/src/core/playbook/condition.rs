/*
condition.rs: 플레이북 조건식(Expr) 평가기

이 파일이 하는 일
- nodes.rs에 정의된 Expr(and/or/조건)을 실제 이벤트(JSON)에 대해 평가해서 bool을 반환
- 필드 경로 탐색("a.b.c" 점 표기), 타입별 비교, IP/CIDR 비교를 담당

평가 규칙
- not_exists: 필드가 없으면 true
- exists: 필드가 있고 null이 아니면 true
- 그 외 모든 연산자: 필드가 없거나 null이면 false (타입이 안 맞아도 false)
- eq/ne: 숫자는 as_f64()로 변환해서 비교, 그 외(문자열 등)는 그대로 비교
- contains/starts_with/ends_with: 대소문자 구분(대소문자 구분하지 않는 것은 포트폴리오에서 제외함)
- in_cidr/not_in_cidr: 필드 값 또는 value가 IP/CIDR로 파싱되지 않으면 false
- and: 비어 있으면 false
- or: 비어 있으면 false

평가 대상은 Evidence를 serde_json::Value로 직렬화한 것 (engine.rs에서 준비)
*/

use serde_json::Value; // JSON 값 표현 (null, bool, number, string, array, object)
use std::net::IpAddr; // Rust 표준 라이브러리에 있는 IP주소 타입: 예시) let ip: IpAddr = "10.1.2.3".parse()?; => 10.1.2.3
use std::str::FromStr; // 문자열을 특정 타입으로 파싱할 수 있게 해주는 trait: 예) IpAddr::from_str("10.1.2.3") => 10.1.2.3

use ipnet::IpNet; // 외부 크레이트 ipnet에서 제공하는 네트워크 대역(CIDR)

use crate::core::playbook::nodes::{Condition, Expr, Op};

// 조건식(Expr)을 재귀적으로 평가하고 최종 결과를 bool로 반환
pub fn eval(expr: &Expr, event: &Value) -> bool {
    match expr {
        // 예:
        // {
        //   "and": [
        //     { "field": "event_type", "op": "eq", "value": "file_download" },
        //     { "field": "file_size_bytes", "op": "gt", "value": 100000 },
        //     { "field": "file_name", "op": "ends_with", "value": ".exe" }
        //   ]
        // }
        //
        // 위 json은 이것과 같음: (event_type == "file_download") && (file_size_bytes > 100000) && (file_name ends_with ".exe")
        //
        // all()은 모든 요소의 결과가 true이면 true 반환
        Expr::And { and } => !and.is_empty() && and.iter().all(|e| eval(e, event)),

        // 예: { "or": [A, B] } -> A || B
        // any()는 요소 중 하나라도 결과가 true이면 true 반환
        Expr::Or { or } => !or.is_empty() && or.iter().any(|e| eval(e, event)),

        // `and`와 `or` 안의 각 Expr을 다시 eval()에 넣어 재귀적으로 평가함
        // 가장 안쪽에서 Expr::Cond를 만나면 eval_condition()에서 실제 조건을 검사하고,
        // 그 결과(true/false)가 다시 all() 또는 any()을 통해 바깥쪽으로 전달됨
        Expr::Cond(condition) => eval_condition(condition, event),
    }
}

/*
┌──────────────────────────────────────────────┐
│              eval_condition() 시작           │
└──────────────────────┬───────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│ get_path(event, &condition.field)            │
│                                              │
│ found = Some(&Value)                         │
│         또는                                 │
│ found = None                                 │
└──────────────────────┬───────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│ match condition.op                           │
└──────────────────────┬───────────────────────┘
                       │
          ┌────────────┼─────────────┐
          │            │             │
          ↓            ↓             ↓
   ┌────────────┐ ┌────────────┐ ┌──────────────┐
   │ Exists     │ │ NotExists  │ │ 그 외 연산자 │
   └─────┬──────┘ └─────┬──────┘ └──────┬───────┘
         ↓              ↓               ↓
 found.is_some()   found.is_none()    아래로 계속
         ↓              ↓               │
 return true/false return true/false    │
         ↓              ↓               │
      함수 종료       함수 종료           │
                                       ↓
                    ┌────────────────────────────────┐
                    │ let Some(field_value) = found  │
                    └───────────────┬────────────────┘
                                    ↓
                             found가 Some?
                              /          \
                           YES            NO
                            │              │
                            ↓              ↓
           event_field_value에         return false
                 내부 &Value 저장       함수 종료
                            │
                            ↓
                    ┌──────────────────────────────┐
                    │ condition.value.as_ref()     │
                    └──────────────┬───────────────┘
                                   ↓
                         조건식 value이 존재?
                              /           \
                           YES             NO
                            │               │
                            ↓               ↓
                condition_value 저장      return false
                            │             함수 종료
                            ↓
             ┌──────────────────────────────────────┐
             │      두 번째 match condition.op      │
             └──────────────────┬───────────────────┘
                                ↓
      ┌──────────────┬──────────────┬──────────────┐
      │              │              │              │
      ↓              ↓              ↓              ↓
   ┌───────┐    ┌──────────┐   ┌───────────┐  ┌────────────┐
   │Eq/Ne  │    │Gt/Gte/   │   │Contains/  │  │InCidr/     │
   │       │    │Lt/Lte    │   │Starts/Ends│  │NotInCidr   │
   └───┬───┘    └────┬─────┘   └─────┬─────┘  └─────┬──────┘
       │             │               │              │
       ↓             ↓               ↓              ↓
 compare_eq()   as_f64()         as_str()       as_str()
       │        두 값 변환          두 값 변환       두 값 변환
       │             │               │              │
       ↓             ↓               ↓              ↓
 true/false      비교 수행       문자열 검사      IP 파싱
                      │               │              ↓
                      │               │          CIDR 파싱
                      │               │              ↓
                      │               │        net.contains()
                      │               │              ↓
                      │               │         is_in / !is_in
                      │               │              │
                      └───────────────┴──────────────┴───────┐
                                                              ↓
                                                     ┌─────────────────┐
                                                     │  최종 bool 반환  │
                                                     └─────────────────┘
*/

fn eval_condition(condition: &Condition, event: &Value) -> bool {
    let found = get_path(event, &condition.field);

    // exists/not_exists는 값 비교 없이 존재 여부만 확인
    match condition.op {
        // 예)
        // {
        //   "target_ip": "192.168.0.10"
        // }
        //
        // let found = get_path(event, "target_ip");
        // get_path()가 "target_ip"를 찾았으므로
        // found = Some(&Value::String("192.168.0.10")) -> is_some() == true
        Op::Exists => return found.is_some(),

        // get_path()가 해당 필드를 찾지 못하면 found = None -> is_none() == true
        Op::NotExists => return found.is_none(),

        _ => {}
    }

    // found가 Some이면 내부의 Value에 대한 참조를 event_field_value 변수에 저장
    // found가 None이면 false를 반환하고 함수를 종료
    // (Exists와 NotExists를 제외한 나머지 연산자는 필드가 없거나 null이면 모두 false)
    let Some(event_field_value) = found else {
        return false;
    };

    // value가 없는 조건(잘못된 설정)은 거짓 처리 (여기까지 오면 exists/not_exists가 아니므로 value가 있어야 정상)
    let Some(condition_value) = condition.value.as_ref() else {
        return false;
    };

    match condition.op {
        // Exists / NotExists는 위에서 이미 return했으므로 정상적인 흐름에서는 여기까지 도달하지 않음
        // nodes.rs -> Op enum 때문에 아래 부분 필요
        // Op의 모든 variant를 처리해야 하는 match의 exhaustiveness를 만족시키기 위해 명시함
        // 만약 실제 도달하면 unreachable!()에 의해 panic 발생
        // Tokio의 spawn된 task에서 실행 중이라면 해당 task가 종료되고, 일반적으로 다른 task와 runtime은 계속 실행됨
        Op::Exists | Op::NotExists => unreachable!("위에서 이미 처리됨"),

        Op::Eq => compare_eq(event_field_value, condition_value).unwrap_or(false),

        // 값이 서로 값으면 Ne는 false가 나와야 함
        // compare_eq에 같은 값이 비교가 되면 true가 반환되므로 map에서 반환된 값을 반전시킴
        Op::Ne => compare_eq(event_field_value, condition_value)
            .map(|eq| !eq)
            .unwrap_or(false),

        // 숫자형 전용 연산자
        Op::Gt | Op::Gte | Op::Lt | Op::Lte => {
            let (Some(a), Some(b)) = (event_field_value.as_f64(), condition_value.as_f64()) else {
                return false;
            };
            // 바깥 match에서 Gt/Gte/Lt/Lte인 경우만 이 블록에 들어오므로
            // 아래의 _까지 정상적으로 도달할 수 없음
            // 하지만 match에서 nodes.rs -> Op enum의 모든 variant를 처리해야 하므로
            // 나머지 variant를 _ => unreachable!()로 명시함
            match condition.op {
                Op::Gt => a > b,
                Op::Gte => a >= b,
                Op::Lt => a < b,
                Op::Lte => a <= b,
                _ => unreachable!(),
            }
        }

        // 문자열 전용 연산자
        Op::Contains | Op::StartsWith | Op::EndsWith => {
            let (Some(a), Some(b)) = (event_field_value.as_str(), condition_value.as_str()) else {
                return false;
            };
            match condition.op {
                Op::Contains => a.contains(b), // 예: a = "invoice_2026.exe", b = "2026" -> true
                Op::StartsWith => a.starts_with(b),
                Op::EndsWith => a.ends_with(b),
                _ => unreachable!(),
            }
        }

        // IP/CIDR 전용 연산자
        Op::InCidr | Op::NotInCidr => {
            // 1. 이벤트 값과 조건 값이 둘 다 문자열인지 확인
            // 예: event_field_value = "10.1.2.3", condition_value = "10.0.0.0/8"
            let (Some(ip_str), Some(cidr_str)) =
                (event_field_value.as_str(), condition_value.as_str())
            else {
                return false;
            };

            // 2. 이벤트 값이 유효한 IP 주소인지 확인
            // 예: "10.1.2.3" -> IpAddr::from_str() 성공 -> IpAddr 타입의 값을 ip 변수에 저장
            let Ok(ip) = IpAddr::from_str(ip_str) else {
                return false;
            };

            // 3. 조건 값이 유효한 CIDR인지 확인
            // 예: "10.0.0.0/8" -> IpNet::from_str() 성공 -> IpNet 타입의 값을 net 변수에 저장
            let Ok(net) = IpNet::from_str(cidr_str) else {
                return false;
            };

            // 4. IP가 해당 CIDR 네트워크 범위에 포함되는지 확인
            // 예: 10.1.2.3은 10.0.0.0/8에 포함됨 -> true
            let is_in = net.contains(&ip);

            match condition.op {
                Op::InCidr => is_in,
                Op::NotInCidr => !is_in,
                _ => unreachable!(),
            }
        }
    }
}

fn compare_eq(a: &Value, b: &Value) -> Option<bool> {
    // 두 값이 모두 숫자이면 f64로 변환해서 비교
    // 예: 10 -> 10.0, 20.0 -> 20.0
    match (a.as_f64(), b.as_f64()) {
        // 둘 다 숫자이면 f64 값으로 비교
        (Some(x), Some(y)) => Some(x == y),

        // 한쪽만 숫자이면 타입이 달라 비교하지 않음
        (Some(_), None) | (None, Some(_)) => None,

        // 둘 다 숫자가 아니면 Value 자체를 비교
        // 예: 문자열, bool, 배열, 객체 등
        _ => Some(a == b),
    }
}

// JSON 객체에서 "user.profile.name"처럼 "."으로 구분된 경로를 따라가
// 원본 Value를 가리키는 참조(&Value)를 반환
// 경로를 찾지 못하거나 값이 null이면 None 반환
// 'a는 반환되는 참조(&Value)가 입력 value의 lifetime과 연결되어 있음을 나타냄
// 즉, 반환되는 &Value는 원본 Value보다 오래 살 수 없음
fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;

    // path를 "." 기준으로 나눠 경로의 각 부분을 순서대로 탐색
    //
    // 예를 들어 다음 JSON이 있다면:
    // {
    //   "user": {
    //     "profile": {
    //       "name": "test123"
    //     }
    //   }
    // }
    //
    // path = "user.profile.name"
    //
    // 1. "user"   -> current = { "profile": { "name": "test123" } }
    // 2. "profile" -> current = { "name": "test123" }
    // 3. "name"    -> current = "test123"
    for segment in path.split('.') {
        // as_object로 object로 변환 후 value를 가져옴
        // JSON object가 아니거나 값이 없다면 None 반환
        //
        // ?:
        // 값이 없으면 즉시 None을 반환하고,
        // 값이 있으면 다음 탐색을 계속함
        current = current.as_object()?.get(segment)?;
    }
    if current.is_null() {
        None
    } else {
        // 원본 value 내부의 Value를 참조로 반환
        Some(current)
    }
}

// cargo test --lib playbook::condition
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cond(field: &str, op: Op, value: Option<Value>) -> Expr {
        Expr::Cond(Condition {
            field: field.to_string(),
            op,
            value,
        })
    }

    // ── 필드 경로 탐색 ──
    // 시나리오 1: 중첩된 JSON 필드 경로를 따라가 최종 Value를 정상적으로 찾는지 확인
    #[test]
    fn resolves_nested_path() {
        let event = json!({ "enrichment_src_ip": { "data": { "abuse_confidence_score": 90 } } });
        assert_eq!(
            get_path(&event, "enrichment_src_ip.data.abuse_confidence_score"),
            Some(&json!(90))
        );
    }

    // 시나리오 2: 존재하지 않는 필드 경로를 조회하면 None을 반환하는지 확인
    #[test]
    fn missing_path_returns_none() {
        let event = json!({ "event_type": "file_download" });
        assert_eq!(get_path(&event, "src_ip"), None);
        assert_eq!(get_path(&event, "a.b.c"), None);
    }

    // 시나리오 3: 필드 값이 null이면 존재하지 않는 것으로 처리하는지 확인
    #[test]
    fn null_value_is_treated_as_missing() {
        let event = json!({ "enrichment_src_ip": { "data": null } });
        assert_eq!(get_path(&event, "enrichment_src_ip.data"), None);
    }

    // ── exists / not_exists ──
    // 시나리오 4: 존재하는 필드, null 필드, 아예 없는 필드에 대해 확인
    #[test]
    fn exists_and_not_exists() {
        let event = json!({ "src_ip": "1.2.3.4", "dst_domain": null });

        assert!(eval(&cond("src_ip", Op::Exists, None), &event)); // src_ip 필드 존재, 값이 null이 아님 -> true
        assert!(!eval(&cond("src_ip", Op::NotExists, None), &event)); // src_ip 필드 존재, 값이 null이 아님 -> false

        // null은 없음으로 취급
        assert!(!eval(&cond("dst_domain", Op::Exists, None), &event)); // dst_domain 값이 null이므로 -> false
        assert!(eval(&cond("dst_domain", Op::NotExists, None), &event)); // dst_domain 값이 null이므로 없음으로 취급 -> true

        // 아예 없는 필드
        assert!(!eval(&cond("file_name", Op::Exists, None), &event)); // file_name 필드가 없음 -> false
        assert!(eval(&cond("file_name", Op::NotExists, None), &event)); // file_name 필드가 없음 -> true
    }

    // 시나리오 5: 필드가 없을 때 exists / not_exists를 제외한 모든 연산자가 false를 반환하는지 확인
    #[test]
    fn missing_field_is_false_for_all_but_not_exists() {
        let event = json!({});

        assert!(!eval(
            &cond("dst_domain", Op::Eq, Some(json!("x.com"))),
            &event
        ));
        assert!(!eval(
            &cond("dst_domain", Op::Ne, Some(json!("x.com"))),
            &event
        ));
        assert!(!eval(&cond("score", Op::Gte, Some(json!(10))), &event));
        assert!(!eval(
            &cond("dst_domain", Op::Contains, Some(json!("x"))),
            &event
        ));
        assert!(!eval(
            &cond("src_ip", Op::InCidr, Some(json!("10.0.0.0/8"))),
            &event
        ));
    }

    // ── eq/ne (숫자는 as_f64, 문자열은 그대로) ──
    // 시나리오 6: 숫자는 정수와 실수를 동일하게 비교하고,
    // 문자열은 문자열 그대로 비교하며,
    // eq / ne가 정상적으로 반대로 동작하는지 확인
    #[test]
    fn eq_ne_number_and_string() {
        let event = json!({ "score": 10, "event_type": "file_download" });

        // 정수 10과 실수 10.0은 as_f64 비교로 같다고 판단됨
        assert!(eval(&cond("score", Op::Eq, Some(json!(10.0))), &event));
        assert!(!eval(&cond("score", Op::Ne, Some(json!(10.0))), &event));

        assert!(eval(
            &cond("event_type", Op::Eq, Some(json!("file_download"))),
            &event
        ));
        assert!(eval(
            &cond("event_type", Op::Ne, Some(json!("login_failure"))),
            &event
        ));
    }

    // ── gt/gte/lt/lte ──
    // 시나리오 7: 숫자형 비교 연산자(gt/gte/lt/lte)가 각각 올바르게 동작하는지 확인
    #[test]
    fn numeric_comparisons() {
        let event = json!({ "score": 50 });

        assert!(eval(&cond("score", Op::Gt, Some(json!(10))), &event)); // 초과: 50 > 10 -> true
        assert!(eval(&cond("score", Op::Gte, Some(json!(50))), &event)); // 이상: 50 >= 50 -> true
        assert!(eval(&cond("score", Op::Lt, Some(json!(100))), &event)); // 미만: 50 < 100 -> true
        assert!(eval(&cond("score", Op::Lte, Some(json!(50))), &event)); // 이하: 50 <= 50 -> true
        assert!(!eval(&cond("score", Op::Gt, Some(json!(50))), &event)); // 초과: 50 > 50 -> false
    }

    // ── contains/starts_with/ends_with (대소문자 구분) ──
    // 시나리오 8: 문자열 연산자가 대소문자를 구분하면서
    // contains / starts_with / ends_with를 정상적으로 처리하는지 확인
    #[test]
    fn string_ops_are_case_sensitive() {
        let event = json!({ "file_name": "invoice_2026.EXE" });

        assert!(eval(
            &cond("file_name", Op::EndsWith, Some(json!(".EXE"))),
            &event
        )); // true
        assert!(!eval(
            &cond("file_name", Op::EndsWith, Some(json!(".exe"))),
            &event
        )); // false
        assert!(eval(
            &cond("file_name", Op::Contains, Some(json!("2026"))),
            &event
        )); // true
        assert!(eval(
            &cond("file_name", Op::StartsWith, Some(json!("invoice"))),
            &event
        )); // true
    }

    // ── in_cidr / not_in_cidr ──
    // 시나리오 9: IP가 지정한 CIDR 네트워크 범위에 포함되는 경우와
    // 포함되지 않는 경우에 in_cidr / not_in_cidr가 올바르게 동작하는지 확인
    #[test]
    fn cidr_matching() {
        let event = json!({ "src_ip": "10.1.2.3" });

        assert!(eval(
            &cond("src_ip", Op::InCidr, Some(json!("10.0.0.0/8"))),
            &event
        )); // 포함: true
        assert!(!eval(
            &cond("src_ip", Op::NotInCidr, Some(json!("10.0.0.0/8"))),
            &event
        )); // 미포함: false

        assert!(!eval(
            &cond("src_ip", Op::InCidr, Some(json!("192.168.0.0/16"))),
            &event
        )); // 포함: false
        assert!(eval(
            &cond("src_ip", Op::NotInCidr, Some(json!("192.168.0.0/16"))),
            &event
        )); // 미포함: true
    }

    // 시나리오 10: 잘못된 IP 또는 CIDR이 입력되었을 때 panic하지 않고 false를 반환하는지 확인
    #[test]
    fn invalid_ip_or_cidr_is_false_not_panic() {
        let event = json!({ "src_ip": "not-an-ip" });
        assert!(!eval(
            &cond("src_ip", Op::InCidr, Some(json!("10.0.0.0/8"))),
            &event
        )); // 포함: false

        let event2 = json!({ "src_ip": "10.1.2.3" });
        assert!(!eval(
            &cond("src_ip", Op::InCidr, Some(json!("not-a-cidr"))),
            &event2
        )); // 포함: false
    }

    // ── and / or 재귀 ──
    // 시나리오 11: AND 안에 조건을 중첩하고,
    // 그 결과를 OR에서 다시 평가하는 재귀적인 조건식이 정상적으로 동작하는지 확인
    #[test]
    fn and_or_nesting() {
        // (A and B) or C
        let event = json!({ "event_type": "file_download", "score": 5 });
        // (A AND B) OR C 결과가 올바르게 계산되는지 확인
        let expr = Expr::Or {
            or: vec![
                Expr::And {
                    and: vec![
                        cond("event_type", Op::Eq, Some(json!("file_download"))), // true
                        cond("score", Op::Gte, Some(json!(10))),                  // false
                    ],
                },
                cond("score", Op::Gte, Some(json!(1))), // true
            ],
        };

        assert!(eval(&expr, &event));
    }

    // 시나리오 12: /32 CIDR이 단일 IP 주소와 정확히 일치할 때만 in_cidr가 true를 반환하는지 확인
    #[test]
    fn cidr_single_ip_with_slash_32() {
        let event = json!({ "src_ip": "185.220.101.45" });
        assert!(eval(
            &cond("src_ip", Op::InCidr, Some(json!("185.220.101.45/32"))),
            &event
        ));
        assert!(!eval(
            &cond("src_ip", Op::InCidr, Some(json!("185.220.101.46/32"))),
            &event
        ));
    }

    // 시나리오 13: AND의 모든 조건이 true이면 true를 반환하고,
    // 하나라도 false이면 false를 반환하는지 확인
    #[test]
    fn and_returns_true_only_when_all_conditions_are_true() {
        let event = json!({
            "event_type": "file_download",
            "score": 50
        });

        assert!(eval(
            &Expr::And {
                and: vec![
                    cond("event_type", Op::Eq, Some(json!("file_download"))),
                    cond("score", Op::Gte, Some(json!(50))), // 50 >= 50 -> true
                ],
            },
            &event
        ));

        assert!(!eval(
            &Expr::And {
                and: vec![
                    cond("event_type", Op::Eq, Some(json!("file_download"))),
                    cond("score", Op::Gt, Some(json!(50))), // 50 > 50 -> false
                ],
            },
            &event
        ));
    }

    // 시나리오 14: AND와 OR에 조건이 하나도 없으면 false를 반환하는지 확인
    #[test]
    fn empty_and_or_are_false() {
        let event = json!({});
        assert!(!eval(&Expr::And { and: vec![] }, &event));
        assert!(!eval(&Expr::Or { or: vec![] }, &event));
    }

    // 시나리오 15: 숫자와 문자열처럼 타입이 다른 값을 eq/ne로 비교하면
    // 비교할 수 없으므로 eq와 ne 모두 false를 반환하는지 확인
    #[test]
    fn eq_ne_type_mismatch_is_always_false() {
        let event = json!({ "score": 10 });

        // 숫자 필드 vs 문자열 value: 타입이 달라 비교 불가 → eq도 ne도 false
        assert!(!eval(&cond("score", Op::Eq, Some(json!("10"))), &event)); // false
        assert!(!eval(&cond("score", Op::Ne, Some(json!("10"))), &event)); // false
    }

    // 시나리오 16: exists 조건이 AND 안에 포함되어도 다른 조건과 함께 정상적으로 평가되는지 확인
    #[test]
    fn and_with_exists_condition() {
        let event = json!({ "event_type": "file_download", "src_ip": "1.2.3.4" });
        let expr = Expr::And {
            and: vec![
                cond("src_ip", Op::Exists, None),
                cond("event_type", Op::Eq, Some(json!("file_download"))),
            ],
        };
        assert!(eval(&expr, &event));
    }
}
