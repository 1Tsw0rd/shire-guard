# #!/bin/bash
# # probe.sh — 동시성을 올려가며 실패 0%의 최대치를 찾는다
# # 사용법: ./probe.sh file_download <start_id>

# event_type=$1
# start=$2
# count=15000

# parallels=(150 200 300 400 500 700 900 1100 1300 1500 2000)

# for p in "${parallels[@]}"; do
#   echo "════════════════════════════════════"
#   echo "동시성 $p 테스트"
#   ./loadgen.exe "$event_type" "$start" "$count" "$p"
#   start=$((start + count))
#   echo ""
#   sleep 8   # 소켓/시스템 안정화용 쿨다운
# done

# echo "════════════════════════════════════"
# echo "모든 동시성 테스트 완료"

#!/bin/bash
# probe.sh — 동시성 150 고정, count를 늘려가며 지속 처리량을 관찰한다
# 사용법: ./probe.sh file_download <start_id>

event_type=$1
start=$2
parallel=150

counts=(10000000)

for c in "${counts[@]}"; do
  echo "════════════════════════════════════"
  echo "count $c 테스트 (동시성 $parallel 고정)"
  ./loadgen.exe "$event_type" "$start" "$c" "$parallel"
  start=$((start + c))
  echo ""
  sleep 15   # count가 커질수록 실행시간도 길어지므로 쿨다운도 넉넉히
done

echo "════════════════════════════════════"
echo "모든 count 테스트 완료"
