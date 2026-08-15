#!/usr/bin/env bash
# trace_scores.sh — 실행 중인 스택에서 코호트별 점수 궤적을 뽑는다.
#
# 왜 클라이언트가 아니라 여기서 재는가:
#   `/queue/status` 는 점수를 돌려주지 않는다. 돌려주게 만들면 대기열 경로가
#   `score:` 를 읽게 되어 불변식 5(탐지 경로와 admit 경로 분리)가 깨진다 —
#   `internal/api/separation_test.go` 가 정확히 그것을 금지한다. 그래서 측정은
#   **밖에서** 한다. Redis 를 직접 읽되, 읽기만 하고 아무것도 바꾸지 않는다.
#
# 무엇이 나오는가:
#   샘플 시각마다 전 참가자의 (token, fp_hash, score, state, shard) 한 줄씩.
#   이 한 파일에서 두 그림이 나온다 —
#     (a) 코호트별 평균 점수의 시간 궤적 (봇 vs 사람이 언제 갈라지는가)
#     (b) 코호트별 격리 비율의 시간 궤적 = time-to-detection 의 CDF
#   (b) 를 k6 쪽에서 재지 않는 이유는 k6 의 Trend 가 백분위만 남기고 원본
#   표본을 버리기 때문이다. 서버 상태를 직접 세면 분포 전체가 남는다.
#
# 코호트는 fp_hash 로 가른다. 지문 해시는 클라이언트가 보낸 값 그대로 저장되므로
# (internal/api/gate.go), loadtest/k6/lib/personas.js 의 상수와 맞대면 된다:
#   fp-farm-image-a → naive + mimic   (둘은 /24 도 같아서 서버 쪽에서 구분되지 않는다)
#   fp-farm-image-b → distributed
#   그 밖             → 사람
#
# usage: trace_scores.sh <out.tsv> [interval_sec] [max_sec]
set -euo pipefail

OUT=${1:?usage: trace_scores.sh <out.tsv> [interval_sec] [max_sec]}
INTERVAL=${2:-5}
MAX=${3:-900}

COMPOSE=${COMPOSE:-docker compose -f deploy/docker-compose.yml}
PW=${SG_REDIS_PASSWORD:-shardgate-local-dev}

# SCAN 을 Lua 안에서 도는 이유는 왕복을 한 번으로 줄이기 위해서다. 참가자가
# 수백 명이면 HGETALL 을 하나씩 보내는 것만으로 샘플 간격을 잡아먹는다.
read -r -d '' SCRIPT <<'LUA' || true
local out = {}
local cursor = "0"
repeat
  local r = redis.call('SCAN', cursor, 'MATCH', 'user:*', 'COUNT', 2000)
  cursor = r[1]
  for _, k in ipairs(r[2]) do
    local h = redis.call('HMGET', k, 'fp_hash', 'score', 'state', 'shard')
    out[#out+1] = k .. '\t' .. (h[1] or '') .. '\t' .. (h[2] or '')
      .. '\t' .. (h[3] or '') .. '\t' .. (h[4] or '')
  end
until cursor == "0"
return out
LUA

printf 'elapsed_s\tuser_key\tfp_hash\tscore\tstate\tshard\n' >"$OUT"

start=$(date +%s)
while :; do
  now=$(date +%s)
  elapsed=$((now - start))
  [ "$elapsed" -ge "$MAX" ] && break

  # 스택이 내려가는 중이면 조용히 끝낸다 — 샘플러가 실행을 실패시키면 안 된다.
  if ! rows=$($COMPOSE exec -T redis redis-cli --no-auth-warning -a "$PW" --raw \
      EVAL "$SCRIPT" 0 2>/dev/null); then
    break
  fi
  if [ -n "$rows" ]; then
    printf '%s\n' "$rows" | awk -v t="$elapsed" 'NF {print t "\t" $0}' >>"$OUT"
  fi

  sleep "$INTERVAL"
done

echo "trace → $OUT ($(wc -l <"$OUT") lines)"
