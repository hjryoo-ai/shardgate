#!/usr/bin/env bash
# sweep.sh — 한 설정(팔)을 N회 반복 측정한다.
#
# 왜 반복인가 (docs/REPORT.md §3.2): 같은 설정을 두 번 돌리면 탐지율이 9pp
# 흔들린다. 1회 실행 표로는 설정 간 차이를 읽을 수 없다.
#
# **시드는 반복 번호에서만 나온다.** 팔 이름은 들어가지 않는다. 그래서 팔 A 의
# 3회차와 팔 B 의 3회차는 클라이언트 행동(폴링 지터·포인터 엔트로피)이 같다 —
# 짝지은 비교가 된다. 다만 이것으로 실행이 결정적이 되지는 않는다(shardgate.js
# 의 rand() 주석 참고). 서버 쪽 도착 순서와 스코어러 창 경계는 여전히 흔들린다.
#
# 매 반복마다 스택을 완전히 새로 만든다. Redis 에 남은 대기열, 스코어러의
# 메모리 창, PG 의 주문 UNIQUE 제약이 전부 다음 실행을 오염시킨다.
#
# usage:
#   ARM=ceiling REPS=5 SG_ADMIT_OFF=1 SG_TOTAL=600 ... loadtest/tools/sweep.sh
#
# 팔 설정은 SG_* 환경변수로 준다(compose 가 그대로 읽는다). 추가로:
#   ARM            결과 디렉터리 이름 (필수)
#   REPS           반복 횟수 (기본 5)
#   SG_ADMIT_OFF   1 이면 admission 서비스를 아예 띄우지 않는다 → 아무도 입장하지 않는다
#   SG_TRACE       0 이면 점수 궤적 샘플링을 끈다 (기본 켬)
#   SG_CAPTCHA_PROXY  봇이 재검증을 통과하는 비율(0~1). 클라이언트 쪽 팔 정의이므로
#                  compose 가 아니라 k6 컨테이너로 넘어간다. 결과 JSON 에 그대로 남는다.
#   SG_CLUMSY      오탐으로 걸릴 만한 사람의 수(기본 0). 켜면 사람 코호트의 구성이
#                  달라지므로 §3.7 세 팔과 나란히 읽을 수 없다 — 별도 팔로만 쓴다.
set -euo pipefail

ARM=${ARM:?ARM 을 줘야 한다}
REPS=${REPS:-5}
ADMIT_OFF=${SG_ADMIT_OFF:-0}
TRACE=${SG_TRACE:-1}

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"

COMPOSE="docker compose -f deploy/docker-compose.yml"
OUTDIR="loadtest/results/$ARM"
PW=${SG_REDIS_PASSWORD:-shardgate-local-dev}
NET=${SG_COMPOSE_NET:-shardgate_default}
K6_IMAGE=${K6_IMAGE:-grafana/k6:latest}

mkdir -p "$OUTDIR"

# 오픈 시각은 매 반복마다 "지금"으로 다시 잡는다. 고정값을 쓰면 두 번째 반복은
# 추첨 구간이 이미 지난 뒤에 시작해 팔이 조용히 바뀐다.
open_at_now() { date -u -v+5S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+5 seconds' +%Y-%m-%dT%H:%M:%SZ; }

# (여기 있던 dur_ns 는 사라졌다. 설정 대조를 서비스가 찍는 **원시 env 문자열**로
#  하게 되면서 "200s"를 나노초로 바꿀 일이 없어졌다 — 손잡이별 검사를 일반 규칙으로
#  바꾸면서 단위 변환이라는 우발적 복잡도까지 같이 없어진 것이다.)

wait_http() {
  local url=$1 tries=${2:-60}
  for _ in $(seq "$tries"); do
    curl -fsS -m 2 "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# 스코어러가 파티션을 몇 개 배정받았는지 본다.
#
# 0개인 채로 Stable 이 되는 고장이 실재한다(ROADMAP 결함 7): 토픽이 없으면
# 조인은 성공하고 ReadMessage 는 오류 없이 블록한다. 아무것도 실패하지 않고
# **탐지만 통째로 꺼진다.** 그 상태로도 부하 시나리오는 멀쩡히 끝나서
# "탐지율 0.0%" 라는 그럴듯한 표를 남긴다. 그래서 측정 전에 확인한다.
scorer_partitions() {
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --members \
    --group "${SG_KAFKA_GROUP_ID:-shardgate-scorer}" 2>/dev/null |
    awk 'NR>1 && NF>=4 {n+=$(NF)} END {print n+0}'
}

echo "══ ARM=$ARM  REPS=$REPS  admit_off=$ADMIT_OFF ══"

for i in $(seq 1 "$REPS"); do
  echo ""
  echo "── $ARM r$i ──────────────────────────────────────────"

  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true

  # **빌드를 오픈 시각 도장보다 먼저 한다.**
  #
  # 이미지를 새로 만들어야 하는 회차는 빌드에 몇 분이 걸린다. 시각을 먼저 찍으면
  # 그 사이에 추첨 구간(기본 90초)이 통째로 지나가고, k6 가 붙었을 땐 전원이 FIFO
  # 밴드로 들어간다 — §3.2 의 공정성 모델이 꺼진 채로 재게 된다(ROADMAP 결함 6).
  # 실제로 코드를 고친 뒤 첫 회차가 그렇게 나왔다: 같은 팔의 r1 만 추첨 0%,
  # r2·r3 은 캐시가 따뜻해 100%.
  #
  # 빌드를 밖으로 빼면 도장 이후에 남는 것은 컨테이너 기동뿐이라 수십 초로 줄어든다.
  $COMPOSE build >/dev/null

  export SG_EVENT_OPEN_AT=${SG_EVENT_OPEN_AT_FIXED:-$(open_at_now)}

  # admit 을 끄는 방법: 서비스를 내리는 게 아니라 **예산을 0 으로 배분한다.**
  #
  #   perCycle = Round(rate_per_min × interval_분) = Round(1 × 1/60) = 0
  #
  # 서비스를 안 띄우면 `/admission/redeem` 자체가 502 가 되어 클라이언트가
  # 겪는 것이 "아직 차례가 아님"이 아니라 "서버 고장"이 된다. 실제로 그렇게
  # 한 번 재서 redeem 실패율 100% 를 얻었다. 예산을 0 으로 두면 배분 루프도
  # redeem 경로도 평소대로 돌고, admit.lua 의 `rank < budget` 만 아무에게도
  # 참이 되지 않는다 — 바꾸는 것이 정확히 하나다.
  if [ "$ADMIT_OFF" = "1" ]; then
    export SG_ADMIT_RATE_PER_MIN=1 SG_ADMIT_INTERVAL=1s
  fi
  $COMPOSE up -d >/dev/null

  wait_http http://localhost:8080/healthz || { echo "gate 가 안 뜬다"; exit 1; }
  wait_http http://localhost:8081/healthz || { echo "queue 가 안 뜬다"; exit 1; }
  wait_http http://localhost:8082/healthz || { echo "admission 이 안 뜬다"; exit 1; }
  wait_http http://localhost:8083/healthz || { echo "scorer 가 안 뜬다"; exit 1; }

  # 파티션이 붙을 때까지 기다린다. 붙지 않으면 측정을 시작하지 않는다.
  parts=0
  for _ in $(seq 40); do
    parts=$(scorer_partitions)
    [ "$parts" -gt 0 ] 2>/dev/null && break
    sleep 2
  done
  if ! [ "$parts" -gt 0 ] 2>/dev/null; then
    echo "스코어러 파티션이 0 이다 — 탐지가 꺼진 채로 재게 된다. 중단."
    exit 1
  fi
  echo "  스코어러 파티션 $parts"

  # **팔이 실제로 적용됐는가 — 손잡이별이 아니라 설정 전체로 확인한다.**
  #
  # 여기 있던 것은 손잡이 세 개(admit rate/게이트, PoW 난이도, 강건 추정)를 각각
  # 로그에서 grep 하는 검사였다. 그 방식은 **손잡이가 늘 때마다 같은 구멍이 다시
  # 열린다** — 새 SG_* 를 팔로 쓰면서 검사를 안 붙이면, 이름이 컨테이너에 닿지 않아도
  # 아무것도 실패하지 않고 팔만 조용히 바뀐다(ROADMAP 결함 8).
  #
  # 그래서 뿌리를 바꿨다. 서비스가 기동 시 **환경에서 실제로 읽은 설정 전체**를
  # 찍고(`internal/app` 의 "effective config"), 여기서 팔 정의와 대조한다.
  # 규칙은 하나다:
  #
  #   팔이 정의한 모든 SG_* 는 (a) 어떤 서비스가 env 에서 읽었고 값이 같거나,
  #   (b) 하네스/클라이언트 전용 목록에 있어야 한다. 떠다니는 이름이 있으면 중단.
  #
  # (b) 를 명시적 목록으로 두는 것이 요점이다. "모르는 이름은 넘어간다"로 두면
  # 오타 하나가 조용히 통과하고, 그게 정확히 결함 8 이 일어난 방식이다.
  services_env=$($COMPOSE logs gate queue admission scorer shop 2>/dev/null |
    grep -h "effective config" || true)
  if [ -z "$services_env" ]; then
    echo "서비스가 적용 설정을 찍지 않았다 — 팔을 확인할 수 없으므로 중단."; exit 1
  fi
  if ! SG_SERVICES_ENV="$services_env" python3 - <<'PY'
import json, os, re, sys

# 서비스가 읽지 않는 이름들. k6 로만 가거나(클라이언트 행동), sweep 자신의 제어값이다.
HARNESS_ONLY = {
    "SG_TOTAL", "SG_BOT_RATIO", "SG_PATIENCE", "SG_POLL", "SG_CAPTCHA_PROXY",
    "SG_CLUMSY", "SG_SEED", "SG_BASE_URL", "SG_OUT", "SG_ARM", "SG_REP",
    "SG_TRACE", "SG_TRACE_INTERVAL", "SG_TRACE_MAX", "SG_ADMIT_OFF",
    "SG_COMPOSE_NET", "SG_EVENT_OPEN_AT_FIXED", "SG_SERVICES_ENV",
    # 비밀 값은 로그에서 가려지므로 값 대조가 불가능하다(가려지는 것이 옳다).
    "SG_REDIS_PASSWORD", "SG_EVENT_SALT", "SG_TOKEN_SIGNING_KEY",
    "SG_CHALLENGE_HMAC_KEY", "SG_POSTGRES_DSN",
}

applied = {}
for line in os.environ["SG_SERVICES_ENV"].splitlines():
    m = re.search(r'\{.*\}\s*$', line.strip())
    if not m:
        continue
    try:
        rec = json.loads(m.group(0))
    except json.JSONDecodeError:
        continue
    for k, v in (rec.get("env") or {}).items():
        applied.setdefault(k, str(v))

bad = []
for k, v in os.environ.items():
    if not k.startswith("SG_") or k in HARNESS_ONLY or not v.strip():
        continue
    if k not in applied:
        bad.append(f"{k}={v} → 어떤 서비스에도 닿지 않았다 (compose 에 이름이 있는가?)")
    elif applied[k] != v.strip():
        bad.append(f"{k}: 팔은 {v.strip()!r} 인데 적용된 값은 {applied[k]!r}")

if bad:
    print("  팔 정의와 적용된 설정이 다르다:")
    for b in bad:
        print("   -", b)
    sys.exit(1)
print(f"  적용 설정 대조 OK ({len(applied)}개 키)")
PY
  then
    echo "중단."; exit 1
  fi

  base="$OUTDIR/$ARM-r$i"

  # **재현 매니페스트.** 표의 수치가 어느 코드·어느 설정·어느 시드에서 나왔는지를
  # 결과 옆에 남긴다. 리포트가 저장소 안에서 검증되려면 결과 JSON 만으로는 부족하다 —
  # 같은 숫자를 다시 만들려면 커밋과 팔 설정이 함께 있어야 한다.
  SG_ARM="$ARM" SG_REP="$i" SG_SERVICES_ENV="$services_env" \
    python3 - "$base.manifest.json" <<'PY'
import json, os, re, subprocess, sys

def git(*args):
    try:
        return subprocess.check_output(("git",) + args, stderr=subprocess.DEVNULL,
                                       text=True).strip()
    except Exception:
        return None

SECRETS = ("SG_REDIS_PASSWORD", "SG_EVENT_SALT", "SG_TOKEN_SIGNING_KEY",
           "SG_CHALLENGE_HMAC_KEY", "SG_POSTGRES_DSN")
env = {k: v for k, v in os.environ.items()
       if k.startswith("SG_") and k not in SECRETS and k != "SG_SERVICES_ENV"}

# 서비스가 실제로 적용한 설정. 위 대조를 통과한 값이므로 팔 정의와 일치하지만,
# **정의가 아니라 관측이라는 점이 중요하다** — 나중에 이 표를 의심할 사람이
# 확인할 것은 "무엇을 주려 했는가"가 아니라 "무엇이 적용됐는가"다.
applied = {}
for line in os.environ.get("SG_SERVICES_ENV", "").splitlines():
    m = re.search(r'\{.*\}\s*$', line.strip())
    if not m:
        continue
    try:
        rec = json.loads(m.group(0))
    except json.JSONDecodeError:
        continue
    for k, v in (rec.get("env") or {}).items():
        applied.setdefault(k, str(v))

commit = git("rev-parse", "HEAD")
json.dump({
    "arm": os.environ.get("SG_ARM"),
    "rep": int(os.environ.get("SG_REP", "0")),
    # 시드는 팔 이름이 아니라 반복 번호에서 나온다 — 팔 간 짝지은 비교가 되도록.
    "seed": "r" + os.environ.get("SG_REP", "0"),
    "commit": commit,
    # git 이 없으면 "깨끗하다"가 아니라 "모른다"다. false 로 적으면 거짓말이 된다.
    "dirty": bool(git("status", "--porcelain")) if commit else None,
    "env": dict(sorted(env.items())),
    "applied": dict(sorted(applied.items())),
}, open(sys.argv[1], "w"), indent=1, ensure_ascii=False, sort_keys=False)
PY

  trace_pid=""
  if [ "$TRACE" = "1" ]; then
    SG_REDIS_PASSWORD="$PW" bash loadtest/tools/trace_scores.sh \
      "$base.trace.tsv" "${SG_TRACE_INTERVAL:-5}" "${SG_TRACE_MAX:-900}" \
      >"$base.trace.log" 2>&1 &
    trace_pid=$!
  fi

  # k6 는 컨테이너로 돌린다(호스트 설치를 요구하지 않는다). 스택 네트워크에
  # 붙여 대기실 오리진(web)을 그대로 때린다 — 브라우저와 같은 경로다.
  set +e
  docker run --rm -i --network "$NET" \
    -v "$ROOT:/work" -w /work \
    -e SG_BASE_URL=http://web:80 \
    -e SG_SEED="r$i" \
    -e SG_TOTAL="${SG_TOTAL:-400}" \
    -e SG_BOT_RATIO="${SG_BOT_RATIO:-0.3}" \
    -e SG_PATIENCE="${SG_PATIENCE:-240}" \
    -e SG_POLL="${SG_POLL:-5}" \
    -e SG_CAPTCHA_PROXY="${SG_CAPTCHA_PROXY:-0}" \
    -e SG_CLUMSY="${SG_CLUMSY:-0}" \
    -e SG_OUT="$base.json" \
    "$K6_IMAGE" run loadtest/k6/mixed.js 2>&1 | tee "$base.k6.log"
  k6rc=${PIPESTATUS[0]}
  set -e

  if [ -n "$trace_pid" ]; then
    kill "$trace_pid" 2>/dev/null || true
    wait "$trace_pid" 2>/dev/null || true
  fi

  # k6 의 종료 코드는 threshold 실패로도 0이 아니게 된다. 그건 측정 실패가
  # 아니므로 결과 파일이 나왔는지로 판단한다.
  if [ ! -f "$base.json" ]; then
    echo "결과 파일이 없다 (k6 rc=$k6rc). 중단."; exit 1
  fi

  # **스코어러가 실제로 판정을 했는가.**
  #
  # 시작 전 파티션 검사(위)는 "붙었는가"만 본다. 붙은 뒤에 생산자가 죽거나 토픽이
  # 비면 컨슈머는 오류 없이 블록하고, 서비스는 전부 살아 있는 채로 **탐지만 통째로
  # 꺼진다.** 그 상태로도 시나리오는 멀쩡히 끝나서 "탐지율 0.0%" 라는 그럴듯한
  # 표를 남긴다 — 실제로 그런 실행을 한 번 얻었다. 끝난 뒤에 판정 수를 세면
  # 그 실행을 표에 넣기 전에 잡을 수 있다.
  judged=$(curl -fsS -m 5 http://localhost:8083/internal/metrics 2>/dev/null |
    awk -F' ' '/^shardgate_botscore_actions_total\{/ {n+=$2} END {print int(n)}')
  if ! [ "${judged:-0}" -gt 0 ] 2>/dev/null; then
    echo "스코어러가 판정을 한 건도 하지 않았다 (actions=$judged) — 탐지가 꺼진 채로 잰 것이다. 중단."
    exit 1
  fi
  echo "  스코어러 판정 $judged 건"

  # 추첨 구간이 실제로 열려 있었는가.
  #
  # 위에서 순서를 고쳤지만 기동 시간은 환경에 따라 달라지므로, 결과로 다시 확인한다.
  # 이 값이 낮으면 그 실행은 §3.2 가 꺼진 다른 팔이다 — 아무것도 실패하지 않고
  # 그럴듯한 표만 남기므로 표에 들어가기 전에 멈춘다.
  if [ "${SG_LOTTERY_WINDOW:-0s}" != "0s" ]; then
    if ! python3 -c "import json,sys
d = json.load(open('$base.json'))
r = (d.get('fairness') or {}).get('lottery_segment_rate')
sys.exit(0 if r is not None and r >= 0.9 else 1)"; then
      echo "추첨 구간 진입 비율이 낮다 — 오픈 시각이 지난 뒤에 측정이 시작된 것이다. 중단."
      exit 1
    fi
  fi

  # 부하 생성기가 PoW 에 묶였는지. 묶였으면 탐지 지표가 시스템이 아니라 k6 를
  # 잰 값이 된다(REPORT §3.7). 표에 섞이기 전에 멈춘다.
  if python3 -c "import json,sys; sys.exit(0 if json.load(open('$base.json')).get('load_generator_cpu_bound') else 1)"; then
    echo "부하 생성기가 PoW 에 묶였다 — 이 실행의 탐지 지표는 무효다. 중단."
    exit 1
  fi

  # **입장 채널 항등식이 성립하는가.**
  #
  #   입장 = 문(재검증으로 나와 입장) + 미플래그(경주 + 미탐)
  #
  # 앞의 네 검사는 전부 "무엇이 고장났는가"를 각각 하나씩 짚는다. 이건 성격이
  # 달라서, 무엇이 고장났는지 모르는 채로 **고장났다는 사실만** 잡는다 — 잔차가
  # 0 이 아니면 입장한 참가자 중 분류기가 모르는 경로로 들어온 사람이 있다는 뜻이고,
  # 그런 경로는 정의상 아직 표에 칸이 없다.
  #
  # 이 검사가 필요한 이유는 §3.7 에서 배운 것이다: 누수는 탐지율을 움직이지 않고
  # 입장 쪽에만 나타나므로, 채널을 세지 않으면 총계는 늘 맞아 보인다.
  if ! python3 -c "import json,sys
d = json.load(open('$base.json'))
ch = d.get('channels') or {}
bad = [(k, v.get('unaccounted')) for k, v in ch.items() if v.get('unaccounted')]
if bad:
    print('  미계측 입장 채널:', bad)
sys.exit(1 if bad else 0)"; then
    echo "입장 채널 항등식이 깨졌다 — 아무도 세지 않은 경로로 입장한 참가자가 있다. 중단."
    exit 1
  fi

  # admit_off 팔은 "입장이 느린" 것이 아니라 "입장이 없어야" 한다.
  # 0 이 아니면 팔의 전제가 틀린 것이므로 표를 만들지 않는다.
  if [ "$ADMIT_OFF" = "1" ]; then
    seats=$(python3 -c "import json,sys; d=json.load(open('$base.json'))['admission']; print((d.get('human_admitted') or 0)+(d.get('bot_admitted') or 0))")
    if [ "$seats" != "0" ]; then
      echo "admit_off 인데 $seats 명이 입장했다. 중단."; exit 1
    fi
  fi
  echo "  → $base.json"
done

echo ""
$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
echo "══ $ARM 완료 — python3 loadtest/tools/report.py summary $OUTDIR ══"
