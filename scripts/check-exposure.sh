#!/usr/bin/env bash
# 포트 노출 점검.
#
# 2026-08-12, 이 저장소의 `make dev` 가 인증 없는 Redis 를 0.0.0.0:6379 에 띄웠고,
# 몇 분 만에 스캐너가 크론 주입 페이로드를 심었다. 호스트에 공인 IP 가 직접
# 붙어 있었고, macOS 방화벽은 Docker 의 포트 게시를 막지 못했다.
#
# 그래서 이 스크립트가 있다. 사람의 주의력이 아니라 검사로 막는다.
#
#   scripts/check-exposure.sh          점검 (노출 있으면 exit 1)
#
# 검사 순서는 "우리 책임"에서 "참고"로 간다:
#   1. compose 파일    — 우리가 고칠 수 있는 것. 어기면 실패. Docker 없이도 돈다(CI용).
#   2. 실행 중 컨테이너 — 우리 프로젝트의 실제 바인딩. 어기면 실패.
#   3. 그 외 컨테이너·호스트 리스너·공인 IP — 남의 것이거나 환경. 경고만.

set -uo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.yml}"
PROJECT="${SG_COMPOSE_PROJECT:-shardgate}"
LOOPBACK_RE='^(127\.0\.0\.1|\[?::1\]?):'

fail=0
warned=0

red()  { printf '\033[31m%s\033[0m\n' "$*"; }
ylw()  { printf '\033[33m%s\033[0m\n' "$*"; }
grn()  { printf '\033[32m%s\033[0m\n' "$*"; }
head_() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── 1. compose 파일 ─────────────────────────────────────────────────────
# 실행 중이 아니어도, Docker 가 없어도 검사된다. 회귀는 여기서 잡는 것이 가장 싸다.
head_ "1. compose 포트 선언 ($COMPOSE_FILE)"
if [ ! -f "$COMPOSE_FILE" ]; then
  ylw "  건너뜀 — 파일이 없다"
else
  bad_lines=$(grep -nE '^\s*ports:' "$COMPOSE_FILE" | grep -vE '127\.0\.0\.1:')
  if [ -n "$bad_lines" ]; then
    red "  ✗ 루프백에 묶이지 않은 포트 선언:"
    printf '%s\n' "$bad_lines" | sed 's/^/      /'
    red "    → 127.0.0.1:<host>:<container> 형식으로 바꿀 것."
    red "      다른 기기에서 붙어야 하면 SSH 터널을 쓴다:"
    red "      ssh -L 8088:127.0.0.1:8088 <this-host>"
    fail=1
  else
    grn "  ✓ 모든 게시 포트가 127.0.0.1 바인딩"
  fi
fi

# Docker 가 없으면 여기까지가 전부다(CI 등).
if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  head_ "요약"
  ylw "  Docker 를 쓸 수 없어 정적 검사만 수행했다."
  [ "$fail" -eq 0 ] && grn "  통과" || red "  실패"
  exit "$fail"
fi

# ── 2. 실행 중인 이 프로젝트의 컨테이너 ─────────────────────────────────
head_ "2. 실행 중인 $PROJECT 컨테이너의 실제 바인딩"
ours=$(docker ps --filter "label=com.docker.compose.project=$PROJECT" \
                 --format '{{.Names}}\t{{.Ports}}' 2>/dev/null)
if [ -z "$ours" ]; then
  ylw "  실행 중인 컨테이너 없음"
else
  exposed=""
  while IFS=$'\t' read -r name ports; do
    [ -z "${ports:-}" ] && continue
    # "0.0.0.0:6379->6379/tcp, [::]:6379->6379/tcp" 를 매핑 단위로 쪼갠다.
    printf '%s' "$ports" | tr ',' '\n' | while read -r m; do
      m=$(printf '%s' "$m" | sed 's/^ *//;s/ *$//')
      case "$m" in
        *'->'*) ;;          # 게시된 것만 본다
        *) continue ;;
      esac
      hostpart="${m%%->*}"
      if ! printf '%s' "$hostpart" | grep -qE "$LOOPBACK_RE"; then
        printf '%s\t%s\n' "$name" "$m"
      fi
    done
  done <<< "$ours" > /tmp/sg_exposed.$$ 2>/dev/null
  exposed=$(cat /tmp/sg_exposed.$$ 2>/dev/null); rm -f /tmp/sg_exposed.$$

  if [ -n "$exposed" ]; then
    red "  ✗ 외부에 게시된 포트:"
    printf '%s\n' "$exposed" | sed 's/^/      /'
    red "    → compose 를 고치고 'make down && make dev' 로 재생성할 것."
    fail=1
  else
    grn "  ✓ 전부 루프백"
  fi
fi

# ── 3. 그 외 컨테이너 (경고) ────────────────────────────────────────────
head_ "3. 다른 컨테이너 (참고)"
others=$(docker ps --format '{{.Label "com.docker.compose.project"}}\t{{.Names}}\t{{.Ports}}' 2>/dev/null \
         | awk -F'\t' -v p="$PROJECT" '$1 != p && $3 ~ /->/ {print $2 "\t" $3}' \
         | grep -vE '127\.0\.0\.1:|\[::1\]:')
if [ -n "$others" ]; then
  ylw "  ⚠ 이 프로젝트 밖에서 외부에 열린 포트:"
  printf '%s\n' "$others" | sed 's/^/      /'
  ylw "    → 이 저장소가 고칠 수 없다. 필요 없다면 127.0.0.1 로 다시 띄울 것."
  warned=1
else
  grn "  ✓ 없음"
fi

# ── 4. 호스트 리스너 (경고) ─────────────────────────────────────────────
head_ "4. 호스트의 외부 바인딩 리스너 (참고)"
listeners=""
if command -v lsof >/dev/null 2>&1; then
  listeners=$(lsof -nP -iTCP -sTCP:LISTEN 2>/dev/null \
    | awk 'NR>1 && $9 !~ /127\.0\.0\.1|\[::1\]/ {print "      " $1 "  " $9}' | sort -u)
elif command -v ss >/dev/null 2>&1; then
  listeners=$(ss -lntH 2>/dev/null \
    | awk '$4 !~ /^127\.0\.0\.1:|^\[::1\]:/ {print "      " $4}' | sort -u)
fi
if [ -n "$listeners" ]; then
  ylw "  ⚠ 다음이 외부에서 접근 가능하다:"
  printf '%s\n' "$listeners"
  warned=1
else
  grn "  ✓ 없음 (또는 확인 도구 없음)"
fi

# ── 5. 공인 IP 직결 여부 (경고) ─────────────────────────────────────────
# 이게 붙어 있으면 위의 모든 노출이 "사설망 노출"이 아니라 "인터넷 노출"이 된다.
head_ "5. 인터페이스에 공인 IP 가 직접 붙어 있는가 (참고)"
public=$( { ifconfig -a 2>/dev/null || ip -o addr show 2>/dev/null; } \
  | awk '
      /^[a-z0-9]+:/ { iface = $1; sub(":$", "", iface) }
      /inet /       { print iface, $2 }
    ' \
  | awk '
      {
        addr = $2
        sub("/.*", "", addr)                  # ip(8) 는 10.0.0.4/24 형태로 준다
        if (split(addr, o, ".") != 4) next    # IPv4 만 본다
        a = o[1] + 0; b = o[2] + 0
        if (a == 127) next                                  # 루프백
        if (a == 10) next                                   # RFC1918
        if (a == 192 && b == 168) next                      # RFC1918
        if (a == 172 && b >= 16 && b <= 31) next            # RFC1918
        if (a == 169 && b == 254) next                      # 링크로컬
        if (a == 100 && b >= 64 && b <= 127) next           # CGNAT
        printf "      %s %s\n", $1, addr
      }
    ')
if [ -n "$public" ]; then
  ylw "  ⚠ NAT 뒤가 아니다 — 0.0.0.0 바인딩은 곧 인터넷 노출이다:"
  printf '%s\n' "$public"
  ylw "    → 랜선을 공유기 LAN 포트로 옮기면 사설 IP 를 받는다."
  warned=1
else
  grn "  ✓ 사설 IP 만 (NAT 뒤)"
fi

# ── 요약 ────────────────────────────────────────────────────────────────
head_ "요약"
if [ "$fail" -ne 0 ]; then
  red "  ✗ 이 저장소가 외부에 포트를 열고 있다. 위 항목을 고칠 것."
elif [ "$warned" -ne 0 ]; then
  ylw "  △ 이 저장소는 깨끗하다. 다만 환경에 노출 요소가 있다(위 참고 항목)."
else
  grn "  ✓ 노출 없음"
fi
exit "$fail"
