#!/usr/bin/env python3
"""powcost.py — 봇 1마리를 입장시키는 데 드는 비용(DESIGN §1.4).

§1.4 는 목표를 "봇의 완전 차단"이 아니라 **봇의 비용을 정상 사용자의 가치 이상으로
끌어올리는 것**이라고 선언한다. 그 선언은 지금까지 한 번도 측정되지 않았다 —
탐지율·오탐율·자리 배분은 재 왔지만 비용은 문장으로만 있었다.

비용을 부하 테스트로 잴 수는 없다. 난이도를 올리면 k6 자신이 CPU 에 묶여
탐지 지표까지 무너진다(REPORT §3.7). 그래서 **부하와 경제를 분리한다**:

  1. 해시율은 참조 하드웨어에서 오프라인으로 잰다      → make bench-pow
  2. 시도 횟수는 §3.7 의 부하 측정에서 가져온다        → loadtest/results/*/​*.json
  3. 둘을 곱한다. PoW 스케줄이 결정적(기대 시도 = 2^d)이라 곱이 성립한다.

사용:
    python3 loadtest/tools/powcost.py --hashrate 16.2e6 loadtest/results/door10

기본값의 출처는 아래 SOURCES 에 적었다. 값이 아니라 **출처가 있는 값**이어야
이 표가 포트폴리오에서 의미를 갖는다.
"""

import argparse
import json
import os
import sys

# ── 단가 출처 ──────────────────────────────────────────────────────────────
#
# 가격은 시장 값이라 시간이 지나면 틀려진다. 그래서 전부 플래그로 덮어쓸 수 있게
# 두고, 기본값에는 언제·어디서 온 값인지를 붙인다.
SOURCES = [
    ("--captcha-usd-1k 2.99",
     "2Captcha 공시 단가, Cloudflare Turnstile 1,000건 (2026-08 조회). "
     "https://2captcha.com/p/cloudflare-turnstile — 같은 조사에서 이미지 캡차 "
     "$0.5~1.0/1k, reCAPTCHA v2 $1.0~3.0/1k 로 Turnstile 은 상단이다."),
    ("--cpu-usd-hour 0.036",
     "AWS EC2 c7g 온디맨드 us-east-1 을 vCPU 로 나눈 값 "
     "($0.145/h ÷ 4 vCPU, 2026-08 조회). https://aws.amazon.com/ec2/pricing/on-demand/ "
     "— 스팟은 여기서 30~90% 더 싸므로, 이 기본값은 **공격자에게 가장 비싼** 쪽이다."),
    ("--hashrate 16.2e6",
     "make bench-pow (Apple M4 Pro, Go 표준 crypto/sha256, 단일 코어): "
     "61.65 ns/op = 16.2 MH/s. BenchmarkSolve 의 8/12/16비트 실측이 이 값의 "
     "2^d 외삽과 ±10% 안에서 맞는다."),
]


def solve_cost(difficulty, hashrate, cpu_usd_hour):
    """난이도 d 의 기대 풀이 비용. (시도 횟수, 초, 달러)"""
    hashes = 2.0 ** difficulty
    seconds = hashes / hashrate
    return hashes, seconds, seconds * cpu_usd_hour / 3600.0


def schedule(base, bump, cap, rounds):
    """회차별 난이도. botscore.Difficulty 와 같은 규칙이다.

    실제 규칙은 max(의심도, 회차)이고 여기서는 회차만 넣는다 — 의심도는 지문·대역의
    함수라 팜 구성에 따라 달라지고, **회차만 쓰면 하한**이 나오기 때문이다.
    즉 아래 비용은 봇이 실제로 무는 값보다 작거나 같다.
    """
    return [min(base + bump * r, cap) for r in range(1, rounds + 1)]


def dig(obj, path, default=None):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur if cur is not None else default


def load_runs(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += [os.path.join(p, f) for f in sorted(os.listdir(p))
                      if f.endswith(".json")]
        else:
            files.append(p)
    return [(f, json.load(open(f))) for f in files]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="*", help="결과 JSON 또는 디렉터리")
    ap.add_argument("--hashrate", type=float, default=16.2e6, help="초당 해시 (단일 코어)")
    ap.add_argument("--cpu-usd-hour", type=float, default=0.036, help="vCPU 시간당 단가(USD)")
    ap.add_argument("--captcha-usd-1k", type=float, default=2.99, help="대행 1,000건 단가(USD)")
    ap.add_argument("--base", type=int, default=16, help="POW_BASE_DIFFICULTY")
    ap.add_argument("--bump", type=int, default=4, help="GREYLIST_DIFFICULTY_BUMP")
    ap.add_argument("--cap", type=int, default=26, help="POW_MAX_DIFFICULTY")
    ap.add_argument("--prize-usd", type=float, default=50.0,
                    help="봇이 얻는 것의 값(티켓 프리미엄 등). §1.4 의 판정 기준")
    args = ap.parse_args()

    captcha_usd = args.captcha_usd_1k / 1000.0

    print(f"\n해시율 {args.hashrate / 1e6:.1f} MH/s · vCPU ${args.cpu_usd_hour}/h "
          f"· 대행 ${args.captcha_usd_1k}/1k\n")

    # ── 난이도별 단가 ──────────────────────────────────────────────────────
    print("| 난이도 | 기대 시도 | 풀이 시간 | 1회 비용 | 대행 1회 대비 |")
    print("|---|---|---|---|---|")
    for d in [8, 12, 16, 20, 24, 26, 30, 32]:
        hashes, sec, usd = solve_cost(d, args.hashrate, args.cpu_usd_hour)
        ratio = usd / captcha_usd
        mark = " ←설정 상한" if d == args.cap else (" ←기본" if d == args.base else "")
        t = f"{sec * 1000:.2f} ms" if sec < 1 else f"{sec:.1f} s"
        print(f"| {d}{mark} | {hashes:,.0f} | {t} | ${usd:.2e} | {ratio * 100:.3f}% |")

    # 두 방어의 단가가 같아지는 지점. PoW 로 대행 한 번의 값을 물리려면
    # 몇 비트가 필요한가 — 이 값이 설정 상한보다 크면 PoW 는 대행의 대체재가 아니다.
    parity = 0
    while solve_cost(parity, args.hashrate, args.cpu_usd_hour)[2] < captcha_usd:
        parity += 1
    print(f"\n대행 1회({captcha_usd * 1000:.2f}/1k)와 같은 값이 되는 난이도: "
          f"**{parity}비트** (설정 상한 {args.cap}비트)")
    if parity > args.cap:
        gap = 2.0 ** (parity - args.cap)
        print(f"  → 상한까지 올려도 대행보다 {gap:,.0f}배 싸다. "
              f"PoW 는 대행의 대체재가 아니라 **하한선**이다.")

    if not args.results:
        print()
        for flag, src in SOURCES:
            print(f"  {flag}\n      {src}")
        print()
        return

    # ── 실측 시도 횟수에 곱한다 ────────────────────────────────────────────
    runs = load_runs(args.results)
    if not runs:
        sys.exit("결과 파일이 없다")

    print(f"\n\n부하 측정 {len(runs)}회 — 입장한 봇 1마리당 비용\n")
    print("| 실행 | 재검증 시도 | 복귀 | 입장 봇 | PoW 총비용 | 대행 총비용 | 봇 1마리당 |")
    print("|---|---|---|---|---|---|---|")

    for path, run in runs:
        attempts = dig(run, "rechallenge.bot_attempts", 0)
        restored = dig(run, "rechallenge.restored", 0)
        bots = dig(run, "observed.bots", 0)
        rate = dig(run, "admission.bot_success_rate", 0) or 0
        admitted = bots * rate

        # 시도를 회차에 배분한다. 상한(max_attempts)이 있으므로 회차는 순환한다.
        # 정확한 회차별 분포는 결과 JSON 에 없으므로 균등 배분으로 근사하고,
        # 근사라는 사실을 표 아래에 적는다.
        rounds = schedule(args.base, args.bump, args.cap, 2)
        per = attempts / len(rounds) if rounds else 0
        pow_usd = sum(solve_cost(d, args.hashrate, args.cpu_usd_hour)[2] * per for d in rounds)
        captcha_total = attempts * captcha_usd

        each = (pow_usd + captcha_total) / admitted if admitted else float("nan")
        print(f"| {os.path.basename(path)} | {attempts:.0f} | {restored:.0f} | "
              f"{admitted:.1f} | ${pow_usd:.4f} | ${captcha_total:.2f} | ${each:.4f} |")

    # ── §1.4 판정 ──────────────────────────────────────────────────────────
    #
    # "봇의 비용 > 정상 사용자의 가치" 인가. 비용은 위에서 나왔고 가치는 입력이다.
    # 두 값을 나란히 놓는 것 자체가 이 리포트에서 처음 하는 일이다 — 지금까지
    # 재 온 것은 전부 **몇 마리를 잡았는가**이지 **얼마에 잡았는가**가 아니었다.
    per_bot = [
        (dig(r, "rechallenge.bot_attempts", 0) * captcha_usd
         + sum(solve_cost(d, args.hashrate, args.cpu_usd_hour)[2]
               * dig(r, "rechallenge.bot_attempts", 0) / 2 for d in schedule(args.base, args.bump, args.cap, 2)))
        / max(1e-9, dig(r, "observed.bots", 0) * (dig(r, "admission.bot_success_rate", 0) or 0))
        for _, r in runs
    ]
    mean_cost = sum(per_bot) / len(per_bot)
    need = args.prize_usd / mean_cost if mean_cost else float("inf")
    print(f"\n§1.4 판정 — 봇 1마리 입장에 ${mean_cost:.4f}, 얻는 것이 ${args.prize_usd:.2f}")
    if mean_cost < args.prize_usd:
        print(f"  → **목표 미달.** 비용이 가치의 1/{need:,.0f} 이다. 챌린지 층만으로는")
        print(f"     이 격차를 메울 수 없다 — 난이도를 상한까지 올려도 PoW 몫은 위 표대로")
        print(f"     대행 단가의 {solve_cost(args.cap, args.hashrate, args.cpu_usd_hour)[2] / captcha_usd * 100:.1f}% 다.")
        print("     남은 지렛대는 §12-1 의 계정·결제 수준 정책이다.")
    else:
        print("  → 목표 달성. 봇의 비용이 얻는 것보다 크다.")

    print(f"\n난이도 스케줄: {schedule(args.base, args.bump, args.cap, 2)} "
          f"(base {args.base} + bump {args.bump} × 회차, 상한 {args.cap})")
    print("시도를 회차에 균등 배분한 근사다 — 결과 JSON 에 회차별 분포가 없다.")
    print("의심도 축(지문·대역)은 빼고 회차만 넣었으므로 **하한**이다(schedule() 주석).\n")

    for flag, src in SOURCES:
        print(f"  {flag}\n      {src}")
    print()


if __name__ == "__main__":
    main()
