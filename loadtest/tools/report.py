#!/usr/bin/env python3
"""반복 측정 결과를 평균 ± 95% CI 로 접는다.

왜 이 도구가 필요한가 (docs/REPORT.md §3.2):
    같은 설정을 두 번 돌리면 탐지율이 9pp, 입장률이 6~15pp 흔들린다. 그 분산
    안에서는 설정 간 순위를 읽을 수 없는데, 1회 실행 표는 그 사실을 숨긴다.
    숫자 하나를 적는 대신 구간을 적으면 "차이가 분산보다 큰가"가 표에서 바로 보인다.

    반복 수가 10 안팎이라 정규분포 대신 t 분포를 쓴다. n=3 에서 1.96 을 쓰면
    구간이 실제보다 40% 좁게 나온다 — 없는 유의성이 생긴다.

usage:
    report.py summary <run-dir-or-json>...        # mixed.js 결과 N개 → 평균±CI
    report.py trace   <trace.tsv>...              # 점수 궤적 → 코호트별 시간 궤적
    report.py final   <trace.tsv>...              # 마지막 관측 상태 분포
"""

import json
import math
import os
import statistics
import sys

# 양측 95% t 임계값. df 30 이상은 정규근사(1.96)와 3% 안쪽으로 붙는다.
T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
       8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160,
       14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093,
       20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
       26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045}

# personas.js 의 상수. 지문 해시는 클라이언트가 보낸 값 그대로 저장된다.
# 지문으로 코호트를 가른다. 모르는 지문은 사람이다.
#
# clumsy(회사 표준 이미지)를 따로 두는 이유: 이 사람들은 사람인데 점수가 봇처럼
# 오른다. 사람 버킷에 섞으면 평균이 끌려 올라가 **정상 사용자의 궤적이 실제보다
# 나빠 보이고**, 오탐 14.3% 가 어느 쪽에서 왔는지도 보이지 않는다.
COHORT_BY_FP = {
    "fp-farm-image-a": "bot:farm-a",
    "fp-farm-image-b": "bot:farm-b",
    "fp-corp-golden-image": "human:clumsy",
}
ISOLATED_STATES = {"greylist", "held", "blocked"}


def ci95(xs):
    """(평균, 95% CI 반폭). 표본이 1개면 반폭은 정의되지 않는다."""
    n = len(xs)
    if n == 0:
        return None, None
    mean = statistics.fmean(xs)
    if n == 1:
        return mean, None
    sd = statistics.stdev(xs)
    t = T95.get(n - 1, 1.96)
    return mean, t * sd / math.sqrt(n)


def fmt(mean, half, scale=1.0, digits=1):
    if mean is None:
        return "n/a"
    if half is None:
        return f"{mean * scale:.{digits}f}"
    return f"{mean * scale:.{digits}f} ± {half * scale:.{digits}f}"


def dig(obj, path, default=None):
    for part in path.split("."):
        if not isinstance(obj, dict) or part not in obj:
            return default
        obj = obj[part]
    return obj


# ── summary ────────────────────────────────────────────────────────────────

# (라벨, JSON 경로, 배율, 소수 자릿수)
FIELDS = [
    ("탐지율(recall) %", "detection.recall", 100, 1),
    ("오탐율(FPR) %", "detection.fpr", 100, 1),
    ("사람 입장 %", "admission.human_success_rate", 100, 1),
    ("봇 입장 %", "admission.bot_success_rate", 100, 1),
    ("나간 자리", "_seats", 1, 1),
    ("사람 점유/인원비", "_share", 1, 3),
    ("격리 중앙값 s", "race.isolate_med_sec", 1, 1),
    ("격리 P90 s", "race.isolate_p90_sec", 1, 1),
    ("입장 중앙값 사람 s", "race.human_wait_med_sec", 1, 1),
    ("입장 중앙값 봇 s", "race.bot_wait_med_sec", 1, 1),
    ("추첨 구간 진입 %", "fairness.lottery_segment_rate", 100, 1),
    ("HTTP 실패 %", "http.failed_rate", 100, 2),
    # 재검증 누수 채널(§3.7). greylist 에 출구가 생기면 그 문으로 봇도 나온다.
    ("재검증 복귀", "rechallenge.restored", 1, 1),
    ("재검증 소진", "rechallenge.exhausted", 1, 1),
    ("난이도로 포기", "rechallenge.abandoned", 1, 1),
    ("재검증 시도 봇", "rechallenge.bot_attempts", 1, 1),
    ("재검증 시도 사람", "rechallenge.human_attempts", 1, 1),
    # 입장 채널 분해. 봇 입장 = 문 + 미플래그이고, 잔차는 sweep.sh 가 막는다.
    # 표에 같이 두는 이유는 **누수가 탐지율을 움직이지 않기 때문**이다 —
    # 탐지율만 보는 표에서는 문으로 새는 자리가 한 칸도 나타나지 않는다.
    ("봇 입장: 문", "channels.bot.door", 1, 1),
    ("봇 입장: 미플래그", "channels.bot.unflagged", 1, 1),
    ("사람 입장: 문", "channels.human.door", 1, 1),
]


def derive(run):
    """리포트가 실제로 읽는 두 파생값을 붙인다."""
    ha = dig(run, "admission.human_admitted", 0) or 0
    ba = dig(run, "admission.bot_admitted", 0) or 0
    humans = dig(run, "observed.humans") or 0
    bots = dig(run, "observed.bots") or 0
    seats = ha + ba
    run["_seats"] = seats
    # 사람이 인원 비율만큼의 자리를 가져갔는가. 1.0 이면 봇이 없을 때와 같다.
    total = humans + bots
    run["_share"] = (ha / seats) / (humans / total) if seats and humans and total else None
    return run


def load_runs(paths):
    """결과 JSON 만 읽는다.

    디렉터리에는 결과 말고도 JSON 이 있다(`*.manifest.json` — 재현 매니페스트).
    확장자만 보고 전부 읽으면 **반복 수가 부풀고 평균이 0 쪽으로 끌린다.**
    실제로 그렇게 한 번 표를 얻었다: 3회 측정이 "반복 6회"로 나오고 나간 자리가
    341 대신 170.7 ± 196.2 이 됐다 — 실행별 값에 `0.0` 이 섞여 있는 것으로만
    알아챌 수 있었다. 재현성을 위해 넣은 파일이 곧바로 표를 오염시킨 셈이고,
    이 리포트가 계속 경계하는 "아무것도 실패하지 않는 고장"과 같은 모양이다.

    그래서 이름으로 거르고, **모양으로 한 번 더 거른다** — 결과 파일이라면 반드시
    있는 키가 없으면 조용히 건너뛰지 않고 중단한다. 조용히 건너뛰면 새로운 종류의
    파일이 생겼을 때 같은 일이 다시 일어난다.
    """
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += [os.path.join(p, f) for f in sorted(os.listdir(p))
                      if f.endswith(".json") and not f.endswith(".manifest.json")]
        else:
            files.append(p)
    runs = []
    for f in files:
        with open(f) as fh:
            raw = json.load(fh)
        if not isinstance(raw, dict) or "detection" not in raw or "admission" not in raw:
            sys.exit(f"{f}: 결과 파일이 아니다 (detection/admission 이 없다). "
                     "결과 디렉터리에 다른 JSON 이 섞였는지 확인할 것.")
        runs.append(derive(raw))
    return files, runs


def cmd_summary(paths):
    files, runs = load_runs(paths)
    if not runs:
        sys.exit("결과 파일이 없다")

    # 부하 생성기가 CPU 에 묶인 실행이 섞이면 탐지 지표가 통째로 무효다.
    # 평균을 내면 그 사실이 숫자 뒤로 사라지므로 표보다 먼저 말한다.
    # 플래그가 없는 옛 결과 파일도 같이 걸러야 하므로 값에서도 판단한다.
    def cpu_bound(r):
        if r.get("load_generator_cpu_bound"):
            return True
        pow_ms = dig(r, "cost.pow_solve_avg_ms")
        return pow_ms is not None and pow_ms > 100

    bad = [f for f, r in zip(files, runs) if cpu_bound(r)]
    if bad:
        print("\n⚠ 부하 생성기가 PoW 에 묶인 실행이 있다 — 이 표의 탐지율/오탐율은")
        print("  시스템이 아니라 k6 를 잰 값이다(REPORT §3.7):")
        for f in bad:
            print(f"    {f}")

    # 입장 채널 항등식이 깨진 실행. sweep.sh 가 이미 막지만, 손으로 돌린 실행이나
    # 그 검사가 생기기 전의 결과 파일이 섞일 수 있다. 잔차가 있는 실행의 입장
    # 수치는 누수 채널 분석에 쓸 수 없다 — 어디로 들어왔는지 모르는 자리가 있다.
    def residual(r):
        ch = r.get("channels") or {}
        return {k: v.get("unaccounted") for k, v in ch.items() if v.get("unaccounted")}

    leaky = [(f, residual(r)) for f, r in zip(files, runs) if residual(r)]
    if leaky:
        print("\n⚠ 입장 채널 항등식이 깨진 실행이 있다 — 분류기가 모르는 경로로")
        print("  입장한 참가자가 있다. 누수 채널 분석에는 쓸 수 없다:")
        for f, res in leaky:
            print(f"    {f}  잔차 {res}")

    print(f"\n반복 {len(runs)}회 — 평균 ± 95% CI\n")
    print("| 지표 | 값 | 실행별 |")
    print("|---|---|---|")
    for label, path, scale, digits in FIELDS:
        xs = [v for v in (dig(r, path) for r in runs) if v is not None]
        mean, half = ci95(xs)
        each = " / ".join(f"{x * scale:.{digits}f}" for x in xs) if xs else "n/a"
        print(f"| {label} | {fmt(mean, half, scale, digits)} | {each} |")
    print()
    for f in files:
        print(f"  {f}")
    print()


# ── trace ──────────────────────────────────────────────────────────────────

def cohort_of(fp):
    return COHORT_BY_FP.get(fp, "human")


def load_trace(path):
    """{(elapsed, cohort): [scores], ...} 와 격리 카운트를 만든다."""
    per = {}
    with open(path) as fh:
        header = fh.readline()
        if not header.startswith("elapsed_s"):
            sys.exit(f"{path}: 헤더가 없다")
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                continue
            elapsed, _key, fp, score, state, shard = parts[:6]
            try:
                t = int(elapsed)
                s = float(score) if score else 0.0
            except ValueError:
                continue
            isolated = state in ISOLATED_STATES or shard.startswith("g")
            slot = per.setdefault((t, cohort_of(fp)), [0, 0.0, 0])
            slot[0] += 1
            slot[1] += s
            slot[2] += 1 if isolated else 0
    return per


def cmd_trace(paths, step=15):
    """여러 실행의 궤적을 같은 시각 버킷에서 평균낸다."""
    runs = [load_trace(p) for p in paths]
    cohorts = sorted({c for r in runs for (_t, c) in r})
    times = sorted({t for r in runs for (t, _c) in r})
    if not times:
        sys.exit("궤적이 비어 있다")

    print(f"\n점수 궤적 — 실행 {len(runs)}개 평균 (버킷 {step}s)\n")
    head = "| t(s) | " + " | ".join(
        f"{c} 점수 | {c} 격리% | {c} n" for c in cohorts) + " |"
    print(head)
    print("|---" * (1 + 3 * len(cohorts)) + "|")

    lo = min(times)
    for bucket in range(lo, max(times) + 1, step):
        cells = []
        any_row = False
        for c in cohorts:
            n = tot = iso = 0
            for r in runs:
                for t in range(bucket, bucket + step):
                    if (t, c) in r:
                        cnt, ssum, icnt = r[(t, c)]
                        n += cnt
                        tot += ssum
                        iso += icnt
            if n:
                any_row = True
                # n 은 (샘플 시각 × 참가자) 라 인원수가 아니다. 비율만 읽는다.
                cells += [f"{tot / n:.1f}", f"{100 * iso / n:.1f}", f"{n // len(runs)}"]
            else:
                cells += ["-", "-", "0"]
        if any_row:
            print(f"| {bucket} | " + " | ".join(cells) + " |")
    print()


def cmd_final(paths):
    """마지막 샘플의 상태 분포. 탐지율 한 숫자가 감추는 것을 편다 —
    같은 '격리'라도 greylist 와 blocked 는 봇에게 드는 비용이 다르다."""
    tally = {}
    for path in paths:
        last = {}
        with open(path) as fh:
            fh.readline()
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                elapsed, key, fp, _score, state, shard = parts[:6]
                # 같은 사용자의 마지막 관측만 남긴다.
                last[key] = (int(elapsed), cohort_of(fp), state, shard)
        for _t, cohort, state, shard in last.values():
            label = state if state != "waiting" or not shard.startswith("g") else "greylist"
            tally.setdefault(cohort, {}).setdefault(label, 0)
            tally[cohort][label] += 1

    print(f"\n마지막 관측 상태 — 실행 {len(paths)}개 합계\n")
    states = sorted({s for c in tally.values() for s in c})
    print("| 코호트 | " + " | ".join(states) + " | 계 |")
    print("|---" * (2 + len(states)) + "|")
    for cohort in sorted(tally):
        row = tally[cohort]
        total = sum(row.values())
        print(f"| {cohort} | " + " | ".join(str(row.get(s, 0)) for s in states)
              + f" | {total} |")
    print()


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    mode, args = sys.argv[1], sys.argv[2:]
    if mode == "summary":
        cmd_summary(args)
    elif mode == "trace":
        step = int(os.environ.get("SG_TRACE_STEP", "15"))
        cmd_trace(args, step)
    elif mode == "final":
        cmd_final(args)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
