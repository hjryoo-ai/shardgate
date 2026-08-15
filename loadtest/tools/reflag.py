#!/usr/bin/env python3
"""복귀 → 재플래그 시간 분포를 코호트별로 뽑는다.

**무엇을 정하려고 재는가.**
    §3.10 이 남긴 후보 설계는 "복귀 후 재관찰 길이(`ADMIT_MIN_DWELL`)를 최초
    진입과 따로 두자"였다. 그 손잡이가 성립하려면 값 하나로 두 집단을 가를 수
    있어야 한다 — 오탐으로 걸린 사람은 재관찰을 마치고 입장하고, 봇은 그 전에
    다시 플래그돼야 한다. 즉 **필요한 조건은 두 코호트의 재플래그 시간 분포가
    갈라지는 것**이고, 겹치면 어떤 길이도 둘을 가르지 못한다.

    그래서 설정을 추가하기 전에 분포부터 본다. 겹친다는 결과가 나오면 그건
    "아직 튜닝을 못 했다"가 아니라 **시간 축에는 답이 없다**는 측정이다.

**어디서 재는가.** 이미 있는 점수 궤적 트레이스(`trace_scores.sh`)다. 새로
측정하지 않는 이유는 §3.10 의 실행이 이미 그 사건을 담고 있어서다 — 복귀와
재플래그는 둘 다 `user:` 해시의 `state` 전이로 나타난다.

    격리 상태 = greylist | held | blocked   (사다리에 올라 있는 상태)
    복귀      = 격리 → waiting
    재플래그  = 그 뒤 다시 격리

**해상도와 편향.** 트레이스는 5초 간격 표본이고 스코어러도 5초마다 판정한다
(`SCORE_FLUSH_EVERY`). 그래서 측정 단위는 5초이고, **한 표본 간격 안에서
복귀했다가 다시 걸린 왕복은 트레이스에 아예 나타나지 않는다.** 이 누락은
짧은 episode 를 지우므로 분포를 **위로** 민다 — 즉 여기서 나온 중앙값은
실제보다 길고, "짧아서 가를 수 없다"는 결론에는 보수적이다.

**중도절단(censored).** 복귀한 뒤 재플래그되지 않은 채 트레이스가 끝나거나
입장·퇴장한 episode 는 "재플래그 시간 > 관측 길이"만 아는 값이다. 평균에
섞으면 분포가 짧아 보이므로 따로 센다.

usage:
    reflag.py <trace.tsv>...
"""

import statistics
import sys

COHORT_BY_FP = {
    "fp-farm-image-a": "bot:farm-a",
    "fp-farm-image-b": "bot:farm-b",
    "fp-corp-golden-image": "human:clumsy",
}
ISOLATED = {"greylist", "held", "blocked"}
# 복귀로 치는 상태. admitted/evicting 은 대기열을 떠난 것이라 복귀가 아니다.
RETURNED = {"waiting"}


def cohort_of(fp):
    return COHORT_BY_FP.get(fp, "human:normal")


def pct(xs, p):
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    i = (len(s) - 1) * p
    lo, hi = int(i), min(int(i) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


def load(path):
    """{user_key: (cohort, [(t, score, state)])}"""
    per = {}
    with open(path) as fh:
        head = fh.readline()
        if not head.startswith("elapsed_s"):
            sys.exit(f"{path}: 헤더가 없다")
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if len(f) < 6:
                continue
            t, key, fp, score, state = int(f[0]), f[1], f[2], f[3], f[4]
            rec = per.setdefault(key, (cohort_of(fp), []))
            try:
                sc = float(score)
            except ValueError:
                sc = 0.0
            rec[1].append((t, sc, state))
    return per


def episodes(per):
    """복귀 episode 를 뽑는다.

    반환: (cohort, kind, dur, score_at_return) 목록.
      kind = 'reflag'   재플래그까지의 시간 (관측된 값)
             'censored' 재플래그 없이 끝남 (관측 길이 = 하한)
    그리고 코호트별 최초 격리 시각도 같이 돌려준다 — 재플래그 시간을 최초
    격리 시간과 비교해야 "두 번째는 훨씬 빠르다"가 수치로 보인다.
    """
    eps, first = [], {}
    for _key, (cohort, samples) in per.items():
        samples.sort()
        prev = None
        open_t = None      # 복귀 관측 시각
        open_score = None
        for t, sc, state in samples:
            if state in ISOLATED:
                if cohort not in first or _key not in first[cohort]:
                    first.setdefault(cohort, {}).setdefault(_key, t)
                if open_t is not None:
                    eps.append((cohort, "reflag", t - open_t, open_score))
                    open_t = None
            elif state in RETURNED and prev in ISOLATED:
                open_t, open_score = t, sc
            elif state not in RETURNED and open_t is not None:
                # 대기열을 떠났다(입장·퇴장). 그 뒤는 관측 대상이 아니므로 여기서
                # 끊는다 — 트레이스 끝까지 열어 두면 중도절단 길이가 부풀려진다.
                eps.append((cohort, "censored", t - open_t, open_score))
                open_t = None
            prev = state
        if open_t is not None:
            eps.append((cohort, "censored", samples[-1][0] - open_t, open_score))
    return eps, first


def main(paths):
    all_eps, all_first = [], {}
    for p in paths:
        eps, first = episodes(load(p))
        all_eps += eps
        for c, d in first.items():
            all_first.setdefault(c, []).extend(d.values())

    cohorts = sorted({c for c, *_ in all_eps} | set(all_first))
    print(f"트레이스 {len(paths)}개 · 표본 간격 5s · 복귀 episode {len(all_eps)}건\n")
    print(f"{'코호트':<16} {'복귀':>5} {'재플래그':>8} {'중도절단':>8} "
          f"{'중앙값':>7} {'p25':>6} {'p75':>6} {'p90':>6} {'복귀시점 점수':>12} {'최초격리 중앙값':>14}")
    print("-" * 104)
    for c in cohorts:
        mine = [e for e in all_eps if e[0] == c]
        durs = [d for _, k, d, _ in mine if k == "reflag"]
        cens = [d for _, k, d, _ in mine if k == "censored"]
        scores = [s for _, _, _, s in mine if s is not None]
        f = all_first.get(c, [])
        print(f"{c:<16} {len(mine):>5} {len(durs):>8} {len(cens):>8} "
              f"{fmt(pct(durs, .5)):>7} {fmt(pct(durs, .25)):>6} {fmt(pct(durs, .75)):>6} "
              f"{fmt(pct(durs, .9)):>6} {fmt(statistics.fmean(scores) if scores else None):>12} "
              f"{fmt(pct(f, .5)):>14}")

    print("\n중도절단 episode 의 관측 길이(= 재플래그 시간의 하한):")
    for c in cohorts:
        cens = [d for co, k, d, _ in all_eps if co == c and k == "censored"]
        if cens:
            print(f"  {c:<16} n={len(cens):>4}  중앙값 {fmt(pct(cens, .5))}s  최대 {fmt(max(cens))}s")

    print("\n재플래그 시간의 누적분포 (초 이내에 다시 걸린 비율):")
    grid = [5, 10, 15, 20, 30, 45, 60, 90, 120]
    print(f"{'코호트':<16}" + "".join(f"{g:>7}s" for g in grid))
    for c in cohorts:
        durs = [d for co, k, d, _ in all_eps if co == c and k == "reflag"]
        n = len([1 for co, _, _, _ in all_eps if co == c])
        if not n:
            continue
        row = "".join(f"{100 * len([d for d in durs if d <= g]) / n:>7.1f}" for g in grid)
        print(f"{c:<16}{row}")


def fmt(v):
    return "—" if v is None else f"{v:.0f}"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
