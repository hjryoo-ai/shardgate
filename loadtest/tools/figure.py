#!/usr/bin/env python3
"""측정 결과에서 그림(SVG)을 만든다.

왜 스크린샷이 아니라 생성인가:
    스크린샷은 어느 실행의 것인지 알 수 없고, 데이터가 바뀌어도 따라오지 않는다.
    이 그림은 `loadtest/results/` 의 결과 JSON 에서 매번 다시 계산되므로 표와
    어긋날 수 없다. 의존성도 없다 — SVG 문자열을 직접 쓴다(GitHub 에서 그대로 렌더된다).

usage:
    figure.py leak  <out.svg> <door0-dir> <door05-dir> <door10-dir>
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402  (같은 디렉터리의 도구를 재사용한다)

W, H = 760, 462
# 여백. 아래를 넉넉히 두는 이유는 x축 제목과 결론 한 줄이 겹치지 않게 하려는 것이다.
L, R, T, B = 70, 210, 60, 102
PLOT_W, PLOT_H = W - L - R, H - T - B

BG = "#ffffff"      # 배경을 명시한다 — 투명하면 다크 모드에서 글씨가 사라진다
FG = "#24292f"
MUTED = "#57606a"
GRID = "#d8dee4"
RECALL = "#2e86ab"
ADMIT = "#d1495b"


def y(v):
    """0~100% → 픽셀."""
    return T + PLOT_H * (1 - v / 100.0)


def x(i, n):
    return L + PLOT_W * (i + 0.5) / n


def series(dirs, path, scale=100):
    """팔별 (평균, CI 반폭). 값도 계산도 report.py 것을 그대로 쓴다 —
    그림과 표가 다른 코드로 계산되면 언젠가 서로 다른 말을 한다."""
    out = []
    for d in dirs:
        _, runs = report.load_runs([d])
        vals = [v * scale for v in (report.dig(r, path) for r in runs) if v is not None]
        mean, half = report.ci95(vals)
        out.append((mean, half))
    return out


def polyline(pts, color):
    d = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
    return f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="2.5"/>'


def errorbar(px, mean, half, color):
    if not half:
        return f'<circle cx="{px:.1f}" cy="{y(mean):.1f}" r="4.5" fill="{color}"/>'
    lo, hi = y(max(0, mean - half)), y(min(100, mean + half))
    return (f'<line x1="{px:.1f}" y1="{lo:.1f}" x2="{px:.1f}" y2="{hi:.1f}" '
            f'stroke="{color}" stroke-width="1.5" opacity="0.55"/>'
            f'<circle cx="{px:.1f}" cy="{y(mean):.1f}" r="4.5" fill="{color}"/>')


def cmd_leak(out, dirs):
    """§3.7 — 탐지율은 움직이지 않고 봇 입장만 벌어진다."""
    labels = ["0%", "50%", "100%"]
    recall = series(dirs, "detection.recall")
    admit = series(dirs, "admission.bot_success_rate")
    n = len(dirs)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
        f'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="{BG}"/>',
        f'<text x="{L}" y="30" font-size="17" font-weight="600" fill="{FG}">'
        f'A recall-only report cannot see this leak</text>',
        f'<text x="{L}" y="48" font-size="12.5" fill="{MUTED}">'
        f'greylist exit, by the rate at which bots pass re-verification '
        f'(REPORT §3.7 · 3 runs per arm, mean ± 95% CI)</text>',
    ]

    # 격자와 y축
    for v in (0, 25, 50, 75, 100):
        parts.append(f'<line x1="{L}" y1="{y(v):.1f}" x2="{L + PLOT_W}" y2="{y(v):.1f}" '
                     f'stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{L - 10}" y="{y(v) + 4:.1f}" font-size="11.5" fill="{MUTED}" '
                     f'text-anchor="end">{v}%</text>')

    # x축 라벨
    for i, lab in enumerate(labels):
        parts.append(f'<text x="{x(i, n):.1f}" y="{T + PLOT_H + 24:.1f}" font-size="12.5" '
                     f'fill="{FG}" text-anchor="middle">{lab}</text>')
    parts.append(f'<text x="{L + PLOT_W / 2:.1f}" y="{T + PLOT_H + 48:.1f}" font-size="12" '
                 f'fill="{MUTED}" text-anchor="middle">bot re-verification pass rate</text>')

    for vals, color in ((recall, RECALL), (admit, ADMIT)):
        pts = [(x(i, n), y(m)) for i, (m, _) in enumerate(vals)]
        parts.append(polyline(pts, color))
        for i, (m, half) in enumerate(vals):
            parts.append(errorbar(x(i, n), m, half, color))

    # 범례 + 이 그림이 말하는 것
    lx = L + PLOT_W + 26
    parts += [
        f'<text x="{lx}" y="{y(recall[0][0]) - 14:.1f}" font-size="13" font-weight="600" '
        f'fill="{RECALL}">detection rate</text>',
        f'<text x="{lx}" y="{y(recall[0][0]) + 4:.1f}" font-size="12" fill="{MUTED}">'
        f'{" / ".join(f"{m:.1f}" for m, _ in recall)} — flat</text>',
        f'<text x="{lx}" y="{y(admit[-1][0]) - 14:.1f}" font-size="13" font-weight="600" '
        f'fill="{ADMIT}">bots admitted</text>',
        f'<text x="{lx}" y="{y(admit[-1][0]) + 4:.1f}" font-size="12" fill="{MUTED}">'
        f'{" → ".join(f"{m:.1f}" for m, _ in admit)}</text>',
        f'<text x="{L}" y="{H - 16}" font-size="12" fill="{FG}">'
        f'Isolation is recorded at first observation, so leaving through the door is not a '
        f'detection failure.</text>',
    ]
    parts.append("</svg>")
    with open(out, "w") as fh:
        fh.write("\n".join(parts) + "\n")
    print(f"→ {out}")


def main(argv):
    if len(argv) < 3 or argv[0] != "leak":
        sys.exit(__doc__)
    cmd_leak(argv[1], argv[2:])


if __name__ == "__main__":
    main(sys.argv[1:])
