# ShardGate measurement report

***English** · [한국어](REPORT.ko.md) — the Korean document is the original; this is a translation.*

The deliverable of the validation plan in DESIGN.md §11. The purpose of this document is
**to leave numbers instead of claims.**

Each table is marked with its measurement status:

- **Measured** — a value obtained by running this repository. The reproduction command is included.
- **Not measured** — a cell still empty because it needs an execution environment. How to fill it is stated.

When copying a number, always carry the execution environment with it (hardware, concurrency,
settings). The same code differs by an order of magnitude between a Docker Desktop VM and bare-metal Linux.

---

## Read this first — the twelve-line summary

The order below is the order in which **each line turned out to be wrong or incomplete**. This
document is long not because there are many results but because **the rewrites were not erased.**

1. **Detection stalling at half was not a weak detector — it raced admission and lost.**
   A bot that gets in first stops sending heartbeats, so it never gets a chance to be scored.
   (§3.3, 53.8%)
2. **The ceiling was measured before any parameter was touched.** With admission off, the same
   detector catches everything — the 53.8% was all lost in the race.
   (§3.5, **100.0 ± 0.0%**, isolation P90 202.3 s)
3. **The lottery window was free.** It removes only the bots' head-start advantage, with no cost
   in seats or human admissions. (§3.4, detection +13pp)
4. **The global gate was rationing, not defense.** It gains another 13pp but buys it by cutting
   the seats released by 38%. (§3.4, 369.8 → 287.5)
5. **The per-user observation gate wins on four axes at once** — because it judges **before** the
   budget decrement, deferring seats instead of destroying them. Its length must be **longer than
   the isolation median** (90 s → 85.4% / 200 s → 96.2%). (§3.6)
6. **A door out of greylist lets bots out too. And detection does not move at all** — isolation
   and staying isolated are different metrics, and a recall-only table does not contain this leak.
   (§3.7, bot admission 3.7 → 12.6 → 20.2%)
7. **Most of that leak was the door bypassing the observation gate.** The gate was measuring entry
   time. Separating the observation clock drove **the door channel to exactly 0 in all six runs.** (§3.8)
8. **The door's cost did not disappear; it moved to total seats.** 0.058 seats per return go to
   nobody, and 86% of the vanished seats were humans'. (§3.8)
9. **§1.4's goal ("bot cost > a normal user's value") misses by 1,400×.** One admitted bot costs
   $0.036 and PoW is 0.2% of it. What this layer does is reduce the **number** of successful bots,
   not their price. (§3.9)
10. **A falsely-flagged person pays in full.** The door really ran (300 re-verifications) and
    **zero people were admitted through it** — the mechanism that stopped the bots stops them
    identically. (§3.10, false positives 14.3%)
11. **The knob that suggested itself as the fix was measured and rejected before being built.**
    Re-flag time is a monotone transform of the score, and that score ranks the falsely-flagged
    **above** the bots. (§3.11)
12. **The attack that relative signals invite was synthesized and sized.** Robust statistics defend
    only up to the breakdown point (50% contamination) and are worse beyond it. (§3.12, 0.780 → 0.320)
13. **That defense was then verified under load, and became the default.** Detection 85.3 → 98.1%
    where false positives exist, unchanged where they do not, with isolation 33 s faster. Since the
    only change was the baseline estimator, **this also retroactively settles the mechanism in
    line 10.** (§3.13, §3.14)

**Three times, the measuring instrument erased the property it was measuring** — the load
generator getting stuck on PoW and blinding the detector (§3.7); patience biasing the metric's
denominator so the admission success rate was 100% by definition (end of §3); and synthetic
jitter being an arithmetic sequence rather than randomness, producing a **clean negative** that
said "no effect" (§3.12). The last one is where this document disproved its own disproof.

---

## 0. Execution environment

| Item | Value |
|---|---|
| Hardware | Apple M4 Pro / Docker Desktop |
| Redis | 8.x single node (not cluster) |
| Kafka | 4.2 KRaft single broker |
| PostgreSQL | 18 |

Stack settings differ per measurement. §3 **measures nothing** if run with compose defaults —
the default admit rate (3,000/min) admits every participant at demo scale, so no queue and no
competition form. Each chapter states its own settings.

| Setting | compose default | §3 mixed load |
|---|---|---|
| Shards / size | 16 / 1,000 | 2 / 250 |
| admit rate | 3,000/min | 60/min |
| admit interval | 5 s | 3 s |
| PoW base difficulty | 16 bits | 8 bits |

```bash
make dev            # bring up the whole stack
make test-int       # integration tests (basis for measurements 1 and 2)
make bench-queue    # queue benchmarks (measurement 1)
make loadtest       # k6 mixed scenario (measurement 3)
```

---

## 1. Queue core throughput — **measured**

`make bench-queue` (Apple M4 Pro, single Redis 8 on Docker Desktop, 64 shards × 64 concurrent workers)

| Item | Value |
|---|---|
| 100k enqueues | 2.87 s (**34,904 enqueue/s**) |
| enqueue | 123 µs/op, 36 allocs |
| position | 123 µs/op, 32 allocs |
| heartbeat | 122 µs/op, 24 allocs |

The reason latency is in the 100 µs range rather than µs is the Docker Desktop VM's network round
trip. Concurrency hides it, so it does not translate directly into throughput — meaning these
numbers measure **the local development environment's round-trip cost, not Redis performance.**
On bare metal the same code produces far lower latency.

---

## 2. Bot detection — **measured**

`go test -tags=integration -run TestBotSimulatorIsDetected ./internal/botscore/`

One shard, 200 normal users + 30 bots (the three types from §11), 60 s window × 30 rounds.
The whole path — signal → score → action → Redis state transition — runs against a real Redis.

| Type | Count | Final action | Ever acted on | Final score |
|---|---|---|---|---|
| Normal users | 200 | — | 0 | 2.3 – 20.6 |
| naive script | 10 | hold 10 | 10 | 74.7 – 82.4 |
| heartbeat mimic | 10 | greylist 7 | 7 | 31.7 – 45.0 |
| distributed IP | 10 | greylist 10 | 10 | 61.4 – 66.7 |

| Metric | Value |
|---|---|
| **Detection (recall)** | **90.0%** (27/30) |
| **False positives (FPR)** | **0.0%** (0/200) |
| Human max / bot min | 20.6 / 31.7 (no overlap) |
| Blocks | 0 |

Two ways to read this:

1. **The score bands separated.** There is an 11-point gap between the humans' maximum and the
   bots' minimum. Without that gap, wherever the threshold sits it merely trades false positives
   against misses.
2. **Zero blocks is not a failure.** It is the action ladder working as designed. Blocking is the
   only irreversible action, so it requires both a consensus of signals (`MinSignalsToBlock`) and
   enough time (exponential smoothing, decay 0.9). No bot satisfied both within 30 windows. The
   naive bots stopped at hold, and hold preserves position.

All three misses are the heartbeat-mimic type. The cause is written up in §12-6 — in short,
forging timing is free so relative signals are erased, and after isolation the comparison
population disappears so the score comes back down.

---

## 3. Mixed load scenario — **measured**

Normal users and bots are put **into the same event at the same time**. Values appear here that do
not appear when they are run separately — the signals in §4-L5 ask "is this unusual compared to
the neighborhood", so a normal population must be present to compare against.

```bash
# stack (the §3 column of §0)
SG_ADMIT_RATE_PER_MIN=60 SG_ADMIT_INTERVAL=3s \
SG_SHARD_COUNT=2 SG_SHARD_SIZE=250 \
SG_POW_BASE_DIFFICULTY=8 make dev

# 400 participants, patience 180 s → 180 seats
k6 run -e SG_TOTAL=400 -e SG_BOT_RATIO=0.2 -e SG_PATIENCE=180 loadtest/k6/mixed.js
```

400 participants arrive simultaneously at t=0 and give up after waiting 180 seconds. 180 seats
open during that time. **Seats must be fewer than participants** for "do bots take people's seats"
to be measurable. Results go both to the stdout summary and to
`loadtest/results/mixed-ratio-<r>.json`.

> **§3.1–3.3 are values with the lottery window switched off.** The command above has no
> `SG_EVENT_OPEN_AT`, and without it `enqueue.lua` receives `lottery_end=0` and puts everyone in
> the FIFO band. §3.2's fairness model is the first line of bot mitigation, and these values were
> measured without knowing it was off (ROADMAP defect 6). Turning it on and re-measuring raises
> detection by 13pp under the same conditions — §3.4. Read the three sections below as
> **a lower bound with the first line of defense disabled.**

> **From §3.5 the way tables are written changes — an interval instead of a single value.**
> In §3.2, running the same configuration twice produced detection rates 9pp apart. Within that
> variance you cannot read a ranking between configurations, and a single-run table hides that
> fact. So a measurement harness was built:
>
> - `loadtest/tools/sweep.sh` — repeats one arm N times. Each repetition tears the stack down with
>   `down -v` (the Redis queue, the scorer's window, and PG's unique constraints are all
>   contaminants), re-stamps `SG_EVENT_OPEN_AT`, and **verifies from the logs that the services
>   came up with that arm's settings** before measuring. The seed (`SG_SEED=r<i>`) is built from
>   the repetition number alone, not the arm name — so that arm A's third run and arm B's third
>   run use the same client behavior, making it a paired comparison.
> - `loadtest/tools/report.py` — folds N results into mean ± 95% CI. With around ten repetitions
>   it uses the t distribution rather than the normal (using 1.96 at n=3 makes the interval 40%
>   too narrow, manufacturing significance that is not there).
> - `loadtest/tools/trace_scores.sh` — samples score trajectories directly from Redis. Making the
>   server return scores in a response would couple the detection path to the queue path
>   (invariant 5, which `internal/api/separation_test.go` forbids), so sampling happens from outside.
>
> The seed fixes client behavior only. Arrival order, Redis/Kafka round trips, and scorer window
> boundaries are server-side and still vary — **a seed does not replace repetition.**

### 3.1 Baseline scenario (20% bots)

320 humans + 80 bots (evenly split across three types), 177 seats.

| Metric | Value | Note |
|---|---|---|
| Detection (recall) | **53.8%** | isolated bots / all bots (42/78) |
| False positives (FPR) | **0.0%** | isolated humans / all humans (0/320) |
| Human admission rate | 46.3% | 148/320 |
| Bot admission rate | 37.2% | 29/78 |
| Entry-to-admission P99 | 179.0 s | `sg_wait_seconds`, mean 91.7 s |
| HTTP P99 | 80 ms | 1.3% failures |
| Throughput | 190 req/s | 35,428 requests |
| Mean PoW solve | 3.4 ms | difficulty 8 bits, P99 64 ms |

Entry-to-admission P99 sitting right at the patience limit (180 s) is not because waits are long
but because **the last seat goes out at the last moment.** In a scenario with fewer seats than
participants this value always converges to the patience limit — it must not be read as a latency
metric.

### 3.2 The key graph — bot ratio vs humans' admission success

The last item of §11 and what this project actually has to demonstrate:
**do humans keep getting in as bots increase?**

| Bot ratio | Human admission | Bot admission | Detection | FPR | Human seat share / population share |
|---|---|---|---|---|---|
| 10% | 44.2% | 46.2% | 43.6% | 0% | 1.00 |
| 20% | 46.3% | 37.2% | 53.8% | 0% | 1.04 |
| 30% | 42.9% | 47.5% | 50.8% | 0% | 0.97 |
| 50% | 39.5% | 48.5% | 39.9% | 0% | 0.90 |
| 70% | 66.7% | 35.1% | 52.0% | 0% | 1.49 |

The last column is this table's answer: **did humans get seats in proportion to their numbers?**
1.0 means the same as if there were no bots; above 1.0 means the queue redirected seats toward humans.

**Conclusion: in this configuration the queue gave humans no meaningful advantage.** The value
stays near 1 (0.90–1.04) and is not even monotone. The 1.49 in the 70%-bots row is arithmetic of
the scenario rather than an achievement of the design — there are only 120 humans for 177 seats,
so there is no competition at all. That row must be excluded from comparison.

Putting two runs of the same configuration side by side shows the table's resolution:

| Bot ratio | Detection | Human admission | Bot admission |
|---|---|---|---|
| 30% (run 1 / run 2) | 50.8% / 60.0% | 42.9% / 48.6% | 47.5% / 32.5% |
| 50% (run 1 / run 2) | 39.9% / 48.7% | 39.5% / 47.0% | 48.5% / 41.6% |

Run-to-run differences are 9pp in detection and 6–15pp in admission. **Most of the differences
between ratios sit inside that variance.** With one run per ratio, no trend can be read from the
table above — only "it stays near 1".

Exactly one thing is robust: **the false-positive rate was precisely 0% in all seven runs.**
Across populations of 60–360 humans, not one normal user was isolated.

> **Why it stays near 1 reads naturally from the next section's result.** What §3.3 shows is that
> in this configuration bots **get admitted before being isolated**. In a regime where isolation
> cannot reach admission, what the queue does is effectively a proportional lottery, and the seat
> share of a proportional lottery is 1 by definition. So "near 1" above is less a signal that
> detection failed than a signal that **detection is not yet participating in the admission
> decision.** In §3.4, once bots are actually admitted later, the same metric rises from
> 1.06–1.12 to 1.18–1.34.

### 3.3 Why detection stalls at half — detection racing admission

Counting the server-side final states reveals that exactly one thing decides a bot's fate.

| Bot ratio | Bots | Isolated | Not isolated | Bots admitted |
|---|---|---|---|---|
| 10% | 39 | 17 | 22 | 18 |
| 20% | 78 | 43 | 35 | 29 |
| 30% | 120 | 62 | 58 | 57 |
| 50% | 198 | 92 | 106 | 96 |
| 70% | 279 | 150 | 129 | 98 |

**Bot admissions land on "bots − isolated" every time.** Isolated bots really are held (the
greylist shard's 0.25 weight works), and bots that escaped isolation essentially all get in.
Humans, under the same conditions, get in at only ~40%. In this system, what stops a bot is not
queue competition but the single question of whether it was isolated.

Then why is only half isolated? The causality is the reverse of the intuition — **they did not
get in by escaping isolation; they escaped isolation by getting in first.** An admitted VU stops
sending heartbeats, so the scorer has nothing more to see. Timing it shows exactly that:

| Bot ratio | Admission median (human) | Admission median (bot) | Isolation median | Isolation P90 |
|---|---|---|---|---|
| 30% | 89.6 s | 90.2 s | **119.5 s** | 169.9 s |
| 50% | 115.8 s | 73.1 s | **150.3 s** | 170.3 s |

The isolation median lags the bot admission median by 30–77 seconds. **The median bot was already
in before being isolated.** In the 50%-bots row, the admitted bots' median (73 s) being earlier
than humans' (116 s) tells the same story — bots that stayed until later were isolated in the
meantime and dropped out of the admission sample.

This delay is a property of the action pipeline, not a bug. Per invariant 3 no single signal can
justify an immediate block, and the score rises through exponential smoothing (decay 0.9).
Filling `MinSamples` (5) takes about 25 seconds, and crossing 40 points takes another 40–50.
During those 70–80 seconds, 40% of the seats in this scenario have already gone out.

Therefore **what caps the detection rate is not detection accuracy but admission speed.**
Lowering the threshold is not the answer — the score has not risen *yet*, it has not risen
*wrongly*, so lowering the threshold adds false positives (currently 0%) rather than recovering
misses. There are two places to look:

1. **The relationship between the lottery window and admission.** §3.2's lottery window (2 minutes
   by default) removes the advantage of arrival order, but admissions keep going out during it.
   Not opening admission until the lottery window ends buys detection time to accumulate score.
   In exchange, total waiting time grows by the length of the window — a trade between fairness
   and throughput.
2. **Initial admit speed.** Start with a low admit rate right after open and raise it once scores
   converge. But this changes the admit-rate distribution algorithm, so it must be measured with
   `make loadtest` first (CLAUDE.md).

Neither was measured as of §3.3. The first is measured in §3.4.

### 3.4 The lottery window and when admission starts — follow-up to §3.3 — **measured**

§3.3 stopped at "a bot that gets in first never gets a chance to be isolated". Here that mechanism
is actually touched. Three arms are measured with **everything else held identical.**

```
# common to all three arms
SG_ADMIT_RATE_PER_MIN=45  SG_ADMIT_INTERVAL=4s   # Round(45×4/60)=3 → nominal = effective
SG_LOTTERY_WINDOW=90s     SG_SHARD_COUNT=2  SG_SHARD_SIZE=250
400 participants (280 humans / 120 bots), patience 240 s, bot ratio fixed at 30%, 3 runs each

nolot  no EVENT_OPEN_AT      → lottery window OFF (everyone FIFO)
off    EVENT_OPEN_AT present → lottery ON, admission open from the start
on     + ADMIT_AFTER_LOTTERY=true → lottery ON, admission opens after 90 s
```

| Arm | Detection | FPR | Human admission | Bot admission | Seats released | Human share/population |
|---|---|---|---|---|---|---|
| nolot | 61.7 / 63.3 / 67.5 | 0.0 | 47.1 / 47.5 / 49.6 | 38.3 / 36.7 / 32.5 | ~178 | 1.06 – 1.12 |
| off | 74.2 / 75.8 / 81.7 | 0.0 | 52.9 / 52.9 / 56.1 | 18.3 / 24.2 / 25.8 | ~178 | 1.18 – 1.25 |
| on | 85.8 / 90.8 / 93.3 | 0.0 | 34.3 / 35.4 / 37.1 | 5.8 / 9.2 / 12.5 | ~111 | 1.24 – 1.34 |

**The detection ranges do not overlap across the three arms** (61.7–67.5 / 74.2–81.7 / 85.8–93.3).
The between-arm difference is larger than the run-to-run variance that made §3.2 unreadable. Bot
admission likewise does not overlap (32.5–38.3 / 18.3–25.8 / 5.8–12.5).

#### The lottery window is free

`nolot → off` differs by `EVENT_OPEN_AT` alone. Detection rises 13pp and bot admission falls 13pp,
while **seats released stay the same (~178) and human admission actually rises from 48% to 54%.**
Nothing was lost.

This is exactly what §3.2 claimed. Under pure FIFO the bots take the front — they queue faster
than people, which is why the lottery window exists in the first place. Once positions are random,
bots spread evenly through the queue, and their share of the seats going out within the ~130
seconds isolation takes drops to their share of the population.

**This value is absent from §3.1–3.3. Those measurements were taken with the lottery window off**
(`EVENT_OPEN_AT` was simply missing from compose — ROADMAP defect 6). The `nolot` arm above is
that condition, and its 61.7–67.5% detection sits right next to the 50.8 / 60.0% §3.2 recorded at
the same ratio. So §3.1–3.3's numbers must be read as **a lower bound with the first line of bot
defense switched off.**

#### The gate is not free — it rations seats

`off → on` also raises detection by 13pp and lowers bot admission by 13pp. Numerically the same
size of improvement, but different in character.

- **Seats released fell 178 → 111, a 38% cut.** Admission is open for that much less time.
- **The human admission rate fell with it, 54% → 35.6%.**
- **Human share/population barely moved, 1.21 → 1.29, and the ranges overlap**
  (off max 1.25, on min 1.24).

The last line matters. The gate **does not divide seats more fairly.** It hands out fewer seats
and lets in proportionally fewer bots. "Fewer bots got in" and "more humans got in" are different
statements, and only the former holds here.

So this is not something measurement decides. Whether to buy more people seated in the same time
or fewer bots admitted is an operational choice, so the default stays off
(`ADMIT_AFTER_LOTTERY=false`) and both columns are kept.

#### Mechanism check

In all three arms `bots admitted = bots − isolated` holds exactly (e.g. on-r1 is 120 = 112 + 7,
nolot-r1 is 120 = 74 + 46). The relationship §3.3 identified survives the change of conditions.

The isolation median barely moved across the three arms, at 115–140 seconds. **What changed is not
detection speed but how many bots slip out during those 130 seconds.** Under FIFO the bots are
bunched at the front so many slip out; under the lottery they are spread evenly so only their
population share slips out; under the gate nobody slips out for 90 seconds.

§3.3's conclusion ("what caps the detection rate is not detection accuracy but admission speed")
still holds, and here it is further confirmed that **that cap can actually be raised.** Thresholds
were never touched — false positives were 0.0% in all nine runs.

#### What this table does not say

- **It was measured at one point, 30% bots.** §3.2's key graph (does it hold as the ratio rises?)
  was not redrawn for the three arms.
- **It is 2 shards of 250.** The limits in §5-7 apply directly.
- **The second candidate (initial admit throttling) is still unmeasured.** It changes the
  admit-rate distribution algorithm, which per CLAUDE.md requires a load test first — and this
  table is that load test, so it has only become touchable, not been touched.
- **The waiting time the gate adds was not shown honestly to users.** `/queue/status`'s estimated
  wait was computed only from the configured drain rate, so it suggested the line was moving even
  while the gate was closed. It is display-only, so it does not affect the numbers in this table.
  Fixed in §3.6 with `EstimateWaitAt`, which reflects both gates according to their character.

### 3.5 The detector's ceiling — measuring with the race removed — **measured**

§3.3 and §3.4 went as far as "detection races admission and loses". One question remains:
**how much does it catch with the race removed?** Without that value there is no way to tell
whether the 53.8%–93.3% of §3.4 is the detector's limit or what was lost in the race, and no basis
for choosing where to put a gate.

The method is to disable admission — but **by distributing a budget of 0, not by removing the service.**

```
perCycle = Round(rate_per_min × interval_min) = Round(1 × 1/60) = 0

1,000 participants (700 humans / 300 bots), 2 shards × 600, patience 360 s
lottery window ON (90 s), PoW 8 bits, fixed seeds, 8 repetitions
```

If the service is not started, `/admission/redeem` itself returns 502 and what the client
experiences is "the server is broken" rather than "not your turn yet". That was actually measured
once, yielding a 100% redeem failure rate, and every metric from that run was discarded. With the
budget at 0, the distribution loop and the redeem path both run as usual and only `rank < budget`
in `admit.lua` is true for nobody — exactly one thing changes.

| Metric | Mean ± 95% CI (n=8) | Per-run range |
|---|---|---|
| **Detection (recall)** | **100.0 ± 0.0 %** | 100.0 in all 8 |
| **False positives (FPR)** | **0.0 ± 0.0 %** | 0.0 in all 8 |
| Isolation median | 139.8 ± 7.7 s | 130.5 – 158.6 |
| Isolation P90 | 202.3 ± 11.7 s | 187.4 – 230.5 |
| Seats released | 0 | 0 in all 8 (the arm's premise) |
| Throughput | 597 req/s | 219,086 requests |
| Lottery window entry | 100.0 % | no regression of defect 6 |

**The 53.8% was all lost in the race.** Given time, the detector itself misses not one of 2,400
bots. False positives are 0 out of 8 × 700 = 5,600 humans.

#### When do humans and bots separate?

Per-cohort mean score and isolation rate, sampled every 5 s by `trace_scores.sh`
(farm-a = naive + heartbeat mimic, farm-b = distributed IP, mean of 8 runs):

| t(s) | farm-a score | farm-a isolated% | farm-b score | farm-b isolated% | human score | human isolated% |
|---|---|---|---|---|---|---|
| 5 | 6.5 | 0.0 | 4.9 | 0.0 | 1.0 | 0.0 |
| 35 | 25.9 | 0.0 | 20.5 | 0.0 | 3.5 | 0.0 |
| 65 | 36.4 | 43.7 | 31.5 | 0.0 | 4.0 | 0.0 |
| 95 | 38.5 | 52.5 | 36.7 | 7.9 | 3.9 | 0.0 |
| 125 | 39.5 | 56.6 | 39.0 | 49.5 | 3.8 | 0.0 |
| 155 | 39.9 | 69.3 | 39.8 | 82.1 | 3.6 | 0.0 |
| 185 | 40.1 | 84.9 | 39.9 | 95.2 | 3.4 | 0.0 |
| 215 | 40.2 | 95.1 | 40.0 | 98.7 | 3.2 | 0.0 |
| 245 | 40.2 | 98.8 | 40.0 | 99.8 | 3.1 | 0.0 |
| 305 | 40.2 | 100.0 | 40.0 | 99.9 | 3.1 | 0.0 |
| 365 | 40.2 | 100.0 | 40.0 | 100.0 | 3.0 | 0.0 |

Humans stay flat below 4 while bots rise monotonically. **The two curves never overlap from t=5 s
onward** — that is the condition under which a threshold stops being a trade between false
positives and misses, and it means the score-band separation seen in the Phase 4 simulator
(human max 20.6 / bot min 31.7) survives a 2.5× larger participant count.

#### 100% is the ceiling of "isolation", not of "blocking" — greylist is a terminus

Counting the final observed states, all 2,400 bots are **in greylist**. Not one hold (70–89), not
one block (90–100). And the score in the table above does not move by even a decimal from 40.2.

**This was first read as the paradox of DESIGN §12-6 (in the greylist shard everyone nearby is
suspect, so relative signals converge to 0). Following the code showed that explanation was wrong.**
In reality three separate defects overlap, and all three are pinned down in code.

**(1) The score is not written to Redis.** For a user already in greylist, `move_shard.lua`
returns `noop` at the `state ~= 'waiting'` guard — and **that guard sits before `HSET scoreKey`.**
The scorer keeps producing verdicts every window, but those verdicts' scores are never stored. The
40.2 in the trajectory table is **the value stamped at the moment of the move, still sitting
there** — not the scorer having stopped.

Live instrumentation shows this directly (Prometheus samples during the §3.6 sweep, a 40 s window):

| | actual moves (greylist) | noop | verdicts in (40,50] | in (50,70] | > 70 |
|---|---|---|---|---|---|
| t=0s | 36 | 9 | 45 | 0 | 0 |
| t=20s | 90 | 331 | 421 | 0 | 0 |
| t=40s | 98 | 721 | 758 | 61 | 0 |

`noop` swells to 7× the actual moves (all of them verdicts discarded without a score write) while
the scorer's internal scores keep spreading into the (40,70] range. **Observation is alive and
recording is dead.**

**(2) The greylist shard is not an observation population.** Telemetry carries `claims.Shard`, and
the queue token is not reissued after a shard move. So the scorer still scores greylist users
**bundled with the origin shard population.** What DESIGN §4 cited as sharding's second benefit —
"herd the suspects into a separate population for closer observation" — is not implemented; a
greylist shard today is just one Redis ZSET, not a statistical unit. For §12-6's paradox to hold,
the scorer would have to treat the greylist shard as a separate population, and it does not.

**(3) No budget is distributed to greylist shards.** `registerShard` is called only from
`Enqueue`, so `shards:{event}` contains only origin shards. The controller finds shards through
that index, so it never sees greylist shards at all, and the `GreylistWeight` branch in `weights()`
is **unreachable code**. In live instrumentation the `shardgate_admission_budget` series has only
the two origin shards. (An earlier draft said "budget leaks away", which was wrong — it does not
leak; it is never distributed.)

Together, greylist is **a terminus with no exit.** Excluded from the admission path (below), with
no budget, no score updates, and no re-verification path. The 40–69 band had become a *heavier*
action than 70–89 — hold preserves position and allows return, while greylist had even the path
for the score to come down blocked.

#### The 6.2% HTTP failure rate is all redeem, and it is the same story from another angle

Per-endpoint failure rates are 0.0% for enter, verify, status, heartbeat, and order; only redeem
is at 19.1%. When a user moved to greylist calls redeem, a 409 comes back — `admit.lua` considers
anything that is not `state == 'waiting'` ineligible.

**Those 409s burned no budget.** The `state ~= 'waiting'` return comes before even the `ZRANK` and
far before the `DECR`, so there is not a single write on that path. And the budget key that redeem
reads is keyed on `claims.Shard`, i.e. **the origin shard** — the greylist shard's budget does not
even exist per (3), so there was no attempt to consume it. Nor does a 409 mean "reached the front":
the load scenario hits redeem on every poll regardless of position.

The reason this mismatch was not fixed during this measurement is that fixing it would make §3.4
and §3.6 below comparisons of different systems. The repair direction is written up in §5-10.

> **Every number in §3.1–§3.6 comes from the "greylist = terminus" regime.**
> (Fixed in Phase 8; the post-fix values are measured separately for one obs200 arm in §3.7.
> §3.1–§3.6 were not re-measured because comparisons between arms must be comparisons of the same
> system.)
> They are biased in two directions:
> - **Optimistic about bot isolation.** Isolated bots have no exit. A bot with a re-verification
>   bypass such as a CAPTCHA solving service could get back out here, but that path is not
>   implemented so the measurement cannot see it. The detection rates in this report are therefore
>   **values from a world where the re-verification leak channel is 0.**
> - **Pessimistic about false-positive UX.** A real person who crosses 40 is permanently excluded
>   with no re-verification, and what they see is one 409 at the end of a long wait. The 0% false
>   positives holding across eight and nine runs is a value about the simulator's humans, not proof
>   that false positives are impossible (§5-4). The ladder exists precisely because of that
>   possibility of error, and the current implementation has no recovery rung on it.

#### This ceiling was measured on a population that never shrinks

With admission off, the shard population stays near 600 to the end. Most of §4-L5's signals are
relative, so a large, stable comparison population favors them. In real operation people keep
leaving and the sample shrinks. **So 100% is "what the detector can produce under these
conditions", not a value achievable with a gate.**

#### This is where the gate value comes from

Isolation P90 = **202.3 ± 11.7 s**. Set the minimum observation time shorter and you release bots
whose score has not risen yet; longer and you only make people wait more. What matters is that this
is a value from measurement rather than taste — §3.6 measures with it.

### 3.6 The per-user observation gate — **measured**

§3.4's gate (`ADMIT_AFTER_LOTTERY`) blocks everyone with a single event timestamp, and in exchange
for 13pp of detection it **cut the seats released by 38%.** What needs observing is the individual
rather than the event, so a gate measured from each person's own entry is built and measured
alongside.

```
common to all four arms — only the gate differs
SG_ADMIT_RATE_PER_MIN=75  SG_ADMIT_INTERVAL=4s   # Round(75×4/60)=5 → nominal = effective
SG_LOTTERY_WINDOW=90s  SG_SHARD_COUNT=2  SG_SHARD_SIZE=600, lottery ON
1,000 participants (700 humans / 300 bots), patience 300 s, bot ratio fixed at 30%, 4 runs each

off     no gate
lot     ADMIT_AFTER_LOTTERY=true  → event-time gate (open + 90 s)
obs90   ADMIT_MIN_DWELL=90s       → per-user gate of the same length as lot
obs200  ADMIT_MIN_DWELL=200s      → chosen from §3.5's isolation P90 (202.3 s)
```

| Arm | Detection % | FPR % | Human adm. % | Bot adm. % | Seats | Share/pop | Human adm. median s |
|---|---|---|---|---|---|---|---|
| off | 81.2 ± 1.4 | 0.0 | 44.8 ± 0.7 | 18.8 ± 1.4 | 369.8 ± 2.0 | 1.211 ± 0.016 | 173.3 ± 2.8 |
| lot | 91.7 ± 2.6 | 0.0 | 37.5 ± 5.3 | 8.3 ± 2.6 | **287.5 ± 43.0** | 1.305 ± 0.028 | 193.0 ± 15.7 |
| obs90 | 85.4 ± 3.4 | 0.0 | 46.6 ± 1.3 | 14.5 ± 3.1 | 370.0 ± 2.3 | 1.261 ± 0.036 | 167.7 ± 5.2 |
| **obs200** | **96.2 ± 1.4** | **0.0** | **51.4 ± 0.6** | **3.9 ± 1.3** | **371.2 ± 3.0** | **1.383 ± 0.014** | 203.7 ± 0.2 |

#### obs200 beats lot on every axis at once

Not one of the four metrics' 95% intervals overlaps:

| | lot | obs200 |
|---|---|---|
| Detection | 89.1 – 94.3 | **94.8 – 97.6** |
| Bot admission | 5.7 – 10.9 | **2.6 – 5.2** |
| Human admission | 32.2 – 42.8 | **50.8 – 52.0** |
| Seats released | 244.5 – 330.5 | **368.2 – 374.2** |

**Catches more, admits fewer bots, admits more humans, and does not shrink the seats.** §3.4
described the gate as something "bought by rationing seats", but that was a property of the
event-time gate, not of gates in general.

#### The mechanism is seats, as expected — burning the budget vs deferring it

`ADMIT_AFTER_LOTTERY` **skips a blocked cycle entirely.** That cycle's share goes nowhere and
disappears (because `refill_budget.lua` is never called). Hence seats released fall 369.8 → 287.5,
a 22% cut, **and the variance grows to ±43** — how many cycles are lost depends on where the gate
falls within the run.

The observation gate judges **before** the `DECR` in `admit.lua`, so the budget survives. The
leftover accumulates into the next cycle and goes out all at once when the gate opens. Seats are
**preserved to the decimal** (369.8 → 370.0 / 371.2) with a small variance of ±2–3.

#### Gate length must be chosen by comparison with the isolation median

obs90 and lot have the same nominal delay but different outcomes. obs90 keeps all the seats and
admits more humans (46.6 vs 37.5) but detects less (85.4 vs 91.7).

The reason is how long isolation takes. Under these conditions the isolation median is
**132–142 seconds**.

- **A 90 s gate is shorter than that.** About half the bots are still un-isolated when the gate
  opens, and out they go. Observation time was bought, but the door opens before the verdict lands.
- **A 200 s gate is longer than that.** Most bots are already isolated before their own gate opens.
  The observation actually reaches the admission decision.

lot catching more than obs90 is not because its gate is longer (it is in fact shorter — k6 starts
about 35 s after the event opens, so the effective delay from a participant's point of view is
around 55 s). It is **because handing out 22% fewer seats made the door bots could slip through
narrower**, which is the rationing §3.4 described. The observation gate does the same job without
that rationing, provided its length exceeds the isolation time.

#### The value transferred successfully

`MIN_DWELL=200s` was chosen from the isolation P90 (202.3 ± 11.7 s) measured in §3.5 **with
admission off.** It was unclear whether a value from a non-shrinking population would carry over,
but in this measurement — with admission running — the isolation P90 came out at
**198.0 ± 5.9 s**, with the median at 135.4 versus 139.8. **A value chosen from the ceiling
measurement held in the real regime** — that is the practical payoff of "measure the ceiling
before touching parameters".

#### The value is not free — the first admission is 30 seconds later

The human admission median rose 173.3 → 203.7 s, **an increase of 30.4 s.** That is the entirety
and the only cost of this gate (seats, human admission rate, and false positives all got no worse).
The value landing almost exactly at the dwell (203.7 ≈ 200 + 4) means that at the front of the
queue the gate, not the position, was the only constraint.

For ticketing, 30 seconds seems an acceptable trade. But **the first-admission delay is what the
user sees on screen**, so it presupposes that `EstimateWaitAt` displays those 30 seconds honestly
(a debt left by §3.4, paid here).

#### Final states — not one human was isolated

Totals over 4 runs. `bots admitted = bots − isolated` holds exactly in all four arms.

| Arm | farm-a adm./isol. | farm-b adm./isol. | human adm./isol. |
|---|---|---|---|
| off | 134 / 666 | 91 / 309 | 1254 / **0** |
| lot | 64 / 736 | 36 / 363 | 1050 / **0** |
| obs90 | 107 / 693 | 67 / 328 | 1306 / **0** |
| obs200 | 45 / 755 | **2 / 398** | 1438 / **0** |

Distributed-IP bots (farm-b): only 2 of 400 got through under obs200. False positives were
**0 out of all 2,800 humans across 16 runs**, and thresholds have not been touched since §3.1.

#### What this table does not say

- **The gate occupies two thirds of the patience limit.** With dwell 200 s against patience 300 s,
  only a 100 s window remains after the gate opens. Seats were preserved because the budget
  accumulates — which means **~125 seats per shard go out at once the moment the gate opens.**
  In real operation, downstream (inventory, payment) absorbs that burst. Backpressure and a circuit
  breaker exist but were never triggered in this measurement.
- **One point, 30% bots.** Whether the spacing between the four arms holds at other ratios is unknown.
- **These are values from the "greylist = terminus" regime** (the note in §3.5). The detection rates
  come from a world where the re-verification leak channel is 0, so they are optimistic about bot isolation.
- **`MIN_BEATS=5` was not binding.** With a 5 s polling interval, by the time a 200 s dwell is
  filled the liveness signals number about 40. What this value actually bites is a client sending
  heartbeats rarely, and no such participant exists in this scenario.

### 3.7 How much leaks when greylist is given a door — **measured**

Phase 8 gave greylist an exit (§5-10). The exit is the way back out for a falsely-flagged person,
but it is simultaneously **a way out for bots.** This section measures that channel's size.

> **The numbers in this section come from the regime where the door bypassed the observation gate.**
> The gate was measuring entry time, so returning participants came back already satisfying it.
> The "bot admission 20.2%" below is a value that entered through that bypass; the value after it
> was closed is in §3.8. This defect **did not move the detection rate at all**, so it passed this
> section's table unchanged — the very property this section identified ("isolation and staying
> isolated are different metrics") applied to itself.
>
> **The table with all three arms re-measured on the fixed code is at the end of §3.8** (door0
> needed no re-measurement, having 0 returns). This section is kept as **a record of the bypassed
> regime** — it is not deleted because how that regime managed to look plausible is one of this
> report's points.

The arm is §3.6's obs200 with exactly one thing changed — the rate at which k6 bots pass
re-verification (`SG_CAPTCHA_PROXY`) at 0 / 50 / 100%. Humans always attempt regardless of this
value. The implemented re-challenge is PoW only, and PoW is a compute cost rather than an
impassable wall, so **100% is the current implementation's actual exposure** and 0% / 50% are
assumed values for when the CAPTCHA path of §4-L2 exists.

#### First: the initial measurement measured the load generator, not the system

Measured with difficulty escalation on, this table came out:

| Arm | Detection % | Bot adm. % | Human share/pop | HTTP req/s | HTTP P99 ms | Mean PoW ms |
|---|---|---|---|---|---|---|
| captcha 0% | 95.2 ± 1.3 | 4.8 ± 1.3 | 1.373 | 541 | 62 | 1.6 |
| captcha 50% | 89.0 ± 9.4 | 13.8 ± 1.9 | 1.210 | 321 | 1,501 | 1,105 |
| captcha 100% | **11.9 ± 4.2** | **47.0 ± 13.1** | **1.000** | 280 | 2,403 | 1,441 |

Detection 96 → 12 looks like the door leaking. **It is not.** The right three columns of the same
table say why: HTTP throughput fell 48%, P99 rose 39×, and PoW solving went from 1.6 ms to
1,441 ms. k6's JS is single-threaded per VU, so a longer solve delays that VU's heartbeat. And that
delay **does not happen only to bots** — it happens to every participant sharing the CPU, so the
regularity and cross-correlation signals disappear for bots and humans alike and **the detector
goes blind.**

The decisive evidence is `human share/population = 1.000`. It means humans got exactly their
population's share of seats, which is **the value when there is no detection at all**, not the
value when a door leaks. Bots that came out through the door total 6 across the three runs
(`re-verification returns`), and 6 bots cannot move detection by 84pp.

This trap was already written down at the end of REPORT §3 in "cautions when running load tests"
(high PoW difficulty binds k6 to the CPU and makes bots and humans equally irregular). The reason
we fell into a known trap again is that difficulty was never raised **directly** — the
re-challenge's per-attempt escalation raised it instead, and since a bot farm shares fingerprints,
suspicion rose together and quickly hit the ceiling (26 bits). A parameter nobody touched moved
through another feature, so it went unnoticed.

Two things were added as a result. `mixed.js` puts a threshold (100 ms) on the mean of
`sg_pow_solve_ms` and records `load_generator_cpu_bound` in the result JSON, and `report.py` prints
a warning before the table if such a run is mixed in. It is a check on **the table's validity**
rather than on a value.

#### Re-measuring — turn off the difficulty axis, keep only the door axis

The re-measurement runs with `SG_GREYLIST_DIFFICULTY_BUMP=0`. With two guardrails (difficulty
escalation and the door itself) mixed into one axis there is no telling which one blocked what,
and above all the difficulty axis breaks the load generator and destroys the measurement. Turning
difficulty off yields **an upper bound on how much leaks through the door**, and difficulty
escalation becomes a device that reduces it further on top.

```
common to all three arms — same as §3.6's obs200
SG_ADMIT_RATE_PER_MIN=75  SG_ADMIT_INTERVAL=4s   SG_ADMIT_MIN_DWELL=200s
SG_LOTTERY_WINDOW=90s  SG_SHARD_COUNT=2  SG_SHARD_SIZE=600  SG_POW_BASE_DIFFICULTY=8
SG_GREYLIST_DIFFICULTY_BUMP=0                    # off the difficulty axis, keep only the door
1,000 participants (700 humans / 300 bots), patience 300 s, bot ratio fixed at 30%, 3 runs each

door0    SG_CAPTCHA_PROXY=0     bots do not attempt re-verification
door05   SG_CAPTCHA_PROXY=0.5   half the bots pass
door10   SG_CAPTCHA_PROXY=1.0   all pass — **the actual exposure of the PoW-only implementation**
```

| Arm | Detection % | FPR % | Human adm. % | Bot adm. % | Seats | Share/pop | Returns | Exhausted |
|---|---|---|---|---|---|---|---|---|
| door0 | 96.3 ± 5.8 | 0.0 | **51.3 ± 2.1** | **3.7 ± 5.8** | 370.0 ± 2.5 | **1.386** | 0 | 0 |
| door05 | 96.6 ± 3.7 | 0.0 | 47.2 ± 0.5 | 12.6 ± 1.0 | 368.3 ± 1.4 | 1.282 | 268 | 98 |
| door10 | 97.0 ± 2.2 | 0.0 | 44.2 ± 1.4 | **20.2 ± 2.9** | 370.0 ± 2.5 | 1.194 | 534 | 187 |

#### Detection does not move. What moves is admission

All three detection intervals overlap (96.3 / 96.6 / 97.0). Naturally — isolation is recorded
**at the moment of first observation**, and leaving through the door afterward is not a detection
failure. The detector caught the same amount in all three arms; what it had caught leaked away.

**So a recall-only report does not contain this leak.** Bot admission tracks the pass rate almost
linearly at 3.7 → 12.6 → 20.2% (with no overlapping intervals), and that is the channel's size.
Reading §3.1–§3.6 with detection at the center gets corrected once here: **isolation and staying
isolated are different metrics, and without counting the latter the price of opening an exit is
invisible.**

#### The seats bots took are exactly the seats humans lost

Total seats released is identical across the three arms (370.0 / 368.3 / 370.0), because the
observation gate judges before the `DECR` so the budget is not lost (§3.6), and re-verification
does not touch that property. With the seat count fixed, bot admission rising 16.5pp makes human
admission fall 7.1pp (51.3 → 44.2). In share/population terms, 1.386 → 1.194.

With 300 bots and 700 humans, 16.5pp × 300 ≈ 50 seats and 7.1pp × 700 ≈ 50 seats — **the books
balance.** The door does not create seats; it only moves them.

> **Share/population is comparable only within a table.** The 1.386 → 1.194 here must not be read
> alongside §3.4's 1.18–1.25. The two are not the same metric:
>
> - **Different scale.** §3.4 has 400 participants (320 humans / 80 bots) with ~178 seats; here it
>   is 1,000 (700/300) with ~370 seats. With a different denominator the same number means a
>   different saturation — the baseline of "proportional to population" is compressed differently
>   in a regime with seats for half the participants versus one third.
> - **Different semantics.** §3.4 has no observation gate; here obs200 is in place. A gate changes
>   the order of admission, so the very rule this metric measures — who gets in first — is different.
>
> The only thing readable across a table is **the difference between arms within the same table.**
> The information in this metric is the difference, not the absolute value, which is why both §3.4
> and §3.7 are designed as paired arm measurements.

#### The exhaustion path really runs under load

In door10, bots attempted re-verification 720.7 ± 17.4 times, returned 533.7 ± 16.2 times, and
187.0 ± 17.4 **exhausted the cap (2) and were promoted to hold.** Without the cap those 187 would
also have been returns and bot admission would be higher. So 20.2% is not "the value with the door
wide open" but **a value guardrail 3 has already filtered once.**

The HTTP failure rate rising in proportion to the pass rate (0.00 → 1.58 → 3.19%) is the same path.
Per endpoint, `reissue` is 0.00% and `reverify` is 0.00%; it is all `redeem` (0.00 → 4.84 → 9.80%) —
403s that bots promoted to hold keep knocking for. **The re-verification path itself failed zero times.**

#### The false-positive side improved for free

All nine runs across three arms have 0.0% false positives, and humans attempted re-verification
0 times — no human was isolated, so there was nothing to use it for. **The value of building the
door does not show up here.** The simulator's humans do not get isolated (§5-4), and the door
exists for the day that 0% breaks.

One thing did improve incidentally. §3.6's obs200 had a 6.01 ± 0.18% HTTP failure rate; door0 has
**0.00%**. That 6% was all greylist users' redeem 409s (§3.5), and they now receive
`200 + challenge_required`. The same system state stopped being an error — a place where the
**meaning** changed rather than the number.

#### What this table does not say

- **It was measured with difficulty escalation off.** With it on, a bot farm shares fingerprints so
  suspicion rises together, cost jumps by 2^bump per attempt, and the ceiling is reached quickly.
  door10's 20.2% would then fall further. How much further **cannot be measured with this load
  generator** — k6 pays that cost itself and the measurement collapses (see above).
- **k6's `SOLVE_LIMIT` (2^22) is the simulator's give-up point, not a bot's limit.** In this
  measurement difficulty was 8 bits so there were zero give-ups, but in a regime with difficulty on,
  a farm spending more compute goes further than the simulator. So the numbers here are a **lower bound.**
- **The human-side cost was not measured.** With 0 false positives, the path where a human passes
  through the door never executed. In real operation a falsely-flagged person must pay a
  raised-difficulty PoW, and on a low-end device that is not light.
- **One point, 30% bots.** The same constraint as §3.4–§3.6.
- **The door was bypassing the observation gate.** The 20.2% above includes that bypass; after
  closing it, the figure is 2.2% (§3.8). That defect also did not move detection, so it passed this
  section's table unchanged — the property this section identified applied to this section itself.

### 3.8 The door was bypassing the observation gate — **measured**

The correction §3.7 left behind was "isolation and staying isolated are different metrics".
Moving that sentence from prose into accounting — counting admitted bots per channel — immediately
revealed **a channel nobody was counting.**

#### The defect: the gate was protecting only the first admission

The minimum observation gate (§3.4) checked `now - joined_at ≥ MIN_DWELL`. `joined_at` is the
**entry time**. But a participant returning via re-challenge already satisfies it:

```
t=0     entry
t=130   isolated (greylist)
t=140   passes re-verification → returns to the origin shard
t=140   redeem → now - joined_at = 140 s … and soon crosses 200 s
        admissible with no observation from the moment of return
```

That is, **the race of §12-7 restarts once per return, every time the door opens.** While the
score climbs from the clamp (35) back to the threshold, the returned bot hammers redeem from the
front of the queue with no observation at all. If the gate only protects the first admission,
every re-entry after it is unprotected.

The reason this defect passed §3.7's table unchanged is the same thing §3.7 itself said —
**it does not move the detection rate at all.** Isolation is recorded at first observation, so
returning and being admitted is not counted as a detection failure. The property that section
identified applied to that section itself.

#### The fix: separate entry time from the observation clock

```
admit if  (rank < budget) AND (now - observe_from ≥ MIN_DWELL)
                          AND (hb_count - hb_base ≥ MIN_BEATS)
```

`rechallenge.lua` stamps `observe_from` / `hb_base` to now on return and `admit.lua` reads those.
`joined_at` is untouched — entry time is the basis for auditing and lottery-window decisions, and
after a return the two become different facts. The screen must read the same clock, so
`position.lua` / `EstimateWaitAt` were fixed too (otherwise the estimated wait comes out short
**only for returned users** — and the side where the screen lies is always the user who has
already been flagged once).

The door that opens when the score comes down on its own (`restore_shard.lua`) does **not** rewind.
The re-challenge's clamp is a **concession** rather than a verdict, so it needs observation to
re-establish the true value; a score decline is a **conclusion** the detector reached across many
windows. Whether this asymmetry is a hole is watched by the residual of the identity below.

#### Re-measurement — the same arm as §3.7's door10, only the code differs

| Metric | §3.7 door10 (bypass present) | §3.8 (bypass closed) |
|---|---|---|
| Detection % | 97.0 ± 2.2 | 97.7 ± 5.8 |
| FPR % | 0.0 | 0.0 |
| **Bot admission %** | **20.2 ± 2.9** | **2.2 ± 5.8** |
| ├ via the door | (not measured) | **0.0 ± 0.0** |
| └ unflagged (race) | (not measured) | 6.7 ± 17.4 bots |
| Human admission % | 44.2 ± 1.4 | 47.3 ± 2.2 |
| Humans admitted | 309.3 | **331.0** |
| Bots admitted | 60.7 | **6.7** |
| Seats released | 370.0 ± 2.5 | 337.7 ± 23.1 |
| Share/population | 1.194 | 1.401 |
| Human admission median s | 203.5 | 204.0 ± 0.5 |
| Isolation median s | 128.7 | 132.1 ± 7.5 |
| Re-verification attempts / exhausted | 720.7 / 187.0 | 778.0 / 216.3 |

**Which tables are affected by this defect.** The rewind only happens on return, so a measurement
with zero returns is unaffected by definition:

- §3.1–§3.6 — the regime with no door at all (pre-Phase 8). Unrelated.
- §3.7 door0 — 0 returns. **Provably unrelated.**
- §3.7 door05 (268 returns) and door10 (534 returns) — affected. door10 was re-measured here, and
  **door05 was re-measured afterward** (see "the three arms become one system's table" below).
  §3.7's table is now read as **three arms of the bypassed regime** — valid as such, but not the
  current system's values.

#### The door channel became exactly 0

All three runs have `bot admission: door = 0`. The remaining bot admissions (0 / 6 / 14) are
**all unflagged** — the §12-7 race, getting in without ever receiving an action. Without counting
per channel, only "bot admission 2.2%" would have remained, and there would be no way to tell
whether that came from the door being closed or detection improving. **The decomposition points
at the answer.**

The admission-channel identity's residual was 0 in all six cases (3 runs × two cohorts). Whether
the decision not to rewind on the score-decay door (`restore_shard`) is a hole is watched by that
residual, and not one participant entered through that path.

#### The mechanism is not "making them wait" but "re-flagging arriving first"

Exhaustions rose 187.0 → 216.3. Bots did not fail to get in because they could not fill the time;
**crossing the threshold again after a return is faster than finishing observation.** So attempt 2
also ends in isolation, the cap is exhausted, and they are promoted to hold. 216 of 300 bots take
that path.

This yields a **second criterion** for gate length. §3.6's was "longer than the isolation median".
Now one more:

> `MIN_DWELL` must be longer than **the time a returned participant takes to reach the threshold
> again.**

That time is far shorter than the initial isolation (median 132 s), because the score starts from
the clamp value 35 rather than 0. 200 s is comfortably longer. Inverted, this means
**re-observation far shorter than 200 s would still stop this leak** — and that difference is the
cost paid by falsely-flagged people, so it is not free (see below).

#### Humans paid no cost — in this arm

The human admission median stays at 203.5 → 204.0 s. False positives are 0.0% in all three runs and
humans attempted re-verification 0 times. Humans do not use the door, so they do not pay the price
attached to it.

**This is this table's biggest limitation.** In a population with zero false positives, the cost of
"charging observation again on return" is 0 by definition. The cost falls only on those who get
flagged, and this scenario has none. That cost is manufactured and measured separately in §3.10.

#### Total seats fell, but the seats humans received rose

370.0 → 337.7, 32 fewer seats released. Yet **the seats humans received rose from 309.3 to 331.0.**
What shrank were the seats bots had been taking (60.7 → 6.7); 22 of those went to humans and 32 did
not go out within the window.

The reason they did not go out is that a bot blocked from admission **stays in the queue holding a
position.** Previously a bot leaving on admission `ZREM`'d itself and pulled the people behind it
forward; now it stays in line until it exhausts the cap and moves to hold. Positions behind it are
pulled forward that much later, and fewer people cross the budget threshold within the patience
limit (300 s).

In §3.6, "the seat count shrinking" was a bad sign, because what shrank there was **humans' seats.**
What shrank here is bots' seats, and humans' seats rose. **Total seats and humans' loss are not the
same thing** — looking at the total alone makes this change look like a loss.

#### Aside: a position moving backward after return is not a defect

In 111.3 of 561.7 returns (20%), the position queried right after return was **behind** the one
just before isolation (per run: 137 / 118 / 79). At first this read as "passed re-verification and
still lost their place". It is not:

> An isolated person is out of the origin queue → meanwhile **the position seen by those behind
> them is inflated forward** → when the person ahead returns, that inflation disappears.

What is preserved is the ZSET score (the place in line), and that invariant holds. What moves
backward is the **displayed position**, and returning to its original place is the correct behavior.
`TestRestoredRankRisesWhenSomeoneAheadReturns` reproduces the mechanism — and the metric was renamed
from `rank_lost` to `rank_backward`. Leaving a name that sounds like a defect sends the next person
hunting a bug that does not exist, and breaking a real invariant while trying to fix it.

Users do need an explanation, though. The waiting screen now says "we watch a little longer right
after verification — your place is unchanged".

#### door05 was re-measured too — the three arms become one system's table

§3.7's three arms came from different code (only door10 was re-measured on the fixed code). door05
was measured three more times with the same arm definition so the three arms form one system's
table. door0 was **not re-measured** — with 0 returns there is by definition nothing for the rewind
to touch.

| Arm | Returns | Detection % | FPR % | Human adm. % | Bot adm. % | ├ door | └ unflagged | Seats |
|---|---|---|---|---|---|---|---|---|
| door0 † | 0 | 96.3 ± 5.8 | 0.0 | 51.3 ± 2.1 | 3.7 ± 5.8 | 0 (structural) | — | 370.0 ± 2.5 |
| door05 | 274.3 | 96.9 ± 3.1 | 0.0 | 49.6 ± 1.3 | 3.1 ± 3.1 | **0.0 ± 0.0** | 9.3 bots | 356.3 ± 10.3 |
| door10 | 561.7 | 97.7 ± 5.8 | 0.0 | 47.3 ± 2.2 | 2.2 ± 5.8 | **0.0 ± 0.0** | 6.7 bots | 337.7 ± 23.1 |

† Old-code value (§3.7). With 0 returns there is no point where the code difference lands.

**The door axis disappeared from bot admission.** What tracked the pass rate linearly at
3.7 → 12.6 → 20.2% in §3.7 became 3.7 / 3.1 / 2.2 — all intervals overlapping, and if there is a
direction it is **downward.** The door channel is exactly 0 in all six runs.

#### Instead the cost moved to total seats

It did not disappear; **where the seats are changed.** Seats released fall monotonically with the
number of returns, 370.0 → 356.3 → 337.7, in a nearly linear relationship:

```
0 returns    370.0 seats
274 returns  356.3 seats     slope ≈ -0.058 seats per return
562 returns  337.7 seats     (the line from 0 → 562 predicts 354.2 at 274; measured 356.3)
```

This slope is a **quantitative confirmation** of the mechanism §3.8 already described: a participant
blocked from admission stays in the queue holding a position. A return preserves position (by
design it must), so the returned person goes back to their original place in line and fills another
200 seconds of observation there. With patience at 300 s most do not cross the threshold in time and
**stay in line to the end.** Positions behind them are pulled forward that much later.

Whose seats were lost adds up: from door0 to door10, human admissions go 359.1 → 331.1 (−28) and bot
admissions 11.1 → 6.7 (−4.4), and the sum −32.4 equals the drop in total seats (−32.3). So
**86% of the vanished seats were humans'.**

The identity §3.7 wrote ("seats bots took = seats humans lost") becomes something else here —
**seats humans lost = seats nobody used.** Which is better is not decided by this table. But humans
received 309.3 seats in §3.7's door10 and 331.1 here, so **even accounting for the lost seats,
humans are 22 people better off.**

One falsifiable prediction: if this loss is "not crossing the threshold within the patience limit",
then **raising the patience limit should bring the seats back.** Not measured.

### 3.9 What it costs to get one bot admitted — measuring §1.4 for the first time — **measured**

Every table so far measures **how many were caught.** That is not the goal §1.4 declared:

> "Complete" bot blocking. The goal is to raise the bot's cost above the value it extracts from a
> normal user.

Detection 96%, false positives 0%, seat accounting balanced — and **the cost was never measured
once.** The criterion the project set for itself was missing from the measurements.

#### Separate load from economics

Cost cannot be measured with a load test. Raising difficulty makes **k6, not the bot**, pay for the
computation, and the detection metrics collapse with it (§3.7's discarded measurement was exactly
that). So the two axes are pulled apart:

1. **Hash rate is measured offline** — `make bench-pow`. Apple M4 Pro, Go's standard
   `crypto/sha256`, single core: **61.65 ns/op = 16.2 MH/s**.
2. **Verify that extrapolation holds** — if expected attempts are 2^d, each bit of difficulty must
   double the time. Measured `BenchmarkSolve` at 8/12/16 bits is 17.6 µs / 232.7 µs / 3.76 ms,
   agreeing with the extrapolated 15.8 µs / 252 µs / 4.04 ms within ±10%. **That is how a cost
   table can be built without actually running 67 million iterations at 26 bits.**
3. **Attempt counts come from §3.7's load measurement and are multiplied in.** The product holds
   because the PoW schedule is deterministic.

`loadtest/tools/powcost.py` does the arithmetic. Every price can be overridden by a flag, and the
defaults carry their source and lookup date — they are market values and go stale.

#### What one PoW costs

```
16.2 MH/s · vCPU $0.036/h · solving service $2.99/1k
```

| Difficulty | Expected attempts | Solve time | Cost per solve | vs one solving-service transaction |
|---|---|---|---|---|
| 8 | 256 | 0.02 ms | $1.6e-10 | 0.000% |
| 16 ←default | 65,536 | 4.05 ms | $4.1e-08 | 0.001% |
| 20 | 1,048,576 | 64.7 ms | $6.5e-07 | 0.022% |
| 24 | 16,777,216 | 1.0 s | $1.0e-05 | 0.346% |
| **26 ←config ceiling** | 67,108,864 | 4.1 s | $4.1e-05 | **1.385%** |
| 32 | 4,294,967,296 | 265 s | $2.7e-03 | 88.7% |

**The difficulty at which one PoW equals one solving-service transaction is 33 bits.** Even at the
configured ceiling (26) it is 128× cheaper, and 33 bits imposes a 265-second computation on a
normal user, so it cannot be enabled.

This is the actual value behind the sentence §12-8 recorded — "PoW does not stop bots, it
**makes them more expensive**". The width of "more expensive" is **one one-thousandth of four
cents.** Raising `POW_MAX_DIFFICULTY` cannot close this gap, because what rises exponentially also
raises normal users' waiting time exponentially.

#### What one admitted bot costs

Multiply §3.7's door10 (100% pass rate, the actual exposure) by the prices above. Difficulty is
computed with the real operating defaults (base 16 · bump 4 · ceiling 26) — §3.7 itself turned the
difficulty axis off for measurement purposes, but cost is only meaningful with that axis on.

| Run | Re-verification attempts | Returns | Bots admitted | Total PoW cost | Total service cost | Per bot |
|---|---|---|---|---|---|---|
| door10-r1 | 714 | 527 | 61.0 | $0.0039 | $2.13 | $0.0351 |
| door10-r2 | 728 | 534 | 64.0 | $0.0040 | $2.18 | $0.0341 |
| door10-r3 | 720 | 540 | 57.0 | $0.0040 | $2.15 | $0.0378 |

**Getting one bot admitted costs $0.036, and PoW is 0.2% of it.** The other 99.8% is the cost of
crossing a CAPTCHA bridge that is not implemented yet. So **the current implementation's (PoW-only)
real cost is 0.006 cents per bot**, and that is exactly what §3.7 meant by calling door10 "the
actual exposure" — that arm's solving-service cost is in reality 0.

#### §1.4 verdict: goal not met

Putting the ticket premium at $50, **cost is 1/1,400 of value.** No tightening of the challenge
layer closes this gap:

- Even at the ceiling, PoW is 1.4% of one solving-service transaction (table above).
- Tightening the attempt cap **reduces** attempts and therefore lowers the cost. The cap is a device
  for stopping the leak, not for raising cost.
- Reaching $50 would require 16,700 solving-service transactions per admitted bot. It is currently 12.

This value **attaches a number** to the place where §12-1 says "account/payment-level policy is
required". What one identity verification costs a bot is a value outside this system, but the fact
that the magnitudes differ by three orders comes from here. What the queue layer does is not making
bots expensive but **creating a place where a cost can be charged**, and the layer that actually
sets the price is above it.

#### After closing the door, the cost goes **down**

The table above is §3.7's door10 — the regime where the door bypassed the gate. When §3.8 closed
that bypass the door channel became 0, and the surviving channel is **unflagged (the race)** alone.
What is the door cost of a bot that came in through that channel?

**Zero.** A bot that came in through the race never went through re-verification once. All it paid
was one entry challenge at the default 16 bits = **$4.1e-08**.

So the more the defense is fixed, **the lower the cost per successful bot.** Closing the expensive
path leaves the cheap one. Applying §1.4's sentence literally makes the system look worse, while
what actually happened is that successful bots fell from 60.7 to 6.7.

**So §1.4's criterion itself does not fit this layer.**

> The goal is to raise the bot's cost above the value it extracts from a normal user.

That is not what the queue layer does. This layer **reduces the number of successful bots** — not by
raising cost but by filtering through observation, isolation, and the ladder. Blocking by cost would
need the 33 bits in §3.9's first table, and a normal user cannot pay that. Pricing bots out is the
job of **the account/payment layer (§12-1)**, not this one, and §1.4 was written presupposing that layer.

This is as far as this report can answer §1.4: **it confirmed with numbers that cost cannot do the
blocking, and therefore also confirmed that what is doing the blocking is not cost.**

#### Directions in which this calculation could be wrong

- **The hash rate is the value least favorable to the attacker.** It is Go's general-purpose SHA-256
  on a single core; a tuned implementation or a GPU is 10–1000× faster. Being wrong in that
  direction makes the PoW cost **lower still** — the direction of the conclusion does not change.
- **The same for the vCPU price.** On-demand was used; spot is 30–90% cheaper.
- **The solving-service price is a market value.** It is the listed price at lookup time (2026-08)
  and could fall further with bulk purchase or self-solving bots. If it falls, the gap widens.
- **The per-attempt distribution was approximated as uniform.** The result JSON has no per-attempt
  counts. Suspicion (fingerprint, IP range) was excluded and only the attempt count used, so the
  difficulty is a **lower bound.**

Sources: [2Captcha Turnstile pricing](https://2captcha.com/p/cloudflare-turnstile) ·
[AWS EC2 on-demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/) ·
hash rate measured with `make bench-pow`.

### 3.10 Does a falsely-flagged person come back through the door? — **measured**

From §3.7 onward the false-positive rate stayed at 0.0%, so **the very reason the door exists was
never once exercised.** If no human is flagged, "a flagged person returns without losing their
place" remains a declaration. So a person likely to be flagged is manufactured and the path walked.

#### The persona — flagged for who they are, not what they do

Not an invented person but a common combination (`SG_CLUMSY`, default 0):

- **Corporate golden-image device** → browser fingerprint identical to colleagues' (fingerprint signal 0.25)
- **Office NAT** → dozens arrive from the same /24 (IP-range signal 0.15)
- **Auto-refresh extension / assistive technology** → almost no polling jitter (regularity signal 0.25)

What differs from a bot is not **what they do** but **why they look that way**. The detector cannot
see that difference (§12-1). The arm is the same as §3.8, with 100 of the 700 humans replaced by
this person.

| Metric | §3.8 (0 clumsy) | §3.10 (100 clumsy) |
|---|---|---|
| FPR % | 0.0 | **14.3 ± 0.0** |
| Detection % | 97.7 ± 5.8 | **85.3 ± 7.1** |
| Bot admission % | 2.2 ± 5.8 | 9.0 ± 5.2 |
| Human admission % | 47.3 ± 2.2 | 40.8 ± 2.7 |
| Seats released | 337.7 ± 23.1 | 312.7 ± 3.8 |
| Isolation median s | 132.1 ± 7.5 | 83.3 ± 6.8 |
| Human re-verification attempts | 0 | **300.0 ± 0.0** |
| **Human admission: door** | 0 | **0.0 ± 0.0** |

#### The door opened, humans actually passed through it, and none of them got in

The 14.3% false-positive rate is **exactly 100/700**, identical to the decimal in all three runs.
The trajectory makes the reason clear — the 600 normal users stay flat at scores of 2.6–3.2 with
0.0% isolation throughout, while the 100 clumsy reach **100% isolation at t=125 s** and keep rising:

| t(s) | normal human score / isolated% | clumsy score / isolated% | farm-a score | farm-b score |
|---|---|---|---|---|
| 65 | 3.1 / 0.0 | 37.1 / 15.7 | 33.8 | 28.6 |
| 125 | 2.9 / 0.0 | 44.7 / **100.0** | 43.0 | 37.3 |
| 215 | 2.6 / 0.0 | **51.7** / 100.0 | 46.2 | 37.9 |

**The clumsy humans' final score (51.7) is higher than the real bots' (46.2 / 37.9).** The detector
rates them as more suspicious than bots. False positives are not "something that occasionally
happens near an ambiguous boundary" but **something that happens deterministically when identity
signals coincide.**

Re-verification attempts are exactly 300 in all three runs = 100 people × 3 attempts. That is,
every one of them passed the door twice and returned to their original position (returning really
works), and on the third exhausted the cap and was promoted to hold. And yet
**`human admission: door = 0`** — not one person who came out through the door was admitted.

The mechanism is **the same** one §3.8 identified for bots:

> Crossing the threshold again after a return is faster than finishing re-observation.

Clumsy signals are identity, so they do not fade with time. Hence return → re-observation begins →
(before observation completes) re-flagged → return → cap exhausted → hold. **Exactly the mechanism
that stopped the bots stops the falsely-flagged people too.**

This is the true value at the place where §3.8 wrote "humans paid no cost". The cost was 0 because
there was nobody to pay it; manufacture someone to pay it and **the cost is the full amount
(failure to be admitted).**

#### With a false-positive cohort present, bots are also caught less — the mechanism was not identified

Detection fell 97.7 → 85.3%. The per-run ranges do not overlap (95.3–100.0 vs 82.3–88.0) and a
two-sample t-test gives p ≈ 0.004. (The individual 95% intervals, being wide at n=3, overlap by
0.6pp — overlapping intervals and no difference are different statements.)

The trajectory points at where they diverge. **The score of farm-b (distributed-IP bots) is flat at 38.2:**

| t(s) | §3.8 farm-b score / isolated% | §3.10 farm-b score / isolated% |
|---|---|---|
| 155 | 37.9 / 14.0 | 38.0 / 3.3 |
| 185 | 38.0 / 21.7 | 38.1 / 2.8 |
| **200** | **39.8 / 37.2** | 38.2 / 0.9 |
| 215 | **42.0 / 64.2** | 38.1 / 0.2 |
| 260 | 41.8 / 96.1 | 38.2 / 4.6 |

In §3.8 the score jumps at t≈200 and isolation begins; in §3.10 **that jump is simply absent.**
t=200 is when `ADMIT_MIN_DWELL` opens and admission starts.

**The first explanation written was wrong.** It said "the 100 clumsy raised the baseline of the
**fingerprint and IP-range** signals", but those two (`groupSignal`) do not use the shard
distribution — they divide the **count** of tokens sharing a value by a fixed cap
(`FP_GROUP_CAP`), so there is no baseline to pollute. It sounded plausible and the direction was
right, but it cannot happen by that path.

What can be polluted is the side that uses z-scores: **the heartbeat relative term and PoW.** And
that relative term is normally asleep — a zero-jitter bot is already at 1.0 from the absolute floor
(`CVFloor`), so the value does not change whatever the baseline does. **For pollution to work the
absolute term must die first, and what kills it is server-side observation noise (= load).**
The conditions and magnitudes, measured with the scorer detached from Redis, are in §3.12.

Whether that condition actually held in this arm was **not settled.** The two arms' load indicators
are nearly identical (554 / 558 rps, PoW 0.97 / 1.05 ms, HTTP P99 77.7 / 69.5 ms — the clumsy arm is
if anything faster), and the magnitude measured synthetically (−0.07 signal at 100 ms noise ≈ −1.75
points) is less than half the observed gap (about 4 points). There is one more candidate —
**admission thinning the population sharpens relative signals** (correlation 0.149 → 0.287 as the
population goes 350 → 100). §3.8's jump coinciding exactly with the admission start (t=200) is its
trace, but the two arms' actual departure rates differ by only 8%, so this does not explain the
magnitude either.

**The causal input is known (100 clumsy). Both paths have the right direction and both fall short
on magnitude.** So this section leaves the observation without naming it. The mechanism
investigation is §3.12.

> **Closed later — §3.13.** Changing only the relative signals' baseline estimator to median/MAD
> brings detection back from 85.3 to **98.1%** and breaks farm-b's freeze. Since that was the only
> change, this drop **was baseline pollution.** The paragraphs above are kept as a record of not
> having gotten there by observation alone — observation and intervention are different grades of
> evidence, and §3.12's synthetic model failed to reproduce the magnitude because it measured only
> a static swap and could not capture the arm's dynamics.

Measured fact: **in a population where false positives are real, false positives and misses move in
the same direction.** Bot admission rose 2.2 → 9.0%, and §12-6's claim that thresholds cannot
improve both at once is confirmed once more from the other side.

#### So what should be fixed — not yet fixed

What this measurement points at is not the threshold but the **re-observation length.** In §3.8,
what stopped the bot leak was "re-observation > time to re-flag after return", and that time is far
shorter than the initial isolation (the score starts at 35, not 0). Today a return is charged the
same 200 seconds as first entry, and whether that is necessary has never been measured.

- Shortening re-observation would let a falsely-flagged person get in within the patience limit.
- Too short and §3.8's leak returns.
- Where in between was **not measured.** A config item for post-return re-observation separate from
  `ADMIT_MIN_DWELL` would be needed, and that is a design change not made here.

> **Measured afterward, and the window is empty — §3.11.** Looking at the distribution before
> building the setting, clumsy's re-flag median is 11 s and bot farm-a's is 10 s. Whatever length is
> chosen, both groups land on the same side. The three lines above are kept as **a rejected candidate.**

The limits, stated too. **This persona is an extreme that produces a 14.3% false-positive rate** —
an organization of 100 people sharing one fingerprint entering a single event simultaneously. There
is no basis for claiming real false positives look like this; what was confirmed here is "what
happens if such a group exists". Arms with a lower ratio were not measured.

### 3.11 Post-return re-observation length is not a knob — **measured**

The candidate §3.10 pointed at was "make post-return re-observation separate from first entry".
For that knob to work, **one value must separate two groups** — the falsely-flagged person finishes
re-observation and gets in, while bots are re-flagged before that.

The premise was measured before building the setting. No new measurement was run. **The events
needed are already in §3.10's runs** — both return and re-flag are `state` transitions on the
`user:` hash, and the score trajectory trace captures them at 5-second intervals
(`loadtest/tools/reflag.py`, 3 traces · 1,302 return episodes).

#### The distribution — the falsely-flagged are re-flagged fastest

| Cohort | Returns | Re-flagged | Censored | Median | p75 | p90 | Score at return | Initial isolation median |
|---|---|---|---|---|---|---|---|---|
| human:clumsy | 493 | **493** | **0** | **11 s** | 16 | 16 | 36 | 78 s |
| bot:farm-a | 626 | 479 | 147 | **10 s** | 11 | 72 | 36 | 87 s |
| bot:farm-b | 183 | 63 | 120 | 67 s | 80 | 88 | 35 | 170 s |

The 600 normal users **do not appear in this table at all** — never isolated, so never returning.
The score at return being 35–36 for all three cohorts confirms the clamp
(`RECHALLENGE_PASS_SCORE=35`) really applies.

What to read is the relationship between two rows. **clumsy (11 s) and farm-a (10 s) are effectively
the same distribution.** And where separation does exist (farm-b, 67 s) the direction is
**reversed** — what a short window lets through is not the falsely-flagged person but the hardest
bot to catch.

#### At every length, bots pass more than humans

The fraction that "is not re-flagged within W and therefore passes", for a chosen re-observation
length W (censored episodes — never re-flagged to the end — count as passing):

| W | clumsy pass % | farm-a pass % | farm-b pass % |
|---|---|---|---|
| 5 s | 100.0 | 100.0 | 100.0 |
| 10 s | 53.8 | 59.1 | 100.0 |
| 15 s | 26.0 | 36.4 | 100.0 |
| 20 s | **1.2** | 36.1 | 100.0 |
| 45 s | 0.0 | 35.6 | 100.0 |
| 60 s | 0.0 | 35.6 | 90.2 |
| 120 s | 0.0 | 25.9 | 68.3 |
| 200 s (current) | 0.0 | 23.5 | 65.6 |

**In every row the bots' pass rate is greater than or equal to clumsy's.** Not merely an absent
knob but **a reversed sign** — the best available on this axis is "let nobody through" (the current
200 s), which is what is already being done.

> **This table is re-flag timing, not admission.** The large values in the right two columns do not
> mean "that many got in" — passing re-observation still leaves position, budget, and the patience
> limit, and the actual admission channel was measured in §3.8 as **door = 0**. Moreover, for a
> cohort with many censored episodes (farm-b, 120 of 183) the pass rate at large W is
> **an upper bound created by the observation ending.** What this table is for is the ordering
> between columns, not the absolute levels.

The top row (W=5 s) must be read separately. The scorer judges every 5 seconds
(`SCORE_FLUSH_EVERY`), so a window shorter than that is **not observation but the absence of
observation.** Everyone passing is not innocence but nobody having been scored yet, and §3.4's
"inability to judge is not innocence" applies to re-observation as well. So the usable range of this
axis is bounded below by the flush interval and above by §3.8's leak — and **the space between is empty.**

#### The same in another arm (§3.8 trace cross-check)

Looking at bots only in §3.8's (door10-obsfix) traces, which have no clumsy: farm-a 10 s, farm-b
36 s. farm-b slowing to 67 s in the clumsy arm is the same phenomenon as its score sitting just
below the threshold in that arm (§3.10 — mechanism not identified). Whichever value is used,
clumsy's 11 s sits **inside** the bot distribution.

#### Why it is empty — time is a monotone transform of the score

After a return, all three cohorts start at 35. When they next cross 40 is decided by **how high
that person's steady-state score is.** But in §3.10's trajectory that value is
clumsy 51.7 > farm-a 46.2 > farm-b 37.9 — **the falsely-flagged person is highest.** Re-flag time
inverts that order: a higher score means a shorter distance from the clamp to the threshold, so it
comes back faster. The difference between clumsy's 11 s and farm-a's 10 s is **not distinguishable**
within the sampling interval (5 s) — they are the same bucket, and the only one separated is
farm-b, which happens to be on the wrong side. So **the time axis is a monotone re-encoding of the
score axis.**

One general conclusion follows: **an ordering the score already got wrong cannot be corrected by a
monotone function of that score.** Threshold, re-observation length, and grace period are all
different names for the same axis. To separate them you must use **information not yet in the score.**

A sentence §3.10 left behind points at what that information is — clumsy is a metronome but
**synchronized with nobody.** Fingerprint, IP range, and regularity are signals a lone person can
produce from identity alone, but **cross-account correlation is structurally impossible alone.**
Accounting for a re-verification pass as "decay the already-adjudicated solo signals, keep
correlation at full weight" separates the two groups. That is a design change rather than a
parameter, so it was not done here — Phase 10.

### 3.12 Baseline pollution — the condition, the size, and robust statistics' breakdown point — **measured**

§3.10 left an observation and no explanation. Here that explanation is dug out **at the scorer
level rather than under load.** Judgment runs without Redis (§4 design notes), so the signals can be
separated one at a time — `internal/botscore/camouflage_test.go`.

#### Where can pollution act?

Of the five signals, only some take the shard distribution as their reference:

| Signal | Reference | Pollutable |
|---|---|---|
| heartbeat **absolute term** | fixed floor `CVFloor=0.02` | no |
| heartbeat **relative term** | mean/σ of the shard's CVs | **yes** |
| correlation | mean bin co-occurrence `expected` | yes (indirectly) |
| fingerprint / ip_prefix | **count** of tokens sharing a value ÷ fixed cap | **no** |
| pow | mean/σ of the shard's solve costs | **yes** |

That fingerprint and IP range cannot be polluted is what collapses §3.10's first explanation. Those
signals have no "mean".

#### The condition: pollution cannot reach while the absolute floor is alive

`heartbeatSignal` is the **maximum** of the absolute and relative terms. A zero-jitter bot has an
absolute term of 1.0, so shaking the baseline changes nothing. Pollution therefore works **only
after the absolute term dies**, and what kills it is noise in the interval the server measures
(`IntervalMS` is a server observation, so a bot's CV rises as load rises).

The change in farm-b's regularity signal when 50 humans are swapped for 50 clumsy and nothing else:

| Server observation noise | 0 ms | 25 ms | 50 ms | 100 ms |
|---|---|---|---|---|
| Signal change | +0.000 | +0.000 | −0.001 | **−0.070** |

**In other words, the detector opens up to pollution as load rises.** The −0.070 at 100 ms times the
0.25 weight is −1.75 points — the direction matches what §3.10 saw, and the magnitude is less than half.

#### The size: how far it moves when built as an attack

Rather than a coincidentally overlapping group, **an attacker deliberately pushing** was
synthesized. Into a population of 350 normal users plus 50 target bots (a "stealth" type with
moderate jitter), F accounts in the same regularity band as the target are pushed. Fingerprints and
IP ranges are all distinct so that the only thing moving is heartbeat.

| Accounts pushed F | Contamination | mean/σ | median/MAD |
|---|---|---|---|
| 0 | 12% | 0.780 | 1.000 |
| 50 | 22% | 0.579 | 1.000 |
| 150 | 36% | 0.420 | 0.616 |
| 300 | **50%** | 0.320 | **0.176** |

**The attack is real** — the mean baseline is pushed monotonically from 0.780 to 0.320. For a bot
near the threshold, 0.46 × 0.25 = 11.5 points is enough to undo an entire rung of the ladder.

#### Robust statistics defend only up to the breakdown point

Median/MAD was added as a setting (`SG_SCORE_ROBUST_BASELINE`). The result is textbook:

- **Clearly better up to 36% contamination** (1.000 / 1.000 / 0.616 vs 0.780 / 0.579 / 0.420).
- **Worse at 50% contamination** (0.176 vs 0.320). That is exactly MAD's breakdown point — past it
  the median flips wholesale toward the attacker's cluster, and the target bot is no longer "more
  regular than the center".

The mean is pushed gradually; the median holds and then falls off a cliff. Which is better depends on
**what fraction of a shard an attacker can hold**, and while the premise that shard assignment is
unpredictable holds (§3.1) that fraction is small. But it must also be known that robust estimation
is worse if that premise breaks.

**The default was left off** at the time of this section. It changes detection logic, so enabling it
makes the system different from §3.1–§3.11's, and this project's rule is to load-test before
changing a default (the same treatment as `ADMIT_AFTER_LOTTERY` in §3.4). What must be measured
before enabling it is detection and false-positive rates, not these signal values.

> **That load measurement was done, and the default is now on — §3.13 and §3.14.** Detection
> 85.3 → 98.1% with false positives unchanged, and no harm in a population without a false-positive
> cohort either. The result also says something about this section's synthetic model:
> **this model underestimates the magnitude.** What was measured here is a static swap (50 people)
> in a fixed population, while in a real arm isolation, returns, and admissions keep changing the
> population. The direction and the condition came out right in this model, but **the magnitude
> comes only from load.**

#### Addendum: measuring the next design's premise in advance

§3.11 closed the time axis, so the remaining axis is "accounting for a re-verification pass as
evidence", and that candidate's core is **"on a pass, decay the already-adjudicated solo signals but
keep cross-account correlation at full weight"**. The rationale was "a lone person structurally
cannot produce cross-correlation". Measuring that premise with the same harness:

| Cohort | Cross-correlation signal (25 ms noise) |
|---|---|
| Normal humans | 0.027 |
| clumsy (false positives) | **0.108** |
| Bot farm | 0.178 |

**The premise is only half true.** clumsy is 4× the humans and 0.6× the bots — an office cohort
**opens the waiting room at the same moment and refreshes on the same interval** without
coordinating, so it emits a synchronization signal. Not a clean separator.

And there is a harder arithmetic problem. **Cross-correlation's weight is 0.20, so the maximum score
that signal alone can produce is 20 points, and the greylist threshold is 40.** Decay all the solo
signals and bots cannot reach the threshold either — it does not merely release the falsely-flagged;
**nobody gets re-isolated at all.** So (b) does not stand on its own and requires **redesigning the
post-pass weights or thresholds together with it.**

Better to know this before starting the design. The cost of measuring it was one more run of a
harness that already existed.

#### Aside: this section's first conclusion was an artifact of the model

After the first measurement it said "a composition change does not touch the bots' signals at any
noise level — §3.10's explanation is disproved". **That disproof was wrong.** The cause was building
the synthetic jitter as `seed*(i*31+k*7+1) % 2R - R`. That is not randomness but an **arithmetic
sequence**, so the humans' CVs clustered at nearly one value — and with no width in the human
distribution the relative term saturates and changing the composition moves nothing.

It was noticed not through a result but through **a different question**: while trying to compare
median/MAD, "MAD is a statistic that measures the width of a distribution — does this model have
any width?" got checked. Switching the randomness to PCG made the −0.070 at 100 ms noise appear.

The lesson is the same kind as §3.7's "we measured the load generator" — **the measuring instrument
can erase the very property it is measuring, and the result comes out not as a failure but as a
clean negative.** The more negative the result, the more the instrument should be suspected first.
Here, "nothing happened" was exactly that shape.

### 3.13 Turning robust estimation on and measuring — attack → defense → **verification** — **measured**

§3.12 built the knob and confirmed it only at the signal level. "The signal is pushed less" and
**"more bots get caught"** are different claims, so this is measured under load.

The arm differs from §3.10 by **the estimator alone.** That arm was chosen for two reasons — it is
where §3.10 observed the detection drop, and because a false-positive cohort is present there,
**the false-positive rate gets verified at the same time.**

```
identical to §3.10 + SG_SCORE_ROBUST_BASELINE=true      3 runs
```

| Metric | §3.10 mean/σ | §3.13 median/MAD |
|---|---|---|
| **Detection %** | 85.3 ± 7.1 | **98.1 ± 5.0** |
| **FPR %** | 14.3 ± 0.0 | **14.3 ± 0.0** (identical) |
| Human admission % | 40.8 ± 2.7 | **48.0 ± 2.8** |
| Bot admission % | 9.0 ± 5.2 | **1.9 ± 5.0** |
| ├ via the door | 0.0 | 0.0 |
| └ unflagged | 27.0 bots | **5.7 bots** |
| Seats released | 312.7 ± 3.8 | **341.3 ± 5.2** |
| Share/population | 1.305 | **1.405** |
| Isolation P90 s | 200.2 | 182.1 |
| Human admission median s | 203.8 | 203.8 |

#### The freeze broke

In §3.10, farm-b (distributed-IP bots) was flat at 38.2 with an isolation rate of 0–6%. The same
cohort, in the same population, now moves like this:

| t(s) | §3.10 score / isolated% | §3.13 score / isolated% |
|---|---|---|
| 125 | 37.3 / 1.1 | 38.3 / 32.2 |
| 140 | 37.8 / 2.9 | **39.7 / 58.4** |
| 155 | 38.0 / 3.3 | **42.4 / 92.9** |
| 185 | 38.1 / 2.8 | 46.7 / **100.0** |
| 245 | 38.2 / 6.7 | 50.4 / 100.0 |

A cohort that could not cross the threshold (40) crosses it at t≈140 and is fully isolated by t=185.

#### §3.10's unresolved mechanism closes here

§3.10 ended with "the causal input is known but the path is not". Both candidates (composition-driven
baseline pollution / admission thinning the population) had the right direction and fell short on
magnitude.

**This measurement answers that question as an intervention experiment.** The only change is the
estimation method for two relative signals' baselines, and that alone brings 12.8pp of detection
back. Had population thinning been the cause, changing the estimator could not have recovered it.
So **§3.10's detection drop was baseline pollution** — §3.12's synthetic model failed to reproduce
the magnitude because it measured only a static swap (50 people) and could not capture the arm's
dynamics.

Observation and intervention are different grades of evidence. §3.12 was observation (how signals
move under synthetic conditions); here it was **changing one thing and seeing whether the result
comes back.**

#### False positives did not rise — in this arm

There was one worry. In §3.12's synthesis, robust estimation was **more sensitive on a clean
distribution** (1.000 vs 0.780 at 0% contamination). More sensitivity can mean more false positives.

It did not. The false-positive rate is **exactly 14.3% (100/700)** in all three runs, the same as
§3.10, and in the trajectory the 600 normal users stay **flat at scores of 3.1–3.6 with 0.0%
isolation throughout.** The clumsy were already at 100% isolation in both arms — at the ceiling — so
what moved was the magnitude of their score (52.7 → 64.5), not whether they were isolated.

That said, this arm is an extreme in which the false-positive cohort is 14% of the population.
Whether the increased sensitivity creates false positives in a population without such a cohort
needs a separate arm (§3.14).

This table alone gives no reason not to enable it — detection +12.8pp, false positives unchanged,
human admission +7.2pp, seats +28.6. But one arm is not enough. **A change in the direction of more
sensitivity must be verified on the false-positive side**, and this arm is an extreme. §3.14 is that
verification.

### 3.14 Is there harm in a clean population? — **measured**

In §3.12's synthesis, robust estimation was **more sensitive** without contamination (1.000 vs
0.780). More sensitivity can create false positives that did not exist. §3.8's arm (0 clumsy) is
measured three times with only the estimator changed.

| Metric | §3.8 mean/σ | §3.14 median/MAD |
|---|---|---|
| Detection % | 97.7 ± 5.8 | 97.4 ± 5.3 |
| **FPR %** | 0.0 ± 0.0 | **0.0 ± 0.0** |
| Human admission % | 47.3 ± 2.2 | 48.7 ± 2.7 |
| Bot admission % | 2.2 ± 5.8 | 2.6 ± 5.3 |
| Seats released | 337.7 ± 23.1 | 348.3 ± 12.5 |
| **Isolation median s** | 132.1 ± 7.5 | **98.7 ± 7.0** |
| Isolation P90 s | 196.2 ± 37.8 | 191.8 ± 15.4 |

**No harm.** False positives are 0.0% in all three runs and in the trajectory the 700 normal users
stay flat at 4.3–4.4 — the increased sensitivity does not turn into false positives in this
population. Detection, bot admission, and human admission all have overlapping intervals.

**Instead, isolation got 33 seconds faster** (132.1 → 98.7 s). Even in a population whose baseline is
not polluted, median/MAD pushes bots over the threshold earlier than mean/σ — this is how the
sensitivity difference that looked like "1.000 vs 0.780 at 0% contamination" in §3.12's synthesis
appears under load. That sensitivity **acted on bots only.**

Incidentally this value forces a recomputation of the gate-length basis — §3.6 set "the gate must be
longer than the isolation median", and that median fell from 132 to 99 s, so there is room to set
`ADMIT_MIN_DWELL` shorter than 200 s. **Not measured.**

#### So the default is turned on

Measured in two populations with no harm, `SCORE_ROBUST_BASELINE`'s default is **changed to on.**
The basis in one line each:

| Population | Detection | FPR | Other |
|---|---|---|---|
| 14% false positives (§3.13) | 85.3 → **98.1** | 14.3 → 14.3 | seats +28.6, human admission +7.2pp |
| No false positives (§3.14) | 97.7 → 97.4 | 0.0 → 0.0 | isolation 33 s faster |

The risk accepted is the **breakdown point** (§3.12). Past 50% contamination it is worse than
mean/σ. Reaching that point requires an attacker to hold half of a shard, and while `event_salt`
is secret the assignment cannot be chosen, so it means holding more than half of all participants
(§3.1). **In that regime relative signals no longer work at all** (§12-6) — the range where this knob
gets worse is the range where the detector does not work anyway. That was judged an acceptable trade.

**Every table in §3.1–§3.12 is a value from the mean/σ regime.** They were not re-measured —
comparisons between arms are valid only within the same system, and those comparisons are each
section's point. The values describing the current system are §3.13 and §3.14.

### Cautions when running load tests

Items that actually produced a wrong table during measurement.

- **Patience (`SG_PATIENCE`) must be shorter than the scenario length.** If it is longer, VUs still
  queuing are cut at `maxDuration`, and a cut VU never records whether it was isolated, so it
  **disappears from the denominator.** What remains is people who finished their own iteration —
  those who were admitted — so the admission success rate is 100% by definition. This combination
  once produced the meaningless table "humans 100% · bots 100% · detection 0%". `mixed.js` now
  derives the length from patience and raises an exception if it is given directly.
- **Set seats below the participant count, but not far below.** With leftover seats, competition is
  not measured. Conversely, with only 60 seats the values swing so much per ratio that a nonexistent
  trend appears — that is how "humans are 3.4× better off" was first obtained, and raising the seats
  to 180 dropped it to 1.04×.
- **PoW cost lands on the k6 CPU too.** At difficulty 16, one VU hashes 65,536 times on average per
  entry. If the load generator is CPU-bound, heartbeat intervals become equally irregular for bots
  and humans and **the regularity signal itself disappears.** Run with
  `SG_POW_BASE_DIFFICULTY=8` and look at PoW cost separately via §3.1's `sg_pow_solve_ms`.
- **`FLUSHALL` between runs.** Leftover `suspicion:` keys make adaptive difficulty inherit the
  previous run's suspicion, so the base difficulty differs from the configured value (12 instead of
  8 actually happened).
- **k6 must enter through the waiting-room origin (:8088).** Hitting service ports directly skips the
  `X-Forwarded-For` handling nginx provides, changing the IP-prefix signal.
- **Check that the scorer was assigned partitions before measuring.**

  ```
  kafka-consumer-groups.sh --describe --group shardgate-scorer --members
  ```

  If `#PARTITIONS` is 0, detection is entirely off. The scenario still finishes cleanly and produces
  the believable table "detection 0.0% · false positives 0.0%" — every service is alive and there is
  no error in any log (see ROADMAP defect 7). `kafka-init` and `waitForTopic` prevent this state, but
  checking is free.
- **Whether the lottery window is on is read from `fairness.lottery_segment_rate` in the result
  file.** 0 means `EVENT_OPEN_AT` was missing and everyone entered FIFO (defect 6).
- **Nominal admit rate and effective rate can differ.** `perCycle = Round(rate × interval)`, so
  20/min × 5 s = Round(1.67) = 2 → an effective 24/min. An interval of 3 s gives Round(1.0) = 1
  exactly. The value written in a report must be the effective one.

---

## 4. Grafana dashboard

After `make dev`, <http://localhost:3000> (anonymous viewing allowed). The dashboard is provisioned
from `deploy/grafana/dashboards/shardgate.json`.

Four panels to watch during a load test:

| Panel | What it says |
|---|---|
| Queue depth per shard | Lines lying evenly means HMAC assignment is uniform. One line standing out means the assignment is biased. |
| Action pipeline | The lower stages (observe, greylist) should be thick and blocks thin. Blocks thickening first means the ladder is being skipped. |
| Bot score distribution | p50 (humans) and p99 (bots) should be far apart. If they converge, detection does not hold. |
| Scorer processing lag | Even if it grows, the queue keeps moving (invariant 5). Only detection is delayed. |

---

## 5. What this report does not prove

Stated honestly. Read alongside DESIGN.md §12.

1. **Residential proxy + real device + human operator** is not reproduced by any scenario here, and
   would not be detected if it were. That domain belongs to account/payment-level policy.
2. **Values measured on single-node Redis.** Cluster-mode slot distribution is prepared in code
   (hash tags) but is not validated by this report's numbers.
3. **The k6 simulator is a bot we wrote.** Only the evasion strategies we know are implemented, so
   the detection rate is a value "for these three types", not for bots in general.
4. **The false-positive rate depends on how well the simulator imitates people.** Assistive-technology
   users, low-end devices, and unstable networks are absent here — the real FPR is higher than this.
   §3's 0% FPR is a value about "people this simulator created".
5. **§3.2's bot-ratio trend was not measured.** One run per ratio, and the re-run variance exceeds
   the differences between ratios (§3.2's second-run table). What can be said is only "humans' seat
   share stays near 1 relative to their population share"; whether it rises or falls as the ratio
   rises is unknown. Claiming a trend would require several runs per ratio with confidence intervals.
   The repetition + CI harness used from §3.5 onward was not applied retroactively to §3.2.
6. **Only one of §3.3's two candidates was measured.** Separating the lottery window from admission
   was measured in §3.4, and the per-user minimum observation gate in §3.6. **Initial admit
   throttling is still pending** — it changes the admit-rate distribution algorithm, which per
   CLAUDE.md requires a load test first, and that load test is §3.4 and §3.6. It has become
   touchable, not been touched.
7. **Measured at one point, 30% bots** (§3.4, §3.5, §3.6 alike). §3.2's key graph was not redrawn per
   arm, so whether the spacing between arms holds as the ratio rises is unknown.
8. **Measured with 2 shards.** The design presupposes dozens to hundreds of shards of 500–2,000; what
   was measured here is two of 250–600. With smaller, more numerous shards the group signals
   (fingerprint, IP range) weaken as one farm spreads across shards, while relative signals become
   unstable as the sample shrinks. Which effect wins cannot be known from this measurement.
9. **§3.5's 100% ceiling was measured on a population that never shrinks.** With admission off the
   shard population holds to the end, the most favorable condition for relative signals. In real
   operation people keep leaving and the sample shrinks. It does not mean a gate can realize all of
   that 100%.
10. **Greylist was a terminus with no exit — fixed in Phase 8** (surfaced in §3.5).
    Three defects overlapped: the noop path in `move_shard.lua` skipped the score write so
    **the score froze**; there was no re-challenge path; and `apply_action.lua` did not know about the
    greylist queue, so hold and block left members in that ZSET. On top of that, `admit.lua`'s
    `state != 'waiting'` exclusion made 40–69 a heavier action than 70–89.

    **The repair direction chosen was to provide the re-challenge path** (preserving design intent).
    Option 2 (documenting current behavior) removes the reason for the ladder to have two rungs and
    makes "false-positive protection" empty — a real person past score 40 would be permanently
    excluded with no re-verification. To keep it from being a cheap exit, the score is clamped to just
    below the threshold rather than 0, difficulty rises per attempt, and exhausting the cap promotes
    to hold (DESIGN §4). Measured in §3.7.

    **§3.1–§3.6's numbers are still from the pre-repair regime.** They were not re-measured —
    reading differences between arms requires comparisons of the same system, and §3.7 measures that
    difference separately on one obs200 arm.
11. **The re-challenge is PoW only.** §4-L2's CAPTCHA path (Turnstile etc.) exists only as an adapter
    slot. PoW is a compute cost rather than an impassable wall for a bot, so §3.7's
    `captcha_proxy=1.0` is **the current implementation's actual exposure** and 0.5 / 0.0 are assumed
    values for when a CAPTCHA bridge exists. Additionally, k6's `SOLVE_LIMIT` (2^22) is the
    simulator's give-up point, not a bot's limit — a farm spending more compute goes past it. So
    §3.7's leak is a **lower bound.**
12. **§3.7 did not catch the door bypassing the observation gate** (surfaced in §3.8). The gate was
    measuring entry time, so returning participants came back already satisfying it, and most of
    §3.7's "bot admission 20.2%" was that bypass. Being **a defect that does not move detection at
    all**, it passed that section's table unchanged — the property §3.7 identified ("a recall-only
    table does not show the leak") applied to §3.7 itself.

    To catch this kind next time, an identity counting admissions per channel was added
    (`admissions = door + unflagged`, residual 0). Unlike the other checks that each point at a
    specific failure, this one catches **only the fact that something broke, without knowing what.**
    But it too rests on the premise of "knowing the channels" — a channel the classifier never
    imagined shows up as residual, yet a residual of 0 is not proof that the channel definitions are right.
13. **Race and miss cannot be separated within a run.** An admitted client stops sending heartbeats so
    the scorer has nothing more to see, and "a bot that would have been caught later" leaves the same
    trace as "a bot that would never have been caught". Which of the two §3.8's 6.7 unflagged bots
    are cannot be known from that run; the ceiling measurement (§3.5, 100% with admission off) only
    suggests **from a different run** that it is the former. So the channels are two plus a residual,
    not three.
14. **The cost calculation is not a load measurement** (§3.9). Hash rate is an offline benchmark,
    attempt counts come from load measurement, and the product rests on the property that the 2^d
    schedule is deterministic. Extrapolation and measurement agreeing within ±10% at 8/12/16 bits
    confirmed the basis for the product, but **26 bits was never actually run.** Prices are market
    values as of the lookup date (2026-08), and the per-attempt distribution was approximated as
    uniform because the result JSON has no per-attempt counts.
15. **§3.10's detection drop was closed in §3.13 — but what closed it was intervention, not
    observation.** By observation (the synthetic model) both candidates had only the right direction
    and fell short on magnitude, and two sections were written in that state. Only on seeing detection
    return after changing the baseline estimator alone could it be settled. The remaining limit is
    **the resolution of the intervention** — "it was baseline pollution" can be said, but which of the
    five signals contributed how much is not separated by this experiment (the heartbeat relative term
    and PoW change together).
16. **§3.12's pollution experiment is a synthetic model, and it underestimates the magnitude.** It
    measures one window (60 s) with the scorer detached, without accumulation, actions, or returns.
    The condition and the direction came out right in that model, but the magnitude came only from
    load (§3.13). What looked like −1.75 points synthetically was 12.8pp of detection under load.
    **Do not use a signal-level experiment as a result-level prediction.**

    That this section's own first conclusion was a model artifact is recorded too (§3.12 aside):
    building the synthetic jitter as an arithmetic sequence left no width in the human CV
    distribution, and in that state "a composition change has no effect" came out as a **clean
    negative.** The more negative the result, the more the instrument should be suspected first —
    the same kind as §3.7's "we measured the load generator".
