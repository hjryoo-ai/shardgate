# ShardGate implementation roadmap

***English** · [한국어](ROADMAP.ko.md) — the Korean document is the original; this is a translation.*

A checklist that breaks the phases of DESIGN.md §10 into work items. A phase closes only when
its "done when" criterion is met.

---

## Phase 0 — Scaffolding / infrastructure / CI

**Done when:** `make dev` brings the whole stack up

- [x] Go module, §9 repository layout
- [x] `internal/config` — env-based settings, magic numbers removed entirely
- [x] `config.Secret` — a type that prevents event_salt / signing keys from leaking into logs
- [x] `internal/keys` — single source of truth for the Redis key schema (including cluster hash tags)
- [x] `internal/obs` — slog JSON logging + Prometheus metrics
- [x] `internal/httpx` — JSON conventions, middleware, graceful shutdown, IP prefixes
- [x] `internal/redisx` — Redis connection + Lua EVALSHA runner
- [x] `deploy/docker-compose.yml` — Redis 8 / Kafka 4.2 KRaft / PG 18 / Prometheus / Grafana
- [x] `deploy/postgres/init/001_schema.sql` — §6 schema
- [x] Makefile (dev/test/test-int/lint/loadtest/bench-queue)
- [x] GitHub Actions CI (build, test, lint, integration)
- [x] Skeleton entry points for all five services

## Phase 1 — Queue core

**Done when:** unit tests pass + a 100k enqueue benchmark

- [x] `internal/shard` — HMAC-SHA256 shard assignment, salt never logged, dynamic growth
- [x] `scripts/lua/enqueue.lua` — lottery/FIFO hybrid position, idempotent
- [x] `scripts/lua/position.lua` — atomic snapshot of position and queue depth
- [x] `scripts/lua/heartbeat.lua`, `evict.lua` — liveness signal and soft-evict
- [x] `internal/queue` — Lua loader (embeds `scripts/lua`) + domain API
- [x] Table-driven unit tests
- [x] testcontainers integration tests (against real Redis)
- [x] `make bench-queue` — enqueue/position/heartbeat + 100k enqueue benchmark

**Measured** (Apple M4 Pro, single Redis 8 on Docker Desktop, 64 shards × 64 concurrent workers):

| Item | Value |
|---|---|
| 100k enqueues | 2.87 s (34,904 enqueue/s) |
| enqueue | 123 µs/op, 36 allocs |
| position | 123 µs/op, 32 allocs |
| heartbeat | 122 µs/op, 24 allocs |

Most of the latency is the Docker Desktop VM's network round trip (which is why these are in
the 100 µs range rather than µs). Concurrency hides it, so it does not translate directly into
throughput.

## Phase 2 — Admission

**Done when:** entry → admission E2E happy path

- [x] `scripts/lua/refill_budget.lua` — per-shard budget distribution
- [x] `scripts/lua/admit.lua` — atomic budget decrement + state transition
- [x] `scripts/lua/redeem.lua` — entry token burn
- [x] `internal/token` — JWT issue/verify (pulled forward from Phase 3, see item 4 below)
- [x] `internal/admission` — drain distribution (largest remainder), backpressure, circuit breaker
- [x] `internal/api` — handlers (keeping the rule that `cmd` only wires things up)
- [x] `internal/store/pg` — order storage, one-purchase-per-person UNIQUE constraint
- [x] `cmd/queue` — status (SSE/polling), heartbeat, soft-evict sweep loop
- [x] `cmd/admission` — distribution loop
- [x] `cmd/shop` — idempotent orders, one purchase per person
- [x] E2E tests (Redis + PostgreSQL testcontainers) — entry→position→admission→purchase happy
      path, idempotent retry, entry-token reuse blocked, one purchase per person, budget cap,
      access without a token blocked, SSE

## Phase 3 — Challenge + token

**Done when:** token-reuse attack tests pass

- [x] `internal/challenge` — PoW issue/verify. Difficulty is only injected, via
      `DifficultyProvider` (the rule is enforced by structure, not a comment — this package
      contains no code that decides difficulty)
- [x] `internal/token` — JWT issue/verify, fp_hash / IP-prefix binding (completed in Phase 2)
- [x] `internal/shard/grower.go` — loop that grows the shard count when arrivals exceed the estimate
- [x] `cmd/gate` — `/queue/enter`, `/challenge/verify`
- [x] Attack scenario tests — challenge reuse, difficulty downgrade, expiry extension, nonce
      substitution, signature removal, wrong solution; queue-token device sharing, shard-claim
      tampering, signature truncation, expiry; entry-token theft; statelessness of the entry path

**Challenge design notes**

- **Issuing is stateless.** On a path taking hundreds of thousands of requests per second at
  open, one Redis write per issued challenge makes the gate its own bottleneck. The challenge
  carries a server HMAC signature, and writes happen only for requests that bring a solution.
- **What the signature protects is the difficulty.** Without it, a client could rewrite
  `difficulty=1` and send it back, disabling PoW entirely. nonce, difficulty, expiry, and
  event_id are signed as one unit.
- **Burn after verifying the solution.** Burning the nonce on a wrong solution enables an
  attack that burns someone else's nonce with an arbitrary answer.
- **Single use is enforced with one `SET NX`.** It is a single-key atomic operation, so
  "only the first arrival succeeds" is guaranteed by itself. Invariant 1 (single Lua execution)
  is a rule about queue state transitions, and this is not the queue.

## Phase 4 — Telemetry → scorer

**Done when:** bot simulator detection confirmed

- [x] `internal/telemetry` — event schema, Kafka producer (partition key = shard_id), drops on
      buffer overflow. Publishing never blocks and never returns an error (the rule is enforced
      by the interface, not a comment — `Publish(Event)` has no return value)
- [x] `internal/botscore` — the five §4-L5 signals, weighted scoring, exponentially smoothed accumulation
- [x] `scripts/lua/apply_action.lua`, `move_shard.lua`, `restore_shard.lua`
- [x] Action pipeline (observe → greylist → hold → block) + return when the score decays
- [x] `internal/store/pg` — asynchronous audit / block-history writes
- [x] `cmd/scorer` — Kafka consumer
- [x] Test proving the detection path and the admission path are decoupled (`internal/api/separation_test.go`)
- [x] Bot simulator detection verification (`internal/botscore/simulator_test.go`)

**Bot simulator results** (1 shard, 200 normal users + 30 bots, 60 s window × 30 rounds):

| Type | Count | Final action | Ever acted on | Final score |
|---|---|---|---|---|
| Normal users | 200 | — | 0 | 2.3 – 20.6 |
| naive script | 10 | hold 10 | 10 | 74.7 – 82.4 |
| heartbeat mimic | 10 | greylist 7 | 7 | 31.7 – 45.0 |
| distributed IP | 10 | greylist 10 | 10 | 61.4 – 66.7 |

Detection 90% (27/30), false positives 0% (0/200). The humans' maximum (20.6) and the bots'
minimum (31.7) do not overlap — the score bands must separate for a threshold to stop being a
trade between false positives and misses.

**Why only the heartbeat-mimic bot is partially detected** (a limit for §12):
forging timing is free, so it erases the regularity and cross-correlation signals. What remains
is fingerprint (0.25), IP range (0.15), and PoW (0.15), and the first two alone land exactly on
the greylist line (40). On top of that, an individual isolated early loses its comparison
population in the greylist shard, so relative signals go to 0 and the score falls.
**Relative signals need something to compare against** — this structural limit cannot be closed
with a threshold; it is where account/payment-level policy is required (§12-1).

**Detection design notes**

- **Judgment and application are separated.** `Scorer.Flush` only produces verdicts and never
  touches Redis; state transitions are done by `Actuator` in Lua. This makes the scoring logic
  testable without Redis, and removes the temptation to "adjust the threshold until the test passes".
- **Only blocking has an extra condition.** Even above 90, if fewer than `MinSignalsToBlock`
  signals point at the user, it is lowered to hold (leaving a trace in `Decision.CappedFrom`).
  Config validation refuses to start if that value is below 2, so invariant 3 is enforced by a
  value rather than a comment.
- **An unobserved signal is 0.** Treating "unknown" as 0.5 raises the score with no basis, and
  the cost falls on people who leak fewer fingerprints. As a consequence, a user with only one
  signal firing structurally cannot reach a high score.
- **Observe-only decisions write no audit row.** If every user wrote one row per window, the
  audit table would grow larger than the queue.

## Phase 5 — Validation / demo

**Done when:** the §11 metrics report is produced

- [x] `loadtest/k6/lib/` — protocol client + participant behavior models.
      Cross-checked that the PoW solver produces the same answers as the Go implementation
      (difficulty 8/16/20)
- [x] `loadtest/k6/normal_users.js` — normal users
- [x] `loadtest/k6/bot_farm.js` — three bot types (naive / heartbeat mimic / distributed IP)
- [x] `loadtest/k6/mixed.js` — mixed scenario + detection/FPR aggregation + §11 report output
- [x] Grafana dashboard (`deploy/grafana/dashboards/shardgate.json`, 27 panels)
- [x] `deploy/nginx/` — waiting-room origin. The CDN slot of §2, and the same-origin guarantee (SSE cookies)
- [x] `web/` waiting-room demo page — actually solves PoW in a worker
- [x] `docs/REPORT.md` — the measurement report

**Nine defects that only appeared when the real stack ran** (unit and integration tests did not
catch them). 1–7 surfaced in Phase 5, 8–9 during Phase 8's measurements. **The further down the
list, the harder the question shifts from "what broke" to "how do we know something broke"** —
8 and 9 are both the kind where nothing fails and a believable table comes out:

1. **compose published ports on `0.0.0.0`** — an unauthenticated Redis was open to the internet
   and was actually compromised. All ports are now bound to `127.0.0.1` and Redis auth is on.
   `make check-exposure` + a CI job prevent regressions. The account is at the head of that script.
2. **Losing the shard index stops admission forever** — the memo in `registerShard` was
   permanent, so after `shards:{event}` was lost it was never re-added. The queue looks healthy;
   it just stops moving. The memo now has a deadline (1 minute) so it self-heals.
   (`TestShardsRegistryRecoversAfterIndexLoss`)
3. **nginx pinned backend IPs at startup** — a 502 every time a service was recreated.
   Switched to request-time resolution with `resolver` + a variable `proxy_pass`.
4. **nginx's `X-Real-IP $remote_addr` disabled the IP-range signal entirely** —
   `httpx.ClientIP` reads `X-Real-IP` **before** `X-Forwarded-For`, so the common idiom
   `X-Real-IP $remote_addr` quietly wins. Every participant's `ip_prefix` collapsed into the
   proxy network's single /24. What makes this nasty is that the signal does not disappear —
   **everyone gets a uniform maximum score on it.** It contributes nothing to discrimination
   while pushing normal users' scores up by 15 points (weight 0.15). In production behind a CDN,
   `$remote_addr` is the CDN edge IP and is equally meaningless. Decide the client IP once at the
   trust boundary and put the same value in both headers.
5. **The load scenario silently biased the metric's denominator** — if patience (`SG_PATIENCE`)
   exceeds the scenario length, VUs still queuing are cut off at `maxDuration` and never record
   whether they were isolated. What remains in the denominator is people who finished their own
   iteration — that is, people who got admitted — so **the admission success rate is 100% by
   definition.** It does not fail; it produces a believable number, so the table alone cannot
   reveal it. `mixed.js` now derives the length from patience and raises an exception if it is
   given directly. The account is at the end of docs/REPORT.md §3.
6. **The lottery window had never once been enabled** — `SG_EVENT_OPEN_AT` was simply absent
   from compose, so `enqueue.lua` received `lottery_end=0` and **put everyone in the FIFO band.**
   The fairness model of §3.2 (nullifying arrival order right after open) is the first line of
   bot mitigation, and every §11 measurement was taken with it off.
   Unit and integration tests cannot catch this — `LotteryEnd` and `enqueue.lua`'s lottery band
   both have tests and all of them pass. **What was missing was not code but the deployment
   giving that code a value.** When the feature exists, the tests exist, and the config key
   exists, but no value reaches the execution path, the feature is simply off with nobody
   failing. Added `SG_EVENT_OPEN_AT` to compose with a comment on why it must not be empty.
7. **The scorer went `Stable` with zero partitions assigned** — topics are auto-created **on
   first produce**, and Kafka has no volume, so the topic disappears on every
   `docker compose down`. If the scorer comes up alongside the producer, it **joins a
   nonexistent topic, is assigned zero partitions, and stabilizes there.** Even when the topic
   is created later there is no reason to rebalance, so it stays at zero forever.

   What makes this worse than the previous six is that **nothing fails.** The broker health
   check passes so `depends_on: service_healthy` is green, the consumer group state is
   `Stable`, and `ReadMessage` just blocks without error. Every service is alive while
   **detection is entirely off.** The load scenario still finishes cleanly and leaves a
   believable "detection 0.0% · false positives 0.0%" table. We only looked at the consumer
   group's `#PARTITIONS` after obtaining that table once.

   Fixed in two places. `kafka-init` pre-creates the topic (partitions = shard-count ceiling)
   and app services start only after it finishes. On the code side, `Consumer.waitForTopic`
   waits for partitions to exist before joining. **Only one of the two would silently revert if
   it were deleted** — all the more so for a defect that gives no signal when it reverts.
   `TestComposeCreatesTopicBeforeServicesStart` prevents regression.

8. **compose's environment block is a whitelist — unlisted names are silently ignored** (Phase 8).
   Three measurements defined an arm with `SG_GREYLIST_DIFFICULTY_BUMP=0`, but that name was not
   in `deploy/docker-compose.yml`, so the container came up with the code default of 4.
   The export succeeds, the container starts normally, and the scenario finishes cleanly —
   **a believable table comes out with only the arm quietly changed.** Exactly the same kind as
   defect 6 (`SG_EVENT_OPEN_AT` missing), and the second time it happened in the same place.

   It was noticed through the results, not the config: `pow_difficulty_avg` was 10.75 when
   bump=0 should give exactly 8.00. So the fix was also two-layered — add the name to compose,
   and have `cmd/gate` log the difficulty settings **as actually applied** so `sweep.sh` can
   verify the arm from that line. This extended a check that had existed only for admission.

9. **Detection can turn off mid-run and the scenario still completes** (Phase 8).
   One run ended at **0.0%** detection. Another arm with the same server config was at 98.8%,
   so it was that run rather than the system — yet all services were alive and the HTTP failure
   rate was 0.00%. The pre-run partition check (a product of defect 7) only sees "did it join";
   if events stop flowing after joining, the consumer blocks without error.

   So `sweep.sh` now counts the scorer's `botscore_actions_total` **after** the run and aborts
   rather than putting a run with 0 into the table. "It joined" at the start and "it actually
   judged" at the end are different facts, and it is the latter that contaminates the table.

**The tenth was not found by running anything — there was no `git` repository until Phase 9.**

While writing `.github/workflows/ci.yml`, adding nine regression guards, and running 27
measurement arms, **there was not one commit.** CI had nowhere to run, the code state of each
phase was not preserved, and there was no way to attach "which code produced this value" to a
measurement (the `commit: null` in the reproduction manifest is the trace of that).

Same kind as the nine above — **nothing fails.** Build, tests, and measurements all pass, and
no signal of absence appears anywhere. The difference is that it is a process defect rather
than a tooling one, so no automated check can catch it. `git init` belonged in Phase 0.

The repair is not reversible. The code state of past phases does not exist, so **we do not
invent a history** — one initial commit for the current tree, with that fact stated in the
commit message and here. Commits split at phase boundaries could be manufactured, but their
contents would all be the final state, making them **a plausible-looking fake history** —
exactly the thing this report spent 1,500 lines learning to distrust. The job of a history
(evidence of incremental work) is already done more accurately by this document and REPORT.md.

---

## Phase 6 — Follow-up to §3.3: can the detection/admission race be fixed?

**Done when:** a gate on/off comparison table lands in REPORT.md §3.4

The mechanism §3.3 identified was "a bot that gets in first never gets a chance to be isolated".
Identifying a mechanism and fixing it are different things, so one of the two proposed
candidates is actually measured.

- [x] `SG_ADMIT_AFTER_LOTTERY` — a gate that does not open admission until the lottery window
      closes (`internal/admission`). A gated cycle **skips distribution entirely** rather than
      distributing a budget of 0 — the budget in `refill_budget.lua` accumulates and its TTL is
      refreshed, so calling it with grant=0 would actually keep the leftover budget alive.
- [x] Refuse to start if the gate is enabled without `EVENT_OPEN_AT` (prevents the gate silently
      becoming a no-op)
- [x] `mixed.js` records the lottery-window entry rate in the results (regression guard for defect 6)
- [x] Three arms (lottery OFF / lottery ON / lottery ON + gate) × 30% bots × 3 runs → REPORT.md §3.4.
      None of the three detection intervals overlap (61.7–67.5 / 74.2–81.7 / 85.8–93.3).
      **The lottery window is free** (seats unchanged, human admissions actually up);
      **the gate buys its gain by cutting seats 38%.** False positives were 0.0% in all nine runs.
- [ ] Initial admit throttling (the second candidate) is still untouched. It changes the admit-rate
      **distribution algorithm**, which per CLAUDE.md requires a load test first — and §3.4 is
      that load test. It has only just become touchable.
- [x] Enabling the gate made `/queue/status`'s estimated wait wrong (`EstimateWait` did not know
      about the gate) → fixed with `Snapshot.EstimateWaitAt`. The two gates differ in character
      so they combine differently: the global gate releases no seats at all before it opens, so
      its remaining time is **added in front of** the position wait, while the observation gate
      lets positions advance meanwhile, so it uses **max**.

---

## Phase 7 — The detector's ceiling and the per-user observation gate

**Done when:** REPORT.md §3.5 (ceiling) and §3.6 (gate comparison) are produced

Every gate up to §3.4 blocked everyone using a single event timestamp. Before going further,
**measuring how much the detector catches when given enough time** is what makes the gate value
defensible.

- [x] Measurement harness — `loadtest/tools/sweep.sh` (arm × N runs, stack recreated each time,
      arm settings verified against the admission log), `report.py` (mean ± 95% CI, t
      distribution), `trace_scores.sh` (samples score trajectories directly from Redis — putting
      them in a server response would break invariant 5)
- [x] `SG_SEED` — fixes client behavior by seed (derived from the **repetition number**, not the
      arm name, so arms are compared pairwise). Server-side variance remains, so it does not
      replace repetition
- [x] Detector ceiling measured (admission disabled by **setting the budget to 0, not by removing
      the service**) — detection 100.0 ± 0.0%, false positives 0.0%, isolation median 139.8 s ·
      P90 202.3 s (1,000 users × 8 runs). The 53.8% of §3.1 was all lost in the race
- [x] Per-cohort score trajectories — humans flat below 4, bots monotonically rising, no overlap
      from t=5 s onward
- [x] `ADMIT_MIN_DWELL` / `ADMIT_MIN_BEATS` — per-user minimum observation gate.
      The check sits **before the DECR** in `admit.lua`, so it defers seats instead of burning them
- [x] `EstimateWaitAt` — estimated wait that accounts for both gates
- [x] Four arms (off / lot / obs90 / obs200) × 4 runs → REPORT.md §3.6.
      **obs200 beats lot on four axes at once** (detection, bot admission, human admission, seats;
      not one 95% interval overlaps). The time gate buys its gain by cutting seats 22%
      (369.8 → 287.5, ±43); the observation gate gains without touching seats (371.2, ±3) —
      because judging before `DECR` lets the budget accumulate into the next cycle.
      The only cost is a 30.4 s delay to the first admission, and false positives were 0.0% in all 16 runs.
- [x] **Gate length is chosen by comparison with the isolation median** — a 90 s gate
      (< median 132–142 s) opens before the verdict lands and detection stops at 85.4%, while a
      200 s gate (> median) reaches 96.2%. The P90 chosen from §3.5's ceiling measurement
      (202.3 s) reproduced as 198.0 s in a regime where admission runs — the practical payoff of
      "measure the ceiling before touching parameters".

**What §3.5 revealed — the greylist ladder had collapsed into one rung**

The final state of 2,400 bots was **all greylist**, with scores stuck at 40.2. Not one hold, not
one block. Two causes:

1. The paradox of §12-6 — in the greylist shard everyone nearby is also suspect, so relative
   signals go to 0 and the score sits just above the greylist line.
2. `admit.lua` excludes everything with `state != 'waiting'`, and `move_shard.lua` sets the state
   to `greylist`. By design, exclusion from admission is a property of 70–89, but **40–69 is
   excluded along with it.** With no re-verification path, there was no way back either.

Fixing it mid-measurement would have made §3.4 and §3.6 comparisons of different systems, so it
was left alone this time. The intended direction was written up in REPORT §5-10.

---

## Phase 8 — Turning greylist back into a checkpoint

**Done when:** the ladder moves both ways, and a leak-channel table lands in REPORT.md §3.7

What §3.5 revealed was not one defect but three layers. Together they had made the 40–69 band a
*heavier* action than 70–89 — hold preserves position and allows return, while greylist had even
the path for the score to come down blocked.

- [x] **Fixed the frozen score** — moved the score write in `move_shard.lua` **ahead of** the
      `noop` guard. Not one verdict about an already-greylisted user was being stored, so the
      scorer kept judging while the value in Redis stayed frozen at the moment of the move
      (2,400 bots stuck at 40.2). Added invariant 7 to CLAUDE.md — when an action stops
      observation, isolation becomes a terminus.
- [x] **Whether the ladder actually climbs** — `apply_action.lua` now also receives the greylist
      queue key. Without it, hold and block only `ZREM` from the origin queue and members remain
      as ghosts in the greylist ZSET — and that `ZCARD` feeds budget distribution, so it disturbs
      other people's seats too. (`TestGreylistClimbsToHoldAndBlock`)
- [x] **Re-challenge path** — `scripts/lua/rechallenge.lua` + `POST /challenge/{reissue,reverify}`.
      On pass, return to the origin shard and original position. Redeem gives greylist users
      **200 + `challenge_required`**, not 409 (the same principle as `observing`).
- [x] **Three things that keep the door from being a cheap exit** — clamp the score to just below
      the threshold (35) rather than 0, raise difficulty per attempt, and promote to hold when
      the attempt cap (2) is exceeded.
- [x] **The clamp reaches the scorer** — the truth of the accumulated score is the scorer's
      memory; the `score` in Redis is a copy. Poking it via Redis would break invariant 5, so it
      is announced with `KindRechallenge` telemetry (the partition key is the origin shard, so it
      reaches the consumer holding that token).
- [x] **Nailed the scoring population to the origin shard** — scoring greylist as a separate
      population is §4's original idea, but then the paradox of §12-6 (everyone nearby suspect →
      relative signals 0) holds exactly. The original idea and §12-6 cannot both be true, so §4
      was changed.
- [x] **Greylist shard budget is 0** — removed `GreylistWeight`. There is no path to admission
      from there, so allocated seats vanish unused. Greylist shards are not even registered in the
      index today, but `weights()` pins 0 so that seats are not burned even if a registration path
      appears.
- [x] **Difficulty escalation is a maximum, not a sum** — adding suspicion (fingerprint, IP range)
      and attempt count (token) made a single isolation→re-verification raise both at once, hitting
      the ceiling (26 bits) within two attempts. In a smoke measurement, 55 of 58 issues went out
      at the ceiling and **nobody could pass** — building an exit and then locking the door, which
      returns to the defect being fixed.
- [x] `SG_CAPTCHA_PROXY` — the rate at which k6 bots pass CAPTCHA via a solving service
      (0/50/100%). The implemented re-challenge is PoW only, so the bots' real pass rate is 1.0.
      That is, **1.0 is the current implementation's actual exposure** and the rest are assumed
      values for when a CAPTCHA bridge exists.
- [x] §3.7 measurement — obs200 arm × three pass rates (0/50/100%) × 3 runs.
      **Detection overlaps in all three (96.3 / 96.6 / 97.0) while only bot admission widens from
      3.7 → 12.6 → 20.2%.** Isolation is recorded at first observation, so leaving through the
      door is not a detection failure — a recall-only table does not show this leak. Total seats
      released is the same across the three arms (370.0 / 368.3 / 370.0), and the 16.5pp × 300 ≈
      50 seats the bots took match the 7.1pp × 700 ≈ 50 seats humans lost. False positives were
      0.0% in all nine runs.
- [x] Harness hardening — checks added after three retractions.
      `mixed.js` puts a threshold (100 ms) on the mean of `sg_pow_solve_ms` and records
      `load_generator_cpu_bound` in the result JSON. `sweep.sh` (a) verifies difficulty settings
      from the gate's startup log, (b) aborts if the scorer's `botscore_actions_total` is 0 after
      a run, (c) aborts if the lottery entry rate is below 0.9, and (d) builds before stamping
      the open time so the lottery window is not eaten by build time.
      `report.py` warns before the table if any CPU-bound run is mixed in.

---

## Phase 9 — Pricing the door, and stopping it from bypassing the gate

**Done when:** REPORT.md §3.8 (observation clock) and §3.9 (cost) are produced, and the admission
channel identity is in the harness

The correction §3.7 left behind was "isolation and staying isolated are different metrics".
Turning that sentence into accounting immediately revealed a channel nobody was counting —
**users coming out of the door were returning with the observation gate already satisfied.**

- [x] **Admission channel conservation identity** — `admissions = door + unflagged`. A nonzero
      residual means someone got in through a path the classifier does not know about.
      `mixed.js` counts per channel and `sweep.sh` blocks on it as a fifth check. **Not creating
      an "other" bucket to absorb the residual** is the whole point — absorbing it makes the
      identity always hold and the check disappear. The previous four checks each point at a
      specific failure; this one catches **only the fact that something broke, without knowing
      what.**
- [x] **Race and miss cannot be separated within a run** — an admitted client stops sending
      heartbeats so the scorer has nothing more to see, and "a bot that would have been caught
      later" leaves the same trace as "a bot that would never have been caught". The value that
      separates them is the ceiling measured with admission off (§3.5), and that is from a
      different run. So the channels are two **plus a residual**, not three.
- [x] **Rewinding the observation clock** — `admit.lua` reads `observe_from` rather than
      `joined_at`, and `rechallenge.lua` stamps that value to now on return. The liveness signal
      is likewise `hb_count - hb_base` rather than the cumulative value. Without the rewind, the
      race of §12-7 restarts every time the door opens. `position.lua` / `EstimateWaitAt` must
      read the same clock or the screen lies to returned users specifically.
- [x] **The door that opens on score decay (`restore_shard`) does not rewind** — the clamp (35) is
      a **concession** rather than a verdict, so it needs observation to re-establish the true
      value; a score decline is a **conclusion** the detector reached across many windows.
      Whether this asymmetry is a hole is watched by the residual of the identity above
      (0 in measurement).
- [x] **PoW cost measurement** — `make bench-pow` (hash rate) + `loadtest/tools/powcost.py`.
      It cannot be measured under load, so **load and economics were separated**: hash rate
      offline, attempt counts from §3.7, and the product holds because the 2^d schedule is
      deterministic. Direct 8/12/16-bit measurements agreeing with extrapolation within ±10%
      confirmed the basis for the product.
- [x] **False-positive persona (`SG_CLUMSY`)** — corporate golden image (shared fingerprint) +
      office NAT (shared range) + regular polling. **A person flagged for who they are, not what
      they do**, and the exact reason the door exists. Default 0 — enabling it changes the human
      cohort composition so it cannot be read alongside §3.7's three arms.
- [x] §3.10 measurement — false positives 14.3% (exactly 100/700, identical three times); the
      other 600 normal users stay flat at 0.0% isolation throughout. **The door really worked**
      (all of them returned twice, 300 attempts) — and yet **zero of them were admitted through
      it.** Re-flagging after return is faster than re-observation, **exactly the mechanism that
      stopped the bots.** Incidentally detection fell 97.7 → 85.3% (t-test p≈0.004): the
      distributed-IP bots' score flattens at 38.2 and the 38 → 42 jump that §3.8 had at t≈200
      (admission opening) disappears. **When false positives are real, misses rise with them.**
- [x] **That mechanism was not identified — the first explanation was disproved** (§3.10 revision).
      It said "clumsy raises the baseline of the fingerprint/IP signals", but `groupSignal` does
      not use the shard distribution (only a fixed cap). Attempting to reproduce it with the
      scorer detached from Redis (`internal/botscore/camouflage_test.go`), **a static composition
      change lowered none of the bots' five signals** (within ±0.03), and the load indicators were
      the same in both arms. The remaining candidate was "admission thins the population, sharpening
      relative signals" (correlation 0.149 → 0.287 as the population goes 350 → 100), but **the
      magnitude was not reproduced.** So it was left unnamed — naming it leads to repairing a
      mechanism that has not been verified, and the experiment above says that repair would have
      changed nothing in this measurement. **→ Closed by intervention in Phase 10. That last
      judgment ("the repair would have changed nothing") was wrong** — the synthetic model was
      underestimating the magnitude (§3.13).
- [x] **A separate post-return re-observation length — measured and rejected** (§3.11). Before
      building the setting, the premise was measured: for that knob to work, the re-flag time
      distributions must separate by cohort. Extracting them from §3.10's traces with
      `loadtest/tools/reflag.py`, **clumsy 11 s and farm-a 10 s** overlap, and the only cohort
      that is separated — farm-b (67 s) — is on the **wrong side**: at any length W, the bots'
      pass rate is greater than or equal to the falsely-flagged humans'. The reason is that
      re-flag time is a monotone transform of the steady-state score, and that score is
      clumsy 51.7 > farm-a 46.2 > farm-b 37.9.
      **An ordering the score already got wrong cannot be corrected by a monotone function of
      that score** — threshold, re-observation length, and grace period are all different names
      for the same axis. No new measurement was taken; the events were already in the traces.
- [x] **A position moving backward after return is not a defect** — while an isolated person is
      out of the origin queue, the position seen by those behind them is inflated forward. When
      the person ahead returns, that inflation disappears. What is preserved is the ZSET score
      (the place in line), not the ZRANK (`TestRestoredRankRisesWhenSomeoneAheadReturns`).
      Failing to distinguish the two leads to fixing a bug that does not exist and breaking a
      real invariant in the process.
- [x] **Re-measured door05** — §3.7's three arms had come from different code (only door10 was
      re-measured). door05 was measured three more times with the same arm definition to make it
      one system's table. door0 was not re-measured (0 returns, so the rewind has nothing to
      touch by definition). **The door axis disappeared from bot admission** (3.7 / 3.1 / 2.2%,
      all intervals overlapping; the door channel is 0 in all six runs). Instead the cost moved to
      **total seats** — 370.0 / 356.3 / 337.7, almost linear in the number of returns
      (−0.058 seats per return), and 86% of the vanished seats were humans'. §3.7's identity
      ("seats bots took = seats humans lost") becomes "seats humans lost = seats nobody used".
      → end of REPORT §3.8.
- [ ] **Initial admit throttling** — still open. It is the untouched one of §3.3's two candidates,
      and being a change to the admit-rate distribution algorithm, a load test comes first.

---

## Phase 10 — Baseline pollution and evidence accounting (in progress)

The two branches left by §3.10–§3.12. One is a statistical vulnerability in the detector; the
other is the structural problem that the ladder does not account for "passed re-verification" as
evidence.

- [x] **Baseline pollution documented as an attack scenario** — DESIGN §4-L5. Three relative
      signals (the heartbeat relative term, cross-correlation, PoW) derive their comparison
      baseline from the shard sample, so supplying part of the sample moves the baseline.
      The magnitude was measured synthetically: the target bot's regularity signal goes
      0.780 → 0.320 as contamination rises 12% → 50% (REPORT §3.12).
- [x] **Identified the condition under which pollution works** — it cannot reach the signal while
      the absolute floor (`CVFloor`) is alive. What kills that floor is **server-side observation
      noise**, and since `IntervalMS` is a server measurement, **the detector opens up to
      pollution as load rises** (0.000 at 0–50 ms noise, −0.070 at 100 ms). Keeping latency low
      is part of detection accuracy.
- [x] **Robust estimation (`SG_SCORE_ROBUST_BASELINE`)** — median/MAD. Clearly better up to 36%
      contamination (0.616 vs 0.420) and **worse at 50%** (0.176 vs 0.320). That is MAD's
      breakdown point. Not an unconditionally better knob but **a knob with different
      characteristics**, and what must be measured before enabling it is detection and
      false-positive rates, not signal values.
- [x] **Robust arm measured — attack → defense → verification is complete** (§3.13). An arm
      differing from §3.10 by the estimator alone, × 3 runs: **detection 85.3 → 98.1%**, false
      positives unchanged at 14.3%, bot admission 9.0 → 1.9%, human admission 40.8 → 48.0%, seats
      released 312.7 → 341.3. In the trajectory, farm-b's freeze at 38.2 breaks and it reaches
      100% isolation at t=185.
- [x] **§3.10's detection drop — closed by intervention.** The only thing changed was the baseline
      estimator and detection came back, so **it was baseline pollution.** Had population thinning
      been the cause, changing the estimator could not have recovered it. What observation
      (synthetic) could not settle for lack of magnitude, intervention settled — **the two are
      different grades of evidence.**
- [x] **No harm in a clean population either → the default was turned on** (§3.14). Robust
      estimation is **more sensitive** without contamination (§3.12, 1.000 vs 0.780), so it could
      have created false positives that did not exist. Changing only the estimator on §3.8's arm,
      × 3 runs: false positives stayed 0.0%, detection / bot admission / human admission all
      overlap, and **isolation got 33 s faster** (132.1 → 98.7 s). The sensitivity worked on bots
      only. The accepted risk is the breakdown point, and reaching it requires holding more than
      half of all participants (§3.1) — **a regime in which relative signals no longer work at
      all** (§12-6).
- [x] **From per-knob checks to a general rule** — `SCORE_ROBUST_BASELINE` leaves no trace in the
      result JSON (only the signal computation changes), so it is verifiable only from logs.
      Adding a check per knob **reopens defect 8's hole every time a knob is added.** So the root
      was changed — services dump **everything actually read from the environment** at startup via
      `EffectiveEnv()` (`internal/app`, secrets masked by name), and `sweep.sh` diffs that against
      the arm definition. One rule covers knobs that do not exist yet, and as a byproduct the
      `dur_ns()` unit conversion disappeared. It is recorded in the manifest as `applied`.
- [x] **`report.py` was reading manifests as results** — the `*.manifest.json` files I added are
      `.json` in the results directory, which produced a table reading "6 runs · seats
      170.7 ± 196.2". **A file added for reproducibility immediately contaminated the table.**
      Fixed by filtering on the name and aborting — rather than silently skipping — when required
      keys are missing.
- [ ] **Evidence accounting (so the ladder stops punishing evidence of innocence)** — §3.11 closed
      the time axis, so the remaining axis is "account for passing re-verification in the score".
      Design candidates:
      (a) promote that token's re-flag threshold on each pass (40 → 55 → 70),
      (b) on a pass, decay the **already-adjudicated solo signals** (regularity, fingerprint, IP
      range) but **keep cross-account correlation at full weight** — it is the only signal a lone
      person structurally cannot produce, and therefore the axis that separates the two groups
      time cannot,
      (c) keep per-attempt difficulty escalation (a rising cost on solving-service-based immunity),
      (d) exhausting the cap → hold should be reachable **only through new correlation-family
      evidence**.
      **The premise was measured first, and it is only half true** (§3.12 addendum):
      cross-correlation is 0.027 for humans, 0.108 for clumsy, 0.178 for bots. An office cohort
      refreshes at the same moment without coordinating, so it is not a clean separator. Worse,
      **cross-correlation's weight is 0.20, so that signal alone caps at 20 points** against a
      threshold of 40 — decay all the solo signals and bots stop being re-isolated too. (b) only
      works **together with** a redesign of post-pass weights and thresholds.

---

## Delivery

Measurements that are never read might as well not exist. This repository's largest defect was
outside the code for a long time — there was no front page and no history (defect 10).

- [x] `README.md` — the five-second hook sells **measurement discipline**, not features. Its
      14-row table is ordered so each row exists because the row above it turned out to be wrong.
      English (canonical) + a Korean summary.
- [x] A 12-line summary at the head of `docs/REPORT.md` — ordered as a reversal narrative rather
      than a list of successes.
- [x] **Figures are generated, not screenshotted** (`loadtest/tools/figure.py`). They are
      recomputed from the result JSONs every time, so they cannot drift from the tables. The first
      one is §3.7 — "a recall-only report cannot see this leak" in a single picture.
- [x] **Results go into the repository** — result JSONs and reproduction manifests (260 KB) are
      committed; raw traces (161 MB) are excluded. Re-running `report.py` over those files
      reproduces the same means and confidence intervals, so the report's tables are verifiable
      inside the repository.
- [x] **Reproduction manifest** — arm, repetition, seed, commit, arm config, and **the settings as
      applied**, stored next to each result. Recording the observation rather than the intent is
      the point.
- [x] **Bilingual documents** — English at the canonical paths, the Korean originals at `*.ko.md`.
      Each document's header states that Korean is the authored text and English the translation.
- [ ] **Waiting-room UI and Grafana screenshots** — not taken; this session had no browser tooling.
      Two images after `make dev`: `http://localhost:8088` (waiting room) and
      `http://localhost:3000` (Grafana).
- [ ] **`git init` + remote** — requires authentication, so a human must run it. The single initial
      commit records defect 10 (see above).

---

## Differences from the design document (intentional extensions)

Items reflected back into DESIGN.md during implementation:

1. **§3.3 Redis key schema extension** — the logical schema is kept, but cluster hash tags are
   made explicit and the keys implementation needed (`seq`, `hold`, `shards`, `entry`,
   `challenge`, `score`, `stats`, `suspicion`, `idem`) were added to the document. Without tags,
   one shard's keys scatter across slots and "a single atomic Lua execution" (invariant 1) does
   not hold in cluster mode.
2. **Representation of the hold state (score 70–89)** — leaving them in the queue ZSET would
   occupy a position and block the people behind them. They move to a `hold:` ZSET with the
   original position preserved, and return at the same position on release. The meaning
   ("stays queued + excluded from admission") is unchanged.
3. **Added the `evicting` state** — §5's soft-evict is "3 missed → remove after a 30 s grace".
   A state representing the grace interval is needed so that a returning heartbeat can restore
   the position intact. One state, `evicting`, was added to §3.3's state set. The burn time is
   recorded in a `redeemed_at` field rather than adding another state.
4. **`internal/token` pulled forward to Phase 2** — invariant 2 ("never write a handler that
   changes state without token verification") makes Phase 2's HTTP handlers impossible without
   tokens. JWT issue/verify and fp_hash / IP-prefix binding were implemented in Phase 2, leaving
   Phase 3 with the PoW challenge, adaptive difficulty, `cmd/gate`, and attack scenario tests.
5. **PG 18 volume path** — the data directory moved to a version-specific subfolder under
   `/var/lib/postgresql`. Mounting a volume at `/var/lib/postgresql/data` as before makes the
   container refuse to start.
6. **The greylist shard shares the origin shard's slot** — §4's "greylist shard move" is three
   actions: remove from the origin queue, insert into the greylist queue, change the state. If
   the two queues are in different hash slots they cannot be wrapped in one Lua call under
   cluster mode and invariant 1 breaks. So `slotOf` in `internal/keys` folds `g0042` into
   `s0042`'s slot, putting `queue:{evt1:s0042}` and `queue:{evt1:s0042}:g0042` in the same slot.
   The `user:` key is the same string in both cases, so the user hash never experiences the move.
7. **Event-global keys are handled outside Lua** — `shards:{event}` and `admitted:{event}` use a
   different hash tag from the shard keys, so they cannot go inside a shard Lua under cluster
   mode. Neither is a queue state transition; they are a discovery index and a cumulative counter,
   and only single-key operations that are idempotent or order-independent (SADD/INCR) are used.
   Everything requiring atomicity — positions, budgets — lives inside the shard tag.
8. **The start of admission was extracted into config (`ADMIT_AFTER_LOTTERY`)** — §3.4's original
   text specifies only the distribution **ratio**, not the **start time**. §12-7 revealed that
   this timing sets the ceiling on detection, so the option of closing admission until the lottery
   window ends was added to DESIGN.md §3.4. It is a trade of throughput against detection rather
   than a fairness question, so the default is off.
