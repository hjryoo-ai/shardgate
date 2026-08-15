# ShardGate

***English** · [한국어](CLAUDE.ko.md) — the Korean file is the original; this is a translation.*

A sharded virtual waiting room that isolates and blocks bots automatically using per-shard
statistics. Full design: @docs/DESIGN.md — always check against that document before changing
architecture, scoring policy, or the API.

## Commands

```bash
make dev            # bring up Redis/Kafka/PG + all services via docker compose
make test           # all unit tests (go test ./...)
make test-int       # testcontainers integration tests (needs Redis/Kafka)
make lint           # golangci-lint run
make loadtest       # k6 mixed scenario (loadtest/k6/mixed.js)
make bench-queue    # enqueue/position benchmarks
make check-exposure # port exposure check (detects 0.0.0.0 binding)
```

- New features must pass `make test && make lint` before being committed.
- Integration tests are slow — run them only when queue/scorer logic changes.

## Architecture Map

- `cmd/{gate,queue,admission,scorer,shop}` — service entry points. No business logic, wiring only.
- `internal/shard` — HMAC-based shard assignment. event_salt must never reach a log.
- `internal/queue` — Redis ZSET operations. **Never compose Redis commands directly; always go through scripts/lua/.**
- `internal/challenge` — PoW issue/verify. Difficulty comes only from what botscore provides.
  Re-challenges (greylist return) use the same issuer/verifier — the attempt count is merely
  carried along in `Subject.Attempt`.
- `internal/token` — JWT issue/verify. Claim schema changes go together with a DESIGN.md §4-L3 update.
- `internal/botscore` — per-shard scoring and actions. Action thresholds live in config, never hardcoded.
  **The scoring population is always the origin shard** — scoring greylist as a separate
  population makes relative signals converge to 0 and the score comes back down (DESIGN §12-6).
  What greylist separates is the queue and the budget.
- `scripts/lua/` — the single source of truth for every queue state transition. Do not reimplement in Go.

## Invariants (absolute rules)

1. Queue state transitions (enqueue, admit, shard move, block) happen **only as a single atomic Lua execution**. Never split them into multiple Redis calls.
2. Never write a handler that changes state without token verification. Entry tokens must be burned on redeem.
3. Bot actions go through the score pipeline (observe → greylist → hold → block). No single signal can justify an immediate block.
4. Every state-changing API must be idempotent (idempotency key or Lua atomicity).
5. The detection path (Kafka consumer) and the admission path must stay decoupled — the queue must keep moving even if the scorer dies.
6. Privacy: store fingerprints as hashes only. Never write raw fingerprints or full IPs to PG.
7. **The score pipeline keeps running regardless of a participant's state.** Being in greylist
   or hold does not stop observation, score updates, or promotion up the ladder. What changes
   with state is **the kind of action**, not whether observation happens. If an action stops
   observation, isolation becomes a terminus, the four rungs of the ladder lose their meaning,
   and a participant who keeps behaving like a bot freezes just above the threshold (this
   actually happened — REPORT.md §3.5, 2,400 bots frozen at 40.2).
   The score must be recorded even on paths where the action script returns `noop`.

## Conventions

- Standard Go layout; wrap errors with `fmt.Errorf("...: %w", err)`; structured logging via slog.
- Redis key naming: `{resource}:{event}:{shard}` — never invent keys outside the DESIGN.md §3.3 schema.
- Config is env-based (`internal/config`); no magic numbers (shard size, admit rate, score thresholds, …).
- Tests: table-driven. Lua scripts are verified against a real Redis (testcontainers), not miniredis.

## Don't

- Don't make queue position depend on a client-supplied value (the server is the only truth).
- Don't duplicate `scripts/lua/` logic in Go.
- Don't change the admit-rate distribution algorithm without a load test (`make loadtest` first).
- Don't adjust bot-detection thresholds "to make a test pass" — fix the scenario instead.
- **Don't make greylist a state with no exit.** Without a way out (re-challenge), 40–69 and
  70–89 become the same thing and a falsely-flagged person is permanently excluded with no
  re-verification. The response to a greylist user is not an error (4xx) but
  `200 + challenge_required` — the same principle as `observing`.
- **Don't let a path back into the queue bypass the observation gate.** The gate measures from
  the observation clock (`observe_from`), not the entry time (`joined_at`), and a re-challenge
  return rewinds that clock (`rechallenge.lua`). Without the rewind, a returning user comes
  back having already satisfied the condition, and **the race of §12-7 restarts once per
  opening of the door.** If the gate only protects the first admission, every re-entry after
  it is unprotected. The liveness signal is read the same way — `hb_count - hb_base`, not the
  cumulative value.
  This defect did not move the detection rate at all, so it passed the §3.7 measurement
  unchanged — isolation is recorded at first observation, so the leak shows up only in the
  **admission count** (REPORT §3.8).
- **Don't distribute admission budget to greylist shards.** There is no path to admission from
  there (the only way out is returning to the origin shard), so allocated seats vanish with
  nobody able to use them.
- **Don't publish compose ports on `0.0.0.0`.** Always `127.0.0.1:<host>:<container>`.
  Docker's port publishing bypasses the macOS application firewall, and there is no guarantee
  the dev machine is behind NAT. On 2026-08-12 an unauthenticated Redis came up on
  `0.0.0.0:6379` and was cron-injected within minutes. Use an SSH tunnel if another machine
  needs to connect. `make check-exposure` enforces this rule and runs in CI.
- **Don't add a per-knob check for "did this setting get applied".** Services dump the entire
  set of settings actually read from the environment at startup (`internal/app`,
  `EffectiveEnv()`), and `sweep.sh` diffs that against the arm definition. There is one rule:
  every `SG_*` the arm defines must either have been read by some service with a matching
  value, or appear in the harness/client-only list. Per-knob logging **reopens the same hole
  every time a new knob is added**: compose's environment block is a whitelist, so an unlisted
  name is silently ignored, the service starts normally on its default, and the measurement
  produces a believable table with only the arm quietly changed.
  When you add a new `SG_*`, **adding the name to compose is part of that work.**

## Working Notes

- Per-phase task list: @docs/ROADMAP.md (progress tracked with checkboxes)
- Local Kafka is a single KRaft broker. Keep the partition count equal to the shard-count ceiling.
