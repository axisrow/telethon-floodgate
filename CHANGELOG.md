# Changelog

## 0.1.4 (unreleased)

Review-driven correctness fixes; no API changes.

- `reset(category=...)` now honours its scope when no phone is given too: it
  clears only that category's peer buckets (it used to wipe every account's
  and every category's buckets — the opposite of what 0.1.3 documented), and
  unknown/no-op categories (`resolve`, `reaction`, typos) are a no-op instead
  of `KeyError` on the consumer's unblock path.
- `reset(phone, category="send_peer")` maps onto `send`: the category
  `TelegramPeerRateLimitedError` carries now clears the refusing bucket via
  the endorsed unblock call instead of silently matching nothing.
- Peer specs validate `window_sec` at gate construction (like category
  specs): a degenerate window raises there instead of from inside
  `try_acquire` mid-flight. Non-finite (NaN/inf) windows are rejected for
  burst and sustained windows alike — a NaN window silently disabled the
  limiter.
- `flood_wait_remaining_seconds` boundary note: a fractional remainder just
  above the transient threshold (the 60–61s band) classifies as blocking,
  consistent with the ≤60s policy (truncation used to call it transient).
- `FloodCircuitBreaker`: one cooldown per trip — `record_flood` no longer
  extends a running cooldown (late reports from calls already in flight keep
  the original trial time; a flood after the deadline expired starts a fresh
  cooldown), and `record_success` lifts a running suspension unconditionally:
  a stale mid-trial report can re-trip the breaker, and the trial's success
  must still close the pair.
- `ResolveRateLimiter.try_acquire_many` raises the same `ValueError` as the
  burst check when `slots` exceeds `sustained_max_calls` (it used to raise
  `IndexError` from an empty sustained window).
- `ResolveRateLimiter` rejects `window_sec <= 0` with `ValueError` — a
  non-positive window silently disabled the limiter.
- `flood_wait_remaining_seconds` ceils fractional remaining time: a flood
  expiring in 0.6s now classifies as transient (remaining=1) instead of
  collapsing to 0 — neither transient nor blocking.
- `run_with_flood_wait_retry` accumulates the per-retry cost (wait + buffer)
  against the transient budget, so total actual sleep can no longer exceed
  `transient_wait_budget_sec`.
- `handle_flood_wait` supports synchronous `report_flood` hooks (the result
  is awaited only when awaitable) — a sync hook used to crash flood handling
  with `TypeError`.
- README: `sleep=` is documented for the gate helpers that actually accept it
  (`acquire`, `wrap_messages_iterator`); the `run_with_flood_wait*` helpers
  always use `asyncio.sleep`.

## 0.1.3 (2026-10-07)

Adaptive flood backoff wired end-to-end, gate observability, jitter everywhere.

- `handle_flood_wait`, `run_with_flood_wait` and `run_with_flood_wait_retry`
  accept `gate=` (structural `FloodReportingGate` protocol, exported): every
  handled FLOOD_WAIT is reported to the gate via `note_flood(phone, seconds)`,
  closing the adaptive-backoff loop — the gate only needs to be passed once
  at the call site. `pool.report_flood` semantics unchanged; if you wired a
  manual `pool.report_flood` hook to call `note_flood`, REMOVE that hook when
  passing `gate=` — keeping both double-counts events. The gate reports are
  a no-op unless the gate was constructed with `flood_backoff=True`.
- All category and peer specs ship non-zero default defer jitter: every spec
  without an explicit `jitter_sec` derives 5% of its window (3s on 60s
  windows, 1.5s on history's 30s, 15s on channel_lifecycle's 300s) through
  the single limiter mechanism; the tight 1.1s user peer keeps a deliberate
  0.15s. Consumer overrides derive the same default unless they pass an
  explicit `jitter_sec` — `jitter_sec=0` disables. Starting ratio —
  recalibrate against production samples in one place.

- `TelegramRateLimitGate.note_flood(phone, seconds)` — opt-in per-gate
  adaptive backoff (`flood_backoff=True`): every reported FLOOD_WAIT **above
  the transient threshold (60s)** doubles the defers the gate hands out for
  that phone until the events decay (1h default window, cap 8x, both
  configurable; `flood_backoff_cap < 1` raises instead of silently
  disabling). Routine ≤60s pacing floods — the sweep protocol working as
  designed — do not escalate; `seconds=None` (severity unknown) counts
  conservatively. The multiplier scales category AND per-peer defers; zero
  defers stay zero — backoff slows callers, it invents no refusals.
  Consumers wire it from their own flood reporting (e.g. a
  `pool.report_flood` hook); the layers stay decoupled. Unlike the
  docker-telethon-plus throttler this was ported from, bucket waits are
  scaled too, not just inter-call gaps.
- `TelegramRateLimitGate.snapshot(phone)` — live per-account state:
  per-category usage and specs, per-peer buckets (incl. defer jitter), the
  current flood multiplier and events-in-window. Event-loop-affine; feed
  for dashboards/health endpoints.
- `TelegramRateLimitGate(jitter_func=...)` — deterministic jitter injection
  threading to every category and peer limiter (mirrors `time_func`).
- `acquire()` sleeps a flood-multiplied defer in ≤30s slices
  (`ACQUIRE_SLEEP_CAP_SEC`) and re-checks on every wake, so `reset()` keeps
  working as the unblock escape hatch during long backoffs.
- `reset()` scoping tightened: `reset(phone, category=...)` clears only that
  category's peer buckets, and a category-scoped reset no longer wipes
  per-phone flood events (they have no category dimension).
- `SEND_PEER_USER_SPEC` gains 0.15s defer jitter so retried sends around the
  calibrated 1/1.1s boundary do not fire metronome-precisely (same lockstep
  rationale as `ResolveRateLimiter`'s jitter).
- `ResolveRateLimiter.used(phone)` — burst-window usage for observability;
  read-only (an unknown phone seeds no window entry).

## 0.1.2 (2026-10-07)

Shared async pacing plus the opt-in live-testing harness.

- `TokenBucket`: smooth-refill outgoing cap extracted from tg_messenger,
  retaining its burst, rate opt-out, injectable time/sleep and fair wait queue
- `TelegramRateLimitGate.acquire`: wait and re-acquire one category slot after
  every deferral; cancellation propagates without a new reservation
- `TelegramRateLimitGate.wrap_messages_iterator`: gate actual Telethon history,
  search and ID-fetch page RPCs without modifying the shared client; no changes
  to category defaults, per-peer policy, or the existing retry/error APIs

## 0.1.1 (2026-10-07)

Opt-in live-testing harness + sustained-volume tier for `ResolveRateLimiter`.

- `ResolveRateLimiter` gains optional `sustained_max_calls` /
  `sustained_window_sec` (second sliding window, same deque math) — caps
  accumulated call volume, not just per-minute bursts. Recommended companion
  to the 20/60s burst: 60 calls / 3600s. Production incident
  (tg_content_factory 2026-10-06): a cold collect of 623 channels fired
  20 resolves/min — every 60s burst window green — for 20+ minutes until
  Telegram answered `FLOOD_WAIT_49613`; the burst window alone cannot see
  accumulated volume.
- Both windows are checked first; timestamps are recorded in both only when
  both admit the call — a deferred call burns no slots (unlike composing two
  separate limiter instances).
- Sustained tier is opt-in per instance (`None` = 0.1.x behaviour);
  validation: params must come in a pair, sustained window cannot be
  narrower than the burst window; `reset()` clears both windows.
- `tests_live/` outside `testpaths`: gated live tests (`RUN_FLOODGATE_LIVE_TG=1`
  + `REAL_TG_*` env) — read-only peer-key classification over real dialogs and
  a bounded send test proving the per-peer 1/s bucket paces Saved-Messages
  traffic with no FloodWaitError
- `scripts/calibrate_send_limits.py`: manual raw-boundary probe with fixed
  interval points, hard budgets and auto-stop on the first flood
- offline invariants in CI: marker registration, default-run exclusion,
  no-secret-literals audit, full gate-policy matrix
- live smoke checks surface short flood waits, measure send-start spacing,
  and skip cleanup after a flood; an offline regression covers variable
  response latency and immediate/mid-run floods without contacting Telegram
- empirical quota guardrails: `history` 24 requests / 30s, user-peer sends
  1 / 1.1s, and channel/chat peer sends 16 / 60s, each with documented
  evidence and a conservative margin

## 0.1.0 (2026-08-31)

Initial release — 1:1 extraction of the flood stack from the tg_content_factory
project, plus per-peer send limits.

- `TelegramRateLimitGate`: proactive per-(phone, category) sliding windows
  plus per-peer send buckets `(phone, category, peer)` — independent from the
  category budget, with `peer_key()` extraction from Telethon entities
  (`peer.py`) and `TelegramPeerRateLimitedError`
- `FloodCircuitBreaker`: pybreaker-based suspension per (operation, phone)
- `ResolveRateLimiter`: sliding-window limiter for `auth.resolveUsername`
- flood-wait helpers: `run_with_flood_wait`, `run_with_flood_wait_retry`,
  transient/blocking classification, sleep helpers, `FloodWaitInfo`
