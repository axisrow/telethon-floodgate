# Changelog

## 0.2.0 (unreleased)

Adaptive flood backoff, gate observability, peer-pacing jitter.

- `TelegramRateLimitGate.note_flood(phone, seconds)` — opt-in per-gate
  adaptive backoff (`flood_backoff=True`): every reported FLOOD_WAIT doubles
  the defers the gate hands out for that phone until the events decay
  (1h default window, cap 8x by default, both configurable). The multiplier
  scales category AND per-peer defers; zero defers stay zero — backoff slows
  callers, it invents no refusals. Consumers wire it from their own flood
  reporting (e.g. a `pool.report_flood` hook); the layers stay decoupled.
  Unlike the docker-telethon-plus throttler this was ported from, bucket
  waits are scaled too, not just inter-call gaps.
- `TelegramRateLimitGate.snapshot(phone)` — live per-account state:
  per-category usage and specs, per-peer buckets, current flood multiplier
  and events-in-window. Feed for dashboards/health endpoints.
- `SEND_PEER_USER_SPEC` gains 0.15s defer jitter so retried sends around the
  calibrated 1/1.1s boundary do not fire metronome-precisely (same lockstep
  rationale as `ResolveRateLimiter`'s jitter).
- `ResolveRateLimiter.used(phone)` — current-window usage for observability.

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
