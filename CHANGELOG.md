# Changelog

## 0.1.0 (unreleased)

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

## 0.1.1 (unreleased)

Shared async pacing plus the opt-in live-testing harness.

- `TokenBucket`: smooth-refill outgoing cap extracted from tg_messenger,
  retaining its burst, rate opt-out, injectable time/sleep and fair wait queue
- `TelegramRateLimitGate.acquire`: wait and re-acquire one category slot after
  every deferral; cancellation propagates without a new reservation
- `TelegramRateLimitGate.wrap_messages_iterator`: gate actual Telethon history,
  search and ID-fetch page RPCs without modifying the shared client; no changes
  to category defaults, per-peer policy, or the existing retry/error APIs

- `tests_live/` outside `testpaths`: gated live tests (`RUN_FLOODGATE_LIVE_TG=1`
  + `REAL_TG_*` env) — read-only peer-key classification over real dialogs and
  a bounded send test proving the per-peer 1/s bucket paces Saved-Messages
  traffic with no FloodWaitError
- `scripts/calibrate_send_limits.py`: manual raw-boundary probe with fixed
  interval points, hard budgets and auto-stop on the first flood
- offline invariants in CI: marker registration, default-run exclusion,
  no-secret-literals audit, full gate-policy matrix
