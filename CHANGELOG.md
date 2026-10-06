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

Dev-only: opt-in live-testing harness (no runtime changes).

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
