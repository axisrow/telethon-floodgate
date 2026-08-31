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
