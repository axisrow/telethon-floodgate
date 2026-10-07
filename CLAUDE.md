# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`telethon-floodgate` — a small typed Python library (src-layout, package `telethon_floodgate`) extracted 1:1 from the production flood stack of the `tg_content_factory` project. Python 3.11+, deps: telethon 1.x, pydantic 2, pybreaker. Telegram publishes no rate quotas; every default here is calibrated against real production incidents and must stay configurable.

## Commands

```bash
pip install -e ".[dev]"            # dev install (local venv: .venv/)

python -m pytest -q                # offline suite (tests/ only — 136 tests, never touches Telegram)
python -m pytest tests/test_peer.py -q          # single file
python -m pytest tests/test_peer.py -k name -q  # single test

ruff check src tests tests_live scripts         # lint (E,F,I,N,W; line-length 120; py311)
python -m build && python -m twine check dist/* # release build check
```

CI (`.github/workflows/ci.yml`): ruff + build/twine on 3.11; pytest matrix on 3.11/3.12/3.13.

### Live Telegram tests (explicit opt-in, never in CI/defaults)

`tests_live/` is deliberately outside `testpaths` — the default run doesn't even collect it. Two gates: `RUN_FLOODGATE_LIVE_TG=1` (exactly `1`) plus the account env `REAL_TG_API_ID`, `REAL_TG_API_HASH`, `REAL_TG_PHONE`, `REAL_TG_SESSION`. Gate closed → tests skip; gate open but env missing → they fail loudly naming the variables. pytest does not read `.env`; use `set -a; source .env; set +a`.

```bash
set -a; source .env; set +a
RUN_FLOODGATE_LIVE_TG=1 pytest tests_live -s            # level 1
RUN_FLOODGATE_LIVE_TG=1 python scripts/calibrate_send_limits.py   # level 2: raw-boundary probe
```

Safety contract for anything live: disposable account, sends only to Saved Messages (`"me"`), respect `--max-messages`/`--max-seconds` budgets, secrets via environment only. Markers: `live_tg_ro` (read-only) / `live_tg_send`. `tests/test_live_policy_static.py` enforces the harness invariants offline (marker registration, testpaths exclusion, no `REAL_TG_*` literals, full gate-policy matrix) — any change to the live harness must keep those green.

## Architecture

Three defensive layers against FLOOD_WAIT, all in-memory, single event loop, no locks, no DB:

1. **Proactive gate — before the call** (`rate_limit_gate.py`): `TelegramRateLimitGate` maps operation tags → categories (`_OPERATION_CATEGORIES`, with suffix-matching fallback for decorated tags) → one independent `ResolveRateLimiter` sliding window per category, keyed by phone. For sends, a second independent bucket per `(phone, category, peer)`, specs keyed `"<category>:<kind>"` (e.g. `send:user`), LRU-bounded by `peer_max_buckets`. `try_acquire` never sleeps and never raises — it returns seconds to defer; a per-peer refusal leaves the account-wide category slot untouched. `resolve` and `reaction` are intentionally no-op categories (owned by dedicated limiters elsewhere). Optional adaptive backoff: with `flood_backoff=True`, `note_flood(phone, seconds)` doubles defers per reported blocking FLOOD_WAIT — ×2^n, 1h decay, cap 8× — across category and peer defers; `handle_flood_wait(..., gate=...)` closes that loop automatically (pass the gate at the call site, don't also call `note_flood` from a pool hook — that double-counts), and `snapshot(phone)` exposes live per-account usage. All category/peer defers carry spec-level defer jitter (`jitter_sec`, sized relative to the window, starting values — calibrate on production). Both are additive and sleep-free.
2. **Reactive circuit breaker — when Telegram answers with a flood anyway** (`flood_breaker.py`): pybreaker per `(operation, phone)`. pybreaker is synchronous, so the async Telegram call runs outside `CircuitBreaker.call`; the breaker is advanced afterwards via `check()` → call → `record_success()`/`record_flood()`. The half-open state grants exactly one trial, tracked in `_probe_in_flight` because the trial spans an `await`.
3. **Flood-wait helpers — classify, sleep, retry, report** (`flood_wait.py`): transient (≤60s) vs blocking classification, `run_with_flood_wait` (wrap one awaitable → `HandledFloodWaitError`), `run_with_flood_wait_retry` (retry transient waits under a 120s total budget), `handle_flood_wait` reporting back via `await pool.report_flood(phone, seconds)` — pool/storage persistence belongs to the consumer, not this library.

Supporting modules: `rate_limiter.py` (`ResolveRateLimiter` — the sliding-window primitive reused for every gate bucket, plus the standalone username-resolve path), `peer.py` (`peer_key()` — purely local attribute access over Telethon TL peers/input peers/entities, never a network round-trip; duck-typed fallback for test doubles), `_datetime.py` (vendored internal UTC helpers, not public API).

### Invariants to preserve when changing code

- **Error hierarchy is load-bearing**: `TelegramPeerRateLimitedError` and `TelegramOperationSuspendedError` subclass `TelegramRateLimitedError` so a single "operation unavailable, move on" handler in consumers covers every layer. Never introduce a sibling error type for a refusal.
- `try_acquire` returning-a-deferral (not sleeping/raising) is the API contract; callers decide to skip, reschedule or await.
- Atomic multi-slot reservations (`try_acquire(..., slots=n)`) must wait for enough window entries to expire for the whole request, not just the oldest.
- Defaults are conservative on purpose (e.g. `dialogs` 1/min traces to the #1330 incident; `dialog_sweep` must leave room for resumptions after Telegram's pacing floods — see comments). Don't loosen a spec without production evidence.
- Concurrency model: one event loop, plain deques/OrderedDicts, no locks. Keep it that way.
- Public API is the `__init__.py` `__all__`; the package ships `py.typed`.

## Conventions

- Comments explain *why* with incident references (`#1330`, `#551`, `#955`…) from the origin project `tg_content_factory` — match that style; don't strip the rationale.
- Tests inject clocks via `time_func` / fake `pool` objects rather than sleeping; `filterwarnings = ["error"]` means any warning (e.g. an unregistered pytest marker) fails the suite.
- pytest-asyncio `auto` mode; fixture loop scope is `function` (the live `live_telegram` fixture relies on that).
- Version lives in both `pyproject.toml` and `__init__.py`; update `CHANGELOG.md` per change (entries accumulate under unreleased versions).
- Live `test_live_send_gate` sends 5 messages to Saved Messages through the real gate and asserts the 1/s user-peer pacing engages with no `FloodWaitError`.
