# telethon-floodgate

Flood-wait handling for Telethon user accounts: a proactive rate-limit gate, a
reactive circuit breaker, and flood-wait sleep/retry helpers — three layers
that keep a working account out of Telegram's multi-hour FLOOD_WAIT bans.

Telegram does not publish rate limits. This library is the production flood
stack of a multi-account Telegram content collector, calibrated against real
incidents: a fresh session firing 160+ `auth.resolveUsername` calls in seconds
escalated into 15–18 hour bans, and repeated `getDialogs` sweeps ran a account
into a 14.8-hour ban. The defaults are deliberately conservative and every
bucket is configurable.

## Install

```bash
pip install telethon-floodgate
```

Requires Python 3.11+, [Telethon](https://github.com/LonamiWebs/Telethon) 1.x,
pydantic 2.x and pybreaker.

## The three layers

### 1. Proactive rate-limit gate — before the call

```python
from telethon_floodgate import TelegramRateLimitGate, TelegramRateLimitedError

gate = TelegramRateLimitGate()          # create once, next to your client pool

def before_telegram_call(phone: str, operation: str) -> None:
    category = gate.category_for(operation)   # "send", "history", "dialogs", ...
    retry_after = gate.try_acquire(phone, category)
    if retry_after > 0:
        raise TelegramRateLimitedError(phone, category, retry_after)
```

Buckets are independent sliding windows keyed by `(phone, category)`:
`dialogs` (1/min — the #1330 incident), `dialog_sweep`, `history`,
`admin_action`, `send` (30/min), `channel_lifecycle` (3 per 5 min), plus a
permissive `default`. Override any of them:

```python
from telethon_floodgate import RateLimitSpec, TelegramRateLimitGate

gate = TelegramRateLimitGate(
    category_limits={"send": RateLimitSpec(max_calls=10, window_sec=60.0)}
)
```

`resolve` and `reaction` are intentionally no-op categories: in the origin
project those paths have dedicated limiters (see `ResolveRateLimiter`, also
shipped here).

### Per-peer send limits

Telegram throttles *sending* per peer, not just per account: roughly one
message per second to the same private chat and about twenty per minute into
the same group or channel. Pass a peer key to `try_acquire` and the gate
checks a second, independent `(phone, category, peer)` bucket before the
category one:

```python
from telethon_floodgate import peer_key

retry_after = gate.try_acquire(phone, "send", peer=peer_key(entity))
if retry_after > 0:
    raise TelegramPeerRateLimitedError(phone, peer_key(entity), retry_after)
```

- A per-peer refusal does **not** consume the account-wide category slot, so a
  burst aimed at one peer cannot burn the account budget.
- `peer_key()` derives `"user:123"` / `"channel:-100123"` / `"chat:-456"` /
  `"username:durov"` / `"id:123"` from ints, strings, Telethon TL peers, input
  peers and full entities — purely local attribute access, never a network
  round-trip.
- Only `send:user`, `send:channel` and `send:chat` are configured by default;
  unknown kinds pass through unthrottled. Override or disable via `peer_limits`
  (a `"send:user"` entry with a permissive spec effectively turns it off) and
  cap memory with `peer_max_buckets` (LRU, default 4096).

```python
gate = TelegramRateLimitGate(
    peer_limits={"send:user": RateLimitSpec(max_calls=1, window_sec=5.0)},
    peer_max_buckets=4096,
)
```

`TelegramPeerRateLimitedError` subclasses `TelegramRateLimitedError`, so one
"operation unavailable, move on" handler covers every layer.

### 2. Reactive circuit breaker — when Telegram answers anyway

```python
from telethon_floodgate import FloodCircuitBreaker

breaker = FloodCircuitBreaker(threshold=3, cooldown_seconds=300)

breaker.check(operation, phone)            # raises TelegramOperationSuspendedError
                                           # while the pair is suspended
try:
    await client.some_call()
    breaker.record_success(operation, phone)
except telethon.errors.FloodWaitError:
    breaker.record_flood(operation, phone)
```

After `threshold` flood waits on the same `(operation, phone)` the pair is
suspended for `cooldown_seconds`, then exactly one half-open trial call is
allowed. `TelegramOperationSuspendedError` subclasses
`TelegramRateLimitedError`, so a single "operation unavailable, move on"
handler covers both layers.

### 3. Flood-wait helpers — classify, sleep, retry, report

```python
from telethon_floodgate import (
    HandledFloodWaitError, FloodWaitInfo,
    handle_flood_wait, run_with_flood_wait, run_with_flood_wait_retry,
    is_transient_flood_wait_seconds, is_blocking_flood_wait_until,
)

# wraps one awaitable: FloodWaitError -> HandledFloodWaitError (+ pool report)
await run_with_flood_wait(client.get_dialogs(), operation="warm", phone=phone, pool=pool)

# retries transient waits (<=60s) under a total budget (120s default)
await run_with_flood_wait_retry(factory, operation="fetch", phone=phone, pool=pool)
```

`handle_flood_wait` reports the wait back to the pool via
`await pool.report_flood(phone, seconds)` so account rotation can skip the
flooded account — persist that to your own storage; the library is
storage-agnostic.

## Design notes

- Everything runs on one event loop; the limiter state is plain in-memory
  deques, no locks, no DB.
- `try_acquire` never sleeps and never raises — it returns seconds to defer,
  so callers decide whether to reschedule, skip or await.
- Atomic multi-slot reservations (`try_acquire(..., slots=n)`) are supported
  for compound operations.

## Quota evidence

Telegram does not publish a complete numeric FLOOD_WAIT table. The gate uses
conservative guardrails from the sources below and keeps every value
configurable.

| Category | Configured guardrail | Evidence |
| --- | --- | --- |
| `history` (`messages.getHistory`) | 24 requests / 30s | A live probe reached FLOOD_WAIT on request 31 after 30 requests; the page size (1 vs 100) did not change the request threshold. Independent empirical notes report about 30 requests / 30s ([copy-history-bot-2](https://github.com/code29563/copy-history-bot-2#switching-between-clients-and-handling-floodwaits)). |
| `send:user` | 1 / 1.1s | Community guidance is about one message per second to one private peer; the live Saved Messages test passed at about 1s, and the guardrail adds margin. |
| `send:channel`, `send:chat` | 16 / 60s | Community guidance is about 20 messages per minute in one group/channel ([Telegram Limits](https://limits.tginfo.me/en)); the guardrail keeps a margin. |
| account-wide `send` and other categories | Existing values | No repeatable public numeric quota was found for these method families; they remain internal conservative guardrails. |

The history value limits each actual `GetHistoryRequest`. A consumer must
call the gate for every page request, not once for an entire logical export.
These values describe observed behavior for a method/account shape; they are
not Telegram's universal quotas. Telegram's official error reference only
defines `FLOOD_WAIT_X` as exceeding the allowed attempts for a method and its
parameters ([API errors](https://core.telegram.org/api/errors#420-flood)).

## License

MIT — see [LICENSE](LICENSE). Unofficial third-party library; not affiliated
with the Telethon project.

## Live testing

The offline suite (default `pytest`, CI) never touches Telegram. Live checks
are a separate, explicit activity, in four levels:

- **Level 0 — consumer suite (free):** every project using this package
  exercises it through its own real-Telegram test rig.
- **Level 1 — `tests_live/` (opt-in):** outside `testpaths`, so the default
  run never even collects them. Gated by `RUN_FLOODGATE_LIVE_TG=1` plus the
  account environment:
  ```bash
  export REAL_TG_API_ID=... REAL_TG_API_HASH=... REAL_TG_PHONE=... REAL_TG_SESSION=...
  RUN_FLOODGATE_LIVE_TG=1 python -m pytest tests_live -x -v -s
  ```
  With the gate closed every test skips; with the gate open but the account
  env missing they fail loudly naming the variables. `test_live_peer_keys`
  (read-only) classifies every real `get_dialogs()` entity and fails on any
  UNKNOWN kind or malformed key; `test_live_send_gate` sends 5 messages to
  Saved Messages through the real gate and proves the 1.1s user-peer pacing
  engages with no FloodWaitError. Automatic flood sleeping is disabled, so
  even short waits fail the run. Pacing is measured between send starts,
  independently of response latency. Successful runs delete their probes;
  on a flood the test stops without cleanup and prints the probe IDs to
  delete after the reported wait. A cleanup failure also fails the test.
- **Level 2 — calibration (manual):** `scripts/calibrate_send_limits.py`
  probes the raw boundary without our gate — fixed interval points, hard
  budgets (`--max-messages`, `--max-seconds`), auto-stop on the first flood
  (which is the measurement), JSON report with a verdict against
  `SEND_PEER_USER_SPEC`:
  ```bash
  RUN_FLOODGATE_LIVE_TG=1 python scripts/calibrate_send_limits.py
  ```
- **Level 3 — artifact smoke (per release):** install the published wheel
  into a clean venv and run the level-1 read-only test against it.

Getting a session string from a tg_content_factory installation:

```bash
python -m src.main account export-session --phone +7... [--json]
```

The session string grants **full access** to the account — pass it via the
environment only. Safety rules: use a disposable account, send only to
Saved Messages (`"me"`), respect the budgets, and expect every live run to
be a conscious decision. pytest does not read `.env` itself; use
`set -a; source .env; set +a` if you keep the variables there.
