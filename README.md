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

## License

MIT — see [LICENSE](LICENSE). Unofficial third-party library; not affiliated
with the Telethon project.
