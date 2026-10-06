"""telethon_floodgate — flood-wait handling for Telethon user accounts.

Three layers of FLOOD_WAIT defence, extracted from a production Telegram
content collector:

- proactive rate-limit gate: sliding-window buckets per (phone, category)
  and, for sends, per (phone, category, peer) — checked *before* a Telegram
  call is made (``TelegramRateLimitGate``);
- reactive circuit breaker: suspends an (operation, phone) pair that keeps
  drawing flood waits instead of hammering into a long ban
  (``FloodCircuitBreaker``);
- flood-wait helpers: classify transient vs blocking waits, sleep/retry with
  a bounded budget, and report waits back to the account pool
  (``run_with_flood_wait``, ``run_with_flood_wait_retry``).

Telegram publishes no quotas; the bundled defaults are calibrated against
observed production behaviour and are configurable.
"""
from telethon_floodgate.flood_breaker import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_FLOOD_THRESHOLD,
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
)
from telethon_floodgate.flood_wait import (
    FLOOD_WAIT_RETRY_BUFFER_SEC,
    TRANSIENT_FLOOD_WAIT_MAX_SEC,
    TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC,
    FloodWaitInfo,
    HandledFloodWaitError,
    coerce_flood_wait_seconds,
    flood_wait_remaining_seconds,
    format_flood_wait_detail,
    handle_flood_wait,
    is_blocking_flood_wait_until,
    is_transient_flood_wait_seconds,
    is_transient_flood_wait_until,
    run_with_flood_wait,
    run_with_flood_wait_retry,
    sleep_for_flood_wait_seconds,
    sleep_for_handled_flood_wait,
)
from telethon_floodgate.peer import (
    PeerKind,
    peer_key,
    peer_kind_and_key,
)
from telethon_floodgate.rate_limit_gate import (
    RateLimitSpec,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)
from telethon_floodgate.rate_limiter import (
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
    ResolveRateLimiter,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)
from telethon_floodgate.token_bucket import TokenBucket

__version__ = "0.1.2"

__all__ = [
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_FLOOD_THRESHOLD",
    "FLOOD_WAIT_RETRY_BUFFER_SEC",
    "FloodCircuitBreaker",
    "FloodWaitInfo",
    "GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC",
    "HandledFloodWaitError",
    "PeerKind",
    "RESOLVE_USERNAME_BACKOFF_BUFFER_SEC",
    "ResolveRateLimiter",
    "RateLimitSpec",
    "TelegramOperationSuspendedError",
    "TelegramPeerRateLimitedError",
    "TelegramRateLimitedError",
    "TelegramRateLimitGate",
    "TokenBucket",
    "TRANSIENT_FLOOD_WAIT_MAX_SEC",
    "TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC",
    "UsernameResolveFloodWaitDeferredError",
    "UsernameResolveRateLimitedError",
    "coerce_flood_wait_seconds",
    "flood_wait_remaining_seconds",
    "format_flood_wait_detail",
    "handle_flood_wait",
    "is_blocking_flood_wait_until",
    "is_transient_flood_wait_seconds",
    "is_transient_flood_wait_until",
    "peer_key",
    "peer_kind_and_key",
    "run_with_flood_wait",
    "run_with_flood_wait_retry",
    "sleep_for_flood_wait_seconds",
    "sleep_for_handled_flood_wait",
]
