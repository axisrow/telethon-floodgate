"""The refusal hierarchy contract (issue #11).

Every refusal the package raises must be a ``TelegramRateLimitedError``
subclass, so the single consumer handler ``except TelegramRateLimitedError:
operation unavailable, move on`` covers the gate, per-peer buckets, the
breaker and the username-resolve limiter alike — and legacy import paths and
error attributes stay byte-for-byte compatible.
"""

from datetime import datetime, timedelta, timezone

from telethon_floodgate import (
    TelegramOperationSuspendedError,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)
from telethon_floodgate.rate_limit_gate import (
    TelegramRateLimitedError as TelegramRateLimitedErrorFromGate,
)


def _resolved_limited(now: datetime) -> UsernameResolveRateLimitedError:
    return UsernameResolveRateLimitedError("+15551234567", 4.2, now=now)


def _resolve_deferred() -> UsernameResolveFloodWaitDeferredError:
    return UsernameResolveFloodWaitDeferredError(
        3600, datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)
    )


def test_username_resolve_errors_subclass_the_refusal_root():
    assert issubclass(UsernameResolveRateLimitedError, TelegramRateLimitedError)
    assert issubclass(UsernameResolveFloodWaitDeferredError, TelegramRateLimitedError)


def test_username_resolve_errors_keep_legacy_attributes_and_messages():
    now = datetime(2026, 10, 7, 11, 59, 0, tzinfo=timezone.utc)
    limited = _resolved_limited(now)
    # Legacy attributes preserved (retry_after_sec stays the pre-ceil float).
    assert limited.phone == "+15551234567"
    assert limited.retry_after_sec == 4.2
    assert limited.retry_after_seconds == 5
    assert limited.next_available_at == now + timedelta(seconds=5)
    assert limited.run_after_with_buffer() == now + timedelta(seconds=10)
    assert str(limited) == (
        "resolve_username rate-limited for +15551234567; retry in 5s"
    )

    deferred = _resolve_deferred()
    assert deferred.wait_seconds == 3600
    assert deferred.next_available_at == datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert str(deferred) == (
        "Username resolve is flood-waited until 2026-10-07T12:00:00+00:00 (retry in 3600s)"
    )


def test_username_resolve_errors_carry_the_base_contract():
    limited = _resolved_limited(datetime(2026, 10, 7, 11, 59, 0, tzinfo=timezone.utc))
    assert limited.category == "resolve_username"
    deferred = _resolve_deferred()
    assert deferred.category == "resolve_username"
    assert deferred.phone == ""  # constructed without an account
    assert deferred.retry_after_sec == 3600.0


def test_single_consumer_handler_covers_every_refusal_layer():
    for err in (
        TelegramPeerRateLimitedError("+15551234567", "user:1", 1.5),
        TelegramOperationSuspendedError("telegram_send_message", "+15551234567", 900.0),
        _resolved_limited(datetime(2026, 10, 7, 11, 59, 0, tzinfo=timezone.utc)),
        _resolve_deferred(),
    ):
        try:
            raise err
        except TelegramRateLimitedError as caught:  # the one "move on" handler
            assert caught is err
        else:
            raise AssertionError(f"{type(err).__name__} escaped the refusal handler")


def test_legacy_gate_import_path_is_unchanged():
    assert TelegramRateLimitedErrorFromGate is TelegramRateLimitedError
