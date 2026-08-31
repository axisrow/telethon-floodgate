"""Circuit breaker that stops hammering a flood-waited (operation, phone) (#1330/#1368)."""

from __future__ import annotations

import pytest

from telethon_floodgate.flood_breaker import (
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
)

OP = "telegram_warm_dialog_cache"
PHONE = "+1234567890"


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _breaker(clock: _Clock, *, threshold: int = 3, cooldown: float = 300.0):
    return FloodCircuitBreaker(
        threshold=threshold, cooldown_seconds=cooldown, time_func=clock
    )


def test_suspension_is_a_rate_limit_error():
    """Existing "operation unavailable" handlers must catch this unchanged.

    Ten call sites already handle TelegramRateLimitedError by moving on. A
    sibling type would sail past all of them and crash the collection pass
    instead of being absorbed as a protective refusal.
    """
    from telethon_floodgate.rate_limit_gate import TelegramRateLimitedError

    assert issubclass(TelegramOperationSuspendedError, TelegramRateLimitedError)
    exc = TelegramOperationSuspendedError(OP, PHONE, 42.0)
    assert isinstance(exc, TelegramRateLimitedError)
    assert exc.phone == PHONE
    assert exc.retry_after_sec == 42.0
    # category carries the operation so existing log lines stay meaningful
    assert exc.category == OP


def test_below_threshold_stays_closed():
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(2):
        breaker.record_flood(OP, PHONE)
        breaker.check(OP, PHONE)  # must not raise


def test_opens_after_threshold_and_refuses_without_network():
    """The whole point: stop calling instead of hammering into a long ban."""
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    with pytest.raises(TelegramOperationSuspendedError) as exc:
        breaker.check(OP, PHONE)
    assert exc.value.operation == OP
    assert exc.value.phone == PHONE
    assert exc.value.retry_after_sec > 0


def test_cooldown_allows_exactly_one_trial_call():
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    clock.advance(301.0)
    breaker.check(OP, PHONE)  # half-open trial is let through

    # Trial succeeded -> closed again, subsequent calls flow.
    breaker.record_success(OP, PHONE)
    breaker.check(OP, PHONE)


def test_trial_call_in_flight_blocks_everyone_else():
    """Exactly ONE trial may be in flight, not one per coroutine.

    The trial spans an await (check -> Telegram call -> record_*). A second
    coroutine arriving in that window used to see HALF_OPEN rather than OPEN and
    return early from check() with no gate at all, so the entire backlog hit an
    account Telegram was actively throttling — the failure mode #955 hit on the
    notifier breaker.
    """
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    clock.advance(301.0)
    breaker.check(OP, PHONE)  # first caller claims the trial slot

    # Second caller, while the trial is still awaiting its Telegram call.
    with pytest.raises(TelegramOperationSuspendedError):
        breaker.check(OP, PHONE)

    # The trial reports back: the slot is released and the breaker decides.
    breaker.record_success(OP, PHONE)
    breaker.check(OP, PHONE)


def test_flooded_trial_reopens_and_a_later_trial_still_runs():
    """A flooded trial must reopen the breaker without wedging later trials.

    The in-flight slot is claimed by check() and released by record_success();
    a flooded trial reopens the breaker instead, and the next cooldown grants a
    fresh trial. This asserts the pair cannot deadlock the operation.
    """
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    clock.advance(301.0)
    breaker.check(OP, PHONE)
    breaker.record_flood(OP, PHONE)  # trial flooded -> breaker reopens

    with pytest.raises(TelegramOperationSuspendedError):
        breaker.check(OP, PHONE)

    # A later cooldown grants a fresh trial, which succeeds and closes it.
    clock.advance(301.0)
    breaker.check(OP, PHONE)
    breaker.record_success(OP, PHONE)
    breaker.check(OP, PHONE)


def test_failed_trial_reopens_the_breaker():
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    clock.advance(301.0)
    breaker.check(OP, PHONE)
    breaker.record_flood(OP, PHONE)  # trial flooded again

    with pytest.raises(TelegramOperationSuspendedError):
        breaker.check(OP, PHONE)


def test_suspension_is_scoped_to_one_operation_and_one_phone():
    """A broken warm-up on one account must not stop everything else."""
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)

    with pytest.raises(TelegramOperationSuspendedError):
        breaker.check(OP, PHONE)

    breaker.check(OP, "+9999999999")  # same operation, other account
    breaker.check("telegram_stream_messages", PHONE)  # other operation, same account


def test_success_clears_accumulated_floods():
    clock = _Clock()
    breaker = _breaker(clock)
    breaker.record_flood(OP, PHONE)
    breaker.record_flood(OP, PHONE)
    breaker.record_success(OP, PHONE)

    # Counter reset: two more floods must not reach the threshold of three.
    breaker.record_flood(OP, PHONE)
    breaker.record_flood(OP, PHONE)
    breaker.check(OP, PHONE)


def test_unattributed_calls_are_never_suspended():
    """A call with no phone cannot be charged to an account."""
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(5):
        breaker.record_flood(OP, None)
    breaker.check(OP, None)


def test_non_positive_cooldown_is_clamped():
    """A zero cooldown would expire instantly and make the breaker a no-op."""
    clock = _Clock()
    breaker = FloodCircuitBreaker(threshold=1, cooldown_seconds=0, time_func=clock)
    breaker.record_flood(OP, PHONE)
    with pytest.raises(TelegramOperationSuspendedError):
        breaker.check(OP, PHONE)


def test_reset_clears_state():
    clock = _Clock()
    breaker = _breaker(clock)
    for _ in range(3):
        breaker.record_flood(OP, PHONE)
    breaker.reset(operation=OP, phone=PHONE)
    breaker.check(OP, PHONE)


