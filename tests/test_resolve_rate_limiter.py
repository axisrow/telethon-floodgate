"""Unit tests for the per-account resolve_username rate limiter (#551)."""
from __future__ import annotations

import pytest

from telethon_floodgate.rate_limiter import ResolveRateLimiter


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _limiter(clock: _FakeClock, *, max_calls: int = 3, window: float = 60.0, jitter: float = 0.0):
    return ResolveRateLimiter(
        max_calls=max_calls,
        window_sec=window,
        jitter_sec=jitter,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )


def test_allows_up_to_max_then_defers():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=3, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") == 0.0
    # Fourth call within the window is throttled.
    retry = limiter.try_acquire("+1")
    assert retry > 0.0
    # Defer until the oldest call (t=1000) exits the 60s window.
    assert retry == 60.0


def test_window_slides_and_frees_slots():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=2, window=60.0)

    assert limiter.try_acquire("+1") == 0.0  # t=1000
    clock.advance(30)
    assert limiter.try_acquire("+1") == 0.0  # t=1030
    # Window full now.
    assert limiter.try_acquire("+1") > 0.0
    # Advance past the first call's expiry (1000 + 60 = 1060).
    clock.advance(31)  # t=1061
    assert limiter.try_acquire("+1") == 0.0


def test_multi_slot_retry_waits_until_all_required_slots_are_free():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=3, window=300.0)

    assert limiter.try_acquire("+1") == 0.0  # t=1000
    clock.advance(100)
    assert limiter.try_acquire("+1") == 0.0  # t=1100
    clock.advance(100)
    assert limiter.try_acquire("+1") == 0.0  # t=1200
    clock.advance(50)

    # Two calls must expire (at t=1300 and t=1400), so retrying after only
    # the oldest call expires would still be rejected.
    assert limiter.try_acquire_many("+1", 2) == 150.0
    clock.advance(150)
    assert limiter.try_acquire_many("+1", 2) == 0.0


def test_limit_is_per_account():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=1, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0  # +1 throttled
    # A different account has its own independent window.
    assert limiter.try_acquire("+2") == 0.0


def test_jitter_added_to_retry():
    clock = _FakeClock()
    limiter = ResolveRateLimiter(
        max_calls=1,
        window_sec=60.0,
        jitter_sec=5.0,
        time_func=clock,
        jitter_func=lambda _a, b: b,  # always max jitter
    )
    assert limiter.try_acquire("+1") == 0.0
    retry = limiter.try_acquire("+1")
    assert retry == 65.0  # 60s window + 5s jitter


def test_reset_clears_history():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=1, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0
    limiter.reset("+1")
    assert limiter.try_acquire("+1") == 0.0

    # reset() with no arg clears all accounts.
    limiter.try_acquire("+2")
    limiter.reset()
    assert limiter.try_acquire("+2") == 0.0


class _TieredFakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _tiered(clock, *, burst_calls=20, sustained_calls=60):
    return ResolveRateLimiter(
        max_calls=burst_calls,
        window_sec=60.0,
        jitter_sec=0.0,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
        sustained_max_calls=sustained_calls,
        sustained_window_sec=3600.0,
    )


def test_sustained_tier_caps_accumulated_volume():
    """Sustained tier caps volume that the burst window admits legally.

    Production incident (tg_content_factory 2026-10-06): a cold collect of
    623 channels fired 20 resolves/min — every 60s burst window green — for
    20+ minutes until Telegram answered FLOOD_WAIT 49613s. 180 minutes of
    the same pattern must now yield at most one sustained window per hour.
    """
    clock = _TieredFakeClock()
    limiter = _tiered(clock)

    allowed = 0
    for _ in range(180):  # 3 hours, 20 "legal" calls per minute
        for _ in range(20):
            if limiter.try_acquire("+1") == 0.0:
                allowed += 1
        clock.now += 60.0

    assert allowed <= 180


def test_without_sustained_tier_volume_is_uncapped():
    """Back-compat: no sustained params -> 0.1.x behaviour (burst only)."""
    clock = _TieredFakeClock()
    limiter = ResolveRateLimiter(
        max_calls=20,
        window_sec=60.0,
        jitter_sec=0.0,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )

    allowed = 0
    for _ in range(180):
        for _ in range(20):
            if limiter.try_acquire("+1") == 0.0:
                allowed += 1
        clock.now += 60.0

    assert allowed == 3600


def test_sustained_window_slides():
    """After an hour the sustained budget is fully restored."""
    clock = _TieredFakeClock()
    # Wide burst window: isolate the sustained tier's behaviour.
    limiter = _tiered(clock, burst_calls=60)

    for _ in range(60):
        assert limiter.try_acquire("+1") == 0.0
    # Burst window is green again, but the sustained hour is exhausted.
    clock.now += 60.0
    assert limiter.try_acquire("+1") > 0.0
    # Sustained calls made at t=0 leave the sustained window at t=3600.
    clock.now = 3600.0
    assert limiter.try_acquire("+1") == 0.0


def test_sustained_tier_is_per_account():
    clock = _TieredFakeClock()
    limiter = _tiered(clock, sustained_calls=1)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0
    assert limiter.try_acquire("+2") == 0.0


def test_sustained_params_must_come_in_pair():
    with pytest.raises(ValueError):
        ResolveRateLimiter(sustained_max_calls=60)
    with pytest.raises(ValueError):
        ResolveRateLimiter(sustained_window_sec=3600.0)


def test_sustained_window_cannot_be_narrower_than_burst():
    with pytest.raises(ValueError):
        ResolveRateLimiter(sustained_max_calls=60, sustained_window_sec=30.0)


def test_reset_clears_sustained_window():
    clock = _TieredFakeClock()
    limiter = _tiered(clock, sustained_calls=1)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0
    limiter.reset("+1")
    assert limiter.try_acquire("+1") == 0.0


def test_slots_above_max_calls_defer_without_raising():
    """Issue #7: an unfulfillable request defers, it does not raise.

    The contract is "try_acquire never sleeps and never raises" — slots above
    ``max_calls`` used to break it with ValueError. No wait can make a window
    hold more than ``max_calls`` entries, so the defer is a state-independent
    full-window cadence that repeats until the caller splits the request, and
    the refused ask reserves nothing.
    """
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=3, window=60.0)

    assert limiter.try_acquire_many("+1", 4) == 60.0
    assert limiter.used("+1") == 0  # a deferral reserves nothing
    assert limiter.peek_many("+1", 4) == 60.0  # the peek half agrees
    # Window state is irrelevant: empty or busy, the cadence defer repeats.
    clock.advance(60.0)
    assert limiter.try_acquire_many("+1", 4) == 60.0
    # Satisfiable requests stay untouched by the refused oversized ones.
    assert limiter.try_acquire_many("+1", 2) == 0.0


def test_slots_above_sustained_max_defer_instead_of_raising():
    """Issue #7: slots above a tier cap defer at the wider tier's cadence.

    The old ValueError (itself a fix for an IndexError on an empty window)
    broke the never-raises contract. The sustained window is the wider tier,
    so its cadence wins the max() against the burst cadence."""
    limiter = _tiered(_TieredFakeClock(), sustained_calls=5)
    assert limiter.try_acquire_many("+1", 6) == 3600.0
    assert limiter.used("+1") == 0


def test_record_many_still_rejects_slots_above_tier_caps():
    """Commit-side guard stays loud (#7): record_many commits a peek-admitted
    request, so an oversized one there is a programming error like slots<1 —
    recording it would widen the window and corrupt every later defer."""
    limiter = _tiered(_TieredFakeClock(), sustained_calls=5)
    with pytest.raises(ValueError, match="max_calls"):
        limiter.record_many("+1", 21)  # above the burst cap of 20
    with pytest.raises(ValueError, match="sustained_max_calls"):
        limiter.record_many("+1", 6)  # above the sustained cap of 5


def test_non_positive_window_is_rejected():
    """A non-positive window prunes every entry instantly: a silent no-op."""
    with pytest.raises(ValueError, match="window_sec"):
        ResolveRateLimiter(max_calls=1, window_sec=0)


def test_peek_many_defers_without_recording():
    """peek_many is read-only: the same defer try_acquire_many would return."""
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=1, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.peek_many("+1", 1) == 60.0
    assert limiter.used("+1") == 1  # unchanged
    clock.advance(60)
    assert limiter.peek_many("+1", 1) == 0.0  # would admit...
    assert limiter.used("+1") == 0  # ...but still recorded nothing


def test_peek_then_record_equals_try_acquire():
    """The gate's two-phase pair composes to the same state as one acquire."""
    clock = _FakeClock()
    two_phase = _limiter(clock, max_calls=2, window=60.0)
    single_phase = _limiter(clock, max_calls=2, window=60.0)

    assert two_phase.peek_many("+1", 1) == 0.0
    two_phase.record_many("+1", 1)
    assert single_phase.try_acquire("+1") == 0.0

    clock.advance(30)
    assert two_phase.try_acquire("+1") == 0.0
    assert single_phase.peek_many("+1", 1) == 0.0
    single_phase.record_many("+1", 1)

    # Identical history: both full, identical honest defer.
    assert two_phase.used("+1") == single_phase.used("+1") == 2
    assert two_phase.peek_many("+1", 1) == single_phase.peek_many("+1", 1) == 30.0


def test_record_many_fills_both_windows_like_acquire():
    clock = _TieredFakeClock()
    limiter = _tiered(clock, sustained_calls=2)

    limiter.record_many("+1", 2)
    # Both windows are now full: the next call defers on the sustained tier.
    assert limiter.try_acquire("+1") > 0.0
    clock.now += 60  # burst freed, sustained entries still hold the window
    assert limiter.peek_many("+1", 1) > 0.0
    clock.now += 3540  # t=3600: the sustained window drained too
    assert limiter.try_acquire("+1") == 0.0
