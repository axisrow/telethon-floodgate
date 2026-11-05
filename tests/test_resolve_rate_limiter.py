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
    """Explicit opt-out (sustained_max_calls=None) -> 0.1.x burst-only.

    Since #26 the ABSENCE of sustained params means the 60/3600s default, so
    the legacy burst-only behaviour requires the explicit None pair."""
    clock = _TieredFakeClock()
    limiter = ResolveRateLimiter(
        max_calls=20,
        window_sec=60.0,
        jitter_sec=0.0,
        sustained_max_calls=None,
        sustained_window_sec=None,
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
    """#26 defaults fill both params; the pair rule guards EXPLICIT partial sets."""
    with pytest.raises(ValueError):
        ResolveRateLimiter(sustained_max_calls=60, sustained_window_sec=None)
    with pytest.raises(ValueError):
        ResolveRateLimiter(sustained_max_calls=None, sustained_window_sec=3600.0)


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


def _default_limiter(clock: _FakeClock):
    return ResolveRateLimiter(
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )


def test_sustained_tier_is_on_by_default():
    """#26: the tier that was opt-in never turned itself on — a second 18h
    FLOOD_WAIT proved it. Defaults now carry the 60/3600s sustained budget."""
    clock = _FakeClock()
    limiter = _default_limiter(clock)
    # Walk one call every 3.1s: the 20/60s burst window never fills, so any
    # deferral past call #60 can only come from the default sustained tier.
    for _ in range(60):
        assert limiter.try_acquire("+1") == 0.0
        clock.advance(3.1)
    assert limiter.try_acquire("+1") > 0.0


def test_explicit_none_still_disables_sustained_tier():
    """0.1.x behaviour stays reachable for callers that deliberately opt out."""
    clock = _FakeClock()
    limiter = ResolveRateLimiter(
        sustained_max_calls=None,
        sustained_window_sec=None,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )
    for _ in range(100):
        assert limiter.try_acquire("+1") == 0.0
        clock.advance(3.1)


def test_tighten_shrinks_sustained_budget_until_expiry():
    clock = _FakeClock()
    limiter = _default_limiter(clock)
    assert limiter.tighten("+1") == 30  # 60 * 0.5, factory default factor

    for _ in range(30):
        assert limiter.try_acquire("+1") == 0.0
        clock.advance(3.1)
    # The tightened cap is exhausted long before the default 60 would be.
    assert limiter.try_acquire("+1") > 0.0

    # After the tightening duration the configured budget returns (and the
    # 4h slide has pruned the whole hour-long window).
    clock.advance(4 * 3600)
    assert limiter.try_acquire("+1") == 0.0


def test_tighten_is_per_account():
    clock = _FakeClock()
    limiter = _default_limiter(clock)
    limiter.tighten("+1")
    # A tightened phone must not shrink anybody else's budget.
    for _ in range(60):
        assert limiter.try_acquire("+2") == 0.0
        clock.advance(3.1)
    assert limiter.try_acquire("+2") > 0.0  # default sustained cap for +2
    for _ in range(30):
        assert limiter.try_acquire("+1") == 0.0
        clock.advance(3.1)
    assert limiter.try_acquire("+1") > 0.0  # tightened cap for +1


def test_tighten_rejects_degenerate_arguments():
    limiter = _default_limiter(_FakeClock())
    with pytest.raises(ValueError, match="factor"):
        limiter.tighten("+1", factor=0)
    with pytest.raises(ValueError, match="factor"):
        limiter.tighten("+1", factor=1.5)
    with pytest.raises(ValueError, match="duration_sec"):
        limiter.tighten("+1", duration_sec=0)


def test_tighten_without_sustained_tier_is_a_no_op():
    limiter = ResolveRateLimiter(
        sustained_max_calls=None,
        sustained_window_sec=None,
        time_func=_FakeClock(),
        jitter_func=lambda _a, _b: 0.0,
    )
    assert limiter.tighten("+1") is None


def test_snapshot_restore_keeps_volume_across_replacement():
    """A restarted daemon builds a fresh limiter whose monotonic clock restarted
    with it; wall-clock stamps must carry the sustained volume over (#26)."""
    clock = _FakeClock()
    limiter = _default_limiter(clock)
    for _ in range(40):
        assert limiter.try_acquire("+1") == 0.0
        clock.advance(3.1)

    snapshot = limiter.snapshot_state()
    # A "restarted" limiter: fresh instance, fresh monotonic clock.
    revived = _default_limiter(_FakeClock())
    revived.restore_state(snapshot)
    # The carried volume still throttles: 19 burst stamps + this call fill the
    # inclusive 20-call burst cap, so the NEXT call defers (the sustained
    # window travels too — 40 of 60 slots).
    assert revived.try_acquire("+1") == 0.0
    assert revived.try_acquire("+1") > 0.0
    # 40 carried + the fresh allowed call = 41 in the sustained window.
    assert len(revived._sustained_calls["+1"]) == 41


def test_restore_replays_wall_clock_stamps_deterministically():
    import time as time_module

    clock = _FakeClock()
    limiter = ResolveRateLimiter(
        max_calls=3,
        window_sec=60.0,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )
    now_wall = time_module.time()
    limiter.restore_state({
        "+1": {
            "burst": [now_wall - 10, now_wall - 30],
            "sustained": [now_wall - 10, now_wall - 30],
            "tightened": {"cap": 30, "expires_in": 3600},
        },
    })
    # Two carried slots (ages 10s/30s): the cap is inclusive — the third call
    # is admitted, the FOURTH defers on the burst window.
    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0
    # And the tightened budget travelled: the sustained cap is 30, not 60.
    assert limiter._effective_sustained_cap("+1", clock.now) == 30


def test_restore_decays_already_elapsed_tightening():

    limiter = _default_limiter(_FakeClock())
    limiter.restore_state({
        "+1": {"tightened": {"cap": 30, "expires_in": 0}},
    })
    # expires_in 0 means the tightening already burnt out in wall-clock terms.
    assert limiter._effective_sustained_cap("+1", 1000.0) == 60


def test_restore_into_live_window_stays_sorted_and_pruned():
    """Merging into a limiter that already served traffic (review finding).

    The merged deque must stay ascending: the front-only prune and the
    ``window[calls_to_expire - 1]`` index both assume it. Appending snapshot
    stamps behind newer live stamps used to strand a dead stamp mid-deque,
    inflating ``used()`` and shifting defers for up to a full window."""
    clock = _FakeClock()
    limiter = _default_limiter(clock)
    assert limiter.try_acquire("+1") == 0.0  # live stamp at t=1000
    clock.advance(30)
    assert limiter.try_acquire("+1") == 0.0  # live stamp at t=1030

    # Snapshot stamps convert to mono 1025 and 990 — one interleaves between
    # the live stamps, one is older than everything live.
    import time as time_module

    now_wall = time_module.time()
    limiter.restore_state({"+1": {"burst": [now_wall - 5, now_wall - 40]}})

    clock.advance(40)  # t=1070: cutoff 1010 evicts 990 and 1000, keeps 1025/1030
    assert limiter.used("+1") == 2  # unsorted merge would strand 990 -> 3
