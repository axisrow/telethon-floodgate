"""Per-account sliding-window rate limiter for live ``auth.resolveUsername``
calls (#551).

The reactive backoff added in #502 only kicks in *after* Telegram has already
returned a multi-hour ``FLOOD_WAIT_X``. By then the damage is done: production
logs show a single fresh session firing 160+ ``resolve_username`` calls in
seconds, escalating to 15–18 hour flood waits.

This limiter caps the *burst* before it happens. It is a pure in-memory,
per-account sliding window — no DB, no locks — so it is cheap to consult on the
hot resolve path. When an account exceeds its window the caller defers the
channel (short reschedule) instead of issuing the live API call.
"""

from __future__ import annotations

import random
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from math import ceil, isfinite
from typing import Any

from telethon_floodgate._errors import TelegramRateLimitedError

# Telegram does not publish the ``auth.resolveUsername`` limit. Production
# evidence points at roughly 30 calls / account / minute before escalation can
# begin, so the default stays materially below that.
DEFAULT_MAX_CALLS = 20
DEFAULT_WINDOW_SEC = 60.0
DEFAULT_JITTER_SEC = 5.0
# Sustained defaults (#26): 60/3600s is the honest companion to the 20/60s
# burst in production — both deep floods on record (2026-10-06 13.8h,
# 2026-11-04 18h) grew out of sustained volume, not bursts. The tier used to
# be opt-in; a second 18h FLOOD_WAIT proved opt-in never turns itself on.
DEFAULT_SUSTAINED_MAX_CALLS = 60
DEFAULT_SUSTAINED_WINDOW_SEC = 3600.0
# Adaptive tightening (#26): after a Telegram-side signal the phone has
# demonstrated the static budget outruns its tolerance — halve the sustained
# budget for a few hours instead of waiting the signal out at full quota.
DEFAULT_TIGHTEN_FACTOR = 0.5
DEFAULT_TIGHTEN_DURATION_SEC = 4 * 3600.0
RESOLVE_USERNAME_BACKOFF_BUFFER_SEC = 5
GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC = 300


class UsernameResolveFloodWaitDeferredError(TelegramRateLimitedError):
    """Raised when username resolution is deferred by Flood Wait backoff.

    Deliberately a subclass of ``TelegramRateLimitedError``, not a sibling on
    ``RuntimeError`` (#11): this is the same class of refusal the hierarchy was
    built for, so the consumer pattern ``except TelegramRateLimitedError:
    operation unavailable, move on`` must cover it too. ``phone`` stays empty —
    a flood-wait deferral is constructed without an account at hand.
    """

    def __init__(self, wait_seconds: int, next_available_at: datetime):
        super().__init__(
            "",
            "resolve_username",
            float(wait_seconds),
            message=(
                "Username resolve is flood-waited until "
                f"{next_available_at.isoformat()} (retry in {wait_seconds}s)"
            ),
        )
        self.wait_seconds = wait_seconds
        self.next_available_at = next_available_at


class UsernameResolveRateLimitedError(TelegramRateLimitedError):
    """Raised when a live username resolve is throttled before hitting Telegram.

    Deliberately a subclass of ``TelegramRateLimitedError`` (#11) for the same
    reason as :class:`UsernameResolveFloodWaitDeferredError`. The historical
    message text and attributes (``retry_after_sec`` float, ceil'ed
    ``retry_after_seconds``, ``next_available_at``) are preserved byte-for-byte;
    ``category`` is ``"resolve_username"``.
    """

    def __init__(self, phone: str, retry_after_sec: float, *, now: datetime | None = None):
        retry_after_sec = max(0.0, float(retry_after_sec))
        retry_after_seconds = ceil(retry_after_sec)
        next_available_at = (now or datetime.now(timezone.utc)) + timedelta(
            seconds=retry_after_seconds
        )
        super().__init__(
            phone,
            "resolve_username",
            retry_after_sec,
            message=f"resolve_username rate-limited for {phone}; retry in {retry_after_seconds}s",
        )
        self.retry_after_seconds = retry_after_seconds
        self.next_available_at = next_available_at

    def run_after_with_buffer(self, buffer_sec: int = RESOLVE_USERNAME_BACKOFF_BUFFER_SEC) -> datetime:
        return self.next_available_at + timedelta(seconds=buffer_sec)


class ResolveRateLimiter:
    """Sliding-window limiter keyed by account phone.

    ``try_acquire`` is the only method that mutates state: it prunes the
    window, and either records the call and returns ``0.0`` (allowed) or
    returns a positive number of seconds the caller should defer for. The
    deferral includes a small ``±jitter`` so that many accounts unblocking at
    the same instant do not re-burst in lockstep.
    """

    def __init__(
        self,
        *,
        max_calls: int = DEFAULT_MAX_CALLS,
        window_sec: float = DEFAULT_WINDOW_SEC,
        jitter_sec: float = DEFAULT_JITTER_SEC,
        time_func=time.monotonic,
        jitter_func=random.uniform,
        sustained_max_calls: int | None = DEFAULT_SUSTAINED_MAX_CALLS,
        sustained_window_sec: float | None = DEFAULT_SUSTAINED_WINDOW_SEC,
    ) -> None:
        self._max_calls = max(1, int(max_calls))
        self._window_sec = float(window_sec)
        if not (isfinite(self._window_sec) and self._window_sec > 0):
            raise ValueError(
                "window_sec must be finite and > 0: a non-positive window "
                "prunes every entry instantly, a NaN one never defers — "
                "either way the limiter silently disables"
            )
        self._jitter_sec = max(0.0, float(jitter_sec))
        self._time = time_func
        self._jitter = jitter_func
        self._calls: dict[str, deque[float]] = defaultdict(deque)
        # Sustained tier (tg_content_factory incident 2026-10-06): the burst
        # window alone admitted an unbounded *stream* of fully-legal calls
        # (20/min for 20+ minutes) and Telegram escalated to a 13.8h
        # FLOOD_WAIT. The second, wider window caps accumulated volume; the
        # recommended companion to the 20/60s burst is 60 calls / 3600s.
        # Disabled (None) keeps the 0.1.x behaviour bit-for-bit.
        if (sustained_max_calls is None) != (sustained_window_sec is None):
            raise ValueError(
                "sustained_max_calls and sustained_window_sec must be set together"
            )
        self._sustained_calls: dict[str, deque[float]] | None = None
        if sustained_max_calls is not None:
            sustained_window_sec_f = float(sustained_window_sec)  # type: ignore[arg-type]
            if not isfinite(sustained_window_sec_f):
                raise ValueError("sustained_window_sec must be finite")
            if sustained_window_sec_f < self._window_sec:
                raise ValueError(
                    "sustained_window_sec must be >= window_sec: the sustained "
                    "window cannot be narrower than the burst window"
                )
            self._sustained_max_calls = max(1, int(sustained_max_calls))
            self._sustained_window_sec = sustained_window_sec_f
            self._sustained_calls = defaultdict(deque)
        else:
            self._sustained_max_calls = 0
            self._sustained_window_sec = 0.0
        # phone → (effective sustained cap, expiry in limiter time); fed by
        # ``tighten`` after Telegram-side signals (#26).
        self._tightened: dict[str, tuple[int, float]] = {}

    def _prune_window(
        self, store: dict[str, deque[float]], phone: str, now: float, window_sec: float
    ) -> deque[float]:
        window = store[phone]
        cutoff = now - window_sec
        while window and window[0] <= cutoff:
            window.popleft()
        return window

    def _prune(self, phone: str, now: float) -> deque[float]:
        return self._prune_window(self._calls, phone, now, self._window_sec)

    def try_acquire(self, phone: str) -> float:
        """Reserve one resolve slot for ``phone``.

        Returns ``0.0`` when the call is allowed (and records it). Otherwise
        returns the number of seconds to defer before retrying — the window is
        full and no slot is consumed.
        """
        return self.try_acquire_many(phone, 1)

    def _validate_slots(self, slots: int) -> int:
        slots = int(slots)
        if slots < 1:
            raise ValueError("slots must be at least 1")
        return slots

    def _jittered_defer(self, retry: float) -> float:
        if self._jitter_sec:
            retry += self._jitter(0.0, self._jitter_sec)
        return max(retry, 0.0)

    def _peek_retry(self, phone: str, slots: int, now: float) -> float | None:
        """Defer the request would get if made at ``now`` — or None if admitted.

        Prunes both windows, records nothing. When BOTH windows are full, the
        sustained retry (~hours) is the honest defer — returning the ~60s
        burst retry here would wake every deferred caller one extra cycle per
        window (review finding on tg_content_factory#1498).

        A request wider than a tier's cap (``slots > max_calls``, #7) can
        never be admitted — even an empty window holds at most ``max_calls``
        entries — so the never-raises contract (README/CLAUDE.md) keeps the
        answer a deferral: the offending tier contributes one full window of
        cadence, state-independent because no wait makes the request
        admissible, and it repeats until the caller splits the request. The
        wider tier wins the ``max()`` below, matching the both-full rule.
        """
        window = self._prune(phone, now)
        retry: float | None = None
        if slots > self._max_calls:
            retry = self._window_sec
        elif len(window) + slots > self._max_calls:
            # Wait until enough calls have expired for the whole atomic request,
            # not merely until the oldest call leaves the window.
            calls_to_expire = len(window) + slots - self._max_calls
            retry = max((window[calls_to_expire - 1] + self._window_sec) - now, 0.0)
        if self._sustained_calls is not None:
            sustained_window = self._prune_window(
                self._sustained_calls, phone, now, self._sustained_window_sec
            )
            # The cap the phone currently earns: the configured budget, shrunk
            # while a ``tighten`` from a Telegram-side signal is in effect (#26).
            sustained_cap = self._effective_sustained_cap(phone, now)
            sustained_retry: float | None = None
            if slots > sustained_cap:
                sustained_retry = self._sustained_window_sec
            elif len(sustained_window) + slots > sustained_cap:
                calls_to_expire = len(sustained_window) + slots - sustained_cap
                sustained_retry = max(
                    (sustained_window[calls_to_expire - 1] + self._sustained_window_sec) - now,
                    0.0,
                )
            if sustained_retry is not None:
                retry = sustained_retry if retry is None else max(retry, sustained_retry)
        return retry

    def peek_many(self, phone: str, slots: int) -> float:
        """The defer ``try_acquire_many(phone, slots)`` would return — no record.

        The peek half of the two-phase peek/commit pair (issue #8): the gate
        peeks its peer and category windows first and commits only when every
        one admits, so a refused call burns no slot anywhere.
        """
        slots = self._validate_slots(slots)
        retry = self._peek_retry(phone, slots, self._time())
        if retry is None:
            return 0.0
        return self._jittered_defer(retry)

    def record_many(self, phone: str, slots: int) -> None:
        """Record ``slots`` calls without checking the window.

        The commit half of the pair: the gate calls it only after every window
        admitted, so it cannot refuse. Gate code is synchronous with no await
        between peek and commit, so on the single event loop the pair is
        atomic by construction.

        Unlike the acquire path (#7), a request wider than a tier cap still
        raises here: committing a peek-admitted reservation no window could
        admit is a programming error like ``slots < 1`` — recording it would
        widen the window and corrupt every later defer.
        """
        slots = self._validate_slots(slots)
        if slots > self._max_calls:
            raise ValueError("slots cannot exceed max_calls")
        if self._sustained_calls is not None and slots > self._sustained_max_calls:
            raise ValueError("slots cannot exceed sustained_max_calls")
        now = self._time()
        self._prune(phone, now).extend([now] * slots)
        if self._sustained_calls is not None:
            self._prune_window(
                self._sustained_calls, phone, now, self._sustained_window_sec
            ).extend([now] * slots)

    def try_acquire_many(self, phone: str, slots: int) -> float:
        """Atomically reserve ``slots`` calls for one compound operation.

        With a sustained tier configured, both windows are checked first and
        timestamps are recorded in both **only when both admit the call** —
        a deferred call burns no slots in any window (unlike composing two
        separate limiter instances, where a burst refusal after a sustained
        admission would burn a sustained slot).

        A request wider than a tier cap (``slots > max_calls``, #7) is never
        admitted: it returns a full-window cadence defer that repeats until
        the caller splits the request.
        """
        slots = self._validate_slots(slots)
        retry = self._peek_retry(phone, slots, self._time())
        if retry is not None:
            return self._jittered_defer(retry)
        self.record_many(phone, slots)
        return 0.0

    def tighten(
        self,
        phone: str,
        *,
        factor: float = DEFAULT_TIGHTEN_FACTOR,
        duration_sec: float = DEFAULT_TIGHTEN_DURATION_SEC,
    ) -> int | None:
        """Shrink the sustained budget for ``phone`` after a Telegram signal.

        Telegram escalates in steps (short waits → rate limits → deep
        multi-hour floods); a phone that earned such a signal has proven the
        static budget outruns its current tolerance, and waiting the signal
        out at full quota only invites the next rung (#26). For
        ``duration_sec`` of limiter time the effective sustained cap becomes
        ``round(sustained_max_calls * factor)`` (min 1), after which the
        configured budget returns on its own.

        Returns the new effective cap, or ``None`` when the sustained tier is
        disabled (nothing to tighten). Invalid arguments raise ``ValueError``.
        """
        if not 0.0 < factor <= 1.0:
            raise ValueError(f"factor must be in (0, 1]: got {factor}")
        if not isfinite(duration_sec) or duration_sec <= 0:
            raise ValueError(f"duration_sec must be finite and > 0: got {duration_sec}")
        if self._sustained_calls is None:
            return None
        effective_cap = max(1, round(self._sustained_max_calls * factor))
        self._tightened[phone] = (effective_cap, self._time() + duration_sec)
        return effective_cap

    def _effective_sustained_cap(self, phone: str, now: float) -> int:
        """Configured sustained cap, shrunk while a tightening is in effect."""
        if self._sustained_calls is None:
            return 0
        entry = self._tightened.get(phone)
        if entry is not None:
            effective_cap, expiry = entry
            if now >= expiry:
                self._tightened.pop(phone, None)
            else:
                return min(self._sustained_max_calls, effective_cap)
        return self._sustained_max_calls

    def snapshot_state(self) -> dict[str, dict[str, Any]]:
        """Export per-phone window contents as wall-clock stamps.

        Stamps (``time.time()``) make the snapshot portable across processes:
        monotonic stamps die with the process, but ``restore_state`` replays
        wall-clock stamps against the next boot's clock, so sustained volume
        stays honest across a daemon restart (#26). Tightened budgets travel
        too, as remaining seconds.
        """
        now_mono = self._time()
        now_wall = time.time()
        phones = set(self._calls) | set(self._sustained_calls or {})
        snapshot: dict[str, dict[str, Any]] = {}
        for phone in phones:
            entry: dict[str, Any] = {
                "burst": [now_wall - (now_mono - t) for t in self._prune(phone, now_mono)]
            }
            if self._sustained_calls is not None:
                sustained = self._prune_window(
                    self._sustained_calls, phone, now_mono, self._sustained_window_sec
                )
                entry["sustained"] = [now_wall - (now_mono - t) for t in sustained]
            tight = self._tightened.get(phone)
            if tight is not None:
                entry["tightened"] = {
                    "cap": tight[0],
                    "expires_in": max(0.0, tight[1] - now_mono),
                }
            snapshot[phone] = entry
        return snapshot

    def restore_state(self, snapshot: dict[str, dict[str, Any]]) -> None:
        """Replay a :meth:`snapshot_state` export into this limiter.

        Wall-clock stamps convert back against this process's monotonic clock;
        entries older than their window are dropped by the regular prune on
        the next acquire. Tightened budgets whose duration already elapsed in
        wall-clock terms decay the same way. Merges into existing state.
        """
        now_mono = self._time()
        now_wall = time.time()
        for phone, entry in snapshot.items():
            for stamp in entry.get("burst", ()):
                self._calls[phone].append(now_mono - (now_wall - float(stamp)))
            sustained = entry.get("sustained")
            if sustained is not None and self._sustained_calls is not None:
                self._sustained_calls[phone].extend(
                    now_mono - (now_wall - float(stamp)) for stamp in sustained
                )
            tight = entry.get("tightened")
            if tight is not None and self._sustained_calls is not None:
                cap, expires_in = tight["cap"], tight["expires_in"]
                self._tightened[phone] = (int(cap), now_mono + max(0.0, float(expires_in)))

    def used(self, phone: str) -> int:
        """Burst-window calls recorded for ``phone`` (prunes first).

        Read-only: an unknown phone seeds no window entry (dashboards may
        ask about arbitrary accounts). A sustained tier, if configured, is
        not included — this reports the burst window only.
        """
        window = self._calls.get(phone)
        if not window:
            return 0
        return len(self._prune(phone, self._time()))

    def reset(self, phone: str | None = None) -> None:
        """Drop recorded history for ``phone`` (or all accounts)."""
        if phone is None:
            self._calls.clear()
            if self._sustained_calls is not None:
                self._sustained_calls.clear()
        else:
            self._calls.pop(phone, None)
            if self._sustained_calls is not None:
                self._sustained_calls.pop(phone, None)
