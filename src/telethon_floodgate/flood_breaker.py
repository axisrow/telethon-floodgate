"""Per-(operation, phone) circuit breaker for Telegram calls (#1330/#1368).

Telegram publishes no numeric rate limits: ``messages.getDialogs`` does not even
list FLOOD_WAIT among its documented errors, and error 420 is defined only as
"the maximum allowed number of attempts to invoke the given method with the
given input parameters has been exceeded". There is nothing to calibrate a
proactive limit against.

So instead of guessing a rate, this reacts to the fact that Telegram is already
throttling us: once one operation has been flood-waited repeatedly on one
account, stop calling it for a while rather than keep hammering. On 2026-08-26 a
single operation (``telegram_warm_dialog_cache``) took 85 flood waits on one
account over a day and ended in a 14.8-hour ban -- every one of them logged at
INFO, so nothing stood out.

The key is (operation, phone), not the account alone: a broken warm-up on one
account must not stop message collection everywhere else.

The state machine is explicit (closed/open/half_open: one deque-free counter,
one deadline dict) rather than pybreaker-driven (#15). pybreaker is
synchronous, so the old integration advanced it after the fact via a
``breaker.call(self._raise_probe)`` failure-counting trick, mirrored the real
deadline in a parallel ``_open_until`` dict, and had its half-open path driven
directly from ``check()`` -- the library was only partially in charge anyway.
The FSM keeps the exact public API (``check`` / ``record_flood`` /
``record_success`` / ``reset``) and thresholds; the semantics were proven in
the telegram-mcp port (axisrow/telegram-mcp#28), where the same test spec runs
against this implementation unchanged.
"""

from __future__ import annotations

import logging
import time

from telethon_floodgate.rate_limit_gate import TelegramRateLimitedError

logger = logging.getLogger(__name__)

# After this many flood waits on the same (operation, phone) the breaker opens.
# Mirrors the notifier's threshold (#553); deliberately small because each
# flood wait is itself already evidence of overuse.
DEFAULT_FLOOD_THRESHOLD = 3
# How long the operation stays blocked for that account before a single trial
# call is allowed through.
DEFAULT_COOLDOWN_SECONDS = 300.0
# Honest retry hint for a HALF_OPEN-busy refusal (#13): the in-flight trial
# typically resolves within seconds, so callers honoring retry_after get this
# instead of the flat cooldown -- which would idle them out of a suspension
# that is already lifting. Capped by the configured cooldown at the raise site.
HALF_OPEN_RETRY_HINT_SECONDS = 2.0
# A half-open probe is one RPC: it cannot legitimately outlive the very
# suspension window that granted it. A claim in flight this many cooldowns
# means its caller died between check() and record_* (#14) -- check() hands
# the trial to the next caller instead of wedging the pair forever (fixed
# the same way in axisrow/telegram-mcp#28).
PROBE_RECLAIM_AFTER_COOLDOWNS = 1.0

_CLOSED = "closed"
_OPEN = "open"
_HALF_OPEN = "half_open"


class TelegramOperationSuspendedError(TelegramRateLimitedError):
    """Raised instead of calling Telegram while the breaker is open.

    Deliberately a subclass of ``TelegramRateLimitedError``: ten call sites
    already handle "this operation is unavailable right now, move on" for the
    proactive gate, and the breaker means exactly the same thing to them. A new
    sibling type would have sailed past every one of those handlers and turned a
    protective refusal into a crashed collection pass.
    """

    def __init__(self, operation: str, phone: str, retry_after_sec: float) -> None:
        # ``category`` carries the operation so existing log lines that print it
        # stay meaningful ("Proactive %s rate limit ...").
        super().__init__(phone, operation, retry_after_sec)
        self.operation = operation


class FloodCircuitBreaker:
    """Tracks repeated flood waits per (operation, phone) and suspends the pair.

    Not thread-safe by design: like the rate-limit gate it guards, it is driven
    from a single event loop. The check/record cycle is synchronous, so no await
    can interleave between them.
    """

    def __init__(
        self,
        *,
        threshold: int = DEFAULT_FLOOD_THRESHOLD,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        time_func=None,
    ) -> None:
        # Both knobs clamped: a non-positive cooldown expires instantly and
        # turns the breaker into a no-op, defeating the point (#955).
        self._threshold = max(1, int(threshold))
        self._cooldown_seconds = max(0.1, float(cooldown_seconds))
        self._time = time_func or time.monotonic
        self._states: dict[tuple[str, str], str] = {}
        self._fails: dict[tuple[str, str], int] = {}
        self._open_until: dict[tuple[str, str], float] = {}
        # Keys whose half-open trial call has been granted but not yet
        # resolved, mapped to the grant time on the time_func clock.
        #
        # Reporting contract, load-bearing: a granted call MUST settle its
        # trial with EXACTLY ONE report -- record_flood() or record_success().
        # Nothing else releases a claim, and nothing here can tell a live
        # trial from a caller that died between check() and its report
        # (unexpected exception, cancellation, a wrapper bug), so the release
        # obligation lives entirely at the call site: settle from a finally
        # around the Telegram call. As a backstop against wedging forever,
        # check() re-claims a claim older than PROBE_RECLAIM_AFTER_COOLDOWNS
        # x cooldown (#14). The trial spans an ``await`` (check -> Telegram
        # call -> record_*), so without a claim a second coroutine arriving
        # mid-flight sees HALF_OPEN, not OPEN, and sails past the check
        # entirely -- the whole point of the cooldown being a *single* probe
        # (#955).
        self._probe_in_flight: dict[tuple[str, str], float] = {}

    def _cooldown_remaining(self, key: tuple[str, str], now: float) -> float | None:
        """Seconds left on a running suspension, else None.

        The single definition of "cooldown running": check() grants a trial
        exactly when this is None, record_flood re-arms exactly then too.
        """
        open_until = self._open_until.get(key)
        if open_until is None or now >= open_until:
            return None
        return open_until - now

    def check(self, operation: str, phone: str | None) -> None:
        """Raise if this operation is suspended for this account.

        Calls with no phone are never suspended: the breaker is per-account and
        an unattributed call cannot be charged to one.
        """
        if not phone:
            return
        key = (operation, phone)
        state = self._states.get(key)
        if state is None or state == _CLOSED:
            return
        now = self._time()
        if state == _OPEN:
            remaining = self._cooldown_remaining(key, now)
            if remaining is not None:
                raise TelegramOperationSuspendedError(operation, phone, remaining)
            # Cooldown elapsed: the upcoming real call becomes the genuine
            # trial -- claimed here, released by record_flood/record_success
            # (or re-claimed after a full cooldown of silence, per the
            # backstop below).
            self._states[key] = _HALF_OPEN
            self._probe_in_flight[key] = now
            return
        # HALF_OPEN: a trial granted to someone else is still awaiting its
        # Telegram call. Until it reports back, this operation is still
        # suspended: letting everyone through would send the whole backlog at
        # an account Telegram is actively throttling.
        granted_at = self._probe_in_flight.get(key)
        if granted_at is not None:
            if now - granted_at < self._cooldown_seconds * PROBE_RECLAIM_AFTER_COOLDOWNS:
                # Honest hint, not the flat cooldown (#13) -- see the constant.
                raise TelegramOperationSuspendedError(
                    operation,
                    phone,
                    min(self._cooldown_seconds, HALF_OPEN_RETRY_HINT_SECONDS),
                )
            # Self-heal (#14): a claim this old has no live caller behind it --
            # a probe cannot legitimately outlive the suspension window that
            # granted it. Hand the trial to this caller and refresh the stamp
            # instead of refusing forever. Loud here on purpose: nothing else
            # observes a reclaim, and the new trial usually settles as a plain
            # success, so this is the only trace a call site violating the
            # settle contract leaves.
            logger.warning(
                "Telegram breaker: %s on %s half-open probe never settled, %.0fs past the %.0fs"
                " reclaim window; handing the trial to this caller",
                operation,
                phone,
                now - granted_at - self._cooldown_seconds * PROBE_RECLAIM_AFTER_COOLDOWNS,
                self._cooldown_seconds * PROBE_RECLAIM_AFTER_COOLDOWNS,
            )
            self._probe_in_flight[key] = now

    def record_flood(self, operation: str, phone: str | None) -> None:
        """Count a flood wait against this (operation, phone)."""
        if not phone:
            return
        key = (operation, phone)
        state = self._states.get(key, _CLOSED)
        now = self._time()
        if state == _HALF_OPEN:
            # The trial flooded: the breaker reopens with a fresh cooldown, so
            # the next deadline grants a fresh trial and the pair cannot wedge
            # (#14 -- a flooded trial must never consume all future trials).
            self._states[key] = _OPEN
            self._open_until[key] = now + self._cooldown_seconds
            logger.warning(
                "Telegram breaker OPEN: %s on %s (half-open probe flooded again); "
                "suspending this operation for %.0fs",
                operation,
                phone,
                self._cooldown_seconds,
            )
            return
        fails = self._fails.get(key, 0) + 1
        self._fails[key] = fails
        # Materialize the key's state even below the threshold: record_success
        # must find it to clear the counter (the pybreaker version created the
        # breaker object here for the same reason).
        self._states[key] = state
        if fails < self._threshold:
            return
        if state != _OPEN:
            logger.warning(
                "Telegram breaker OPEN: %s on %s (%d flood waits); "
                "suspending this operation for %.0fs",
                operation,
                phone,
                fails,
                self._cooldown_seconds,
            )
        self._states[key] = _OPEN
        # One cooldown per trip: reports from calls already in flight when the
        # breaker opened (duplicate report paths included) land while the
        # deadline still runs and must not extend it; a flood arriving after
        # the deadline expired starts a fresh cooldown. The predicate is the
        # negation of check()'s grant boundary on purpose — one definition in
        # _cooldown_remaining keeps them from drifting.
        if self._cooldown_remaining(key, now) is None:
            self._open_until[key] = now + self._cooldown_seconds

    def record_success(self, operation: str, phone: str | None) -> None:
        """Clear accumulated flood waits after a call went through."""
        if not phone:
            return
        key = (operation, phone)
        # The trial reported back, so release the slot. This is bookkeeping
        # hygiene rather than a guard: check() re-claims the key on every
        # grant (and via the stuck-probe backstop), so a leftover entry is
        # never consulted for a key that is not HALF_OPEN (no test can fail
        # on its absence). Kept so the dict means what its name says.
        self._probe_in_flight.pop(key, None)
        state = self._states.get(key)
        if state is None:
            return
        # A successful trial is the freshest evidence that the operation
        # works, so it lifts a running suspension unconditionally: a stale
        # report from a pre-open call can re-trip the breaker mid-trial (and
        # reset the fail counter), and without this the success would leave
        # the pair suspended for a full extra cooldown despite real success.
        self._open_until.pop(key, None)
        self._fails.pop(key, None)
        self._states[key] = _CLOSED
        if state in (_OPEN, _HALF_OPEN):
            logger.info(
                "Telegram breaker closed: %s on %s resumed",
                operation,
                phone,
            )

    def reset(self, operation: str | None = None, phone: str | None = None) -> None:
        """Drop breaker state (all, or one operation/phone slice)."""
        if operation is None and phone is None:
            self._states.clear()
            self._fails.clear()
            self._open_until.clear()
            self._probe_in_flight.clear()
            return
        for key in [k for k in self._states if _matches(k, operation, phone)]:
            self._states.pop(key, None)
            self._fails.pop(key, None)
            self._open_until.pop(key, None)
            self._probe_in_flight.pop(key, None)


def _matches(key: tuple[str, str], operation: str | None, phone: str | None) -> bool:
    return (operation is None or key[0] == operation) and (phone is None or key[1] == phone)
