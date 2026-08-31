"""Proactive Telegram operation rate limiting: per-account and per-peer.

The category values below are deliberately boring guardrails rather than a
claim that Telegram publishes quotas (it does not).  They are calibrated to
the observed production shape: history is a high-volume read path, while
admin and channel-lifecycle calls are sparse writes.  Keep them configurable
so a new production sample can be applied without changing call sites.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable

from telethon_floodgate.rate_limiter import ResolveRateLimiter

# One sweep = the initial pass plus its resumptions. Kept in step with
# DIALOG_FETCH_MAX_PASSES in pool_dialogs (12) so the gate can never be the
# thing that truncates a sweep the loop is still willing to continue; the
# loop's own limiters (max passes, total time budget, no-progress check) and
# the flood breaker are what bound it.
DIALOG_SWEEP_MAX_CALLS = 12
# A logical dialogs operation reserves the conservative ``dialogs`` slot once.
# Its internal paginated requests use this separate safety budget so a normal
# multi-page account can finish without consuming another logical-operation
# slot. This is an implementation guard, not Phase 2 quota calibration.
DIALOG_PAGE_MAX_CALLS = 1000


@dataclass(frozen=True)
class RateLimitSpec:
    max_calls: int
    window_sec: float
    jitter_sec: float = 0.0


class TelegramRateLimitedError(RuntimeError):
    """Raised when an operation is deferred before making a Telegram call."""

    def __init__(self, phone: str, category: str, retry_after_sec: float) -> None:
        super().__init__(f"Telegram {category} rate-limited for {phone}; retry in {retry_after_sec:.1f}s")
        self.phone = phone
        self.category = category
        self.retry_after_sec = retry_after_sec


class TelegramPeerRateLimitedError(TelegramRateLimitedError):
    """Raised when a per-peer bucket defers an operation before the call.

    A subclass so every existing ``TelegramRateLimitedError`` handler — "this
    operation is unavailable right now, move on" — absorbs it unchanged.
    """

    def __init__(self, phone: str, peer: str, retry_after_sec: float) -> None:
        super().__init__(phone, "send_peer", retry_after_sec)
        self.peer = peer


_OPERATION_CATEGORIES = {
    "telegram_warm_dialog_cache": "dialogs",
    # A sweep is ONE user-facing operation that Telethon splits into ~N/100
    # paginated requests under a single reservation, and a flood mid-pagination
    # means "slow down", not "stop" (Telethon documents this outright). Sharing
    # the warm-up's 1/min bucket meant the second pass was always refused, so
    # the resumable sweep could never resume (#1359).
    "telegram_stream_dialogs": "dialog_sweep",
    # Live username resolution is already protected by ResolveGuardMixin.
    # Cached entity lookups share these operation tags, so generic throttling
    # must stay disabled for the whole transport operation rather than risk a
    # second, conflicting limiter on the live path.
    "telegram_resolve_entity": "resolve",
    "telegram_resolve_input_entity": "resolve",
    "telegram_stream_messages": "history",
    "telegram_edit_admin": "admin_action",
    "telegram_edit_permissions": "admin_action",
    "telegram_kick_participant": "admin_action",
    "telegram_edit_folder": "admin_action",
    "telegram_send_message": "send",
    "telegram_publish_files": "send",
    "telegram_edit_message": "send",
    "telegram_forward_messages": "send",
    "telegram_pin_message": "send",
    # _ensure_reaction_can_run remains the sole reaction gate in Phase 1.
    "telegram_send_reaction": "reaction",
    "telegram_create_channel": "channel_lifecycle",
    "telegram_update_channel_username": "channel_lifecycle",
    "telegram_join_channel": "channel_lifecycle",
    "telegram_import_chat_invite": "channel_lifecycle",
    "telegram_delete_channel": "channel_lifecycle",
    "telegram_delete_chat": "channel_lifecycle",
}


def _category_for_operation(operation: str) -> str:
    """Return a category for both canonical and decorated operation tags.

    Warm operations are decorated with the caller name (for example
    ``resolve_channel_warm_dialog_cache``) so that flood diagnostics retain
    their useful context.  Matching the stable suffix prevents those paths
    from silently falling back to the broad default bucket.
    """
    exact = _OPERATION_CATEGORIES.get(operation)
    if exact is not None:
        return exact
    if operation.endswith("_stream_dialogs"):
        return "dialog_sweep"
    if operation.endswith("_warm_dialog_cache"):
        return "dialogs"
    return "default"


class TelegramRateLimitGate:
    """Registry of independent sliding-window buckets keyed by phone/category."""

    DEFAULT_SPEC = RateLimitSpec(max_calls=1000, window_sec=60.0)
    # #1330 showed repeated getDialogs floods even with multi-minute pauses.
    # Keep this deliberately low until production logs calibrate the value.
    DIALOGS_SPEC = RateLimitSpec(max_calls=1, window_sec=60.0)
    # A dialog sweep is one operation continued across passes, not repeated
    # calls: each pass resumes from the cursor with DIFFERENT offsets, which is
    # not the "same method, same parameters" shape error 420 is defined
    # against. It needs room for the initial pass plus resumptions after the
    # transient floods Telegram uses to pace pagination (27-30s in our logs,
    # inside this window). Still bounded -- a sweep that makes no progress is
    # stopped by the flood breaker (#1372) and the loop's own no-progress
    # check, not by starving it here.
    DIALOG_SWEEP_SPEC = RateLimitSpec(max_calls=DIALOG_SWEEP_MAX_CALLS, window_sec=60.0)
    DIALOG_PAGE_SPEC = RateLimitSpec(max_calls=DIALOG_PAGE_MAX_CALLS, window_sec=60.0)
    # Phase 2 calibration.  These are intentionally permissive for normal
    # workloads and should be revisited when a larger production sample is
    # available; they are not Telegram's documented quotas.
    HISTORY_SPEC = RateLimitSpec(max_calls=600, window_sec=60.0)
    ADMIN_ACTION_SPEC = RateLimitSpec(max_calls=10, window_sec=60.0)
    SEND_SPEC = RateLimitSpec(max_calls=30, window_sec=60.0)
    CHANNEL_LIFECYCLE_SPEC = RateLimitSpec(max_calls=3, window_sec=300.0)
    # Per-peer send limits (community-observed, not published by Telegram):
    # roughly one message per second to the same private chat and about
    # twenty per minute into the same group or channel. Applied as a second,
    # INDEPENDENT bucket on top of the per-account ``send`` category — an
    # account bursting into many different peers is still bounded by the
    # category, while an account hammering one peer is stopped long before it.
    SEND_PEER_USER_SPEC = RateLimitSpec(max_calls=1, window_sec=1.0)
    SEND_PEER_CHANNEL_SPEC = RateLimitSpec(max_calls=20, window_sec=60.0)
    SEND_PEER_CHAT_SPEC = RateLimitSpec(max_calls=20, window_sec=60.0)

    def __init__(
        self,
        *,
        category_limits: dict[str, RateLimitSpec] | None = None,
        peer_limits: dict[str, RateLimitSpec] | None = None,
        peer_max_buckets: int = 4096,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        specs = {
            "dialogs": self.DIALOGS_SPEC,
            "dialog_sweep": self.DIALOG_SWEEP_SPEC,
            "dialogs_page": self.DIALOG_PAGE_SPEC,
            "history": self.HISTORY_SPEC,
            "admin_action": self.ADMIN_ACTION_SPEC,
            "send": self.SEND_SPEC,
            "channel_lifecycle": self.CHANNEL_LIFECYCLE_SPEC,
            "default": self.DEFAULT_SPEC,
        }
        specs.update(category_limits or {})
        self._limiters = {
            category: ResolveRateLimiter(
                max_calls=spec.max_calls,
                window_sec=spec.window_sec,
                jitter_sec=spec.jitter_sec,
                **({"time_func": time_func} if time_func is not None else {}),
            )
            for category, spec in specs.items()
        }
        self._time_func = time_func
        # Per-peer specs are keyed "<category>:<kind>" (e.g. "send:user"); the
        # kind prefix comes from the peer key built by telethon_floodgate.peer.
        # An unconfigured pair has no per-peer bucket, so passing a peer for it
        # degrades to category-only limiting — the safe default.
        self._peer_specs: dict[str, RateLimitSpec] = {
            "send:user": self.SEND_PEER_USER_SPEC,
            "send:channel": self.SEND_PEER_CHANNEL_SPEC,
            "send:chat": self.SEND_PEER_CHAT_SPEC,
        }
        self._peer_specs.update(peer_limits or {})
        self._peer_max_buckets = max(1, int(peer_max_buckets))
        self._peer_buckets: OrderedDict[tuple[str, str, str], ResolveRateLimiter] = OrderedDict()

    @staticmethod
    def category_for(operation: str) -> str:
        # resolve is explicitly a no-op category: ResolveGuardMixin owns it.
        return _category_for_operation(operation)

    def try_acquire(
        self, phone: str, category: str, *, slots: int = 1, peer: str | None = None
    ) -> float:
        if category in {"resolve", "reaction"}:
            return 0.0
        if peer is not None:
            peer_retry_after = self._try_acquire_peer(phone, category, peer, slots)
            if peer_retry_after > 0:
                # The tighter per-peer bucket refused first and its slot for
                # the broad category is left untouched, so a burst aimed at
                # one peer cannot burn the account-wide budget.
                return peer_retry_after
        return self._limiters.get(category, self._limiters["default"]).try_acquire_many(
            phone, slots
        )

    def reset(self, phone: str | None = None, category: str | None = None) -> None:
        limiters = self._limiters.values() if category is None else [self._limiters[category]]
        for limiter in limiters:
            limiter.reset(phone)
        if phone is None:
            self._peer_buckets.clear()
        else:
            stale = [key for key in self._peer_buckets if key[0] == phone]
            for key in stale:
                del self._peer_buckets[key]

    def _peer_spec_for(self, category: str, peer: str) -> RateLimitSpec | None:
        kind = peer.split(":", 1)[0] if ":" in peer else ""
        return self._peer_specs.get(f"{category}:{kind}")

    def _try_acquire_peer(
        self, phone: str, category: str, peer: str, slots: int
    ) -> float:
        """Reserve a slot in the (phone, category, peer) bucket.

        Returns ``0.0`` when allowed (and records it) or the seconds to defer.
        Buckets are created lazily and kept in LRU order, bounded by
        ``peer_max_buckets``; eviction may briefly re-allow an idle peer — the
        bound trades that for a hard cap on memory.
        """
        spec = self._peer_spec_for(category, peer)
        if spec is None:
            return 0.0
        key = (phone, category, peer)
        limiter = self._peer_buckets.get(key)
        if limiter is None:
            limiter = ResolveRateLimiter(
                max_calls=spec.max_calls,
                window_sec=spec.window_sec,
                jitter_sec=spec.jitter_sec,
                **({"time_func": self._time_func} if self._time_func is not None else {}),
            )
            self._peer_buckets[key] = limiter
            while len(self._peer_buckets) > self._peer_max_buckets:
                self._peer_buckets.popitem(last=False)
        else:
            self._peer_buckets.move_to_end(key)
        return limiter.try_acquire_many(phone, slots)
