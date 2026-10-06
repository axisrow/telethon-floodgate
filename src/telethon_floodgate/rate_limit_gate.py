"""Proactive Telegram operation rate limiting: per-account and per-peer.

The category values below are deliberately boring guardrails rather than a
claim that Telegram publishes quotas (it does not).  They are calibrated to
the observed production shape: history is a high-volume read path, while
admin and channel-lifecycle calls are sparse writes.  Keep them configurable
so a new production sample can be applied without changing call sites.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict, defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from telethon.requestiter import RequestIter
from telethon.tl.functions import channels, messages

from telethon_floodgate.rate_limiter import ResolveRateLimiter

logger = logging.getLogger(__name__)

# Only message-fetch RPCs belong to an iterator's history budget. Telethon also
# retains the iterator's client on returned Message objects, whose later calls
# (edits, reactions, etc.) must not accidentally consume history slots.
_MESSAGE_REQUESTS = (
    messages.GetHistoryRequest,
    messages.SearchRequest,
    messages.SearchGlobalRequest,
    messages.GetRepliesRequest,
    messages.GetScheduledHistoryRequest,
    messages.GetMessagesRequest,
    channels.GetMessagesRequest,
)

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
    # messages.getHistory empirical boundary: 30 requests in roughly 30s on
    # the calibrated account/channel (31st request returned FLOOD_WAIT_3).
    # Keep a 20% margin; Telegram does not publish this quota.
    HISTORY_SPEC = RateLimitSpec(max_calls=24, window_sec=30.0)
    ADMIN_ACTION_SPEC = RateLimitSpec(max_calls=10, window_sec=60.0)
    SEND_SPEC = RateLimitSpec(max_calls=30, window_sec=60.0)
    CHANNEL_LIFECYCLE_SPEC = RateLimitSpec(max_calls=3, window_sec=300.0)
    # Per-peer send limits (community-observed, not published by Telegram):
    # roughly one message per second to the same private chat and about
    # twenty per minute into the same group or channel. Applied as a second,
    # INDEPENDENT bucket on top of the per-account ``send`` category — an
    # account bursting into many different peers is still bounded by the
    # category, while an account hammering one peer is stopped long before it.
    # A small margin absorbs clock/network variation around the one-second
    # observation; the live test proved this pacing on a real user peer. A
    # 0.15s defer jitter keeps retried sends from firing metronome-precisely
    # on the window boundary (same lockstep rationale as ResolveRateLimiter's
    # own jitter).
    SEND_PEER_USER_SPEC = RateLimitSpec(max_calls=1, window_sec=1.1, jitter_sec=0.15)
    SEND_PEER_CHANNEL_SPEC = RateLimitSpec(max_calls=16, window_sec=60.0)
    SEND_PEER_CHAT_SPEC = RateLimitSpec(max_calls=16, window_sec=60.0)

    def __init__(
        self,
        *,
        category_limits: dict[str, RateLimitSpec] | None = None,
        peer_limits: dict[str, RateLimitSpec] | None = None,
        peer_max_buckets: int = 4096,
        time_func: Callable[[], float] | None = None,
        flood_backoff: bool = False,
        flood_backoff_cap: float = 8.0,
        flood_decay_sec: float = 3600.0,
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
        self._category_specs = dict(specs)
        # Adaptive backoff: every FLOOD_WAIT the consumer reports via
        # note_flood doubles the defers this gate hands out for that phone
        # until the events decay out of the window. Static windows assume the
        # calibration holds; a server-side flood is proof it currently does
        # not. Opt-in — it changes returned defers — like the sustained tier.
        self._flood_backoff = flood_backoff
        self._flood_backoff_cap = max(1.0, float(flood_backoff_cap))
        self._flood_decay_sec = max(1.0, float(flood_decay_sec))
        self._flood_events: dict[str, deque[float]] = defaultdict(deque)
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

    def note_flood(self, phone: str, seconds: float = 0.0) -> float:
        """Record a server-side FLOOD_WAIT; returns the new defer multiplier.

        The consumer wires this from its flood reporting (e.g. a
        ``pool.report_flood`` hook) — the gate never sees Telegram errors
        itself. ``seconds`` is accepted for call-site symmetry, but only the
        event *count* inside the decay window drives the multiplier: one long
        wait and several short ones mean the same thing — the static
        calibration is stale right now.
        """
        if not self._flood_backoff:
            return 1.0
        self._flood_events[phone].append(self._now())
        return self._flood_multiplier(phone)

    def snapshot(self, phone: str) -> dict[str, object]:
        """Live gate state for one account — dashboards/health endpoints.

        Read-only view; pruning expired entries is the same bookkeeping the
        next ``try_acquire`` would do anyway.
        """
        categories: dict[str, dict[str, object]] = {}
        for category, spec in self._category_specs.items():
            categories[category] = {
                "max_calls": spec.max_calls,
                "window_sec": spec.window_sec,
                "jitter_sec": spec.jitter_sec,
                "used": self._limiters[category].used(phone),
            }
        peers: dict[str, dict[str, object]] = {}
        for bucket_phone, category, peer in self._peer_buckets:
            if bucket_phone != phone:
                continue
            spec = self._peer_spec_for(category, peer)
            peers[f"{category}:{peer}"] = {
                "max_calls": spec.max_calls if spec else None,
                "window_sec": spec.window_sec if spec else None,
                "used": self._peer_buckets[(bucket_phone, category, peer)].used(phone),
            }
        return {
            "flood_backoff": self._flood_backoff,
            "flood_multiplier": self._flood_multiplier(phone),
            "flood_events_in_window": len(self._flood_events.get(phone, ())),
            "categories": categories,
            "peer_buckets": peers,
        }

    def _now(self) -> float:
        return self._time_func() if self._time_func is not None else time.monotonic()

    def _flood_multiplier(self, phone: str) -> float:
        if not self._flood_backoff:
            return 1.0
        events = self._flood_events.get(phone)
        if not events:
            return 1.0
        cutoff = self._now() - self._flood_decay_sec
        while events and events[0] <= cutoff:
            events.popleft()
        if not events:
            return 1.0
        return min(2.0 ** len(events), self._flood_backoff_cap)

    def try_acquire(
        self, phone: str, category: str, *, slots: int = 1, peer: str | None = None
    ) -> float:
        if category in {"resolve", "reaction"}:
            return 0.0
        # A reported flood scales every defer for this phone until the events
        # decay; 0.0 stays 0.0 — backoff slows callers, it invents nothing.
        multiplier = self._flood_multiplier(phone)
        if peer is not None:
            peer_retry_after = self._try_acquire_peer(phone, category, peer, slots)
            if peer_retry_after > 0:
                # The tighter per-peer bucket refused first and its slot for
                # the broad category is left untouched, so a burst aimed at
                # one peer cannot burn the account-wide budget.
                return peer_retry_after * multiplier
        limiter = self._limiters.get(category, self._limiters["default"])
        return limiter.try_acquire_many(phone, slots) * multiplier

    async def acquire(
        self,
        phone: str,
        category: str,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Wait for one category slot; a deferral reserves nothing.

        Re-check after every sleep, including when another waiter took the
        available slot first. Cancellation propagates without reserving a slot.
        Per-peer/compound reservations retain the explicit ``try_acquire`` API.
        """
        while (retry_after := self.try_acquire(phone, category)) > 0:
            logger.info("%s: rate-limit gate defers %.1fs", category, retry_after)
            await sleep(retry_after)

    def wrap_messages_iterator(
        self,
        iterator: RequestIter,
        phone: str,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> RequestIter:
        """Gate every message-fetch RPC of a fresh Telethon ``iter_messages``.

        Wrap before iterating; do not also reserve a logical history slot.
        The raw client is unchanged, so concurrent iterators remain independent.
        Flood-wait retry stays with the caller. Unsupported/already wrapped
        iterators fail explicitly rather than silently bypassing the gate.
        """
        client = getattr(iterator, "client", None)
        if not callable(client):
            raise TypeError("message iterator must expose a callable client")
        if isinstance(client, _MessageGateClient):
            raise ValueError("message iterator is already rate-limited")
        iterator.client = _MessageGateClient(client, self, phone, sleep)
        return iterator

    def reset(self, phone: str | None = None, category: str | None = None) -> None:
        limiters = self._limiters.values() if category is None else [self._limiters[category]]
        for limiter in limiters:
            limiter.reset(phone)
        if phone is None:
            self._peer_buckets.clear()
            self._flood_events.clear()
        else:
            stale = [key for key in self._peer_buckets if key[0] == phone]
            for key in stale:
                del self._peer_buckets[key]
            self._flood_events.pop(phone, None)

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


class _MessageGateClient:
    """Per-iterator proxy, not a monkeypatch of the shared TelegramClient."""

    def __init__(
        self,
        client: Any,
        gate: TelegramRateLimitGate,
        phone: str,
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._client = client
        self._gate = gate
        self._phone = phone
        self._sleep = sleep

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    async def __call__(self, request: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(request, _MESSAGE_REQUESTS):
            await self._gate.acquire(self._phone, "history", sleep=self._sleep)
        return await self._client(request, *args, **kwargs)
