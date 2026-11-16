"""RPC dispatch-boundary enforcement for an existing TelegramClient (#34).

Public calls like ``send_message`` make additional RPCs behind the consumer's
back: cold-cache entity resolution (``ResolveUsername``), paginated writes
(``delete_messages`` -> N chunk requests), upload parts, retries inside
Telethon's ``_call`` loop, and calls from native objects
(``Message.reply``) or event-builder resolution. None of those pass the
consumer's own limiter, so no amount of limiter arithmetic on public calls
can prevent the traffic.

The mechanism here is the library-side fix: it wraps the one Telethon seam
every outgoing RPC goes through — ``MTProtoSender.send`` on each sender the
client dispatches on (the main sender, exported senders handed out by
``_borrow_exported_sender`` for downloads, and CDN senders) — and charges
each actual dispatch attempt against a shared :class:`TelegramRateLimitGate`
plus consumer-supplied admission hooks (e.g. the username budget).

Opt-in per client via :func:`attach_rpc_gate`; detaching restores the exact
original send callables. Refusals raise :class:`TelegramRpcDeferredError`, a
``TelegramRateLimitedError`` subclass, so every existing "operation
unavailable, move on" handler absorbs them unchanged. The boundary never
sleeps: deferral is the caller's decision (``TelegramRateLimitGate.acquire``
remains the documented way to wait), because sleeping inside Telethon's
dispatch would hold library-internal machinery hostage.

Defaults are deliberately narrow: only the send family, username resolves
and file transfer RPCs are priced (transfer reads/writes land in the
permissive ``default`` bucket, not in calibrated send/history quotas);
message-fetch pages stay exempt because ``wrap_messages_iterator`` already
owns the history budget — double-charging would silently halve it. Unknown
RPCs fall through to the warn-once ``default`` category (#10) and, under
``strict_categories=True`` (#29), fail closed with
``TelegramUnregisteredOperationError`` instead of becoming a silent bypass.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from telethon import errors, utils
from telethon.tl import functions as tl_functions

from telethon_floodgate._errors import TelegramRateLimitedError
from telethon_floodgate.peer import peer_key
from telethon_floodgate.rate_limit_gate import TelegramRateLimitGate

logger = logging.getLogger(__name__)

# Instance markers: the handle records itself on the client so a second
# attach fails loudly instead of stacking wrappers (#34 lifecycle), and the
# pristine send callable is parked on the sender so detach restores exactly
# what was there before — including a consumer-installed transport recorder.
_DISPATCH_ATTR = "_telethon_floodgate_dispatch"
_ORIGINAL_SEND_ATTR = "_telethon_floodgate_original_send"

# Flood families Telethon's _call itself special-cases: everything here is a
# real pacing signal from Telegram, worth exposing even above the consumer's
# own flood handling.
_FLOOD_ERRORS = (
    errors.FloodWaitError,
    errors.FloodPremiumWaitError,
    errors.SlowModeWaitError,
    errors.FloodTestPhoneWaitError,
)

# RPC classes priced out of the box, keyed by the actual TL class — the
# dispatch boundary classifies by the real RPC, not the outer operation tag
# (#34: "classify by the actual RPC rather than only the outer operation
# tag"). ``None`` means explicitly exempt: no charge, no warn-once noise.
#
# Tags reuse the calibrated categories; categories are backward compatible:
#   rpc_send -> send, rpc_resolve -> resolve (no-op category; the username
#   budget is a consumer admission hook), rpc_upload/rpc_download -> default.
# Transfers must NOT eat send/resolve/history quotas (#34), and read pages
# stay exempt because wrap_messages_iterator owns the history budget —
# re-pricing pages against the logical-history quota here would silently
# halve it ("do not silently reinterpret calibrated logical-history quotas
# as per-page quotas", #34).
DEFAULT_RPC_OPERATIONS: dict[object, str | None] = {
    tl_functions.messages.SendMessageRequest: "rpc_send",
    tl_functions.messages.SendMediaRequest: "rpc_send",
    tl_functions.messages.SendMultiMediaRequest: "rpc_send",
    tl_functions.messages.EditMessageRequest: "rpc_send",
    tl_functions.messages.ForwardMessagesRequest: "rpc_send",
    tl_functions.contacts.ResolveUsernameRequest: "rpc_resolve",
    tl_functions.upload.SaveFilePartRequest: "rpc_upload",
    tl_functions.upload.SaveBigFilePartRequest: "rpc_upload",
    tl_functions.upload.GetFileRequest: "rpc_download",
    tl_functions.upload.GetCdnFileRequest: "rpc_download",
    tl_functions.messages.GetHistoryRequest: None,
    tl_functions.messages.SearchRequest: None,
    tl_functions.messages.SearchGlobalRequest: None,
    tl_functions.messages.GetRepliesRequest: None,
    tl_functions.messages.GetScheduledHistoryRequest: None,
    tl_functions.messages.GetMessagesRequest: None,
    tl_functions.channels.GetMessagesRequest: None,
}


class TelegramRpcDeferredError(TelegramRateLimitedError):
    """Raised at the RPC dispatch boundary when a budget defers the RPC.

    Subclass of ``TelegramRateLimitedError`` per the project invariant: one
    "operation unavailable, move on" handler covers the proactive gate,
    per-peer buckets, the breaker, the username limiter and now every nested,
    event-driven or native-object RPC the boundary intercepts. The RPC was
    NOT dispatched; ``retry_after_sec`` is the defer the budgets returned.
    """

    def __init__(self, phone: str, operation: str, retry_after_sec: float, rpc: str) -> None:
        message = (
            f"rpc {rpc} deferred by {operation} budget for {phone}; "
            f"not dispatched, retry in {retry_after_sec:.1f}s"
        )
        super().__init__(phone, operation, retry_after_sec, message=message)
        self.rpc = rpc


@dataclass(frozen=True)
class RpcDispatchAdmission:
    """What is about to be dispatched; input for consumer admission hooks."""

    phone: str
    rpc: str
    operation: str
    category: str
    peer: str | None


@dataclass(frozen=True)
class RpcDispatchDecision:
    """Structured outcome of one admission, per dispatched member/attempt.

    Emitted for every admission (admitted or refused, including exempt RPCs)
    so a consumer can correlate boundary traffic with its outer operation.
    """

    phone: str
    rpc: str
    operation: str | None
    category: str | None
    peer: str | None
    admitted: bool
    defer_sec: float


@dataclass(frozen=True)
class RpcDispatchPolicy:
    """Classification of RPC classes into gate operation tags.

    ``operations`` maps a TL request class (or its ``__name__`` for
    duck-typed doubles) to a registered operation tag, or to ``None`` for an
    explicit exemption. RPC classes missing from the map classify as
    ``rpc:<Name>`` — unregistered, so the gate's warn-once default category
    (#10) applies, and ``strict_categories=True`` gates fail closed (#29).
    """

    operations: Mapping[object, str | None] = field(
        default_factory=lambda: dict(DEFAULT_RPC_OPERATIONS)
    )

    def operation_for(self, request: Any) -> str | None:
        request_type = type(request)
        if request_type in self.operations:
            return self.operations[request_type]
        if request_type.__name__ in self.operations:
            return self.operations[request_type.__name__]
        return f"rpc:{request_type.__name__}"


AdmissionHook = Callable[[RpcDispatchAdmission], float]
DecisionHook = Callable[[RpcDispatchDecision], None]
FloodHook = Callable[[float, str], None]


class RpcDispatchGate:
    """Live attachment of a gate to one client's dispatch boundary.

    Created by :func:`attach_rpc_gate`; ``detach`` restores every wrapped
    callable. Instances are not thread-safe — like everything in this
    package, they belong to the client's event loop.
    """

    def __init__(
        self,
        client: Any,
        gate: TelegramRateLimitGate,
        phone: str,
        *,
        policy: RpcDispatchPolicy | None = None,
        admissions: Sequence[AdmissionHook] = (),
        on_decision: DecisionHook | None = None,
        on_flood: FloodHook | None = None,
        flood_feedback: bool = False,
    ) -> None:
        self._client = client
        self._gate = gate
        self._phone = phone
        self._policy = policy if policy is not None else RpcDispatchPolicy()
        self._admissions = tuple(admissions)
        self._on_decision = on_decision
        self._on_flood = on_flood
        self._flood_feedback = flood_feedback
        self._patched_senders: list[tuple[Any, Any]] = []
        self._patched_methods: list[tuple[Any, str, Any | None]] = []

    def attach(self) -> None:
        if getattr(self._client, _DISPATCH_ATTR, None) is not None:
            raise ValueError("client is already attached to an rpc dispatch gate")
        try:
            self._wrap_send(self._client._sender)
            self._wrap_method(
                "_borrow_exported_sender", lambda sender: self._wrap_send(sender)
            )
            self._wrap_method(
                "_get_cdn_client", lambda cdn_client: self._wrap_send(cdn_client._sender)
            )
        except BaseException:
            self._restore()
            raise
        setattr(self._client, _DISPATCH_ATTR, self)

    def detach(self) -> None:
        if getattr(self._client, _DISPATCH_ATTR, None) is not self:
            raise ValueError("client is not attached to this rpc dispatch gate")
        self._restore()
        delattr(self._client, _DISPATCH_ATTR)

    # -- wrapping ------------------------------------------------------------

    def _wrap_send(self, sender: Any) -> None:
        if getattr(sender, _ORIGINAL_SEND_ATTR, None) is not None:
            return  # borrowed senders are cached per DC: wrap once
        original = sender.send

        def gated_send(request: Any, ordered: bool = False) -> Any:
            return self._dispatch(original, request, ordered)

        setattr(sender, _ORIGINAL_SEND_ATTR, original)
        sender.send = gated_send
        self._patched_senders.append((sender, original))

    def _wrap_method(self, name: str, on_result: Callable[[Any], None]) -> None:
        original = getattr(self._client, name)
        had_own = name in vars(self._client)

        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            result = await original(*args, **kwargs)
            on_result(result)
            return result

        # Records whether the instance carried its own attribute before us so
        # detach restores an instance attr (e.g. a test double) or removes
        # ours to reveal the class method again.
        self._patched_methods.append((self._client, name, original if had_own else None))
        setattr(self._client, name, wrapped)

    def _restore(self) -> None:
        for sender, original in self._patched_senders:
            sender.send = original
            delattr(sender, _ORIGINAL_SEND_ATTR)
        self._patched_senders.clear()
        for client, name, own_original in self._patched_methods:
            if own_original is None:
                delattr(client, name)
            else:
                setattr(client, name, own_original)
        self._patched_methods.clear()

    # -- admission -----------------------------------------------------------

    def _dispatch(self, original: Callable[..., Any], request: Any, ordered: bool) -> Any:
        single = not utils.is_list_like(request)
        members = [request] if single else list(request)

        decisions: list[RpcDispatchDecision] = []
        for rpc in members:
            rpc_name = type(rpc).__name__
            operation = self._policy.operation_for(rpc)
            if operation is None:  # explicit exemption: no charge, no hook
                decisions.append(self._decision(rpc_name, None, None, None, True, 0.0))
                continue
            category = self._gate.category_for(operation)
            peer = peer_key(getattr(rpc, "peer", None)) or peer_key(
                getattr(rpc, "to_peer", None)
            )
            defer = self._gate.try_acquire(self._phone, category, peer=peer)
            admission = RpcDispatchAdmission(
                phone=self._phone, rpc=rpc_name, operation=operation,
                category=category, peer=peer,
            )
            for hook in self._admissions:
                hook_defer = float(hook(admission))
                if hook_defer > defer:
                    defer = hook_defer
            decisions.append(
                self._decision(rpc_name, operation, category, peer, defer <= 0, defer)
            )
            if defer > 0:
                for decision in decisions:
                    self._emit(decision)
                raise TelegramRpcDeferredError(self._phone, operation, defer, rpc_name)

        for decision in decisions:
            self._emit(decision)

        futures = original(request, ordered=ordered)
        observed = (
            zip(members, futures) if isinstance(futures, list) else [(members[0], futures)]
        )
        for rpc, future in observed:
            future.add_done_callback(
                lambda fut, rpc_name=type(rpc).__name__: self._observe_flood(rpc_name, fut)
            )
        return futures

    def _decision(
        self,
        rpc: str,
        operation: str | None,
        category: str | None,
        peer: str | None,
        admitted: bool,
        defer_sec: float,
    ) -> RpcDispatchDecision:
        return RpcDispatchDecision(
            phone=self._phone,
            rpc=rpc,
            operation=operation,
            category=category,
            peer=peer,
            admitted=admitted,
            defer_sec=defer_sec,
        )

    def _emit(self, decision: RpcDispatchDecision) -> None:
        if self._on_decision is None:
            return
        try:
            self._on_decision(decision)
        except Exception:  # noqa: BLE001 — metadata must never break dispatch
            logger.warning(
                "on_decision callback raised for %s", decision.rpc, exc_info=True
            )

    def _observe_flood(self, rpc_name: str, future: Any) -> None:
        if future.cancelled():
            return
        exception = future.exception()
        if not isinstance(exception, _FLOOD_ERRORS):
            return
        seconds = float(getattr(exception, "seconds", 0.0) or 0.0)
        if self._on_flood is not None:
            try:
                self._on_flood(seconds, rpc_name)
            except Exception:  # noqa: BLE001 — metadata must never break dispatch
                logger.warning(
                    "on_flood callback raised for %s", rpc_name, exc_info=True
                )
        if self._flood_feedback:
            self._gate.note_flood(self._phone, seconds)


def attach_rpc_gate(
    client: Any,
    gate: TelegramRateLimitGate,
    phone: str,
    *,
    policy: RpcDispatchPolicy | None = None,
    admissions: Sequence[AdmissionHook] = (),
    on_decision: DecisionHook | None = None,
    on_flood: FloodHook | None = None,
    flood_feedback: bool = False,
) -> RpcDispatchGate:
    """Enforce flood control at the client's RPC dispatch boundary (#34).

    Every RPC the client dispatches — public calls, nested entity resolves,
    paginated writes, upload/download parts, exported/CDN senders, native
    object and event-builder traffic, each actual retry attempt — is charged
    once against ``gate`` (per ``phone``) and each ``admissions`` hook before
    it is sent. An exhausted budget raises :class:`TelegramRpcDeferredError`
    and nothing is dispatched.

    ``phone`` is the caller-supplied account identity: concurrent clients of
    the same account share the gate's budgets; reconnections keep the same
    sender instance, so budgets never reset. Attaching twice raises;
    ``detach`` on the returned handle restores the original send callables.

    Callbacks are synchronous and event-loop affine: ``on_decision`` sees one
    :class:`RpcDispatchDecision` per dispatched member/attempt, ``on_flood``
    sees ``(seconds, rpc_name)`` for pacing floods surfacing through the
    boundary. ``flood_feedback=True`` additionally feeds those waits into
    ``gate.note_flood`` — enable it only when no call site ALSO reports the
    same flood via ``handle_flood_wait(..., gate=...)``, or backoff
    double-counts (see the gate docs).

    ``admissions`` hooks receive an :class:`RpcDispatchAdmission` and return
    seconds-to-defer (``0.0`` = admitted), mirroring ``try_acquire``; a
    positive return refuses the dispatch. This is where an existing username
    budget plugs in: hidden ``ResolveUsername`` fan-out stops dispatching
    once the budget is exhausted.
    """
    handle = RpcDispatchGate(
        client,
        gate,
        phone,
        policy=policy,
        admissions=admissions,
        on_decision=on_decision,
        on_flood=on_flood,
        flood_feedback=flood_feedback,
    )
    handle.attach()
    return handle
