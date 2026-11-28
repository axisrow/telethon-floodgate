"""Telethon dispatch admission, including nested RPCs and native objects.

Attach before connecting. The default path is synchronous. Optional wait
budgets preserve MTProtoSender.send's Future/list[Future] return contract.
Final checks, commits and sender invocation never yield to another caller.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Literal

from telethon import errors, utils
from telethon.tl import functions as tl_functions

from telethon_floodgate._errors import TelegramRateLimitedError
from telethon_floodgate.peer import peer_key
from telethon_floodgate.rate_limit_gate import TelegramRateLimitGate

logger = logging.getLogger(__name__)
_DISPATCH_ATTR = "_telethon_floodgate_dispatch"
_ORIGINAL_SEND_ATTR = "_telethon_floodgate_original_send"
_FLOOD_ERRORS = (
    errors.FloodWaitError, getattr(errors, "FloodPremiumWaitError", errors.FloodWaitError),
    errors.SlowModeWaitError, errors.FloodTestPhoneWaitError,
)
_QUERY_WRAPPERS = frozenset({
    "InvokeWithLayerRequest", "InvokeWithoutUpdatesRequest", "InitConnectionRequest",
    "InvokeAfterMsgRequest", "InvokeAfterMsgsRequest", "InvokeWithTakeoutRequest",
    "InvokeWithBusinessConnectionRequest", "InvokeWithGooglePlayIntegrityRequest",
    "InvokeWithApnsSecretRequest", "InvokeWithMessagesRangeRequest",
})
_INFRASTRUCTURE = frozenset({
    tl_functions.PingRequest, tl_functions.PingDelayDisconnectRequest,
    tl_functions.help.GetConfigRequest, tl_functions.help.GetCdnConfigRequest,
    tl_functions.auth.ExportAuthorizationRequest, tl_functions.auth.ImportAuthorizationRequest,
})
# None exempts a category charge, NOT common consumer budgets. Protocol
# infrastructure is a separate, narrow leaf-constructor exemption.
DEFAULT_RPC_OPERATIONS: dict[object, str | None] = {
    tl_functions.messages.SendMessageRequest: "rpc_send",
    tl_functions.messages.SendMediaRequest: "rpc_send",
    tl_functions.messages.SendMultiMediaRequest: "rpc_send",
    tl_functions.messages.EditMessageRequest: "rpc_send",
    tl_functions.messages.ForwardMessagesRequest: "rpc_send",
    tl_functions.messages.DeleteMessagesRequest: "rpc_send",
    tl_functions.channels.DeleteMessagesRequest: "rpc_send",
    tl_functions.messages.DeleteScheduledMessagesRequest: "rpc_send",
    tl_functions.messages.UpdatePinnedMessageRequest: "rpc_send",
    tl_functions.contacts.ResolveUsernameRequest: "rpc_resolve",
    tl_functions.upload.SaveFilePartRequest: "rpc_upload",
    tl_functions.upload.SaveBigFilePartRequest: "rpc_upload",
    # messages.uploadMedia closes every media upload after its parts
    # (tg_content_factory#1985): unregistered, it fell into "default" with a
    # warn-once line on the first album.
    tl_functions.messages.UploadMediaRequest: "rpc_upload",
    tl_functions.upload.GetFileRequest: "rpc_download",
    tl_functions.upload.GetCdnFileRequest: "rpc_download",
    # The CDN leg of a download: its hashes and the re-upload it may ask for.
    tl_functions.upload.GetCdnFileHashesRequest: "rpc_download",
    tl_functions.upload.GetFileHashesRequest: "rpc_download",
    tl_functions.upload.ReuploadCdnFileRequest: "rpc_download",
    tl_functions.upload.GetWebFileRequest: "rpc_download",
    tl_functions.messages.GetHistoryRequest: None,
    tl_functions.messages.SearchRequest: None,
    tl_functions.messages.SearchGlobalRequest: None,
    tl_functions.messages.GetRepliesRequest: None,
    tl_functions.messages.GetScheduledHistoryRequest: None,
    tl_functions.messages.GetMessagesRequest: None,
    tl_functions.channels.GetMessagesRequest: None,
    tl_functions.messages.GetDialogsRequest: "rpc_dialogs_page",
}
# Names keep optional constructors compatible with older supported Telethon.
DEFAULT_RPC_OPERATIONS.update({
    "CreateChannelRequest": "rpc_channel_lifecycle", "CreateChatRequest": "rpc_channel_lifecycle",
    "DeleteChannelRequest": "telegram_delete_channel", "DeleteChatUserRequest": "telegram_delete_chat",
    "LeaveChannelRequest": "telegram_delete_channel", "JoinChannelRequest": "telegram_join_channel",
    "ImportChatInviteRequest": "telegram_join_channel", "EditAdminRequest": "rpc_admin_action",
    "EditBannedRequest": "rpc_admin_action", "InviteToChannelRequest": "rpc_admin_action",
    "GetMessageReactionsListRequest": "default", "GetParticipantsRequest": "default",
    "GetUserPhotosRequest": "default", "GetAdminLogRequest": "default",
    "GetUsersRequest": "default", "GetChannelsRequest": "default", "GetChatsRequest": "default",
    "ReadHistoryRequest": "default", "ReadDiscussionRequest": "default",
    "ReadMentionsRequest": "default", "ReadReactionsRequest": "default", "SendReactionRequest": "reaction",
    "GetStateRequest": "default", "GetDifferenceRequest": "default", "GetChannelDifferenceRequest": "default",
    "EditChatAdminRequest": "rpc_admin_action", "EditChatDefaultBannedRightsRequest": "rpc_admin_action",
    "EditPeerFoldersRequest": "rpc_admin_action", "UpdateUsernameRequest": "telegram_update_channel_username",
    "SendScheduledMessagesRequest": "rpc_send", "DeleteHistoryRequest": "rpc_channel_lifecycle",
})
DEFAULT_RPC_OPERATIONS.update({key: "rpc_default" for key, value in DEFAULT_RPC_OPERATIONS.items()
                               if value == "default"})
DEFAULT_RPC_OPERATIONS["SendReactionRequest"] = "rpc_reaction"
_READ_ONLY_NAMES = frozenset({
    "ResolveUsernameRequest", "ResolvePhoneRequest", "CheckChatInviteRequest",
    "GetHistoryRequest", "SearchRequest", "SearchGlobalRequest", "GetRepliesRequest",
    "GetScheduledHistoryRequest", "GetMessagesRequest", "GetDialogsRequest",
    "GetParticipantsRequest", "GetUserPhotosRequest", "GetAdminLogRequest",
    "GetUsersRequest", "GetChannelsRequest", "GetChatsRequest", "GetMessageReactionsListRequest",
    "GetFullChannelRequest", "GetFullChatRequest", "GetFullUserRequest",
    "GetForumTopicsRequest", "GetForumTopicsByIDRequest", "GetFileRequest", "GetCdnFileRequest",
    "GetCdnFileHashesRequest", "GetFileHashesRequest", "GetStateRequest", "GetDifferenceRequest",
    "GetChannelDifferenceRequest", "GetAllDraftsRequest", "GetSavedGifsRequest", "GetInlineBotResultsRequest",
})


def _leaf(request: Any) -> Any:
    seen: set[int] = set()
    while type(request).__name__ in _QUERY_WRAPPERS and id(request) not in seen:
        seen.add(id(request))
        request = request.query
    return request


@dataclass(frozen=True)
class RpcDispatchAdmission:
    phone: str
    rpc: str
    operation: str
    category: str | None
    peer: str | None
    read_only: bool = False
    infrastructure: bool = False


@dataclass(frozen=True)
class RpcDispatchDecision:
    phone: str
    rpc: str
    operation: str | None
    category: str | None
    peer: str | None
    admitted: bool
    defer_sec: float
    dispatched: bool = False
    read_only: bool = False


@dataclass(frozen=True)
class RpcDispatchBudget:
    """A two-phase budget over a complete application-RPC batch.

    peek_batch returns a nonnegative delay without spending. commit_batch
    cannot refuse or yield. Share state across clients. wait budgets enable
    asynchronous pacing; defer budgets refuse. max_batch_size prevents an
    impossible wait. prepare optionally refreshes external state before checks.
    Legacy admissions are validation callbacks; use this API for mutable state.
    """
    peek_batch: Callable[[Sequence[RpcDispatchAdmission]], float]
    commit_batch: Callable[[Sequence[RpcDispatchAdmission]], None]
    name: str = "consumer"
    mode: Literal["defer", "wait"] = "defer"
    max_batch_size: int | None = None
    prepare: Callable[[Sequence[RpcDispatchAdmission]], Awaitable[None]] | None = None
    applies: Callable[[RpcDispatchAdmission], bool] = lambda admission: True


class TelegramRpcDeferredError(TelegramRateLimitedError):
    """A local refusal: the whole dispatch batch is still unsent."""

    def __init__(self, phone: str, operation: str, retry_after_sec: float, rpc: str, *,
                 category: str | None = None, budget: str = "gate") -> None:
        super().__init__(phone, category or operation, retry_after_sec,
                         message=f"rpc {rpc} deferred by {budget}; not dispatched, retry in {retry_after_sec:.1f}s")
        self.rpc, self.operation, self.budget = rpc, operation, budget
        self.rpc_category = category
        self.source, self.rpc_dispatched = "local", False


@dataclass(frozen=True)
class RpcDispatchPolicy:
    operations: Mapping[object, str | None] = field(default_factory=lambda: dict(DEFAULT_RPC_OPERATIONS))

    def operation_for(self, request: Any) -> str | None:
        request_type = type(request)
        if request_type in self.operations:
            return self.operations[request_type]
        return self.operations.get(request_type.__name__, f"rpc:{request_type.__name__}")


AdmissionHook = Callable[[RpcDispatchAdmission], float]
DecisionHook = Callable[[RpcDispatchDecision], None]
FloodHook = Callable[[float, str], None]
ResultHook = Callable[[RpcDispatchAdmission, BaseException | None], None]


class RpcDispatchGate:
    """One attachment per client; account budgets survive reconnections."""

    def __init__(self, client: Any, gate: TelegramRateLimitGate, phone: str, *,
                 policy: RpcDispatchPolicy | None = None, admissions: Sequence[AdmissionHook] = (),
                 budgets: Sequence[RpcDispatchBudget] = (), on_decision: DecisionHook | None = None,
                 on_flood: FloodHook | None = None, on_result: ResultHook | None = None,
                 flood_feedback: bool = False,
                 gate_wait_max_sec: float = 0.0,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self._client, self._gate, self._phone = client, gate, phone
        self._policy = policy or RpcDispatchPolicy()
        self._admissions, self._budgets = tuple(admissions), tuple(budgets)
        self._on_decision, self._on_flood, self._on_result = on_decision, on_flood, on_result
        self._flood_feedback, self._sleep = flood_feedback, sleep
        self._gate_wait_max_sec = self._delay(gate_wait_max_sec)
        self._patched_senders: list[tuple[Any, Any]] = []
        self._patched_methods: list[tuple[Any, str, Any | None]] = []
        self._pending: set[asyncio.Task] = set()
        self._attached = False
        if any(b.mode not in {"wait", "defer"} for b in budgets):
            raise ValueError("budget mode must be wait or defer")

    def attach(self) -> None:
        if getattr(self._client, _DISPATCH_ATTR, None) is not None:
            raise ValueError("client is already attached to an rpc dispatch gate")
        try:
            self._wrap_send(self._client._sender)
            self._wrap_method("_borrow_exported_sender", self._wrap_send)
            self._wrap_method("_get_cdn_client", lambda client: self._wrap_send(client._sender))
        except BaseException:
            self._restore()
            raise
        self._attached = True
        setattr(self._client, _DISPATCH_ATTR, self)

    def detach(self) -> None:
        if getattr(self._client, _DISPATCH_ATTR, None) is not self:
            raise ValueError("client is not attached to this rpc dispatch gate")
        self._attached = False
        for task in tuple(self._pending):
            task.cancel()
        self._restore()
        delattr(self._client, _DISPATCH_ATTR)

    def _wrap_send(self, sender: Any) -> None:
        if getattr(sender, _ORIGINAL_SEND_ATTR, None) is not None:
            return
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

        self._patched_methods.append((self._client, name, original if had_own else None))
        setattr(self._client, name, wrapped)

    def _restore(self) -> None:
        for sender, original in self._patched_senders:
            sender.send = original
            delattr(sender, _ORIGINAL_SEND_ATTR)
        self._patched_senders.clear()
        for client, name, original in self._patched_methods:
            if original is None:
                delattr(client, name)
            else:
                setattr(client, name, original)
        self._patched_methods.clear()

    def _classify(self, request: Any) -> RpcDispatchAdmission:
        leaf = _leaf(request)
        rpc = type(leaf).__name__
        infrastructure = type(leaf) in _INFRASTRUCTURE
        operation = None if infrastructure else self._policy.operation_for(leaf)
        category = self._gate.category_for(operation) if operation is not None else None
        peer = peer_key(getattr(leaf, "peer", None)) or peer_key(getattr(leaf, "to_peer", None))
        return RpcDispatchAdmission(self._phone, rpc, operation or f"rpc:{rpc}", category, peer,
                                    rpc in _READ_ONLY_NAMES or infrastructure, infrastructure)

    @staticmethod
    def _delay(value: float) -> float:
        delay = float(value)
        if not isfinite(delay) or delay < 0:
            raise ValueError("budget delay must be finite and non-negative")
        return delay

    def _check(self, members: Sequence[RpcDispatchAdmission]) -> float:
        priced = [(m.category, m.peer) for m in members if m.category is not None]
        delay = self._gate.peek_batch(self._phone, priced)
        if delay > 0 and (self._gate_wait_max_sec == 0 or delay > self._gate_wait_max_sec):
            self._refuse(members, delay, "gate", refused=next(m for m in members if m.category is not None))
        waiting = delay
        for budget in self._budgets:
            selected = [m for m in members if budget.applies(m)]
            if not selected:
                continue
            delay = self._delay(budget.peek_batch(selected))
            if budget.max_batch_size is not None and len(selected) > budget.max_batch_size:
                self._refuse(members, max(delay, 1.0), budget.name, refused=selected[0])
            if delay > 0 and budget.mode == "defer":
                self._refuse(members, delay, budget.name, refused=selected[0])
            waiting = max(waiting, delay)
        if waiting > 0:
            return waiting
        for member in members:
            for hook in self._admissions:
                delay = self._delay(hook(member))
                if delay > 0:
                    self._refuse(members, delay, "admission", refused=member)
        return 0.0

    def _refuse(self, members: Sequence[RpcDispatchAdmission], delay: float, budget: str,
                *, refused: RpcDispatchAdmission | None = None) -> None:
        refused = refused or members[0]
        for member in members:
            self._emit(member, admitted=False, delay=delay)
        exc = TelegramRpcDeferredError(self._phone, refused.operation, delay, refused.rpc,
                                      category=refused.category, budget=budget)
        exc.rpc_read_only = refused.read_only
        raise exc

    def _send(self, original: Callable, request: Any, ordered: bool,
              members: Sequence[RpcDispatchAdmission]) -> Any:
        application = [m for m in members if not m.infrastructure]
        self._gate.commit_batch(self._phone, [(m.category, m.peer) for m in application if m.category is not None])
        for budget in self._budgets:
            selected = [m for m in application if budget.applies(m)]
            if selected:
                budget.commit_batch(selected)
        # No await between the final check, commits and sender invocation.
        for member in members:
            self._emit(member, admitted=True, dispatched=True)
        try:
            futures = original(request, ordered=ordered)
        except BaseException as exc:
            for member in members:
                self._result(member, exc)
            raise
        actual = futures if isinstance(futures, list) else [futures]
        for member, future in zip(members, actual):
            if future.done():
                self._observe(member, future)
            else:
                future.add_done_callback(lambda fut, m=member: self._observe(m, fut))
        return futures

    def _dispatch(self, original: Callable, request: Any, ordered: bool) -> Any:
        batch = utils.is_list_like(request)
        request = list(request) if batch else request
        members = [self._classify(r) for r in request] if batch else [self._classify(request)]
        application = [m for m in members if not m.infrastructure]
        if not application:
            return self._send(original, request, ordered, members)
        if self._gate_wait_max_sec > 0 or any(b.mode == "wait" or b.prepare is not None for b in self._budgets):
            return self._schedule(original, request, ordered, members, application, batch)
        self._check(application)
        return self._send(original, request, ordered, members)

    def _schedule(self, original: Callable, request: Any, ordered: bool,
                  members: Sequence[RpcDispatchAdmission], application: Sequence[RpcDispatchAdmission],
                  batch: bool) -> Any:
        loop = asyncio.get_running_loop()
        outputs = [loop.create_future() for _ in members]
        # Telethon stops awaiting a batch on non-RPC exceptions (local refusal
        # included). Observe the sibling failures too, without hiding them
        # from anyone who does await those futures.
        for output in outputs:
            output.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        sent = False

        async def dispatch() -> None:
            nonlocal sent
            try:
                while True:
                    for budget in self._budgets:
                        if budget.prepare is not None:
                            await budget.prepare([m for m in application if budget.applies(m)])
                    delay = self._check(application)
                    if delay <= 0:
                        break
                    await self._sleep(delay)
                if any(f.cancelled() for f in outputs) or not self._attached:
                    raise asyncio.CancelledError
                actual = self._send(original, request, ordered, members)
                sent = True
                inputs = actual if isinstance(actual, list) else [actual]
                for output, future in zip(outputs, inputs):
                    future.add_done_callback(lambda f, out=output: relay(f, out))
                    output.add_done_callback(lambda out, f=future: f.cancel() if out.cancelled() else None)
            except asyncio.CancelledError:
                for output in outputs:
                    output.cancel()
            except Exception as exc:
                for output in outputs:
                    if not output.done():
                        output.set_exception(exc)

        def relay(future: asyncio.Future, output: asyncio.Future) -> None:
            if future.cancelled():
                output.cancel()
                return
            exception = future.exception()
            if output.done():
                return
            if exception is not None:
                output.set_exception(exception)
            else:
                output.set_result(future.result())

        task = loop.create_task(dispatch(), name="telethon-rpc-admission")
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        task.add_done_callback(lambda done: [f.cancel() for f in outputs] if done.cancelled() else None)
        for output in outputs:
            output.add_done_callback(lambda f: task.cancel() if f.cancelled() and not sent else None)
        return outputs if batch else outputs[0]

    def _emit(self, member: RpcDispatchAdmission, *, admitted: bool, delay: float = 0,
              dispatched: bool = False) -> None:
        if self._on_decision is not None:
            decision = RpcDispatchDecision(member.phone, member.rpc,
                                           None if member.infrastructure else member.operation,
                                           member.category, member.peer, admitted, delay, dispatched, member.read_only)
            self._callback(self._on_decision, decision)

    @staticmethod
    def _callback(callback: Callable, *args: Any) -> None:
        try:
            callback(*args)
        except Exception:
            logger.warning("RPC observer failed", exc_info=True)

    def _observe(self, member: RpcDispatchAdmission, future: asyncio.Future) -> None:
        self._result(member, asyncio.CancelledError() if future.cancelled() else future.exception())

    def _result(self, member: RpcDispatchAdmission, exception: BaseException | None) -> None:
        if exception is not None:
            exception.rpc = member.rpc
            exception.rpc_category = member.category
            exception.rpc_read_only = member.read_only
            exception.rpc_dispatched = True
            exception.source = "telegram" if isinstance(exception, _FLOOD_ERRORS) else "transport"
        if self._on_result is not None and not member.infrastructure:
            self._callback(self._on_result, member, exception)
        if isinstance(exception, _FLOOD_ERRORS):
            seconds = float(getattr(exception, "seconds", 0.0) or 0.0)
            if self._on_flood is not None:
                self._callback(self._on_flood, seconds, member.rpc)
            if self._flood_feedback:
                self._gate.note_flood(self._phone, seconds)


def attach_rpc_gate(client: Any, gate: TelegramRateLimitGate, phone: str, *,
                    policy: RpcDispatchPolicy | None = None, admissions: Sequence[AdmissionHook] = (),
                    budgets: Sequence[RpcDispatchBudget] = (), on_decision: DecisionHook | None = None,
                    on_flood: FloodHook | None = None, on_result: ResultHook | None = None,
                    flood_feedback: bool = False,
                    gate_wait_max_sec: float = 0.0,
                    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> RpcDispatchGate:
    """Attach before connect; use two-phase budgets for mutable limiter state.

    Legacy admissions remain non-mutating validation predicates. Wait budgets
    recheck all budgets after waiting. Each actual RPC retry is charged again;
    this integration never introduces automatic write replay.
    """
    handle = RpcDispatchGate(client, gate, phone, policy=policy, admissions=admissions,
                             budgets=budgets, on_decision=on_decision, on_flood=on_flood,
                             on_result=on_result, flood_feedback=flood_feedback,
                             gate_wait_max_sec=gate_wait_max_sec, sleep=sleep)
    handle.attach()
    return handle
