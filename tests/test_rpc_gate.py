"""Offline contract tests for the RPC dispatch-boundary gate (issue #34).

Real Telethon internals with a recording sender: the recorder replaces
``client._sender.send`` BEFORE the gate attaches, so the gate wraps the
recorder and every counted admission sits between Telethon's dispatch and
the recording transport. Mocking ``send_message``/``get_input_entity``
themselves would prove nothing about nested-RPC coverage.
"""

import asyncio
import datetime

import pytest
from telethon import TelegramClient, errors, functions, types, utils
from telethon.sessions import MemorySession

from telethon_floodgate import (
    RateLimitSpec,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
    TelegramRpcDeferredError,
    TelegramUnregisteredOperationError,
    attach_rpc_gate,
)

PHONE = "+15550001"


class RecordingSender:
    """Stands in for MTProtoSender.send; deterministic TL responses by type."""

    def __init__(self) -> None:
        self.dispatches: list[object] = []
        self._responses: dict[type, object] = {}
        self._flood_then: dict[type, object] = {}

    def respond(self, request_type: type, response: object) -> None:
        self._responses[request_type] = response

    def flood_then(self, request_type: type, response: object) -> None:
        self._flood_then[request_type] = response

    def send(self, request, ordered=False):
        requests = list(request) if utils.is_list_like(request) else [request]
        futures = []
        for single in requests:
            future = asyncio.get_running_loop().create_future()
            first = self._flood_then.pop(type(single), None)
            response = first if first is not None else self._responses.get(type(single))
            if response is None:
                raise AssertionError(f"no canned response for {type(single).__name__}")
            if isinstance(response, BaseException):
                future.set_exception(response)
            else:
                future.set_result(response)
            futures.append(future)
        self.dispatches.extend(requests)
        return futures if utils.is_list_like(request) else futures[0]


def make_client() -> tuple[TelegramClient, RecordingSender]:
    client = TelegramClient(MemorySession(), 1, "test")
    recorder = RecordingSender()
    client._sender.send = recorder.send
    return client, recorder


def canned_user(user_id: int = 7, username: str = "newuser") -> types.User:
    return types.User(id=user_id, access_hash=42, username=username, first_name="New")


def install_defaults(recorder: RecordingSender) -> None:
    recorder.respond(
        functions.contacts.ResolveUsernameRequest,
        types.contacts.ResolvedPeer(
            peer=types.PeerUser(7), chats=[], users=[canned_user()]
        ),
    )
    recorder.respond(
        functions.messages.SendMessageRequest,
        types.UpdateShortSentMessage(
            id=5, pts=0, pts_count=0,
            date=datetime.datetime.now(datetime.timezone.utc), out=True
        ),
    )
    recorder.respond(functions.upload.SaveFilePartRequest, True)
    recorder.respond(functions.upload.SaveBigFilePartRequest, True)


def lifted_peer_gate(**kwargs) -> TelegramRateLimitGate:
    """Gate whose per-peer 1/s send bucket is widened: tests here exercise
    per-attempt accounting, not the production one-message-per-second pacing
    (that has its own suite in test_rate_limit_gate)."""
    return TelegramRateLimitGate(
        peer_limits={"send:user": RateLimitSpec(10, 60.0)}, **kwargs
    )


async def _instant_sleep(_seconds) -> None:
    return None


# --- The red-test contract: nested RPC interception -------------------------


async def test_send_message_charges_nested_resolve_and_send(monkeypatch):
    """send_message with a cold username resolves the entity INSIDE the call —
    both the hidden resolve and the outer send pass the dispatch boundary."""
    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    client, recorder = make_client()
    install_defaults(recorder)
    gate = lifted_peer_gate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client.send_message("newuser", "hi")

    assert [d.rpc for d in decisions] == [
        "ResolveUsernameRequest",
        "SendMessageRequest",
    ]
    assert len(recorder.dispatches) == 2
    assert gate.snapshot(PHONE)["categories"]["send"]["used"] == 1
    handle.detach()


async def test_exhausted_username_budget_blocks_whole_send():
    """A consumer-supplied budget (e.g. the username limiter) refusing the
    nested resolve must prevent the whole send_message — zero dispatches."""
    client, recorder = make_client()
    install_defaults(recorder)
    gate = TelegramRateLimitGate()
    seen: list = []

    def exhausted(admission):
        seen.append(admission)
        return 12.0

    handle = attach_rpc_gate(client, gate, PHONE, admissions=[exhausted])

    with pytest.raises(TelegramRateLimitedError):
        await client.send_message("newuser", "hi")

    assert recorder.dispatches == []
    assert seen[0].rpc == "ResolveUsernameRequest"
    handle.detach()


async def test_send_budget_blocks_second_dispatch():
    client, recorder = make_client()
    install_defaults(recorder)
    gate = TelegramRateLimitGate(category_limits={"send": RateLimitSpec(1, 60.0)})
    handle = attach_rpc_gate(client, gate, PHONE)

    await client.send_message("newuser", "one")
    with pytest.raises(TelegramRpcDeferredError):
        await client.send_message("newuser", "two")

    assert len(recorder.dispatches) == 2  # resolve + first send; second refused
    handle.detach()


async def test_warm_cache_resolve_is_free():
    """Second send to the same username hits the session cache: no resolve
    dispatch and no resolve admission."""
    client, recorder = make_client()
    install_defaults(recorder)
    gate = lifted_peer_gate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client.send_message("newuser", "one")
    await client.send_message("newuser", "two")

    assert [d.rpc for d in decisions].count("ResolveUsernameRequest") == 1  # cold only
    assert len(recorder.dispatches) == 3  # resolve + send + send
    handle.detach()


async def test_native_message_reply_is_charged():
    """Message objects returned from a guarded send keep their client; their
    reply() must dispatch through the gate, not around it."""
    client, recorder = make_client()
    install_defaults(recorder)
    gate = lifted_peer_gate(category_limits={"send": RateLimitSpec(2, 60.0)})
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    message = await client.send_message("newuser", "hi")
    await message.reply("answer")

    assert [d.rpc for d in decisions if d.rpc == "SendMessageRequest"] and len(
        [d for d in decisions if d.rpc == "SendMessageRequest"]
    ) == 2
    assert len(recorder.dispatches) == 3  # resolve + send + reply-send
    handle.detach()


async def test_batch_charged_per_member():
    """250 ids -> 3 DeleteMessages RPCs in one ordered call: three admissions,
    three actual dispatches."""
    client, recorder = make_client()
    install_defaults(recorder)
    recorder.respond(
        functions.messages.DeleteMessagesRequest,
        types.messages.AffectedMessages(pts=0, pts_count=0),
    )
    gate = TelegramRateLimitGate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    # Warm the entity cache so the batch itself is the only RPC traffic.
    await client.send_message("newuser", "warm")
    await client.delete_messages("newuser", list(range(250)))

    assert len([d for d in decisions if d.rpc == "DeleteMessagesRequest"]) == 3
    assert len(recorder.dispatches) == 5  # resolve + warm send + 3 delete chunks
    handle.detach()


async def test_retry_attempt_charged_again(monkeypatch):
    """Telethon's internal flood retry (wait <= flood_sleep_threshold) is a
    real second dispatch and must be charged again."""
    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    client, recorder = make_client()
    install_defaults(recorder)
    recorder.flood_then(
        functions.messages.SendMessageRequest,
        errors.FloodWaitError(request=None, capture=1),
    )
    client.flood_sleep_threshold = 30
    gate = lifted_peer_gate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client.send_message("newuser", "hi")

    assert len([d for d in decisions if d.rpc == "SendMessageRequest"]) == 2
    assert len(recorder.dispatches) == 3  # resolve + flood attempt + retry
    handle.detach()


async def test_blocking_flood_metadata_reported():
    """A blocking FloodWait surfacing through the boundary is exposed via
    on_flood; without flood_feedback the gate budgets stay untouched."""
    client, recorder = make_client()
    install_defaults(recorder)
    recorder.flood_then(
        functions.messages.SendMessageRequest,
        errors.FloodWaitError(request=None, capture=90),
    )
    gate = TelegramRateLimitGate()
    floods: list = []
    handle = attach_rpc_gate(
        client, gate, PHONE, on_flood=lambda s, r: floods.append((s, r))
    )

    with pytest.raises(errors.FloodWaitError):
        await client.send_message("newuser", "hi")

    await asyncio.sleep(0)  # let the done-callbacks run on the loop
    assert floods == [(90.0, "SendMessageRequest")]
    assert gate.snapshot(PHONE)["flood_events_in_window"] == 0
    handle.detach()


async def test_flood_feedback_feeds_gate_backoff():
    client, recorder = make_client()
    install_defaults(recorder)
    recorder.flood_then(
        functions.messages.SendMessageRequest,
        errors.FloodWaitError(request=None, capture=90),
    )
    gate = TelegramRateLimitGate(flood_backoff=True)
    handle = attach_rpc_gate(client, gate, PHONE, flood_feedback=True)

    with pytest.raises(errors.FloodWaitError):
        await client.send_message("newuser", "hi")

    await asyncio.sleep(0)  # let the done-callbacks run on the loop
    assert gate.snapshot(PHONE)["flood_events_in_window"] == 1
    handle.detach()


async def test_upload_parts_do_not_consume_send_quota():
    """Upload part RPCs get their own operation; the send category stays at
    zero even though the dispatches went through the boundary."""
    client, recorder = make_client()
    install_defaults(recorder)
    gate = TelegramRateLimitGate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client.upload_file(b"x" * 600000, file_size=600000)

    assert len([d for d in decisions if d.operation == "rpc_upload"]) >= 2
    assert gate.snapshot(PHONE)["categories"]["send"]["used"] == 0
    handle.detach()


async def test_unknown_rpc_lands_in_default():
    client, recorder = make_client()
    recorder.respond(functions.users.GetUsersRequest, [canned_user()])
    gate = TelegramRateLimitGate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client.get_me()

    assert decisions[0].rpc == "GetUsersRequest"
    assert decisions[0].operation == "rpc:GetUsersRequest"
    assert decisions[0].category == "default"
    handle.detach()


async def test_strict_categories_fail_closed_on_unknown_rpc():
    client, recorder = make_client()
    recorder.respond(functions.users.GetUsersRequest, [canned_user()])
    gate = TelegramRateLimitGate(strict_categories=True)
    handle = attach_rpc_gate(client, gate, PHONE)

    with pytest.raises(TelegramUnregisteredOperationError):
        await client.get_me()

    assert recorder.dispatches == []
    handle.detach()


async def test_infrastructure_rpcs_exempt_even_in_strict_mode():
    """Keepalive ping (sender-dispatched every ~60s) and the connect-init
    invoke (every connection incl. auto-reconnect) must pass the boundary
    without charging anything — strict mode must not kill the keepalive
    task or reconnection over them."""
    client, recorder = make_client()
    pong = types.Pong(msg_id=1, ping_id=12345)
    recorder.respond(functions.PingRequest, pong)
    recorder.respond(functions.InvokeWithLayerRequest, pong)
    recorder.respond(functions.InvokeWithoutUpdatesRequest, pong)
    gate = TelegramRateLimitGate(strict_categories=True)
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    await client._sender.send(functions.PingRequest(12345))
    await client._sender.send(functions.InvokeWithLayerRequest(200, functions.help.GetConfigRequest()))
    await client._sender.send(functions.InvokeWithoutUpdatesRequest(functions.PingRequest(12346)))

    assert len(recorder.dispatches) == 3
    assert [d.operation for d in decisions] == [None, None, None]
    assert all(d.admitted for d in decisions)
    assert gate.snapshot(PHONE)["categories"]["default"]["used"] == 0
    handle.detach()


async def test_event_filter_resolution_goes_through_boundary():
    """Event builders resolve their chats/filters via the same
    get_input_entity primitive (_into_id_set is what NewMessage(chats=...)
    runs per dispatched update) — a hidden resolve per update, charged."""
    client, recorder = make_client()
    install_defaults(recorder)
    gate = TelegramRateLimitGate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    from telethon.events import common as events_common

    chats = await events_common._into_id_set(client, ["otheruser"])

    assert chats
    assert [d.rpc for d in decisions] == ["ResolveUsernameRequest"]
    assert len(recorder.dispatches) == 1
    handle.detach()


async def test_exhausted_budget_blocks_event_filter_resolution():
    client, recorder = make_client()
    install_defaults(recorder)
    gate = TelegramRateLimitGate()

    def exhausted(admission):
        return 5.0

    handle = attach_rpc_gate(client, gate, PHONE, admissions=[exhausted])

    from telethon.events import common as events_common

    with pytest.raises(TelegramRateLimitedError):
        await events_common._into_id_set(client, ["otheruser"])

    assert recorder.dispatches == []  # resolution never dispatched
    handle.detach()


async def test_exported_sender_is_registered():
    """Senders handed out by _borrow_exported_sender (downloads, big sends)
    get their own gated dispatch, keyed to the same phone and budgets."""

    class BorrowedSender:
        def __init__(self) -> None:
            self.dispatches: list = []

        def send(self, request, ordered=False):  # real senders: sync, returns future
            self.dispatches.append(request)
            future = asyncio.get_running_loop().create_future()
            future.set_result(True)
            return future

    client, recorder = make_client()
    install_defaults(recorder)

    async def fake_borrow(dc_id):
        return BorrowedSender()

    client._borrow_exported_sender = fake_borrow
    gate = TelegramRateLimitGate()
    decisions: list = []
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    borrowed = await client._borrow_exported_sender(2)

    request = functions.messages.SendMessageRequest(
        peer=types.InputPeerSelf(), message="x"
    )
    await borrowed.send(request)

    assert len(borrowed.dispatches) == 1
    assert [d.rpc for d in decisions] == ["SendMessageRequest"]
    assert gate.snapshot(PHONE)["categories"]["send"]["used"] == 1
    handle.detach()


async def test_attach_detach_lifecycle():
    client, recorder = make_client()
    install_defaults(recorder)
    gate = lifted_peer_gate()
    decisions: list = []
    raw_send = client._sender.send
    handle = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)

    with pytest.raises(ValueError, match="already attached"):
        attach_rpc_gate(client, gate, PHONE)

    handle.detach()
    assert client._sender.send is raw_send  # recorder exposed again
    with pytest.raises(ValueError, match="not attached"):
        handle.detach()

    # Re-attach must not stack wrappers: exactly one decision per dispatch.
    handle2 = attach_rpc_gate(client, gate, PHONE, on_decision=decisions.append)
    await client.send_message("newuser", "hi")
    assert len(decisions) == 2  # one resolve + one send, no doubling
    handle2.detach()
