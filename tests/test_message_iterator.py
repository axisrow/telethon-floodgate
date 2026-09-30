"""Use Telethon's real pagination with an offline transport, not a fake iterator."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions, types

from telethon_floodgate import RateLimitSpec, TelegramRateLimitGate


@pytest.mark.parametrize("mode", ["history", "search", "ids", "channel_ids"])
async def test_every_message_page_is_gated_without_mutating_shared_client(mode):
    now = 0.0
    sleeps = []
    requests = []

    async def sleep(seconds):
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    channel = mode == "channel_ids"
    entity = types.InputPeerChannel(7, 0) if channel else types.InputPeerUser(7, 0)
    peer = types.PeerChannel(7) if channel else types.PeerUser(7)
    raw = TelegramClient(StringSession(), 1, "test", flood_sleep_threshold=0)
    raw.get_input_entity = AsyncMock(return_value=entity)
    raw._get_peer = AsyncMock(return_value=peer)

    async def call(sender, request, **kwargs):
        requests.append((type(request), now))
        if hasattr(request, "id"):
            ids = [getattr(value, "id", value) for value in request.id]
        else:
            start = request.offset_id - 1 if request.offset_id else 250
            ids = range(start, max(0, start - request.limit), -1)
        return SimpleNamespace(
            count=250, users=[], chats=[], messages=[
                types.Message(id=i, peer_id=peer, message=str(i), date=datetime(2026, 1, 1, tzinfo=timezone.utc))
                for i in ids
            ],
        )

    raw._call = call
    kwargs = {"ids": list(range(1, 251))} if "ids" in mode else {"limit": 250}
    if mode == "search":
        kwargs["search"] = "test"
    iterator = raw.iter_messages(7, **kwargs)
    gate = TelegramRateLimitGate(category_limits={"history": RateLimitSpec(1, 10)}, time_func=lambda: now)
    assert gate.wrap_messages_iterator(iterator, "+1", sleep=sleep) is iterator
    result = [message async for message in iterator]

    assert len(result) == 250
    assert len({message.id for message in result}) == 250
    assert [time for _, time in requests] == [0.0, 10.0, 20.0]
    assert sleeps == [10.0, 10.0]
    assert gate.try_acquire("+1", "history") == 10.0  # third page reserved
    assert raw.iter_messages(7).client is raw  # no global monkeypatch
    assert iterator.client.get_input_entity is raw.get_input_entity

    # Telethon keeps the proxy on returned messages. Unrelated calls bypass history.
    raw._call = AsyncMock(return_value="unrelated")
    assert await iterator.client(functions.messages.ReadHistoryRequest(entity, 0)) == "unrelated"
    assert sleeps == [10.0, 10.0]


async def test_unsupported_and_double_wrapped_iterators_fail_loudly():
    gate = TelegramRateLimitGate()
    with pytest.raises(TypeError, match="callable client"):
        gate.wrap_messages_iterator(object(), "+1")
    raw = TelegramClient(StringSession(), 1, "test")
    iterator = gate.wrap_messages_iterator(raw.iter_messages(7), "+1")
    with pytest.raises(ValueError, match="already rate-limited"):
        gate.wrap_messages_iterator(iterator, "+1")
