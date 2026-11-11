"""Bounded live check for ``strict_categories=True`` (#29).

Proves on the real call path that the strict gate refuses unregistered
operation tags at the resolution point — the 11.11.2026 incident tags
``telegram_invoke_request`` / ``telegram_fetch_me`` raise
``TelegramUnregisteredOperationError`` before any API call — while a
registered ``send`` still flows through the same strict gate into a real
Saved Messages send. One probe message total, deleted on success.
"""
from __future__ import annotations

import uuid

import pytest
from telethon.tl import types as tl_types

from telethon_floodgate import (
    TelegramRateLimitedError,
    TelegramRateLimitGate,
    TelegramUnregisteredOperationError,
    peer_key,
)

# The exact unregistered tags from the 11.11.2026 production incident (#29):
# under the permissive default they bypassed the sustained send budget and
# fed a ~530 fails/hour retry storm into the whole account pool.
INCIDENT_TAGS = ("telegram_invoke_request", "telegram_fetch_me")


@pytest.mark.live_tg_send
@pytest.mark.timeout(120)
async def test_strict_gate_refuses_unknown_tags_and_sends_registered(live_telegram) -> None:
    key = peer_key(live_telegram.peer)
    assert key is not None and key.startswith("user:"), f"self peer key is not user-kind: {key!r}"

    # 1. Strict mode fails closed on the incident tags. Resolution happens at
    #    the call site, so these raise before any network traffic at all —
    #    that is the whole point of fail-closed (#29).
    strict = TelegramRateLimitGate(strict_categories=True)
    for tag in INCIDENT_TAGS:
        with pytest.raises(TelegramUnregisteredOperationError) as excinfo:
            strict.category_for(tag)
        assert excinfo.value.operation == tag
        # A configuration error, not a refusal: must not subclass the shared
        # "operation unavailable, move on" handler type.
        assert isinstance(excinfo.value, ValueError)
        assert not isinstance(excinfo.value, TelegramRateLimitedError)

    # 2. The default stays permissive (#29 explicitly changed no default):
    #    the same tags still resolve to the 'default' bucket with warn-once.
    permissive = TelegramRateLimitGate()
    for tag in INCIDENT_TAGS:
        assert permissive.category_for(tag) == "default"

    # 3. A registered send under strict mode reaches the live API: the
    #    production call-site shape is tag -> category -> try_acquire.
    #    ('send' is the category literal; the registered *tag* is
    #    telegram_send_message — strict mode refuses bare category names
    #    at the resolution point, as #29 intends.)
    category = strict.category_for("telegram_send_message")
    assert category == "send", f"telegram_send_message resolved to {category!r}, expected 'send'"
    retry_after = strict.try_acquire(live_telegram.phone, category, peer=key)
    assert retry_after == 0, f"fresh strict gate deferred the first send by {retry_after}s"

    nonce = uuid.uuid4().hex[:8]
    message = await live_telegram.client.send_message("me", f"floodgate strict live probe {nonce}")
    assert isinstance(message, tl_types.Message), (
        f"send_message returned {type(message).__name__}, expected Message"
    )
    try:
        await live_telegram.client.delete_messages("me", message.id)
    except Exception as exc:  # noqa: BLE001 - cleanup must never mask the result
        print(f"\ncleanup warning: delete_messages failed: {exc} (probe {nonce}, id {message.id})")
