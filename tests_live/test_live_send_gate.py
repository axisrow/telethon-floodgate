"""Bounded live check: the per-peer send bucket paces real sends.

Sends ONLY to Saved Messages (peer = the account itself), so no third party
ever sees test traffic. The gate is the real ``TelegramRateLimitGate`` on
wall-clock time; the test proves the 1/s user-peer bucket actually engages
against the live API and that a paced burst draws no FloodWaitError.
On a flood, leave the probes in Saved Messages until the reported wait has
expired: cleanup would otherwise make another API call on a flooded account.

Note on the timeout marker: if pytest-timeout fires mid-send the finally
block may not run and a probe message can stay in Saved Messages. That is
acceptable for a disposable sandbox account and is the documented trade-off
of having a deadlock guard at all.
"""
from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from telethon.errors import FloodWaitError
from telethon.tl import types as tl_types

from telethon_floodgate import TelegramRateLimitGate, peer_key

MESSAGE_COUNT = 5
# The user-peer spec allows 1 send/second: 5 sends need at least 4 gaps of
# ~1s. Small tolerance below 1.0 absorbs clock granularity, not much more.
MIN_GAP_SEC = 0.95
MIN_ELAPSED_SEC = 3.9


@pytest.mark.live_tg_send
@pytest.mark.timeout(240)
async def test_gate_paces_sends_to_one_per_second(live_telegram) -> None:
    # Saved Messages is a user peer, but the STRING "me" would classify as
    # "username:me" (UNKNOWN, no bucket). The key must come from the resolved
    # self entity — this guard keeps the test from silently degrading into a
    # category-only check.
    key = peer_key(live_telegram.peer)
    assert key is not None and key.startswith("user:"), f"self peer key is not user-kind: {key!r}"

    gate = TelegramRateLimitGate()  # real time.monotonic, package defaults
    nonce = uuid.uuid4().hex[:8]
    sent_ids: list[int] = []
    send_stamps: list[float] = []
    deferrals = 0
    flooded = False
    cleaned_up = False

    try:
        for i in range(MESSAGE_COUNT):
            retry_after = gate.try_acquire(live_telegram.phone, "send", peer=key)
            while retry_after > 0:
                deferrals += 1
                await asyncio.sleep(retry_after)
                retry_after = gate.try_acquire(live_telegram.phone, "send", peer=key)
            # Measure starts: response latency must not shorten the measured gap.
            send_stamps.append(time.monotonic())
            message = await live_telegram.client.send_message(
                "me", f"floodgate live probe {nonce} {i}"
            )
            assert isinstance(message, tl_types.Message), (
                f"send_message returned {type(message).__name__}, expected Message"
            )
            sent_ids.append(message.id)
    except FloodWaitError as exc:
        flooded = True
        print(
            f"\nFloodWait: stopped; cleanup skipped. After waiting {exc.seconds}s, "
            f"delete Saved Messages probes {nonce} (message IDs: {sent_ids})."
        )
        raise
    finally:
        if sent_ids and not flooded:
            try:
                if live_telegram.client.is_connected():
                    await live_telegram.client.delete_messages("me", sent_ids)
                    cleaned_up = True
            except Exception as exc:  # noqa: BLE001 - cleanup must never mask the result
                print(f"\ncleanup warning: delete_messages failed: {exc}")

    gaps = [b - a for a, b in zip(send_stamps, send_stamps[1:])]
    elapsed = send_stamps[-1] - send_stamps[0]

    assert deferrals >= 1, "the gate never deferred a send — the peer bucket did not engage"
    assert gaps, "no send timestamps collected"
    assert min(gaps) >= MIN_GAP_SEC, f"unpaced burst detected, gaps: {gaps}"
    assert elapsed >= MIN_ELAPSED_SEC, f"burst finished too fast: {elapsed:.2f}s"
    print(
        f"\nsent {len(sent_ids)} messages in {elapsed:.2f}s "
        f"(min gap {min(gaps):.2f}s, {deferrals} deferrals), cleaned_up={cleaned_up}"
    )
    assert cleaned_up, f"probe cleanup incomplete; delete Saved Messages probes {nonce} (message IDs: {sent_ids})"
