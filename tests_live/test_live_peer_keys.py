"""Read-only live check: peer keys derived from REAL get_dialogs() entities.

The offline unit tests build Telethon TL objects by hand; this test feeds the
classifier the exact shapes a connected account actually receives. A drift
between the two (a new TL form, a renamed attribute) shows up here as an
UNKNOWN kind or a malformed key long before it misroutes a send bucket in
production.
"""
from __future__ import annotations

import re
from collections import Counter

import pytest
from telethon.tl import types as tl_types

from telethon_floodgate import PeerKind, peer_kind_and_key, run_with_flood_wait

_KEY_RE = re.compile(r"^(user|channel|chat):-?\d+$|^(username:[a-z0-9_]+|id:-?\d+)$")

# Degenerate server-side shapes that legitimately carry no peer identity of
# their own: forbidden/empty variants keep only a bare id, so UNKNOWN is the
# correct classification for them and must not fail the test.
_EXPECTED_UNKNOWN_TYPES = (
    getattr(tl_types, "ChannelForbidden", ()),
    getattr(tl_types, "ChatForbidden", ()),
    getattr(tl_types, "UserEmpty", ()),
)


@pytest.mark.live_tg_ro
@pytest.mark.timeout(240)
async def test_real_dialog_entities_map_to_well_formed_peer_keys(live_telegram) -> None:
    dialogs = await run_with_flood_wait(
        live_telegram.client.get_dialogs(limit=200),
        operation="floodgate_live_get_dialogs",
        phone=live_telegram.phone,
    )
    assert dialogs, "the sandbox account has no dialogs to classify"

    kind_counts: Counter[str] = Counter()
    for dialog in dialogs:
        entity = getattr(dialog, "entity", None)
        if entity is None:
            kind_counts["<no-entity>"] += 1
            continue

        kind, key = peer_kind_and_key(entity)
        entity_type = type(entity).__name__

        assert key is not None and kind is not None, f"{entity_type} produced no peer key"
        assert _KEY_RE.match(key), f"malformed peer key {key!r} for {entity_type}"

        if isinstance(entity, _EXPECTED_UNKNOWN_TYPES):
            assert kind is PeerKind.UNKNOWN, f"{entity_type} should classify as UNKNOWN: {key}"
            kind_counts[f"unknown({entity_type})"] += 1
            continue

        # The core assertion: every fully-shaped real entity classifies into a
        # concrete kind, and that kind agrees with the entity's own TL type.
        assert kind is not PeerKind.UNKNOWN, f"{entity_type} classified UNKNOWN: {key}"
        expected = None
        if isinstance(entity, tl_types.User):
            expected = PeerKind.USER
        elif isinstance(entity, tl_types.Channel):
            expected = PeerKind.CHANNEL
        elif isinstance(entity, tl_types.Chat):
            expected = PeerKind.CHAT
        if expected is not None:
            assert kind is expected, f"{entity_type}: classified {kind.value}, isinstance says {expected.value}"
        kind_counts[kind.value] += 1

    print(f"\npeer kind distribution over {len(dialogs)} dialogs: {dict(kind_counts)}")
