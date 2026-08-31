"""Derive a stable per-peer key from a Telethon entity without network I/O.

Telegram throttles *sending* per peer (roughly one message per second to the
same private chat, ~20 per minute into the same group or channel), not just
per account — so the send-side buckets in :class:`TelegramRateLimitGate` are
keyed by a peer string produced here.

The key embeds the peer kind (``user:123``, ``channel:-100123``,
``chat:-456``, ``username:durov``, ``id:123``) so the gate can pick a
kind-specific limit by parsing the prefix, without holding a reference to the
entity. Resolution is purely local attribute access: no ``get_entity``, no
request, so it is safe to call on the hot send path.
"""
from __future__ import annotations

from enum import Enum

from telethon.tl import types as tl_types


class PeerKind(str, Enum):
    """The kind of peer a key points at, mapped onto limit buckets."""

    USER = "user"  # private chat counterpart
    CHANNEL = "channel"  # broadcast channel or megagroup supergroup
    CHAT = "chat"  # classic (basic) group
    UNKNOWN = "unknown"


# Telethon TL shapes per kind: bare peers, input peers and full entities.
_USER_TYPES = (tl_types.PeerUser, tl_types.InputPeerUser, tl_types.User)
_CHANNEL_TYPES = (tl_types.PeerChannel, tl_types.InputPeerChannel, tl_types.Channel)
_CHAT_TYPES = (tl_types.PeerChat, tl_types.InputPeerChat, tl_types.Chat)

# (kind, preferred attribute) pairs — full entities expose ``id`` instead of
# the peer-specific attribute, so the attribute is a preference, not a must.
_KIND_ATTRS = (
    (PeerKind.USER, "user_id"),
    (PeerKind.CHANNEL, "channel_id"),
    (PeerKind.CHAT, "chat_id"),
)


def peer_kind_and_key(entity: object) -> tuple[PeerKind, str] | None:
    """Return ``(kind, key)`` for a peer-like ``entity``, or ``None``.

    Accepts ints (raw ids), strings (usernames, ``@`` and case normalised),
    Telethon TL peers/input peers/entities, and any duck-typed object exposing
    ``user_id``/``channel_id``/``chat_id``/``id`` (test doubles included).
    Never touches the network.
    """
    if entity is None:
        return None
    if isinstance(entity, bool):
        # bool is an int subclass; True/False are not peer ids.
        return None
    if isinstance(entity, int):
        return PeerKind.UNKNOWN, f"id:{entity}"
    if isinstance(entity, str):
        return PeerKind.UNKNOWN, f"username:{entity.strip().lstrip('@').lower()}"

    for types_, kind in (
        (_USER_TYPES, PeerKind.USER),
        (_CHANNEL_TYPES, PeerKind.CHANNEL),
        (_CHAT_TYPES, PeerKind.CHAT),
    ):
        if isinstance(entity, types_):
            for _kind, attr in _KIND_ATTRS:
                if _kind is kind:
                    value = getattr(entity, attr, None)
                    if isinstance(value, int):
                        return kind, f"{kind.value}:{value}"
                    break
            entity_id = getattr(entity, "id", None)
            if isinstance(entity_id, int):
                return kind, f"{kind.value}:{entity_id}"
            return None

    # Duck-typed fallback for fakes and custom wrappers.
    for kind, attr in _KIND_ATTRS:
        value = getattr(entity, attr, None)
        if isinstance(value, int):
            return kind, f"{kind.value}:{value}"
    entity_id = getattr(entity, "id", None)
    if isinstance(entity_id, int):
        return PeerKind.UNKNOWN, f"id:{entity_id}"
    return None


def peer_key(entity: object) -> str | None:
    """Return just the key string (``kind:id``), or ``None``."""
    pair = peer_kind_and_key(entity)
    return pair[1] if pair is not None else None
