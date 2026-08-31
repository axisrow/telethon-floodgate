"""peer_key extraction from Telethon entities — purely local, no network."""
from __future__ import annotations

from types import SimpleNamespace

from telethon.tl import types as tl_types

from telethon_floodgate.peer import PeerKind, peer_key, peer_kind_and_key


def test_none_yields_none() -> None:
    assert peer_kind_and_key(None) is None
    assert peer_key(None) is None


def test_int_yields_unknown_id_key() -> None:
    assert peer_kind_and_key(123) == (PeerKind.UNKNOWN, "id:123")
    assert peer_kind_and_key(-1001234567890) == (PeerKind.UNKNOWN, "id:-1001234567890")


def test_bool_is_not_an_id() -> None:
    assert peer_kind_and_key(True) is None


def test_string_normalises_to_username() -> None:
    assert peer_kind_and_key("@Durov") == (PeerKind.UNKNOWN, "username:durov")
    assert peer_kind_and_key("  Telegram_Durov ") == (
        PeerKind.UNKNOWN,
        "username:telegram_durov",
    )


def test_tl_peer_shapes() -> None:
    assert peer_kind_and_key(tl_types.PeerUser(user_id=42)) == (PeerKind.USER, "user:42")
    assert peer_kind_and_key(
        tl_types.InputPeerUser(user_id=42, access_hash=0)
    ) == (PeerKind.USER, "user:42")
    assert peer_kind_and_key(tl_types.User(id=42)) == (PeerKind.USER, "user:42")
    assert peer_kind_and_key(
        tl_types.PeerChannel(channel_id=157)
    ) == (PeerKind.CHANNEL, "channel:157")
    assert peer_kind_and_key(
        tl_types.InputPeerChannel(channel_id=157, access_hash=0)
    ) == (PeerKind.CHANNEL, "channel:157")
    assert peer_kind_and_key(
        tl_types.Channel(id=157, title="t", photo=tl_types.ChatPhotoEmpty(), date=None)
    ) == (PeerKind.CHANNEL, "channel:157")
    assert peer_kind_and_key(tl_types.PeerChat(chat_id=99)) == (PeerKind.CHAT, "chat:99")
    assert peer_kind_and_key(
        tl_types.Chat(
            id=99,
            title="t",
            photo=tl_types.ChatPhotoEmpty(),
            participants_count=2,
            date=None,
            version=0,
        )
    ) == (PeerKind.CHAT, "chat:99")


def test_duck_typed_fakes_map_by_attribute() -> None:
    assert peer_kind_and_key(SimpleNamespace(user_id=1)) == (PeerKind.USER, "user:1")
    assert peer_kind_and_key(SimpleNamespace(channel_id=-100123)) == (
        PeerKind.CHANNEL,
        "channel:-100123",
    )
    assert peer_kind_and_key(SimpleNamespace(chat_id=7)) == (PeerKind.CHAT, "chat:7")
    assert peer_kind_and_key(SimpleNamespace(id=55)) == (PeerKind.UNKNOWN, "id:55")


def test_unrecognised_object_yields_none() -> None:
    assert peer_kind_and_key(object()) is None


def test_peer_key_returns_string_only() -> None:
    assert peer_key(tl_types.PeerUser(user_id=42)) == "user:42"
