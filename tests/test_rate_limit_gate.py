from __future__ import annotations

import pytest

from telethon_floodgate.rate_limit_gate import (
    RateLimitSpec,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_dialogs_gate_is_per_phone_and_conservative() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+2", "dialogs") == 0.0
    assert gate.try_acquire("+1", "dialogs") == 60.0


def test_categories_have_independent_buckets() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"history": RateLimitSpec(max_calls=1, window_sec=60)},
        time_func=clock,
    )
    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "history") == 60.0


def test_resolve_and_reaction_keep_their_existing_dedicated_gates() -> None:
    gate = TelegramRateLimitGate(
        category_limits={"default": RateLimitSpec(max_calls=1, window_sec=60)},
    )

    for operation, expected_category in (
        ("telegram_resolve_entity", "resolve"),
        ("telegram_resolve_input_entity", "resolve"),
        ("telegram_send_reaction", "reaction"),
    ):
        category = gate.category_for(operation)
        assert category == expected_category
        assert gate.try_acquire("+1", category) == 0.0
        assert gate.try_acquire("+1", category) == 0.0


@pytest.mark.parametrize(
    "operation",
    (
        "resolve_channel_warm_dialog_cache",
        "fetch_channel_meta_warm_dialog_cache",
        "get_forum_topics_warm_dialog_cache",
        "search_warm_dialog_cache",
        "leave_channels:123_warm_dialog_cache",
        "delete_dialogs:123_warm_dialog_cache",
    ),
)
def test_decorated_dialog_operations_use_the_dialogs_bucket(operation: str) -> None:
    assert TelegramRateLimitGate.category_for(operation) == "dialogs"


@pytest.mark.parametrize(
    "operation",
    ("telegram_stream_dialogs", "resume_stream_dialogs"),
)
def test_dialog_sweep_has_its_own_bucket(operation: str) -> None:
    """A sweep is one operation continued across passes, not repeated calls.

    Sharing the warm-up's 1/min bucket meant the second pass of a resumable
    sweep was always refused, so the sweep could never resume (#1359). The
    warm-up keeps its own strict bucket — that is what #1330 needs.
    """
    assert TelegramRateLimitGate.category_for(operation) == "dialog_sweep"
    assert TelegramRateLimitGate.category_for("telegram_warm_dialog_cache") == "dialogs"


def test_phase_two_categories_are_separately_calibrated() -> None:
    gate = TelegramRateLimitGate()
    assert gate.category_for("telegram_stream_messages") == "history"
    assert gate.category_for("telegram_edit_admin") == "admin_action"
    assert gate.category_for("telegram_send_message") == "send"
    assert gate.category_for("telegram_publish_files") == "send"
    assert gate.category_for("telegram_create_channel") == "channel_lifecycle"
    assert gate.category_for("telegram_import_chat_invite") == "channel_lifecycle"
    assert gate.category_for("telegram_delete_chat") == "channel_lifecycle"

    # A history stream must not consume a write-operation slot.
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "send") == 0.0


def test_compound_slot_reservation_is_atomic() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 0.0
    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 300.0
    # The rejected two-slot reservation must not consume the one remaining slot.
    assert gate.try_acquire("+1", "channel_lifecycle") == 0.0


# --- per-peer send limits --------------------------------------------------


def test_peer_user_bucket_allows_one_per_second() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "send", peer="user:42") == 0.0
    assert gate.try_acquire("+1", "send", peer="user:42") > 0.0
    # The 1s window slides: after a second the same peer is allowed again.
    clock.now += 1.0
    assert gate.try_acquire("+1", "send", peer="user:42") == 0.0


def test_peer_refusal_does_not_burn_the_category_slot() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"send": RateLimitSpec(max_calls=2, window_sec=60)},
        time_func=clock,
    )

    # A successful call consumes BOTH its peer slot and one category slot.
    assert gate.try_acquire("+1", "send", peer="user:42") == 0.0
    # The refused call burns neither: the peer bucket rejects it first and the
    # account-wide category keeps its remaining slot.
    assert gate.try_acquire("+1", "send", peer="user:42") > 0.0
    assert gate.try_acquire("+1", "send") == 0.0
    # The peer bucket passes for a fresh peer, but the category is now full:
    # deferral with a retry hint, not a silent pass.
    assert gate.try_acquire("+1", "send", peer="user:43") > 0.0


def test_peer_channel_bucket_allows_twenty_per_minute() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    for _ in range(20):
        assert gate.try_acquire("+1", "send", peer="channel:-100123") == 0.0
    assert gate.try_acquire("+1", "send", peer="channel:-100123") > 0.0
    # A classic chat has the same shape of limit, in its own bucket.
    assert gate.try_acquire("+1", "send", peer="chat:7") == 0.0


def test_peer_buckets_are_per_phone() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "send", peer="user:42") == 0.0
    assert gate.try_acquire("+1", "send", peer="user:42") > 0.0
    assert gate.try_acquire("+2", "send", peer="user:42") == 0.0


def test_unknown_peer_kind_is_not_limited() -> None:
    """``send:unknown`` has no peer bucket — only the category still applies."""
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"send": RateLimitSpec(max_calls=1000, window_sec=60)},
        time_func=clock,
    )

    for _ in range(50):
        assert gate.try_acquire("+1", "send", peer="id:999") == 0.0


def test_peer_limits_are_configurable() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(
        peer_limits={"send:user": RateLimitSpec(max_calls=5, window_sec=60)},
        time_func=clock,
    )

    for _ in range(5):
        assert gate.try_acquire("+1", "send", peer="user:42") == 0.0
    assert gate.try_acquire("+1", "send", peer="user:42") > 0.0


def test_peer_buckets_are_bounded_by_lru_eviction() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(peer_max_buckets=2, time_func=clock)

    assert gate.try_acquire("+1", "send", peer="user:1") == 0.0
    assert gate.try_acquire("+1", "send", peer="user:2") == 0.0
    assert gate.try_acquire("+1", "send", peer="user:3") == 0.0  # evicts user:1
    assert gate.try_acquire("+1", "send", peer="user:1") == 0.0  # fresh bucket
    assert gate.try_acquire("+1", "send", peer="user:3") > 0.0  # survivor stays limited


def test_reset_clears_peer_buckets_for_one_phone() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    gate.try_acquire("+1", "send", peer="user:42")
    gate.try_acquire("+2", "send", peer="user:42")
    gate.reset("+1")
    assert gate.try_acquire("+1", "send", peer="user:42") == 0.0
    assert gate.try_acquire("+2", "send", peer="user:42") > 0.0
    gate.reset()
    assert gate.try_acquire("+2", "send", peer="user:42") == 0.0


def test_peer_error_is_a_rate_limit_error() -> None:
    """Existing TelegramRateLimitedError handlers must absorb the peer flavour."""
    exc = TelegramPeerRateLimitedError("+7", "user:42", 3.5)

    assert isinstance(exc, TelegramRateLimitedError)
    assert exc.peer == "user:42"
    assert exc.category == "send_peer"
    assert exc.retry_after_sec == 3.5

