"""Offline invariants of the opt-in live harness (collected by CI).

These tests are the CI-visible half of the live-testing contract: the live
tests themselves never run in CI, but every structural guarantee about them —
gating policy, marker registration, exclusion from the default run, absence
of secret literals — is enforced here.
"""
from __future__ import annotations

import importlib.util
import re
import sys
import tomllib
from collections.abc import Mapping
from contextlib import aclosing, nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import pytest
from telethon.errors import FloodWaitError
from telethon.tl import types as tl_types

from telethon_floodgate import RateLimitSpec, TelegramRateLimitGate

ROOT = Path(__file__).resolve().parents[1]
# A literal assignment to a REAL_TG_* name (only env READS are allowed).
_SECRET_ASSIGN_RE = re.compile(r"REAL_TG_[A-Z_]+\s*=\s*[\"'][^\"']+")


def _load_live_module(name: str) -> Any:
    spec = importlib.util.spec_from_file_location(f"tests_live.{name}", ROOT / "tests_live" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec so lazy annotation lookups inside the module work.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


policy = _load_live_module("_live_policy")


def _pytest_config() -> Mapping[str, Any]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["pytest"]["ini_options"]


def test_tests_live_is_excluded_from_the_default_run():
    assert "tests_live" not in _pytest_config().get("testpaths", [])


def test_live_markers_are_registered():
    markers = {
        marker.split(":", 1)[0].strip() for marker in _pytest_config().get("markers", [])
    }
    assert {"live_tg_ro", "live_tg_send"} <= markers, (
        "unregistered markers turn into PytestUnknownMarkWarning, "
        "which filterwarnings=['error'] escalates at collection"
    )


def test_dev_dependencies_include_pytest_timeout():
    dev = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"][
        "dev"
    ]
    assert any(dep.startswith("pytest-timeout") for dep in dev)


def test_every_live_test_file_declares_a_live_marker():
    files = sorted((ROOT / "tests_live").glob("test_*.py"))
    assert files, "tests_live/ should contain the live test files"
    for path in files:
        assert "pytest.mark.live_tg_" in path.read_text(), f"{path.name} has no live marker"


def test_no_real_tg_secret_literals_in_live_code():
    for base in (ROOT / "tests_live", ROOT / "scripts"):
        for path in sorted(base.rglob("*.py")):
            assert not _SECRET_ASSIGN_RE.search(path.read_text()), (
                f"secret-like REAL_TG_* literal in {path.relative_to(ROOT)}"
            )


def test_gate_is_strict_opt_in():
    assert policy.gate_open({"RUN_FLOODGATE_LIVE_TG": "1"}) is True
    for value in ("", "0", "true", "yes", "on", None):
        environ = {} if value is None else {"RUN_FLOODGATE_LIVE_TG": value}
        assert policy.gate_open(environ) is False, f"gate must not open for {value!r}"


_FULL_ENV = {name: "value" for name in policy.REQUIRED_ENV}


class _Cases:
    """(modes, fixturenames, environ, expected_action) matrix."""

    CLOSED = {}  # gate not set at all
    OPEN = {"RUN_FLOODGATE_LIVE_TG": "1"}
    OPEN_FULL = {**OPEN, **_FULL_ENV}


@pytest.mark.parametrize(
    ("modes", "fixturenames", "environ", "expected"),
    [
        # gate closed → the normal opt-in skip
        (("live_tg_ro",), {"live_telegram"}, _Cases.CLOSED, "skip"),
        (("live_tg_send",), {"live_telegram"}, {"RUN_FLOODGATE_LIVE_TG": "0"}, "skip"),
        # gate open but account env missing → loud fail naming the variables
        (("live_tg_ro",), {"live_telegram"}, _Cases.OPEN, "fail"),
        # structural violations → fail regardless of the gate
        (("live_tg_ro",), {"live_telegram"}, _Cases.OPEN_FULL, None),
        (("live_tg_ro", "live_tg_send"), {"live_telegram"}, _Cases.OPEN_FULL, "fail"),
        (("live_tg_ro",), set(), _Cases.OPEN_FULL, "fail"),
        ((), {"live_telegram"}, _Cases.OPEN_FULL, "fail"),
        ((), set(), _Cases.OPEN_FULL, "fail"),
        # structural violations fail regardless of the gate: they are
        # misconfigurations, not opt-out decisions
        (("live_tg_ro", "live_tg_send"), set(), _Cases.CLOSED, "fail"),
    ],
)
def test_evaluate_live_policy_matrix(modes, fixturenames, environ, expected):
    action, _message = policy.evaluate_live_policy(tuple(modes), fixturenames, environ)
    assert action == expected


def test_missing_env_failure_names_every_missing_variable():
    action, message = policy.evaluate_live_policy(("live_tg_ro",), {"live_telegram"}, {"RUN_FLOODGATE_LIVE_TG": "1"})
    assert action == "fail"
    for name in policy.REQUIRED_ENV:
        assert name in message


@pytest.mark.parametrize("flood_at", [None, 0, 2])
async def test_live_send_pacing_and_flood_stop_offline(monkeypatch, flood_at):
    harness = _load_live_module("conftest")
    send_test = _load_live_module("test_live_send_gate")
    clock = SimpleNamespace(now=1000.0)
    send_starts = []
    flood_call_count = 0

    async def sleep(seconds):
        clock.now += seconds

    async def send_message(peer, text):
        nonlocal flood_call_count
        assert peer == "me"
        index = len(send_starts)
        send_starts.append(clock.now)
        if index == flood_at:
            flood_call_count = len(client.mock_calls)
            raise FloodWaitError(request=None, capture=5)
        # Completion gaps would be <0.95s despite correctly paced starts.
        await sleep((0.8, 0.1, 0.65, 0.2, 0.05)[index])
        return tl_types.Message(id=index + 1, peer_id=tl_types.PeerUser(42))

    client = Mock(
        connect=AsyncMock(),
        is_user_authorized=AsyncMock(return_value=True),
        get_me=AsyncMock(return_value=tl_types.User(id=42)),
        send_message=AsyncMock(side_effect=send_message),
        is_connected=Mock(return_value=True),
        delete_messages=AsyncMock(),
        disconnect=AsyncMock(),
    )
    constructor = Mock(return_value=client)
    for name in policy.REQUIRED_ENV:
        monkeypatch.setenv(name, "1")
    monkeypatch.setattr(harness, "StringSession", Mock())
    monkeypatch.setattr(harness, "TelegramClient", constructor)
    monkeypatch.setattr(send_test, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(send_test, "asyncio", SimpleNamespace(sleep=sleep))
    # Jitter-free send:user spec: this regression asserts the exact 1.1s
    # pacing math; the default 0.15s defer jitter is covered by the gate tests.
    monkeypatch.setattr(
        send_test,
        "TelegramRateLimitGate",
        lambda: TelegramRateLimitGate(
            time_func=lambda: clock.now,
            peer_limits={"send:user": RateLimitSpec(max_calls=1, window_sec=1.1)},
        ),
    )

    async with aclosing(harness.live_telegram.__wrapped__()) as fixture:
        sandbox = await anext(fixture)
        with pytest.raises(FloodWaitError) if flood_at is not None else nullcontext():
            await send_test.test_gate_paces_sends_to_one_per_second(sandbox)

    assert constructor.call_args.kwargs["flood_sleep_threshold"] == 0
    client.disconnect.assert_awaited_once()
    if flood_at is None:
        assert send_starts == pytest.approx([1000.0, 1001.1, 1002.2, 1003.3, 1004.4])
        client.delete_messages.assert_awaited_once_with("me", [1, 2, 3, 4, 5])
    else:
        assert len(send_starts) == flood_at + 1
        client.delete_messages.assert_not_awaited()
        assert client.mock_calls[flood_call_count:] == [call.disconnect()]
