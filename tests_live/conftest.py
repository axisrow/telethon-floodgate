"""Conftest for the opt-in live Telegram tests (loaded only for tests_live/).

Two jobs:

- gate every test under this directory through ``_live_policy`` (skip when
  the opt-in variable is absent, fail loudly on structural misconfiguration);
- provide the ``live_telegram`` sandbox fixture: a real Telethon client built
  from the environment session string, plus the account's own entity so send
  tests can derive a proper ``user:<id>`` peer key (Sending to ``"me``" is a
  user peer, but ``peer_key("me")`` itself would be an unclassified string).

The path guard in the setup hook is mandatory: a combined
``pytest tests tests_live`` run must not let this hook touch the offline
suite.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import User

_LIVE_ROOT = Path(__file__).resolve().parent


def _load_policy() -> Any:
    """Load the sibling leaf module (tests_live/ is not a package)."""
    module_name = "tests_live._live_policy"
    spec = importlib.util.spec_from_file_location(module_name, _LIVE_ROOT / "_live_policy.py")
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("cannot load tests_live/_live_policy.py")
    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec so lazy annotation lookups inside the module work.
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_policy = _load_policy()


def pytest_runtest_setup(item: pytest.Item) -> None:
    if _LIVE_ROOT not in Path(item.fspath).resolve().parents:
        return  # offline tests: this directory's policy must not touch them
    modes = tuple(m.name for m in item.iter_markers() if m.name in _policy.LIVE_MARKS)
    action, message = _policy.evaluate_live_policy(modes, item.fixturenames, os.environ)
    if action == "skip":
        pytest.skip(message)
    if action == "fail":
        pytest.fail(message, pytrace=False)


@dataclass
class LiveSandbox:
    """A connected sandbox account plus the context live tests need."""

    client: TelegramClient
    phone: str
    peer: User  # the account itself: the Saved Messages send counterpart


@pytest.fixture
async def live_telegram() -> AsyncIterator[LiveSandbox]:
    """Connect a real account from the environment (function scope on purpose:
    ``asyncio_default_fixture_loop_scope = "function"`` in pyproject, and a
    fresh client per test keeps each ``get_dialogs()`` isolated — dialog-list
    floods were a real production incident class in the origin project).
    """
    client = TelegramClient(
        StringSession(os.environ["REAL_TG_SESSION"]),
        int(os.environ["REAL_TG_API_ID"]),
        os.environ["REAL_TG_API_HASH"],
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            pytest.fail("REAL_TG_SESSION is not authorized for this account", pytrace=False)
        me = await client.get_me()
        if not isinstance(me, User):
            # The send test's peer-key derivation relies on the full User
            # shape — the same form production code sees.
            pytest.fail(
                f"get_me() returned unexpected entity type {type(me).__name__}", pytrace=False
            )
        yield LiveSandbox(
            client=client,
            phone=os.environ["REAL_TG_PHONE"],
            peer=me,
        )
    finally:
        await client.disconnect()
