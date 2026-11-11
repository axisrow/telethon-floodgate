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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import urlsplit

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


_PROXY_ENV = "REAL_TG_PROXY"
_PROXY_SCHEMES = ("socks4", "socks5", "http")


def _parse_proxy_url(value: str) -> dict[str, Any]:
    """Parse ``scheme://[user:pass@]host:port`` into a Telethon proxy dict.

    Telethon's dict form maps 1:1 onto its ``Connection._parse_proxy``
    signature (string proxy types are resolved through python-socks, the
    only proxy backend Telethon uses since 1.24). Errors name REAL_TG_PROXY
    and never echo the offending value — it may carry credentials.
    """

    def bad(reason: str) -> ValueError:
        return ValueError(
            f"{_PROXY_ENV} is set but invalid: {reason}; expected "
            "scheme://[user:pass@]host:port with scheme socks4/socks5/http"
        )

    try:
        split = urlsplit(value)
        port = split.port  # raises ValueError itself on a non-numeric port
        hostname = split.hostname
        scheme = split.scheme.lower()
    except ValueError:
        raise bad("not a parseable URL") from None
    if scheme not in _PROXY_SCHEMES:
        raise bad(f"unsupported scheme {scheme!r}")
    if not hostname:
        raise bad("missing host")
    if port is None:
        raise bad("missing port")
    return {
        "proxy_type": scheme,
        "addr": hostname,
        "port": port,
        "username": split.username,
        "password": split.password,
    }


def _proxy_from_env(environ: Mapping[str, str]) -> dict[str, Any] | None:
    """The ``live_telegram`` proxy setting: REAL_TG_PROXY or ``None``.

    Absent/empty means the direct route — the historical behavior, unchanged.
    A set-but-missing python-socks must fail HERE, not later: Telethon only
    warns ("proxy argument will be ignored") and silently falls back to the
    direct route, which is exactly the failure mode issue #31 forbids —
    the override exists precisely because the direct route is refused.
    """
    value = environ.get(_PROXY_ENV, "").strip()
    if not value:
        return None
    if importlib.util.find_spec("python_socks") is None:
        raise ValueError(
            f"{_PROXY_ENV} is set but the python-socks package is not installed; "
            "install the live extra: pip install 'telethon-floodgate[live]'"
        )
    return _parse_proxy_url(value)


@pytest.fixture
async def live_telegram() -> AsyncIterator[LiveSandbox]:
    """Connect a real account from the environment (function scope on purpose:
    ``asyncio_default_fixture_loop_scope = "function"`` in pyproject, and a
    fresh client per test keeps each ``get_dialogs()`` isolated — dialog-list
    floods were a real production incident class in the origin project).
    """
    try:
        proxy = _proxy_from_env(os.environ)
    except ValueError as err:
        pytest.fail(str(err), pytrace=False)
    client_kwargs: dict[str, Any] = {
        # Surface short floods instead of silently sleeping/retrying.
        "flood_sleep_threshold": 0,
    }
    if proxy is not None:
        # Unset REAL_TG_PROXY → no proxy kwarg at all: the exact pre-#31
        # client construction, so the default path is bit-for-bit unchanged.
        client_kwargs["proxy"] = proxy
    client = TelegramClient(
        StringSession(os.environ["REAL_TG_SESSION"]),
        int(os.environ["REAL_TG_API_ID"]),
        os.environ["REAL_TG_API_HASH"],
        **client_kwargs,
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
