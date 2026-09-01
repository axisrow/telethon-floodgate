"""Gating policy for the opt-in live Telegram tests.

Leaf module with no pytest imports so the offline static tests can load it
via importlib without a pytest context (mirrors the donor project's
``_live_readiness.py`` pattern). The live tests are NEVER part of the default
run: ``tests_live/`` sits outside ``testpaths``, and even an explicit
``pytest tests_live`` skips everything unless the gate variable is set.

Environment contract (same names as the donor project, so operator muscle
memory carries over):

- ``RUN_FLOODGATE_LIVE_TG=1`` — the strict opt-in gate (literal ``1`` only);
- ``REAL_TG_API_ID`` / ``REAL_TG_API_HASH`` / ``REAL_TG_PHONE`` /
  ``REAL_TG_SESSION`` — a Telethon ``StringSession`` account. The session
  string grants FULL access to the account: pass it via the environment,
  never commit it.
"""
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping

GATE_ENV = "RUN_FLOODGATE_LIVE_TG"
RO_MARK = "live_tg_ro"
SEND_MARK = "live_tg_send"
LIVE_MARKS = (RO_MARK, SEND_MARK)
LIVE_FIXTURE = "live_telegram"
REQUIRED_ENV = ("REAL_TG_API_ID", "REAL_TG_API_HASH", "REAL_TG_PHONE", "REAL_TG_SESSION")


def gate_open(environ: Mapping[str, str] = os.environ) -> bool:
    """Strict opt-in: only the literal ``1`` opens the gate."""
    return environ.get(GATE_ENV) == "1"


def missing_env(environ: Mapping[str, str] = os.environ) -> tuple[str, ...]:
    return tuple(name for name in REQUIRED_ENV if not environ.get(name))


def evaluate_live_policy(
    modes: tuple[str, ...],
    fixturenames: Iterable[str],
    environ: Mapping[str, str],
) -> tuple[str | None, str | None]:
    """Decide what to do with one test under ``tests_live/``.

    Returns ``(action, message)``:

    - ``("skip", msg)`` — the gate is closed. This is the normal state for
      every run without an explicit opt-in; skipping (not failing) keeps a
      stray ``pytest tests_live`` green.
    - ``("fail", msg)`` — a structural violation (marker/fixture mismatch,
      no marker at all) or a misconfigured environment (gate open but
      account variables missing). Both must be loud.
    - ``(None, None)`` — proceed with the live test.
    """
    uses_fixture = LIVE_FIXTURE in set(fixturenames)
    if len(modes) > 1:
        return "fail", f"exactly one live marker expected, got {sorted(modes)}"
    mode = modes[0] if modes else None
    if mode is None:
        return "fail", "tests under tests_live/ must carry a live marker (live_tg_ro / live_tg_send)"
    if not uses_fixture:
        return "fail", f"live marker {mode} requires the {LIVE_FIXTURE} fixture"
    if not gate_open(environ):
        return (
            "skip",
            f"live Telegram tests are opt-in: set {GATE_ENV}=1 and the REAL_TG_* "
            "environment variables to run them",
        )
    missing = missing_env(environ)
    if missing:
        return "fail", f"live gate is open but environment variables are missing: {', '.join(missing)}"
    return None, None
