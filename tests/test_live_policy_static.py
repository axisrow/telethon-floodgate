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
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "tests_live" / "_live_policy.py"
# A literal assignment to a REAL_TG_* name (only env READS are allowed).
_SECRET_ASSIGN_RE = re.compile(r"REAL_TG_[A-Z_]+\s*=\s*[\"'][^\"']+")


def _load_policy() -> Any:
    spec = importlib.util.spec_from_file_location("tests_live._live_policy", POLICY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec so lazy annotation lookups inside the module work.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


policy = _load_policy()


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
