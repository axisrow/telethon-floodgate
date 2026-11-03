"""Offline unit tests for the calibration core (no Telegram involved).

The script's ``run_calibration`` is a seam: a fake client with a scripted
FloodWaitError plus a fake clock reproduce the dangerous logic — pacing,
auto-stop, budgets, post-flood wait, cleanup — without touching an account.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon.errors import FloodWaitError

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "calibrate_send_limits.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("calibrate_send_limits", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec: the script's dataclasses resolve string
    # annotations through sys.modules[cls.__module__].__dict__.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


calibrate = _load_script()


class _FakeClock:
    """monotonic + async sleep in one: sleeping advances the clock."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class _FakeClient:
    """send_message/delete_messages double with scripted failure points."""

    def __init__(
        self,
        *,
        flood_at: int | None = None,
        flood_seconds: int = 30,
        fail_at: int | None = None,
    ) -> None:
        self.flood_at = flood_at
        self.flood_seconds = flood_seconds
        self.fail_at = fail_at
        self.sent: list[str] = []
        self.sent_peers: list[Any] = []
        self.deleted: list[list[int]] = []
        self.deleted_peers: list[Any] = []
        self._next_id = 1

    async def send_message(self, peer: Any, text: str) -> Any:
        if self.flood_at is not None and len(self.sent) == self.flood_at:
            err = FloodWaitError(request=None, capture=0)
            err.seconds = self.flood_seconds
            raise err
        if self.fail_at is not None and len(self.sent) == self.fail_at:
            raise RuntimeError("scripted non-flood failure")
        self.sent.append(text)
        self.sent_peers.append(peer)
        message = SimpleNamespace(id=self._next_id)
        self._next_id += 1
        return message

    async def delete_messages(self, peer: Any, ids: list[int]) -> None:
        self.deleted_peers.append(peer)
        self.deleted.append(list(ids))


async def test_flood_stops_the_run_and_is_reported_as_the_measurement():
    clock = _FakeClock()
    client = _FakeClient(flood_at=4, flood_seconds=30)

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0, 0.5),
        messages_per_point=3,
        settle_seconds=5.0,
        sleep=clock.sleep,
        monotonic=clock,
    )

    # Point one held 3 clean sends; point two sent one more and flooded.
    assert [(p.interval, p.sent, p.flooded) for p in report.points] == [
        (1.0, 3, False),
        (0.5, 1, True),
    ]
    assert report.flooded_at_interval == 0.5
    assert report.stopped_reason == "flood wait observed"
    assert report.safe_interval == 1.0
    # The recommendation flags the packaged default as too permissive.
    assert "TOO PERMISSIVE" in report.recommendation
    # Post-flood wait (30s + 2s buffer) happened BEFORE the delete call.
    assert calibrate.POST_FLOOD_BUFFER_SEC + 30.0 in clock.slept
    assert client.deleted == [[1, 2, 3, 4]]
    assert report.cleaned_up is True
    # Pacing engaged: sends within point one slept 1.0 between each other.
    assert clock.slept[:2] == [1.0, 1.0]
    # The report is JSON-serialisable (the CLI prints exactly this).
    payload = json.loads(report.to_json())
    assert payload["flooded_at_interval"] == 0.5
    assert payload["safe_interval"] == 1.0
    # Every send and the cleanup targeted the same peer.
    assert set(client.sent_peers) == {"me"}
    assert client.deleted_peers == ["me"]


async def test_non_flood_failure_still_cleans_up_probe_messages():
    """Review finding: cleanup must not depend on a graceful loop exit."""
    clock = _FakeClock()
    client = _FakeClient(fail_at=2)

    with pytest.raises(RuntimeError, match="scripted non-flood failure"):
        await calibrate.run_calibration(
            client,
            peer="me",
            intervals=(1.0,),
            messages_per_point=5,
            sleep=clock.sleep,
            monotonic=clock,
        )

    # Two probes went out; the finally-path deleted them without any
    # post-flood sleep (none was observed).
    assert len(client.sent) == 2
    assert client.deleted == [[1, 2]]
    assert client.deleted_peers == ["me"]


async def test_zero_second_flood_still_waits_before_deleting():
    """A malformed FloodWaitError (seconds <= 0) must still pace the cleanup."""
    clock = _FakeClock()
    client = _FakeClient(flood_at=0, flood_seconds=0)

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0,),
        messages_per_point=3,
        sleep=clock.sleep,
        monotonic=clock,
    )

    assert report.points[0].flooded
    # max(0, 1.0) + the 2s buffer — the minimum safe pause happened.
    assert 1.0 + calibrate.POST_FLOOD_BUFFER_SEC in clock.slept


async def test_max_messages_budget_stops_without_a_flood():
    clock = _FakeClock()
    client = _FakeClient()

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0, 0.5),
        messages_per_point=3,
        settle_seconds=5.0,
        max_messages=5,
        sleep=clock.sleep,
        monotonic=clock,
    )

    assert len(client.sent) == 5
    assert report.stopped_reason.startswith("max-messages budget")
    assert report.flooded_at_interval is None
    # The 0.5s point got only 2 of its 3 sends before the budget fired —
    # truncated, so the flood-free verdict rests on the 1.0s point alone.
    assert [(p.interval, p.sent, p.truncated) for p in report.points] == [
        (1.0, 3, False),
        (0.5, 2, True),
    ]
    assert report.safe_interval == 1.0
    assert client.deleted == [[1, 2, 3, 4, 5]]
    assert "TOO PERMISSIVE" in report.recommendation


async def test_budget_truncated_point_never_becomes_the_safe_interval():
    """#9: a budget-cut point must not count as a flood-free interval.

    The old clean-filter (only flooded / sent>0 checked) let the 0.2s point
    below — validated by a SINGLE send before the budget fired — become
    safe_interval and the report recommend window_sec=0.25 for an interval
    that never saw its 8 messages.
    """
    clock = _FakeClock()
    client = _FakeClient()

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0, 0.2),
        messages_per_point=8,
        settle_seconds=5.0,
        max_messages=9,
        sleep=clock.sleep,
        monotonic=clock,
    )

    assert [(p.interval, p.sent, p.truncated) for p in report.points] == [
        (1.0, 8, False),
        (0.2, 1, True),
    ]
    assert report.stopped_reason.startswith("max-messages budget")
    assert report.flooded_at_interval is None
    assert report.safe_interval == 1.0
    # The truncated point stays visible in the JSON report.
    payload = json.loads(report.to_json())
    assert payload["points"][1]["truncated"] is True


async def test_budget_cut_before_any_full_point_recommends_no_lowering():
    """#9: budget stopped inside the FIRST point → no evidence at all."""
    clock = _FakeClock()
    client = _FakeClient()

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0, 0.5),
        messages_per_point=8,
        max_messages=1,
        sleep=clock.sleep,
        monotonic=clock,
    )

    assert [(p.interval, p.sent, p.truncated) for p in report.points] == [(1.0, 1, True)]
    assert report.safe_interval is None
    assert report.stopped_reason.startswith("max-messages budget")
    # The recommendation names the real cause (budget, not floods).
    assert "budget" in report.recommendation
    assert "do not lower the default" in report.recommendation


def test_recommendation_without_clean_points_names_the_cause():
    """No full point: flood stop keeps the old wording, budget stop is honest."""
    flood_text = calibrate.build_recommendation(None)
    assert "no interval held without a flood" in flood_text
    assert "do not lower the default" in flood_text

    budget_text = calibrate.build_recommendation(None, budget_stopped=True)
    assert "budget" in budget_text
    assert "held its full send count" in budget_text
    assert "do not lower the default" in budget_text

    # With a safe interval the budget flag changes nothing (flood verdicts only).
    assert calibrate.build_recommendation(0.5, budget_stopped=True) == calibrate.build_recommendation(0.5)


async def test_max_seconds_budget_stops_the_run():
    clock = _FakeClock()
    client = _FakeClient()

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0,),
        messages_per_point=10,
        max_seconds=3.5,
        sleep=clock.sleep,
        monotonic=clock,
    )

    # Sends happen at t=0,1,2,3; the next pacing sleep crosses the budget.
    assert len(client.sent) == 4
    assert report.stopped_reason.startswith("max-seconds budget")
    assert report.cleaned_up is True


async def test_immediate_flood_reports_no_clean_point():
    clock = _FakeClock()
    client = _FakeClient(flood_at=0, flood_seconds=15)

    report = await calibrate.run_calibration(
        client,
        peer="me",
        intervals=(1.0,),
        messages_per_point=3,
        sleep=clock.sleep,
        monotonic=clock,
    )

    assert report.points[0].sent == 0 and report.points[0].flooded
    assert report.safe_interval is None
    assert "no clean point" in report.recommendation
    # Nothing was sent, so cleanup is trivially complete with no delete call.
    assert client.deleted == []
    assert report.cleaned_up is True
