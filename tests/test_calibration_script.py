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
    """send_message/delete_messages double with a scripted flood point."""

    def __init__(self, *, flood_at: int | None = None, flood_seconds: int = 30) -> None:
        self.flood_at = flood_at
        self.flood_seconds = flood_seconds
        self.sent: list[str] = []
        self.deleted: list[list[int]] = []
        self._next_id = 1

    async def send_message(self, peer: Any, text: str) -> Any:
        if self.flood_at is not None and len(self.sent) == self.flood_at:
            err = FloodWaitError(request=None, capture=0)
            err.seconds = self.flood_seconds
            raise err
        self.sent.append(text)
        message = SimpleNamespace(id=self._next_id)
        self._next_id += 1
        return message

    async def delete_messages(self, peer: Any, ids: list[int]) -> None:
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
    assert report.safe_interval == 0.5  # both points held clean sends
    assert client.deleted == [[1, 2, 3, 4, 5]]
    assert "conservative and safe" in report.recommendation


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
