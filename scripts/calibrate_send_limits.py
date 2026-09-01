#!/usr/bin/env python3
"""Calibrate the real per-peer send limit for one account (manual, level 2).

Measures the RAW Telegram boundary WITHOUT our gate: fixed interval points
(descending), a fixed number of sends per point, a settle pause between
points so server-side flood windows do not carry over. The first
``FloodWaitError`` is the measurement, not a failure — the script stops
there, waits out the reported seconds, deletes the probe messages and prints
a JSON report with a verdict against the packaged ``SEND_PEER_USER_SPEC``.

Safety contract:
- sends ONLY to Saved Messages (``--peer`` defaults to the account itself);
- hard budgets: ``--max-messages`` and ``--max-seconds`` stop the run even
  without a flood;
- every FloodWaitError is a real strike against the account, so the defaults
  are deliberately conservative — the budget is a ceiling, not a goal.

Usage (same environment as tests_live):
    RUN_FLOODGATE_LIVE_TG=1 REAL_TG_API_ID=... REAL_TG_API_HASH=... \
    REAL_TG_PHONE=... REAL_TG_SESSION=... python scripts/calibrate_send_limits.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from telethon_floodgate import RateLimitSpec, TelegramRateLimitGate

GATE_ENV = "RUN_FLOODGATE_LIVE_TG"
REQUIRED_ENV = ("REAL_TG_API_ID", "REAL_TG_API_HASH", "REAL_TG_PHONE", "REAL_TG_SESSION")
DEFAULT_INTERVALS = (1.0, 0.8, 0.6, 0.45, 0.35, 0.25, 0.2)
DEFAULT_MESSAGES_PER_POINT = 8
DEFAULT_SETTLE_SECONDS = 5.0
DEFAULT_MAX_MESSAGES = 50
DEFAULT_MAX_SECONDS = 180.0
POST_FLOOD_BUFFER_SEC = 2.0

SleepFn = Callable[[float], Awaitable[None]]
MonotonicFn = Callable[[], float]


@dataclass
class PointResult:
    interval: float
    sent: int
    flooded: bool = False
    flood_wait_sec: float | None = None


@dataclass
class CalibrationReport:
    points: list[PointResult] = field(default_factory=list)
    message_ids: list[int] = field(default_factory=list)
    flooded_at_interval: float | None = None
    safe_interval: float | None = None
    recommendation: str = ""
    stopped_reason: str = "completed"
    cleaned_up: bool = False

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False)


def build_recommendation(safe_interval: float | None) -> str:
    """Verdict comparing the measured boundary to the packaged default."""
    default_window = TelegramRateLimitGate.SEND_PEER_USER_SPEC.window_sec
    if safe_interval is None:
        return (
            "no clean point measured — no interval held without a flood; "
            "do not lower the default (1 message / "
            f"{default_window:.0f}s per user peer) based on this run"
        )
    suggested = RateLimitSpec(max_calls=1, window_sec=round(safe_interval * 1.25, 2))
    if safe_interval >= 1.0:
        return (
            f"flood-free only down to {safe_interval:.2f}s between sends — the packaged "
            f"default (1 message / {default_window:.0f}s per user peer) is TOO PERMISSIVE; "
            f"suggest RateLimitSpec(max_calls=1, window_sec={suggested.window_sec})"
        )
    return (
        f"flood-free down to {safe_interval:.2f}s between sends — the packaged default "
        f"(1 message / {default_window:.0f}s per user peer) is conservative and safe; "
        f"a tighter option would be RateLimitSpec(max_calls=1, window_sec={suggested.window_sec})"
    )


async def run_calibration(
    client: Any,
    peer: Any,
    *,
    intervals: tuple[float, ...] = DEFAULT_INTERVALS,
    messages_per_point: int = DEFAULT_MESSAGES_PER_POINT,
    settle_seconds: float = DEFAULT_SETTLE_SECONDS,
    max_messages: int = DEFAULT_MAX_MESSAGES,
    max_seconds: float = DEFAULT_MAX_SECONDS,
    sleep: SleepFn = asyncio.sleep,
    monotonic: MonotonicFn = time.monotonic,
) -> CalibrationReport:
    """Probe the raw send boundary; the core seam (unit-tested offline).

    ``client`` needs only ``send_message(peer, text)`` and
    ``delete_messages(peer, ids)`` awaitables — enough for a fake in tests.

    Contract: this function SENDS FOR REAL when given a real client and is
    deliberately not gated — the ``RUN_FLOODGATE_LIVE_TG`` opt-in is enforced
    at the real-client boundary in ``main()``, which also pins ``peer`` to the
    authenticated account itself (Saved Messages). The ``peer`` parameter
    exists for the offline fake; passing a third-party peer here is the
    caller's explicit choice, not something the CLI exposes.

    Cleanup is guaranteed: the probe loop runs under ``finally``, so probe
    messages are deleted even when a non-flood exception interrupts the run.
    """
    report = CalibrationReport()
    started = monotonic()
    flood_wait_sec: float | None = None

    def _budget_exhausted() -> bool:
        if len(report.message_ids) >= max_messages:
            report.stopped_reason = f"max-messages budget ({max_messages}) reached"
            return True
        if monotonic() - started >= max_seconds:
            report.stopped_reason = f"max-seconds budget ({max_seconds:.0f}s) reached"
            return True
        return False

    try:
        paced_once = False  # at least one send has happened in the whole run
        budget_stop = False
        for interval in intervals:
            point = PointResult(interval=interval, sent=0)
            for _ in range(messages_per_point):
                if paced_once:
                    # Pace consecutive sends at `interval`. After a settle
                    # pause this over-waits by one interval — deliberately
                    # conservative for an account-probing script.
                    await sleep(interval)
                # Budgets are checked right before the actual send (after the
                # pacing sleep), so a budget stop never fires an extra message.
                if _budget_exhausted():
                    budget_stop = True
                    break
                try:
                    message = await client.send_message(peer, _probe_text(interval, point.sent))
                    report.message_ids.append(message.id)
                    point.sent += 1
                    paced_once = True
                except FloodWaitError as exc:
                    seconds = float(getattr(exc, "seconds", 0) or 0)
                    point.flooded = True
                    point.flood_wait_sec = seconds
                    flood_wait_sec = seconds
                    report.flooded_at_interval = interval
                    break
            report.points.append(point)
            if budget_stop or point.flooded:
                if point.flooded:
                    report.stopped_reason = "flood wait observed"
                break
            if interval != intervals[-1]:
                await sleep(settle_seconds)
    finally:
        clean = [p.interval for p in report.points if not p.flooded and p.sent > 0]
        report.safe_interval = min(clean) if clean else None
        if report.stopped_reason == "completed" and not clean:
            report.stopped_reason = "no sends succeeded"
        await _finish(client, peer, report, flood_wait_sec, sleep)

    return report


async def _finish(
    client: Any,
    peer: Any,
    report: CalibrationReport,
    flood_wait_sec: float | None,
    sleep: SleepFn,
) -> None:
    if flood_wait_sec is not None:
        # Wait out the flood BEFORE deleting: deletion is an API call too and
        # must not strike the already-flooded account. Any observed flood
        # (even one reporting seconds <= 0) earns at least a 1s pause.
        await sleep(max(flood_wait_sec, 1.0) + POST_FLOOD_BUFFER_SEC)
    report.recommendation = build_recommendation(report.safe_interval)
    if not report.message_ids:
        report.cleaned_up = True
        return
    try:
        await client.delete_messages(peer, list(report.message_ids))
        report.cleaned_up = True
    except Exception as exc:  # noqa: BLE001 - a cleanup failure must not lose the report
        report.cleaned_up = False
        print(f"cleanup warning: delete_messages failed: {exc}", file=sys.stderr)


def _probe_text(interval: float, index: int) -> str:
    return f"floodgate calibration probe interval={interval:.2f} #{index}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--messages-per-point", type=int, default=DEFAULT_MESSAGES_PER_POINT)
    parser.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES)
    parser.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    parser.add_argument("--settle-seconds", type=float, default=DEFAULT_SETTLE_SECONDS)
    args = parser.parse_args(argv)

    if os.environ.get(GATE_ENV) != "1":
        print(f"refusing to run: set {GATE_ENV}=1 (this script sends real messages)", file=sys.stderr)
        return 2
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        print(f"missing environment variables: {', '.join(missing)}", file=sys.stderr)
        return 2

    async def _run() -> CalibrationReport:
        client = TelegramClient(
            StringSession(os.environ["REAL_TG_SESSION"]),
            int(os.environ["REAL_TG_API_ID"]),
            os.environ["REAL_TG_API_HASH"],
        )
        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise RuntimeError("REAL_TG_SESSION is not authorized")
            return await run_calibration(
                client,
                await client.get_me(),
                messages_per_point=args.messages_per_point,
                settle_seconds=args.settle_seconds,
                max_messages=args.max_messages,
                max_seconds=args.max_seconds,
            )
        finally:
            await client.disconnect()

    try:
        report = asyncio.run(_run())
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(report.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
