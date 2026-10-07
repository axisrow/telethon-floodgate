from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, Awaitable, Protocol, TypeVar

from pydantic import BaseModel
from telethon.errors import FloodWaitError

from telethon_floodgate._datetime import try_parse_utc_datetime


class FloodReportingGate(Protocol):
    """Structural slice of TelegramRateLimitGate the flood helpers need.

    The helpers deliver EVERY wait — including routine ≤60s pacing floods;
    filtering those is the implementation's contract (the concrete gate
    ignores them). The return is ``float | None`` so lenient reporting hooks
    may return nothing; the concrete gate returns the new multiplier.
    """

    def note_flood(self, phone: str, seconds: float | None = None) -> float | None: ...


logger = logging.getLogger(__name__)

T = TypeVar("T")

TRANSIENT_FLOOD_WAIT_MAX_SEC = 60
TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC = 120
FLOOD_WAIT_RETRY_BUFFER_SEC = 1.0


class FloodWaitInfo(BaseModel):
    operation: str
    phone: str | None = None
    wait_seconds: int
    next_available_at_utc: datetime
    detail: str


class HandledFloodWaitError(RuntimeError):
    def __init__(self, info: FloodWaitInfo):
        super().__init__(info.detail)
        self.info = info


def coerce_flood_wait_seconds(value: int | float | str | None) -> int:
    return max(1, int(value or 0))


def is_transient_flood_wait_seconds(
    wait_seconds: int | None,
    *,
    max_seconds: int = TRANSIENT_FLOOD_WAIT_MAX_SEC,
) -> bool:
    if wait_seconds is None:
        return False
    return 0 < int(wait_seconds) <= max_seconds


def flood_wait_remaining_seconds(value: object, *, now: datetime | None = None) -> int | None:
    if value is not None and not isinstance(value, (str, datetime)):
        return None
    flood_until = try_parse_utc_datetime(value)
    if flood_until is None:
        return None
    now = now or datetime.now(timezone.utc)
    # ceil, not truncate: a flood expiring in 0.6s must stay classified
    # (remaining=1 -> transient); truncation collapses it to 0, which is
    # neither transient nor blocking and leaves the caller no branch.
    return max(0, ceil((flood_until - now).total_seconds()))


def is_transient_flood_wait_until(
    value: object,
    *,
    now: datetime | None = None,
    max_seconds: int = TRANSIENT_FLOOD_WAIT_MAX_SEC,
) -> bool:
    remaining = flood_wait_remaining_seconds(value, now=now)
    return is_transient_flood_wait_seconds(remaining, max_seconds=max_seconds)


def is_blocking_flood_wait_until(
    value: object,
    *,
    now: datetime | None = None,
    max_seconds: int = TRANSIENT_FLOOD_WAIT_MAX_SEC,
) -> bool:
    remaining = flood_wait_remaining_seconds(value, now=now)
    return remaining is not None and remaining > max_seconds


def format_flood_wait_detail(
    *,
    wait_seconds: int,
    next_available_at_utc: datetime,
    phone: str | None = None,
) -> str:
    detail = (
        f"Flood wait {wait_seconds}s until {next_available_at_utc.isoformat()} UTC"
    )
    if phone:
        detail += f" for {phone}"
    return detail


async def handle_flood_wait(
    exc: FloodWaitError,
    *,
    operation: str,
    phone: str | None = None,
    pool: Any | None = None,
    gate: FloodReportingGate | None = None,
    logger_: logging.Logger | None = None,
) -> FloodWaitInfo:
    wait_seconds = coerce_flood_wait_seconds(getattr(exc, "seconds", 0))
    next_available_at_utc = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
    detail = format_flood_wait_detail(
        wait_seconds=wait_seconds,
        next_available_at_utc=next_available_at_utc,
        phone=phone,
    )

    if pool is not None and phone:
        reporter = getattr(pool, "report_flood", None)
        if callable(reporter):
            # The pool object belongs to the consumer: sync hooks are just as
            # legitimate — awaiting their None result used to crash flood
            # handling with TypeError instead of absorbing the flood.
            reported = reporter(phone, wait_seconds)
            if inspect.isawaitable(reported):
                await reported

    active_logger = logger_ or logger

    # Feed the gate's adaptive backoff — the contract (delivery replaces any
    # manual hook, the 60s filter is the gate's own, no-op without
    # flood_backoff=True) lives on FloodReportingGate.note_flood.
    if gate is not None and phone:
        multiplier = gate.note_flood(phone, wait_seconds)
        if multiplier and multiplier > 1.0:
            active_logger.debug(
                "%s: gate backoff multiplier now %.1fx for %s",
                operation,
                multiplier,
                phone,
            )

    if is_transient_flood_wait_seconds(wait_seconds):
        active_logger.info("%s: transient %s", operation, detail)
    else:
        active_logger.warning("%s: %s", operation, detail)

    return FloodWaitInfo(
        operation=operation,
        phone=phone,
        wait_seconds=wait_seconds,
        next_available_at_utc=next_available_at_utc,
        detail=detail,
    )


async def sleep_for_flood_wait_seconds(
    wait_seconds: int,
    *,
    operation: str,
    phone: str | None = None,
    logger_: logging.Logger | None = None,
    buffer_seconds: float = FLOOD_WAIT_RETRY_BUFFER_SEC,
) -> None:
    sleep_seconds = max(0.0, float(wait_seconds)) + max(0.0, buffer_seconds)
    active_logger = logger_ or logger
    phone_suffix = f" for {phone}" if phone else ""
    active_logger.info(
        "%s: waiting %.1fs for transient FloodWait%s",
        operation,
        sleep_seconds,
        phone_suffix,
    )
    await asyncio.sleep(sleep_seconds)


async def sleep_for_handled_flood_wait(
    info: FloodWaitInfo,
    *,
    logger_: logging.Logger | None = None,
    buffer_seconds: float = FLOOD_WAIT_RETRY_BUFFER_SEC,
) -> None:
    await sleep_for_flood_wait_seconds(
        info.wait_seconds,
        operation=info.operation,
        phone=info.phone,
        logger_=logger_,
        buffer_seconds=buffer_seconds,
    )


async def run_with_flood_wait(
    awaitable: Awaitable[T],
    *,
    operation: str,
    phone: str | None = None,
    pool: Any | None = None,
    gate: FloodReportingGate | None = None,
    logger_: logging.Logger | None = None,
    timeout: float | None = None,
) -> T:
    try:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except FloodWaitError as exc:
        info = await handle_flood_wait(
            exc,
            operation=operation,
            phone=phone,
            pool=pool,
            gate=gate,
            logger_=logger_,
        )
        raise HandledFloodWaitError(info) from exc


async def run_with_flood_wait_retry(
    awaitable_factory: Callable[[], Awaitable[T]],
    *,
    operation: str,
    phone: str | None = None,
    pool: Any | None = None,
    gate: FloodReportingGate | None = None,
    logger_: logging.Logger | None = None,
    timeout: float | None = None,
    transient_wait_max_sec: int = TRANSIENT_FLOOD_WAIT_MAX_SEC,
    transient_wait_budget_sec: int = TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC,
) -> T:
    waited_seconds = 0
    while True:
        try:
            return await run_with_flood_wait(
                awaitable_factory(),
                operation=operation,
                phone=phone,
                pool=pool,
                gate=gate,
                logger_=logger_,
                timeout=timeout,
            )
        except HandledFloodWaitError as exc:
            wait_seconds = exc.info.wait_seconds
            if not is_transient_flood_wait_seconds(
                wait_seconds,
                max_seconds=transient_wait_max_sec,
            ):
                raise
            # Count what a retry actually sleeps — the wait plus the buffer;
            # counting the bare wait let n retries overshoot the documented
            # budget by up to n seconds.
            if (
                waited_seconds + wait_seconds + FLOOD_WAIT_RETRY_BUFFER_SEC
                > transient_wait_budget_sec
            ):
                raise
            await sleep_for_handled_flood_wait(exc.info, logger_=logger_)
            # Accumulate what the retry actually cost (wait + buffer) —
            # accounting bare waits still let n retries overshoot by n-1.
            waited_seconds += wait_seconds + FLOOD_WAIT_RETRY_BUFFER_SEC
