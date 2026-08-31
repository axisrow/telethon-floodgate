"""Vendored UTC datetime helpers.

Copied verbatim from the origin project's ``src/utils/datetime.py`` so that
``flood_wait`` keeps its exact parsing semantics without importing from the
application. Internal module: not part of the public API.
"""
from __future__ import annotations

from datetime import datetime, timezone


def normalize_utc(value: datetime | None) -> datetime | None:
    """Ensure a datetime is UTC-aware; treat naive values as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def try_parse_datetime(value: str | datetime | None) -> datetime | None:
    try:
        return parse_datetime(value)
    except ValueError:
        return None


def try_parse_utc_datetime(value: str | datetime | None) -> datetime | None:
    return normalize_utc(try_parse_datetime(value))
