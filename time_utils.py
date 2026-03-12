from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE_NAME = "Asia/Shanghai"
DISPLAY_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DISPLAY_DATE_FORMAT = "%Y-%m-%d"


def get_configured_timezone_name() -> str:
    configured = str(os.getenv("TZ") or "").strip()
    return configured or DEFAULT_TIMEZONE_NAME


def get_configured_timezone() -> tzinfo:
    timezone_name = get_configured_timezone_name()
    try:
        return ZoneInfo(timezone_name)
    except Exception:
        try:
            return ZoneInfo(DEFAULT_TIMEZONE_NAME)
        except Exception:
            return timezone(timedelta(hours=8), name=DEFAULT_TIMEZONE_NAME)


def local_now() -> datetime:
    return datetime.now(get_configured_timezone())


def local_now_str(fmt: str = DISPLAY_TIME_FORMAT) -> str:
    return local_now().strftime(fmt)


def local_today_str(fmt: str = DISPLAY_DATE_FORMAT) -> str:
    return local_now().strftime(fmt)


def parse_display_timestamp(value: object, *, assume_utc: bool = False) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        return None

    target_timezone = get_configured_timezone()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc if assume_utc else target_timezone)
    return parsed.astimezone(target_timezone)


def format_display_timestamp(
    value: object,
    *,
    fallback: str = "",
    assume_utc: bool = False,
    fmt: str = DISPLAY_TIME_FORMAT,
) -> str:
    parsed = parse_display_timestamp(value, assume_utc=assume_utc)
    if parsed is None:
        return fallback
    return parsed.strftime(fmt)
