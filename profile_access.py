import os
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo


RESTAURANT_TIMEZONE = ZoneInfo("Asia/Tashkent")
LOCATION_MAX_AGE = timedelta(hours=int(os.getenv("LOCATION_MAX_AGE_HOURS", "24")))
LOCATION_REFRESH_MESSAGE = "Location refresh required. Please return to the Telegram bot and share your current location."


def ensure_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=RESTAURANT_TIMEZONE).astimezone(ZoneInfo("UTC"))
    return value.astimezone(ZoneInfo("UTC"))


def has_profile_identity(profile: Optional[Any]) -> bool:
    return bool(profile and (getattr(profile, "name", "") or "").strip() and (getattr(profile, "phone", "") or "").strip())


def has_profile_location(profile: Optional[Any]) -> bool:
    return bool(profile and getattr(profile, "latitude", None) is not None and getattr(profile, "longitude", None) is not None)


def is_profile_complete(profile: Optional[Any]) -> bool:
    return has_profile_identity(profile) and has_profile_location(profile)


def is_profile_verified(profile: Optional[Any]) -> bool:
    return bool(profile and getattr(profile, "verified", False) and is_profile_complete(profile))


def is_location_fresh(profile: Optional[Any], now: Optional[datetime] = None) -> bool:
    if not has_profile_location(profile) or not getattr(profile, "last_location_at", None):
        return False
    current_time = ensure_utc_datetime(now or datetime.now(RESTAURANT_TIMEZONE))
    return current_time - ensure_utc_datetime(profile.last_location_at) <= LOCATION_MAX_AGE


def get_location_refresh_message() -> str:
    return LOCATION_REFRESH_MESSAGE


def is_profile_app_ready(profile: Optional[Any], now: Optional[datetime] = None) -> dict[str, Any]:
    readiness = {
        "has_name": bool(profile and (getattr(profile, "name", "") or "").strip()),
        "has_phone": bool(profile and (getattr(profile, "phone", "") or "").strip()),
        "has_location": has_profile_location(profile),
        "verified": is_profile_verified(profile),
        "last_location_at": getattr(profile, "last_location_at", None) if profile else None,
        "location_fresh": is_location_fresh(profile, now=now),
        "location_refresh_message": get_location_refresh_message(),
    }
    readiness["location_refresh_required"] = not readiness["location_fresh"]
    readiness["verification_required"] = not readiness["verified"]
    readiness["app_ready"] = readiness["verified"] and readiness["location_fresh"]
    return readiness
