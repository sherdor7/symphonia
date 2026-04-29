import asyncio
import base64
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from init_data_py import InitData
from pydantic import BaseModel
from sqlalchemy.orm import joinedload, selectinload

from database import (
    CartItem,
    DEFAULT_ORDER_STATUS,
    FINAL_ORDER_STATUSES,
    MenuItem,
    ORDER_STATUSES,
    Order,
    OrderItem,
    Profile,
    SessionLocal,
    TERMINAL_ORDER_STATUSES,
    User,
    init_db,
    normalize_order_status,
)
from bot import send_ready_order_to_group_sync
from bot import send_ready_order_to_group_result_sync
from profile_access import (
    LOCATION_MAX_AGE as SHARED_LOCATION_MAX_AGE,
    RESTAURANT_TIMEZONE as SHARED_RESTAURANT_TIMEZONE,
    get_location_refresh_message as shared_get_location_refresh_message,
    is_location_fresh as shared_is_location_fresh,
    is_profile_app_ready as shared_is_profile_app_ready,
    is_profile_verified as shared_is_profile_verified,
)

BOT_TOKEN = os.getenv("BOT_TOKEN") or "8524810543:AAHCihTyuTHCm5QmPiKelN6awOEhuvRxSLA"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD") or "admin123"
MANAGER_PASSWORD = os.getenv("MANAGER_PASSWORD") or ADMIN_PASSWORD
FAST_FOOD_CHEF_PASSWORD = os.getenv("FAST_FOOD_CHEF_PASSWORD") or "fastfood123"
MILLIY_CHEF_PASSWORD = os.getenv("MILLIY_CHEF_PASSWORD") or "milliy123"
CASHIER_PASSWORD = os.getenv("CASHIER_PASSWORD") or "cashier123"
WAITER_PASSWORD = os.getenv("WAITER_PASSWORD") or "waiter123"
ADMIN_SESSION_TOKEN = os.getenv("ADMIN_SESSION_TOKEN") or secrets.token_urlsafe(32)
FAST_FOOD_CHEF_SESSION_TOKEN = os.getenv("FAST_FOOD_CHEF_SESSION_TOKEN") or secrets.token_urlsafe(32)
MILLIY_CHEF_SESSION_TOKEN = os.getenv("MILLIY_CHEF_SESSION_TOKEN") or secrets.token_urlsafe(32)
CASHIER_SESSION_TOKEN = os.getenv("CASHIER_SESSION_TOKEN") or secrets.token_urlsafe(32)
WAITER_SESSION_TOKEN = os.getenv("WAITER_SESSION_TOKEN") or secrets.token_urlsafe(32)
ROLE_MANAGER = "manager"
ROLE_FAST_FOOD_CHEF = "fast_food_chef"
ROLE_MILLIY_CHEF = "milliy_chef"
ROLE_CASHIER = "cashier"
ROLE_WAITER = "waiter"
ADMIN_PASSWORDS_FILE = Path("admin_role_passwords.json")
RESTAURANT_SETTINGS_FILE = Path("restaurant_settings.json")
MIN_ADMIN_PASSWORD_LENGTH = 6
DEFAULT_ADMIN_ROLE_PASSWORDS = {
    ROLE_MANAGER: MANAGER_PASSWORD,
    ROLE_FAST_FOOD_CHEF: FAST_FOOD_CHEF_PASSWORD,
    ROLE_MILLIY_CHEF: MILLIY_CHEF_PASSWORD,
    ROLE_CASHIER: CASHIER_PASSWORD,
    ROLE_WAITER: WAITER_PASSWORD,
}
ADMIN_ROLE_LABELS = {
    ROLE_MANAGER: "Manager",
    ROLE_FAST_FOOD_CHEF: "Fast Food Chef",
    ROLE_MILLIY_CHEF: "Milliy Taom Chef",
    ROLE_CASHIER: "Cashier",
    ROLE_WAITER: "Waiter",
}
ADMIN_ROLE_SESSION_TOKENS = {
    ROLE_MANAGER: ADMIN_SESSION_TOKEN,
    ROLE_FAST_FOOD_CHEF: FAST_FOOD_CHEF_SESSION_TOKEN,
    ROLE_MILLIY_CHEF: MILLIY_CHEF_SESSION_TOKEN,
    ROLE_CASHIER: CASHIER_SESSION_TOKEN,
}
ADMIN_SESSION_TOKEN_TO_ROLE = {
    token: role for role, token in ADMIN_ROLE_SESSION_TOKENS.items()
}
AUTO_DELIVERING_AFTER = timedelta(minutes=8)
READY_NOTIFICATION_RETRY_SECONDS = int(os.getenv("READY_NOTIFICATION_RETRY_SECONDS", "15"))
READY_NOTIFICATION_BATCH_SIZE = int(os.getenv("READY_NOTIFICATION_BATCH_SIZE", "10"))
LOCATION_MAX_AGE = SHARED_LOCATION_MAX_AGE
WAITER_INTERNAL_USER_ID = "waiter-internal"
UPLOADS_DIR = Path("webapp/uploads")
ALLOWED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
MENU_CATEGORIES = (
    "Sets",
    "Burgers",
    "Shawarma",
    "Samsa",
    "Hot Meals",
    "Snacks",
    "Sauces",
    "Salads",
    "Drinks",
    "Others",
)
MILLIY_TAOM_CATEGORIES = {"Hot Meals", "Samsa"}
KITCHEN_GROUPS = {"fast_food", "milliy_taom"}
RESTAURANT_TIMEZONE = SHARED_RESTAURANT_TIMEZONE
DEFAULT_WORKING_HOURS_SETTINGS = {
    "opening_time": "10:00",
    "closing_time": "23:00",
    "enabled": False,
}

app = FastAPI(title="Food Delivery Telegram Mini App")
init_db()
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger(__name__)
ready_notification_retry_task: Optional[asyncio.Task] = None


class AddToCartRequest(BaseModel):
    product_id: int
    quantity: int = 1


class UpdateCartRequest(BaseModel):
    product_id: int
    quantity: int


class CheckoutRequest(BaseModel):
    delivery_type: str
    address: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    resolved_address: Optional[str] = None
    payment_method: str
    comment: str = ""


class ProfileUpdateRequest(BaseModel):
    name: str
    phone: str
    language: str = "English"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    readable_address: Optional[str] = None


class BotProfileUpdateRequest(BaseModel):
    telegram_user_id: str
    name: str
    phone: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    language: str = "English"
    telegram_username: Optional[str] = None
    readable_address: Optional[str] = None


class AdminLoginRequest(BaseModel):
    password: str


class WaiterLoginRequest(BaseModel):
    name: str
    password: str


class AdminStatusUpdateRequest(BaseModel):
    status: str
    payment_method: Optional[str] = None


class MarkPaidRequest(BaseModel):
    payment_method: str


class AdminSettingsUpdatePasswordsRequest(BaseModel):
    manager_password: str = ""
    fast_food_password: str = ""
    milliy_password: str = ""
    cashier_password: str = ""
    waiter_password: str = ""


class AdminWorkingHoursSettingsRequest(BaseModel):
    opening_time: str
    closing_time: str
    enabled: bool = False


class AdminSettingsUpdateRolePasswordRequest(BaseModel):
    role: str
    new_password: str


class AdminMenuUpsertRequest(BaseModel):
    category: str
    category_group: str
    name: str
    price: float
    description: str = ""
    image_data_url: Optional[str] = None
    image_filename: Optional[str] = None


class AdminMenuAvailabilityRequest(BaseModel):
    is_available: bool


class AdminCategoryRenameRequest(BaseModel):
    category: str
    new_name: str


class AdminCategoryRemoveRequest(BaseModel):
    category: str


class WaiterOrderItemRequest(BaseModel):
    product_id: int
    quantity: int


class WaiterOrderCreateRequest(BaseModel):
    table_number: Optional[int] = None
    items: list[WaiterOrderItemRequest]
    comment: str = ""
    waiter_name: str = ""


def validate_waiter_table_number(table_number: Optional[int]) -> int:
    if table_number is None or table_number <= 0:
        raise HTTPException(status_code=400, detail="Please select table number")
    return table_number


def normalize_kitchen_group(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in KITCHEN_GROUPS:
        raise HTTPException(status_code=400, detail="Invalid kitchen group")
    return normalized


def normalize_order_item_kitchen_status(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"new", "preparing", "ready"}:
        return normalized
    return "new"


def normalize_cashier_payment_method(value: str) -> str:
    normalized = (value or "").strip().lower()
    mapping = {
        "cash": "Cash",
        "card": "Card",
        "transfer": "Transfer",
    }
    if normalized not in mapping:
        raise HTTPException(status_code=400, detail="Invalid payment method")
    return mapping[normalized]


def normalize_waiter_name(value: Optional[str]) -> str:
    normalized = " ".join((value or "").strip().split())
    if not normalized:
        return "Unknown"
    return normalized[:80]


def get_telegram_user_payload(x_telegram_init_data: Optional[str]):
    if not x_telegram_init_data:
        return None

    try:
        parsed = InitData.parse(x_telegram_init_data)

        if not parsed.validate(BOT_TOKEN):
            raise HTTPException(status_code=401, detail="Invalid Telegram init data")

        if not parsed.user:
            raise HTTPException(status_code=401, detail="Telegram user not found")

        return parsed.user
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Failed to parse Telegram init data")


def get_telegram_user_id(x_telegram_init_data: Optional[str]) -> str:
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    if not telegram_user:
        return "demo-user-1"
    return str(telegram_user.id)


def get_admin_role_for_token(x_admin_token: Optional[str]) -> Optional[str]:
    if not x_admin_token:
        return None
    return ADMIN_SESSION_TOKEN_TO_ROLE.get(x_admin_token)


def load_stored_admin_role_passwords() -> dict[str, str]:
    if not ADMIN_PASSWORDS_FILE.exists():
        return {}

    try:
        raw_data = json.loads(ADMIN_PASSWORDS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.exception("Failed to load stored admin role passwords: %s", exc)
        return {}

    if not isinstance(raw_data, dict):
        logger.warning("Stored admin role passwords file is not a JSON object")
        return {}

    stored_passwords: dict[str, str] = {}
    for role in DEFAULT_ADMIN_ROLE_PASSWORDS:
        value = raw_data.get(role)
        if isinstance(value, str) and value.strip():
            stored_passwords[role] = value.strip()
    return stored_passwords


def get_admin_role_passwords() -> dict[str, str]:
    passwords = dict(DEFAULT_ADMIN_ROLE_PASSWORDS)
    passwords.update(load_stored_admin_role_passwords())
    return passwords


def save_admin_role_passwords(passwords: dict[str, str]) -> None:
    payload = {
        role: str(password).strip()
        for role, password in passwords.items()
        if role in DEFAULT_ADMIN_ROLE_PASSWORDS and str(password).strip()
    }
    temp_path = ADMIN_PASSWORDS_FILE.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    temp_path.replace(ADMIN_PASSWORDS_FILE)


def normalize_working_hours_time(value: Optional[str], field_name: str) -> str:
    normalized = (value or "").strip()
    try:
        parsed = datetime.strptime(normalized, "%H:%M")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be in HH:MM format") from exc
    return parsed.strftime("%H:%M")


def get_restaurant_settings() -> dict:
    settings = DEFAULT_WORKING_HOURS_SETTINGS.copy()
    if not RESTAURANT_SETTINGS_FILE.exists():
        return settings

    try:
        loaded = json.loads(RESTAURANT_SETTINGS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Failed to read restaurant settings. Using defaults.")
        return settings

    settings["opening_time"] = normalize_working_hours_time(
        loaded.get("opening_time", settings["opening_time"]),
        "Opening time",
    )
    settings["closing_time"] = normalize_working_hours_time(
        loaded.get("closing_time", settings["closing_time"]),
        "Closing time",
    )
    settings["enabled"] = bool(loaded.get("enabled", settings["enabled"]))
    return settings


def save_restaurant_settings(settings: dict) -> None:
    payload = {
        "opening_time": normalize_working_hours_time(settings.get("opening_time"), "Opening time"),
        "closing_time": normalize_working_hours_time(settings.get("closing_time"), "Closing time"),
        "enabled": bool(settings.get("enabled", False)),
    }
    temp_path = RESTAURANT_SETTINGS_FILE.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    temp_path.replace(RESTAURANT_SETTINGS_FILE)


def is_within_working_hours(now: Optional[datetime] = None, settings: Optional[dict] = None) -> bool:
    current_settings = settings or get_restaurant_settings()
    if not current_settings.get("enabled"):
        return True

    opening_time = datetime.strptime(current_settings["opening_time"], "%H:%M").time()
    closing_time = datetime.strptime(current_settings["closing_time"], "%H:%M").time()
    local_now = (now or datetime.now(RESTAURANT_TIMEZONE)).astimezone(RESTAURANT_TIMEZONE)
    current_time = local_now.time()

    if opening_time == closing_time:
        return True
    if opening_time < closing_time:
        return opening_time <= current_time < closing_time
    return current_time >= opening_time or current_time < closing_time


def detect_admin_role_from_password(password: str) -> Optional[str]:
    normalized_password = (password or "").strip()
    role_passwords = get_admin_role_passwords()
    for role in [ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF, ROLE_CASHIER]:
        if normalized_password == role_passwords.get(role):
            return role
    return None


def verify_waiter_password(password: str) -> bool:
    normalized_password = (password or "").strip()
    if not normalized_password:
        return False
    return normalized_password == get_admin_role_passwords().get(ROLE_WAITER)


def require_waiter_auth(x_waiter_token: Optional[str], action_name: str = "waiter_access") -> None:
    if not x_waiter_token or x_waiter_token != WAITER_SESSION_TOKEN:
        logger.warning("Waiter authentication failed action=%s", action_name)
        raise HTTPException(status_code=401, detail="Waiter authentication required")
    logger.info("Waiter access granted action=%s", action_name)


def require_admin_auth(x_admin_token: Optional[str], action_name: str = "admin_access") -> str:
    role = get_admin_role_for_token(x_admin_token)
    if not role:
        logger.warning("Admin authentication failed action=%s", action_name)
        raise HTTPException(status_code=401, detail="Admin authentication required")
    logger.info("Admin access granted action=%s role=%s", action_name, role)
    return role


def require_admin_roles(
    x_admin_token: Optional[str],
    allowed_roles: set[str],
    action_name: str,
) -> str:
    role = require_admin_auth(x_admin_token, action_name)
    if role not in allowed_roles:
        logger.warning(
            "Admin access denied action=%s role=%s allowed_roles=%s",
            action_name,
            role,
            sorted(allowed_roles),
        )
        raise HTTPException(status_code=403, detail="Access denied")
    logger.info(
        "Admin role authorized action=%s role=%s allowed_roles=%s",
        action_name,
        role,
        sorted(allowed_roles),
    )
    return role


def is_profile_verified(profile: Optional[Profile]) -> bool:
    return shared_is_profile_verified(profile)


def is_location_fresh(profile: Optional[Profile], now: Optional[datetime] = None) -> bool:
    return shared_is_location_fresh(profile, now=now)


def get_location_refresh_message() -> str:
    return shared_get_location_refresh_message()


def preferred_language_to_app_language(preferred_language: Optional[str]) -> str:
    mapping = {
        "en": "English",
        "uz_latn": "Uzbek",
        "ru": "Russian",
        "uz_cyrl": "Uzbek Cyrillic",
    }
    return mapping.get((preferred_language or "").strip(), "English")


def app_language_to_preferred_language(language: Optional[str]) -> str:
    mapping = {
        "English": "en",
        "Uzbek": "uz_latn",
        "Russian": "ru",
        "Uzbek Cyrillic": "uz_cyrl",
    }
    return mapping.get((language or "").strip(), "en")


def require_verified_profile_for_ordering(profile: Optional[Profile]) -> None:
    readiness = shared_is_profile_app_ready(profile)
    if readiness["verification_required"]:
        raise HTTPException(
            status_code=403,
            detail="Verification required. Please complete verification in the Telegram bot.",
        )
    if readiness["location_refresh_required"]:
        raise HTTPException(status_code=403, detail=readiness["location_refresh_message"])


def is_pickup_order(order: Order) -> bool:
    if (order.order_type or "").strip().lower() == "pickup":
        return True
    delivery_type = (order.delivery_type or "").strip().lower()
    return delivery_type == "pickup"


def is_dine_in_order(order: Order) -> bool:
    return (order.order_type or "").strip().lower() == "dine_in"


def validate_ready_transition(order: Order) -> None:
    effective_status = get_effective_order_status(order)
    if effective_status != "preparing":
        raise HTTPException(status_code=400, detail="Only preparing orders can be marked as ready")


def get_ready_target_status(order: Order) -> str:
    if is_dine_in_order(order):
        return "ready"
    return "ready_for_pickup" if is_pickup_order(order) else "ready"


def validate_admin_status_transition(order: Order, value: str) -> str:
    normalized = normalize_order_status(value)
    effective_status = get_effective_order_status(order)

    if is_pickup_order(order):
        allowed_statuses = {
            "new": {"preparing", "cancelled"},
            "preparing": {"ready_for_pickup", "cancelled"},
            "ready_for_pickup": {"picked_up", "cancelled"},
            "picked_up": set(),
            "cancelled": set(),
        }
    elif is_dine_in_order(order):
        allowed_statuses = {
            "new": {"preparing", "cancelled"},
            "preparing": {"ready", "cancelled"},
            "ready": {"served", "completed", "cancelled"},
            "served": set(),
            "completed": set(),
            "cancelled": set(),
        }
    else:
        allowed_statuses = {
            "new": {"preparing", "cancelled"},
            "preparing": {"ready", *FINAL_ORDER_STATUSES},
            "ready": {"completed", *FINAL_ORDER_STATUSES},
            "delivering": set(FINAL_ORDER_STATUSES),
            "delivered": set(),
            "completed": set(),
            "cancelled": set(),
        }

    if normalized not in allowed_statuses.get(effective_status, set()):
        raise HTTPException(
            status_code=400,
            detail=f"Order cannot move to {normalized} from {effective_status}",
        )

    return normalized


def format_datetime(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    formatted = ensure_utc_datetime(value).isoformat()
    logger.info("Formatted datetime for API output raw=%s formatted=%s", value, formatted)
    return formatted


def get_order_type_code(order_type: str) -> str:
    mapping = {
        "delivery": "DEL",
        "pickup": "PCU",
        "dine_in": "DNI",
    }
    return mapping.get((order_type or "").strip().lower(), "DEL")


def get_food_type_code_from_menu_items(menu_items: list[MenuItem]) -> str:
    kitchen_groups = {
        normalize_category_group(item.category_group, item.category)
        for item in menu_items
    }
    if not kitchen_groups:
        return "MX"
    if kitchen_groups == {"fast_food"}:
        return "FF"
    if kitchen_groups == {"milliy_taom"}:
        return "MT"
    return "MX"


def get_order_item_state_from_menu_item(menu_item: MenuItem) -> tuple[str, bool, str]:
    category_group = normalize_category_group(menu_item.category_group, menu_item.category)
    is_drink = (menu_item.category or "").strip().lower() == "drinks"
    kitchen_status = "ready" if is_drink else "new"
    return category_group, is_drink, kitchen_status


def generate_order_code(db, order_type: str, food_type_code: str, now: Optional[datetime] = None) -> str:
    current_time = ensure_utc_datetime(now or datetime.now(RESTAURANT_TIMEZONE))
    logger.info("Generating order code using Tashkent time=%s", current_time.isoformat())
    day_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    date_part = current_time.strftime("%y%m%d")
    prefix = f"{get_order_type_code(order_type)}-{food_type_code}-{date_part}-"

    existing_codes = (
        db.query(Order.order_id)
        .filter(Order.created_at >= day_start, Order.created_at < day_end)
        .all()
    )

    sequence = len(existing_codes) + 1
    existing_code_set = {code for (code,) in existing_codes}

    while True:
        candidate = f"{prefix}{sequence:03d}"
        if candidate not in existing_code_set:
            return candidate
        sequence += 1


def ensure_utc_datetime(value: Optional[datetime]) -> datetime:
    if not value:
        return datetime.now(RESTAURANT_TIMEZONE)
    if value.tzinfo is None:
        return value.replace(tzinfo=RESTAURANT_TIMEZONE)
    return value.astimezone(RESTAURANT_TIMEZONE)


def get_effective_order_status(order: Order, now: Optional[datetime] = None) -> str:
    stored_status = normalize_order_status(order.status)
    if stored_status in TERMINAL_ORDER_STATUSES:
        return stored_status
    if stored_status == "delivering":
        return "delivering"
    relevant_items = get_order_kitchen_items(order)
    if relevant_items:
        statuses = {normalize_order_item_kitchen_status(item.kitchen_status) for item in relevant_items}
        if all(status == "ready" for status in statuses):
            return get_ready_target_status(order)
        if "preparing" in statuses or "ready" in statuses:
            return "preparing"
        return "new"
    if stored_status in {"ready", "ready_for_pickup"}:
        return stored_status

    if is_pickup_order(order) or is_dine_in_order(order):
        if not order.preparing_started_at:
            return "new"
        return "preparing"

    current_time = ensure_utc_datetime(now or datetime.now(RESTAURANT_TIMEZONE))
    preparing_started_at = order.preparing_started_at
    if not preparing_started_at:
        return "new"

    elapsed = current_time - ensure_utc_datetime(preparing_started_at)
    if elapsed < AUTO_DELIVERING_AFTER:
        return "preparing"
    return "delivering"


def get_order_map_url(order: Order) -> str:
    if is_dine_in_order(order):
        return ""
    if order.latitude is not None and order.longitude is not None:
        map_url = (
            f"https://yandex.com/maps/?ll={order.longitude},{order.latitude}"
            f"&z=16&pt={order.longitude},{order.latitude},pm2rdm"
        )
        logger.info(
            "Generated Yandex map URL lat=%s lng=%s url=%s",
            order.latitude,
            order.longitude,
            map_url,
        )
        return map_url

    location_text = order.resolved_address or order.address or ""
    if not location_text:
        return ""
    map_url = f"https://yandex.com/maps/?text={quote_plus(location_text)}"
    logger.info(
        "Generated Yandex map URL from address lat=%s lng=%s url=%s",
        order.latitude,
        order.longitude,
        map_url,
    )
    return map_url


def send_ready_order_notification(order: Order) -> bool:
    serialized_order = serialize_admin_order_details(order)
    logger.info("Attempting ready-order Telegram notification for %s", serialized_order["order_id"])
    return send_ready_order_to_group_sync(serialized_order)


def truncate_notification_error(message: Optional[str]) -> Optional[str]:
    if not message:
        return None
    message = message.strip()
    if len(message) <= 500:
        return message
    return f"{message[:497]}..."


def calculate_ready_notification_next_attempt(
    now: datetime,
    retry_after_seconds: Optional[float] = None,
) -> datetime:
    delay_seconds = READY_NOTIFICATION_RETRY_SECONDS
    if retry_after_seconds and retry_after_seconds > delay_seconds:
        delay_seconds = int(retry_after_seconds)
    return now + timedelta(seconds=delay_seconds)


def reset_ready_notification_tracking(order: Order) -> None:
    order.ready_notification_sent = False
    order.ready_notification_attempts = 0
    order.ready_notification_last_error = None
    order.ready_notification_last_attempt_at = None
    order.ready_notification_next_attempt_at = datetime.now(RESTAURANT_TIMEZONE)


def attempt_ready_notification_for_order(order_id: str, *, is_retry: bool = False) -> dict:
    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if is_pickup_order(order) or is_dine_in_order(order):
            reason = "pickup_order" if is_pickup_order(order) else "dine_in_order"
            logger.info("Skipping ready notification for %s because reason=%s", order_id, reason)
            return {
                "sent": False,
                "skipped": True,
                "reason": reason,
            }
        effective_status = get_effective_order_status(order)
        if effective_status != "ready":
            logger.info(
                "Skipping ready notification for %s because effective_status=%s",
                order_id,
                effective_status,
            )
            return {
                "sent": False,
                "skipped": True,
                "reason": f"status={effective_status}",
            }

        if order.ready_notification_sent:
            logger.info("Skipping ready notification retry for %s because it is already sent", order_id)
            return {
                "sent": True,
                "skipped": True,
                "reason": "already_sent",
            }

        order.ready_notification_attempts = (order.ready_notification_attempts or 0) + 1
        order.ready_notification_last_attempt_at = datetime.now(RESTAURANT_TIMEZONE)
        db.commit()
        db.refresh(order)

        logger.info(
            "%s ready notification attempt=%s for order %s",
            "Retrying" if is_retry else "Initial",
            order.ready_notification_attempts,
            order_id,
        )

        result = send_ready_order_to_group_result_sync(serialize_admin_order_details(order))
        if result["sent"]:
            order.ready_notification_sent = True
            order.ready_notification_last_error = None
            order.ready_notification_next_attempt_at = None
            db.commit()
            logger.info(
                "Ready notification sent successfully for %s after attempts=%s",
                order_id,
                order.ready_notification_attempts,
            )
            return {
                "sent": True,
                "skipped": False,
                "attempts": order.ready_notification_attempts,
            }

        retry_after = result.get("retry_after")
        next_attempt_at = calculate_ready_notification_next_attempt(datetime.now(RESTAURANT_TIMEZONE), retry_after)
        order.ready_notification_sent = False
        order.ready_notification_last_error = truncate_notification_error(result.get("error"))
        order.ready_notification_next_attempt_at = next_attempt_at
        db.commit()
        logger.warning(
            "Ready notification %s for %s failed. attempts=%s retry_after=%s next_attempt_at=%s error=%s",
            "retry" if is_retry else "initial send",
            order_id,
            order.ready_notification_attempts,
            retry_after,
            next_attempt_at.isoformat(),
            order.ready_notification_last_error,
        )
        return {
            "sent": False,
            "skipped": False,
            "attempts": order.ready_notification_attempts,
            "error": order.ready_notification_last_error,
            "retry_after": retry_after,
            "next_attempt_at": format_datetime(next_attempt_at),
        }


def get_pending_ready_notification_order_ids(limit: int = READY_NOTIFICATION_BATCH_SIZE) -> list[str]:
    now = datetime.now(RESTAURANT_TIMEZONE)
    with SessionLocal() as db:
        orders = (
            db.query(Order.order_id)
            .filter(Order.status == "ready")
            .filter(Order.delivery_type == "Delivery")
            .filter(Order.ready_notification_sent.is_(False))
            .filter(Order.ready_notification_next_attempt_at.is_not(None))
            .filter(Order.ready_notification_next_attempt_at <= now)
            .order_by(Order.ready_notification_next_attempt_at.asc(), Order.id.asc())
            .limit(limit)
            .all()
        )
        return [order_id for (order_id,) in orders]


async def retry_pending_ready_notifications() -> None:
    order_ids = get_pending_ready_notification_order_ids()
    if not order_ids:
        return

    logger.info("Retry worker found %s pending ready notifications", len(order_ids))
    for order_id in order_ids:
        await asyncio.to_thread(attempt_ready_notification_for_order, order_id, is_retry=True)


async def ready_notification_retry_loop() -> None:
    logger.info(
        "Starting ready notification retry loop. interval_seconds=%s batch_size=%s",
        READY_NOTIFICATION_RETRY_SECONDS,
        READY_NOTIFICATION_BATCH_SIZE,
    )
    while True:
        try:
            await retry_pending_ready_notifications()
        except asyncio.CancelledError:
            logger.info("Ready notification retry loop cancelled")
            raise
        except Exception as exc:
            logger.exception("Ready notification retry loop failed: %s", exc)
        await asyncio.sleep(READY_NOTIFICATION_RETRY_SECONDS)


def get_or_create_user(user_id: str, telegram_username: Optional[str] = None):
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        if not user:
            user = User(telegram_user_id=user_id)
            db.add(user)
            db.flush()
            db.add(Profile(user_id=user.id, name="", phone="", language="English", preferred_language="en"))
        if user.profile and telegram_username:
            user.profile.telegram_username = telegram_username
        db.commit()
    return user_id


def get_or_create_internal_waiter_user(db) -> User:
    user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == WAITER_INTERNAL_USER_ID).first()
    if user:
        return user

    user = User(telegram_user_id=WAITER_INTERNAL_USER_ID)
    db.add(user)
    db.flush()
    db.add(
        Profile(
            user_id=user.id,
            name="Waiter Station",
            phone="",
            language="English",
            preferred_language="en",
            verified=True,
            verified_at=datetime.now(RESTAURANT_TIMEZONE),
        )
    )
    db.flush()
    return user


def get_menu_image_url(image_path: Optional[str]) -> Optional[str]:
    if not image_path:
        return None
    return image_path


def normalize_category_group(value: Optional[str], fallback_category: Optional[str] = None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"fast_food", "milliy_taom"}:
        return normalized
    if fallback_category:
        return "milliy_taom" if (fallback_category or "").strip() in MILLIY_TAOM_CATEGORIES else "fast_food"
    return "fast_food"


def get_kitchen_group_for_category(category: Optional[str]) -> str:
    if (category or "").strip() in MILLIY_TAOM_CATEGORIES:
        return "milliy_taom"
    return "fast_food"


def build_menu_item_metadata_map(db, product_ids: list[int]) -> dict[int, MenuItem]:
    unique_ids = [product_id for product_id in set(product_ids) if product_id is not None]
    if not unique_ids:
        return {}
    items = db.query(MenuItem).filter(MenuItem.id.in_(unique_ids)).all()
    return {item.id: item for item in items}


def serialize_order_items(order_items: list[OrderItem], menu_item_metadata_map: Optional[dict[int, MenuItem]] = None):
    metadata_map = menu_item_metadata_map
    if metadata_map is None:
        with SessionLocal() as db:
            metadata_map = build_menu_item_metadata_map(db, [item.product_id for item in order_items])

    serialized = []
    for item in order_items:
        menu_item = metadata_map.get(item.product_id) if metadata_map else None
        category = menu_item.category if menu_item else None
        stored_group = (item.kitchen_group or "").strip().lower()
        category_group = stored_group if stored_group in KITCHEN_GROUPS else normalize_category_group(
            menu_item.category_group if menu_item else None,
            category,
        )
        serialized.append(
            {
                "id": item.id,
                "product_id": item.product_id,
                "name": item.name,
                "price": item.price,
                "quantity": item.quantity,
                "line_total": item.line_total,
                "menu_category": category,
                "kitchen_group": category_group,
                "kitchen_status": normalize_order_item_kitchen_status(item.kitchen_status),
                "is_drink": bool(item.is_drink),
            }
        )
    return serialized


def get_order_group_items(order: Order, kitchen_group: str) -> list[OrderItem]:
    normalized_group = normalize_kitchen_group(kitchen_group)
    missing_group_product_ids = [
        item.product_id
        for item in order.items
        if not item.is_drink and (item.kitchen_group or "").strip().lower() not in KITCHEN_GROUPS
    ]
    metadata_map: dict[int, MenuItem] = {}
    if missing_group_product_ids:
        with SessionLocal() as db:
            metadata_map = build_menu_item_metadata_map(db, missing_group_product_ids)

    return [
        item
        for item in order.items
        if not item.is_drink
        and (
            (item.kitchen_group or "").strip().lower()
            or normalize_category_group(
                metadata_map.get(item.product_id).category_group if metadata_map.get(item.product_id) else None,
                metadata_map.get(item.product_id).category if metadata_map.get(item.product_id) else None,
            )
        ) == normalized_group
    ]


def get_order_kitchen_items(order: Order) -> list[OrderItem]:
    kitchen_items: list[OrderItem] = []
    for kitchen_group in KITCHEN_GROUPS:
        for item in get_order_group_items(order, kitchen_group):
            if item not in kitchen_items:
                kitchen_items.append(item)
    return kitchen_items


def recalculate_order_kitchen_state(order: Order) -> str:
    effective_status = get_effective_order_status(order)
    order.status = effective_status
    if effective_status == "new":
        order.preparing_started_at = None
    elif effective_status == "preparing":
        if order.preparing_started_at is None:
            order.preparing_started_at = datetime.now(RESTAURANT_TIMEZONE)
    elif effective_status == "ready":
        if order.preparing_started_at is None:
            order.preparing_started_at = datetime.now(RESTAURANT_TIMEZONE)
    if is_dine_in_order(order) and effective_status in {"new", "preparing", "ready"}:
        order.waiter_ready_acknowledged_at = None
    return effective_status


def recalculate_dine_in_order_state(order: Order) -> str:
    return recalculate_order_kitchen_state(order)


def serialize_menu_item(item: MenuItem):
    return {
        "id": item.id,
        "category": item.category,
        "category_group": normalize_category_group(item.category_group, item.category),
        "is_available": bool(item.is_available),
        "name": item.name,
        "price": item.price,
        "description": item.description,
        "image_path": item.image_path,
        "image_url": get_menu_image_url(item.image_path),
        "created_at": format_datetime(item.created_at),
        "updated_at": format_datetime(item.updated_at),
    }


def get_grouped_menu(db):
    grouped = {}
    items = db.query(MenuItem).order_by(MenuItem.category.asc(), MenuItem.id.asc()).all()
    for item in items:
        grouped.setdefault(item.category or "Others", []).append(serialize_menu_item(item))
    return grouped


def find_product(product_id: int):
    with SessionLocal() as db:
        item = db.query(MenuItem).filter(MenuItem.id == product_id).first()
        if not item:
            return None
        return serialize_menu_item(item)


def calculate_cart(cart):
    if not cart:
        return {"items": [], "total": 0}

    detailed = []
    total = 0
    product_ids = [item["product_id"] for item in cart]

    with SessionLocal() as db:
        products = db.query(MenuItem).filter(MenuItem.id.in_(product_ids)).all()
        product_map = {item.id: item for item in products}

        for item in cart:
            product = product_map.get(item["product_id"])
            if not product:
                continue

            line_total = product.price * item["quantity"]
            total += line_total
            detailed.append(
                {
                    "product_id": product.id,
                    "name": product.name,
                    "price": product.price,
                    "quantity": item["quantity"],
                    "line_total": line_total,
                    "image_url": get_menu_image_url(product.image_path),
                }
            )

    return {"items": detailed, "total": total}


def get_cart_items(user_id: str):
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == user_id).first()
        if not user:
            return []
        return [
            {"product_id": item.product_id, "quantity": item.quantity}
            for item in db.query(CartItem).filter(CartItem.user_id == user.id).all()
        ]


def serialize_profile(profile: Optional[Profile]):
    if not profile:
        return {
            "name": "",
            "phone": "",
            "language": "English",
            "preferred_language": "en",
            "telegram_username": "",
            "latitude": None,
            "longitude": None,
            "readable_address": "",
            "verified_at": None,
            "last_location_at": None,
            "app_ready": False,
            "verification_required": True,
            "location_refresh_required": True,
            "location_refresh_message": get_location_refresh_message(),
            "addresses": [],
            "payment_methods": [],
        }

    readiness = shared_is_profile_app_ready(profile)
    return {
        "name": profile.name,
        "phone": profile.phone,
        "language": preferred_language_to_app_language(profile.preferred_language),
        "preferred_language": profile.preferred_language or "en",
        "telegram_username": profile.telegram_username or "",
        "latitude": profile.latitude,
        "longitude": profile.longitude,
        "readable_address": profile.readable_address or "",
        "verified_at": format_datetime(profile.verified_at),
        "last_location_at": format_datetime(profile.last_location_at),
        "app_ready": readiness["app_ready"],
        "verification_required": readiness["verification_required"],
        "location_refresh_required": readiness["location_refresh_required"],
        "location_refresh_message": readiness["location_refresh_message"],
        "addresses": [],
        "payment_methods": [],
    }


def upsert_profile_for_user(
    db,
    user: User,
    *,
    name: str,
    phone: str,
    language: str = "English",
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    telegram_username: Optional[str] = None,
    readable_address: Optional[str] = None,
    verified: Optional[bool] = None,
    verified_at: Optional[datetime] = None,
):
    if not user.profile:
        user.profile = Profile(name="", phone="", language="English", preferred_language="en")

    user.profile.name = name
    user.profile.phone = phone
    user.profile.language = language
    user.profile.preferred_language = app_language_to_preferred_language(language)
    user.profile.latitude = latitude
    user.profile.longitude = longitude
    if telegram_username is not None:
        user.profile.telegram_username = telegram_username
    if readable_address is not None:
        user.profile.readable_address = readable_address
    if latitude is not None and longitude is not None:
        user.profile.last_location_at = datetime.now(RESTAURANT_TIMEZONE)
    if verified is not None:
        user.profile.verified = verified
    else:
        user.profile.verified = is_profile_verified(user.profile)
    if verified_at is not None:
        user.profile.verified_at = verified_at
    elif user.profile.verified and not user.profile.verified_at:
        user.profile.verified_at = datetime.now(RESTAURANT_TIMEZONE)
    elif not user.profile.verified:
        user.profile.verified_at = None
    return user.profile


def get_customer_name(order: Order) -> str:
    if is_dine_in_order(order):
        return f"Table {order.table_number}" if order.table_number else "Dine-In Order"
    profile = order.user.profile if order.user and order.user.profile else None
    if not profile or not profile.name:
        return "Unknown customer"
    return profile.name


def get_customer_phone(order: Order) -> str:
    if is_dine_in_order(order):
        return ""
    profile = order.user.profile if order.user and order.user.profile else None
    if not profile or not profile.phone:
        return ""
    return profile.phone


def serialize_order(order: Order, menu_item_metadata_map: Optional[dict[int, MenuItem]] = None):
    return {
        "order_id": order.order_id,
        "items": serialize_order_items(order.items, menu_item_metadata_map),
        "total": order.total,
        "status": get_effective_order_status(order),
        "order_type": order.order_type,
        "delivery_type": order.delivery_type,
        "table_number": order.table_number,
        "waiter_name": order.waiter_name,
        "address": order.address,
        "latitude": order.latitude,
        "longitude": order.longitude,
        "resolved_address": order.resolved_address,
        "payment_method": order.payment_method,
        "comment": order.comment,
        "courier_name": order.courier_name,
        "courier_phone": order.courier_phone,
        "created_time": format_datetime(order.created_at),
        "preparing_started_at": format_datetime(order.preparing_started_at),
        "delivered_at": format_datetime(order.delivered_at),
        "after_hours": bool(order.after_hours),
    }


def serialize_admin_order_summary(order: Order):
    return {
        "order_id": order.order_id,
        "customer_name": get_customer_name(order),
        "phone": get_customer_phone(order),
        "courier_name": order.courier_name,
        "courier_phone": order.courier_phone,
        "total": order.total,
        "status": get_effective_order_status(order),
        "order_type": order.order_type,
        "delivery_type": order.delivery_type,
        "table_number": order.table_number,
        "waiter_name": order.waiter_name,
        "address": order.address,
        "latitude": order.latitude,
        "longitude": order.longitude,
        "resolved_address": order.resolved_address,
        "created_time": format_datetime(order.created_at),
        "delivered_at": format_datetime(order.delivered_at),
        "after_hours": bool(order.after_hours),
    }


def serialize_admin_order_details(order: Order, menu_item_metadata_map: Optional[dict[int, MenuItem]] = None):
    effective_status = get_effective_order_status(order)
    available_actions = []
    if is_pickup_order(order):
        if effective_status == "preparing":
            available_actions = ["ready_for_pickup", "cancelled"]
        elif effective_status == "ready_for_pickup":
            available_actions = ["picked_up", "cancelled"]
        elif effective_status == "new":
            available_actions = ["cancelled"]
    elif is_dine_in_order(order):
        if effective_status == "preparing":
            available_actions = ["cancelled"]
        elif effective_status == "ready":
            available_actions = ["served", "cancelled"]
        elif effective_status == "new":
            available_actions = ["cancelled"]
    else:
        if effective_status == "preparing":
            available_actions = ["ready", *FINAL_ORDER_STATUSES]
        elif effective_status == "ready":
            available_actions = list(FINAL_ORDER_STATUSES)
        elif effective_status == "delivering":
            available_actions = list(FINAL_ORDER_STATUSES)
        elif effective_status == "new":
            available_actions = ["cancelled"]
    return {
        "order_id": order.order_id,
        "customer_name": get_customer_name(order),
        "phone": get_customer_phone(order),
        "address": order.address,
        "resolved_address": order.resolved_address,
        "comment": order.comment,
        "courier_name": order.courier_name,
        "courier_phone": order.courier_phone,
        "items": serialize_order_items(order.items, menu_item_metadata_map),
        "total": order.total,
        "status": effective_status,
        "order_type": order.order_type,
        "delivery_type": order.delivery_type,
        "table_number": order.table_number,
        "waiter_name": order.waiter_name,
        "latitude": order.latitude,
        "longitude": order.longitude,
        "map_url": get_order_map_url(order),
        "payment_method": order.payment_method,
        "after_hours": bool(order.after_hours),
        "ready_notification_sent": order.ready_notification_sent,
        "ready_notification_attempts": order.ready_notification_attempts,
        "ready_notification_last_error": order.ready_notification_last_error,
        "ready_notification_last_attempt_at": format_datetime(order.ready_notification_last_attempt_at),
        "ready_notification_next_attempt_at": format_datetime(order.ready_notification_next_attempt_at),
        "waiter_ready_acknowledged_at": format_datetime(order.waiter_ready_acknowledged_at),
        "created_time": format_datetime(order.created_at),
        "preparing_started_at": format_datetime(order.preparing_started_at),
        "delivered_at": format_datetime(order.delivered_at),
        "available_actions": available_actions,
    }


def get_order_for_admin(db, order_id: str):
    order = (
        db.query(Order)
        .options(joinedload(Order.items), joinedload(Order.user).joinedload(User.profile))
        .filter(Order.order_id == order_id)
        .first()
    )
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


def save_menu_image_from_data_url(image_data_url: Optional[str], filename_hint: Optional[str] = None) -> Optional[str]:
    if not image_data_url:
        return None

    if "," not in image_data_url:
        raise HTTPException(status_code=400, detail="Invalid image data")

    header, encoded = image_data_url.split(",", 1)
    extension = Path(filename_hint or "").suffix.lower()
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        if "image/png" in header:
            extension = ".png"
        elif "image/webp" in header:
            extension = ".webp"
        elif "image/gif" in header:
            extension = ".gif"
        else:
            extension = ".jpg"

    filename = f"{uuid4().hex}{extension}"
    destination = UPLOADS_DIR / filename

    try:
        image_bytes = base64.b64decode(encoded)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Failed to decode image") from exc

    destination.write_bytes(image_bytes)

    return f"/static/uploads/{filename}"


def validate_menu_fields(name: str, category: str, category_group: str, price: float):
    if not name.strip():
        raise HTTPException(status_code=400, detail="Name is required")
    if not category.strip():
        raise HTTPException(status_code=400, detail="Category is required")
    if normalize_category_group(category_group) not in {"fast_food", "milliy_taom"}:
        raise HTTPException(status_code=400, detail="Category group is required")
    if price <= 0:
        raise HTTPException(status_code=400, detail="Price must be greater than 0")


@app.get("/api/menu")
def get_menu():
    with SessionLocal() as db:
        return get_grouped_menu(db)


@app.post("/api/cart/add")
def add_to_cart(
    payload: AddToCartRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))

    product = find_product(payload.product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    if not product.get("is_available", True):
        raise HTTPException(status_code=400, detail="Product is run out")

    if payload.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        require_verified_profile_for_ordering(user.profile)
        existing = (
            db.query(CartItem)
            .filter(CartItem.user_id == user.id, CartItem.product_id == payload.product_id)
            .first()
        )
        if existing:
            existing.quantity += payload.quantity
        else:
            db.add(CartItem(user_id=user.id, product_id=payload.product_id, quantity=payload.quantity))
        db.commit()

    return {"message": "Added to cart", "user_id": user_id, "cart": calculate_cart(get_cart_items(user_id))}


@app.get("/api/cart/me")
def get_cart(x_telegram_init_data: Optional[str] = Header(default=None)):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        require_verified_profile_for_ordering(user.profile)
    return calculate_cart(get_cart_items(user_id))


@app.post("/api/cart/update")
def update_cart(
    payload: UpdateCartRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))

    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        require_verified_profile_for_ordering(user.profile)
        item = (
            db.query(CartItem)
            .filter(CartItem.user_id == user.id, CartItem.product_id == payload.product_id)
            .first()
        )
        if item:
            if payload.quantity <= 0:
                db.delete(item)
            else:
                item.quantity = payload.quantity
            db.commit()
            return {"message": "Cart updated", "cart": calculate_cart(get_cart_items(user_id))}

    raise HTTPException(status_code=404, detail="Product not found in cart")


@app.post("/api/checkout")
def checkout(
    payload: CheckoutRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))

    cart_items = get_cart_items(user_id)
    if not cart_items:
        raise HTTPException(status_code=400, detail="Cart is empty")

    cart_data = calculate_cart(cart_items)

    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        require_verified_profile_for_ordering(user.profile)
        working_hours_settings = get_restaurant_settings()
        within_working_hours = is_within_working_hours(settings=working_hours_settings)
        order_after_hours = bool(working_hours_settings.get("enabled")) and not within_working_hours
        logger.info(
            "Checkout working-hours validation user_id=%s enabled=%s within_hours=%s opening=%s closing=%s",
            user_id,
            working_hours_settings.get("enabled"),
            within_working_hours,
            working_hours_settings.get("opening_time"),
            working_hours_settings.get("closing_time"),
        )
        order_type = "pickup" if (payload.delivery_type or "").strip().lower() == "pickup" else "delivery"
        menu_items = db.query(MenuItem).filter(MenuItem.id.in_([item["product_id"] for item in cart_data["items"]])).all()
        menu_item_map = {item.id: item for item in menu_items}
        unavailable_items = [item.name for item in menu_items if not item.is_available]
        if unavailable_items:
            raise HTTPException(status_code=400, detail=f"{', '.join(unavailable_items)} is run out")
        food_type_code = get_food_type_code_from_menu_items(menu_items)
        order_id = generate_order_code(db, order_type, food_type_code)
        order = Order(
            user_id=user.id,
            order_id=order_id,
            total=cart_data["total"],
            status=DEFAULT_ORDER_STATUS,
            order_type=order_type,
            delivery_type=payload.delivery_type,
            address=payload.address,
            latitude=payload.latitude,
            longitude=payload.longitude,
            resolved_address=payload.resolved_address,
            payment_method=payload.payment_method,
            comment=payload.comment,
            preparing_started_at=None,
            after_hours=order_after_hours,
        )
        db.add(order)
        db.flush()

        for item in cart_data["items"]:
            menu_item = menu_item_map.get(item["product_id"])
            kitchen_group, is_drink, kitchen_status = get_order_item_state_from_menu_item(menu_item)
            db.add(
                OrderItem(
                    order_id=order.id,
                    product_id=item["product_id"],
                    name=item["name"],
                    price=item["price"],
                    quantity=item["quantity"],
                    line_total=item["line_total"],
                    kitchen_group=kitchen_group,
                    kitchen_status=kitchen_status,
                    is_drink=is_drink,
                )
            )

        db.query(CartItem).filter(CartItem.user_id == user.id).delete()
        db.commit()
        db.refresh(order)
        order.items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()

        return {
            "message": "We are currently closed. Your order will be delivered during working hours."
            if order_after_hours
            else "Order placed successfully",
            "after_hours": order_after_hours,
            "order": serialize_order(order),
        }


@app.post("/api/waiter/orders")
def create_waiter_dine_in_order(
    payload: WaiterOrderCreateRequest,
    x_waiter_token: Optional[str] = Header(default=None),
):
    require_waiter_auth(x_waiter_token, "waiter_create_dine_in_order")

    table_number = validate_waiter_table_number(payload.table_number)
    if not payload.items:
        raise HTTPException(status_code=400, detail="At least one item is required")

    requested_quantities: dict[int, int] = {}
    for item in payload.items:
        if item.quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0")
        requested_quantities[item.product_id] = requested_quantities.get(item.product_id, 0) + item.quantity

    with SessionLocal() as db:
        waiter_user = get_or_create_internal_waiter_user(db)
        waiter_name = normalize_waiter_name(payload.waiter_name)
        menu_items = db.query(MenuItem).filter(MenuItem.id.in_(requested_quantities.keys())).all()
        product_map = {item.id: item for item in menu_items}

        for product_id in requested_quantities:
            if product_id not in product_map:
                raise HTTPException(status_code=404, detail="Product not found")
            if not product_map[product_id].is_available:
                raise HTTPException(status_code=400, detail=f"{product_map[product_id].name} is run out")

        order = Order(
            user_id=waiter_user.id,
            order_id=generate_order_code(db, "dine_in", get_food_type_code_from_menu_items(menu_items)),
            total=0,
            status=DEFAULT_ORDER_STATUS,
            order_type="dine_in",
            delivery_type="Dine In",
            table_number=table_number,
            waiter_name=waiter_name,
            address="",
            latitude=None,
            longitude=None,
            resolved_address=None,
            payment_method="Dine In",
            comment=payload.comment.strip(),
            ready_notification_sent=True,
            ready_notification_attempts=0,
            ready_notification_last_error=None,
            ready_notification_last_attempt_at=None,
            ready_notification_next_attempt_at=None,
            preparing_started_at=None,
        )
        db.add(order)
        db.flush()

        total = 0
        for product_id, quantity in requested_quantities.items():
            product = product_map[product_id]
            line_total = product.price * quantity
            total += line_total
            kitchen_group, is_drink, kitchen_status = get_order_item_state_from_menu_item(product)
            db.add(
                OrderItem(
                    order_id=order.id,
                    product_id=product.id,
                    name=product.name,
                    price=product.price,
                    quantity=quantity,
                    line_total=line_total,
                    kitchen_group=kitchen_group,
                    kitchen_status=kitchen_status,
                    is_drink=is_drink,
                )
            )

        order.total = total
        recalculate_dine_in_order_state(order)
        db.commit()
        order = get_order_for_admin(db, order.order_id)

        return {
            "message": f"Dine-in order created for table {table_number}.",
            "order": serialize_admin_order_details(order),
        }


@app.get("/api/waiter/orders")
def get_waiter_dine_in_orders(x_waiter_token: Optional[str] = Header(default=None)):
    require_waiter_auth(x_waiter_token, "waiter_get_dine_in_orders")

    with SessionLocal() as db:
        orders = (
            db.query(Order)
            .options(joinedload(Order.items), joinedload(Order.user).joinedload(User.profile))
            .filter(Order.order_type == "dine_in")
            .order_by(Order.created_at.desc(), Order.id.desc())
            .all()
        )
        menu_item_metadata_map = build_menu_item_metadata_map(
            db,
            [item.product_id for order in orders for item in order.items],
        )
        return [serialize_admin_order_details(order, menu_item_metadata_map) for order in orders]


@app.put("/api/waiter/orders/{order_id}")
def update_waiter_dine_in_order(
    order_id: str,
    payload: WaiterOrderCreateRequest,
    x_waiter_token: Optional[str] = Header(default=None),
):
    require_waiter_auth(x_waiter_token, "waiter_update_dine_in_order")

    table_number = validate_waiter_table_number(payload.table_number)
    if not payload.items:
        raise HTTPException(status_code=400, detail="At least one item is required")

    requested_quantities: dict[int, int] = {}
    for item in payload.items:
        if item.quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0")
        requested_quantities[item.product_id] = requested_quantities.get(item.product_id, 0) + item.quantity

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if not is_dine_in_order(order):
            raise HTTPException(status_code=400, detail="Only dine-in orders can be edited here")

        effective_status = get_effective_order_status(order)
        if effective_status in {"served", "cancelled"}:
            raise HTTPException(status_code=400, detail="Completed dine-in orders cannot be edited")

        menu_items = db.query(MenuItem).filter(MenuItem.id.in_(requested_quantities.keys())).all()
        product_map = {item.id: item for item in menu_items}
        for product_id in requested_quantities:
            if product_id not in product_map:
                raise HTTPException(status_code=404, detail="Product not found")
            if not product_map[product_id].is_available:
                raise HTTPException(status_code=400, detail=f"{product_map[product_id].name} is run out")

        existing_items_by_product = {item.product_id: item for item in order.items}
        order.table_number = table_number
        order.waiter_name = normalize_waiter_name(payload.waiter_name) if (payload.waiter_name or "").strip() else (order.waiter_name or "Unknown")
        order.comment = payload.comment.strip()

        total = 0
        for product_id, quantity in requested_quantities.items():
            product = product_map[product_id]
            line_total = product.price * quantity
            total += line_total
            kitchen_group, is_drink, initial_kitchen_status = get_order_item_state_from_menu_item(product)
            existing_item = existing_items_by_product.pop(product_id, None)
            if existing_item:
                was_changed = (
                    existing_item.quantity != quantity
                    or existing_item.price != product.price
                    or existing_item.name != product.name
                    or (existing_item.kitchen_group or "").strip().lower() != kitchen_group
                    or bool(existing_item.is_drink) != is_drink
                )
                existing_item.name = product.name
                existing_item.price = product.price
                existing_item.quantity = quantity
                existing_item.line_total = line_total
                existing_item.kitchen_group = kitchen_group
                existing_item.is_drink = is_drink
                if was_changed:
                    existing_item.kitchen_status = initial_kitchen_status
            else:
                db.add(
                    OrderItem(
                        order_id=order.id,
                        product_id=product.id,
                        name=product.name,
                        price=product.price,
                        quantity=quantity,
                        line_total=line_total,
                        kitchen_group=kitchen_group,
                        kitchen_status=initial_kitchen_status,
                        is_drink=is_drink,
                    )
                )

        for removed_item in existing_items_by_product.values():
            db.delete(removed_item)

        order.total = total
        db.flush()
        db.refresh(order)
        order = get_order_for_admin(db, order.order_id)
        recalculate_dine_in_order_state(order)
        db.commit()
        order = get_order_for_admin(db, order.order_id)

        return {
            "message": f"Dine-in order updated for table {table_number}.",
            "order": serialize_admin_order_details(order),
        }


@app.get("/api/orders/me")
def get_orders(x_telegram_init_data: Optional[str] = Header(default=None)):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        require_verified_profile_for_ordering(user.profile)
        orders = (
            db.query(Order)
            .options(joinedload(Order.items))
            .filter(Order.user_id == user.id)
            .order_by(Order.created_at.desc(), Order.id.desc())
            .all()
        )
        return [serialize_order(order) for order in orders]


@app.get("/api/profile/me")
def get_profile(x_telegram_init_data: Optional[str] = Header(default=None)):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        readiness = shared_is_profile_app_ready(user.profile if user else None)
        logger.info(
            "App profile check result user_id=%s profile_exists=%s last_location_at=%s app_ready=%s location_refresh_required=%s",
            user_id,
            bool(user and user.profile),
            user.profile.last_location_at.isoformat() if user and user.profile and user.profile.last_location_at else None,
            readiness["app_ready"],
            readiness["location_refresh_required"],
        )
        return serialize_profile(user.profile)


@app.post("/api/profile/update")
def update_profile(
    payload: ProfileUpdateRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    telegram_user = get_telegram_user_payload(x_telegram_init_data)
    user_id = str(telegram_user.id) if telegram_user else "demo-user-1"
    get_or_create_user(user_id, getattr(telegram_user, "username", None))
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == user_id).first()
        upsert_profile_for_user(
            db,
            user,
            name=payload.name,
            phone=payload.phone,
            language=payload.language,
            latitude=payload.latitude,
            longitude=payload.longitude,
            telegram_username=getattr(telegram_user, "username", None),
            readable_address=payload.readable_address,
        )
        db.commit()
        db.refresh(user.profile)
        return {"message": "Profile updated", "profile": serialize_profile(user.profile)}


@app.post("/api/user/profile/from-bot")
def update_profile_from_bot(payload: BotProfileUpdateRequest):
    with SessionLocal() as db:
        user = db.query(User).options(joinedload(User.profile)).filter(User.telegram_user_id == payload.telegram_user_id).first()
        if not user:
            user = User(telegram_user_id=payload.telegram_user_id)
            db.add(user)
            db.flush()

        profile = upsert_profile_for_user(
            db,
            user,
            name=payload.name,
            phone=payload.phone,
            language=payload.language,
            latitude=payload.latitude,
            longitude=payload.longitude,
            telegram_username=payload.telegram_username,
            readable_address=payload.readable_address,
            verified=True,
            verified_at=datetime.now(RESTAURANT_TIMEZONE),
        )
        db.commit()
        db.refresh(profile)
        return {"message": "Profile saved from bot", "profile": serialize_profile(profile)}


@app.post("/api/admin/login")
def admin_login(payload: AdminLoginRequest):
    role = detect_admin_role_from_password(payload.password)
    if not role:
        logger.warning("Admin login failed: invalid password")
        raise HTTPException(status_code=401, detail="Invalid admin password")

    logger.info("Admin login success role=%s", role)
    return {
        "token": ADMIN_ROLE_SESSION_TOKENS[role],
        "role": role,
        "statuses": list(ORDER_STATUSES),
        "manual_statuses": list(FINAL_ORDER_STATUSES),
        "poll_interval_ms": 5000,
    }


@app.post("/api/waiter/login")
def waiter_login(payload: WaiterLoginRequest):
    waiter_name = normalize_waiter_name(payload.name)
    logger.info("Waiter login attempt name=%s", waiter_name)
    if waiter_name == "Unknown":
        logger.warning("Waiter login failed: missing waiter name")
        raise HTTPException(status_code=400, detail="Waiter name is required")
    if not verify_waiter_password(payload.password):
        logger.warning("Waiter login failed: invalid password name=%s", waiter_name)
        raise HTTPException(status_code=401, detail="Invalid waiter password")

    logger.info("Waiter login success name=%s", waiter_name)
    return {
        "token": WAITER_SESSION_TOKEN,
        "role": ROLE_WAITER,
        "waiter_name": waiter_name,
        "poll_interval_ms": 5000,
    }


@app.post("/api/admin/settings/update-passwords")
def update_admin_settings_passwords(
    payload: AdminSettingsUpdatePasswordsRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    role = require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_update_settings_passwords")
    logger.info(
        "Admin password update attempt role=%s manager_change=%s fast_food_change=%s milliy_change=%s cashier_change=%s waiter_change=%s",
        role,
        bool((payload.manager_password or "").strip()),
        bool((payload.fast_food_password or "").strip()),
        bool((payload.milliy_password or "").strip()),
        bool((payload.cashier_password or "").strip()),
        bool((payload.waiter_password or "").strip()),
    )
    role_passwords = get_admin_role_passwords()

    password_updates = {
        ROLE_MANAGER: (payload.manager_password or "").strip(),
        ROLE_FAST_FOOD_CHEF: (payload.fast_food_password or "").strip(),
        ROLE_MILLIY_CHEF: (payload.milliy_password or "").strip(),
        ROLE_CASHIER: (payload.cashier_password or "").strip(),
        ROLE_WAITER: (payload.waiter_password or "").strip(),
    }
    provided_updates = {
        role_name: password_value
        for role_name, password_value in password_updates.items()
        if password_value
    }

    if not provided_updates:
        logger.warning("Admin password update failed: no password fields were provided role=%s", role)
        raise HTTPException(status_code=400, detail="Fill at least one new password to update")

    for role_name, password_value in provided_updates.items():
        if len(password_value) < MIN_ADMIN_PASSWORD_LENGTH:
            logger.warning(
                "Admin password update failed: password too short role=%s target_role=%s",
                role,
                role_name,
            )
            raise HTTPException(
                status_code=400,
                detail=f"{ADMIN_ROLE_LABELS.get(role_name, role_name)} password must be at least {MIN_ADMIN_PASSWORD_LENGTH} characters",
            )

    role_passwords.update(provided_updates)
    save_admin_role_passwords(role_passwords)
    logger.info(
        "Admin password update success role=%s updated_roles=%s",
        role,
        sorted(provided_updates.keys()),
    )
    return {"message": "Passwords updated successfully"}


@app.post("/api/admin/settings/update-role-password")
def update_admin_settings_role_password(
    payload: AdminSettingsUpdateRolePasswordRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    role = require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_update_settings_role_password")
    target_role = (payload.role or "").strip().lower()
    logger.info("Admin single-role password update attempt actor_role=%s target_role=%s", role, target_role)

    if target_role not in DEFAULT_ADMIN_ROLE_PASSWORDS:
        logger.warning("Admin single-role password update failed: invalid target_role=%s", target_role)
        raise HTTPException(status_code=400, detail="Invalid role")

    new_password = (payload.new_password or "").strip()
    role_passwords = get_admin_role_passwords()
    if len(new_password) < MIN_ADMIN_PASSWORD_LENGTH:
        logger.warning(
            "Admin single-role password update failed: password too short actor_role=%s target_role=%s",
            role,
            target_role,
        )
        raise HTTPException(
            status_code=400,
            detail=f"{ADMIN_ROLE_LABELS.get(target_role, target_role)} password must be at least {MIN_ADMIN_PASSWORD_LENGTH} characters",
        )

    role_passwords[target_role] = new_password
    save_admin_role_passwords(role_passwords)
    logger.info("Admin single-role password update success actor_role=%s target_role=%s", role, target_role)
    return {"message": f"{ADMIN_ROLE_LABELS.get(target_role, target_role)} password updated successfully"}


@app.get("/api/admin/settings/working-hours")
def get_admin_working_hours_settings(x_admin_token: Optional[str] = Header(default=None)):
    role = require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_get_working_hours_settings")
    settings = get_restaurant_settings()
    logger.info("Working hours settings requested by role=%s settings=%s", role, settings)
    return settings


@app.post("/api/admin/settings/working-hours")
def update_admin_working_hours_settings(
    payload: AdminWorkingHoursSettingsRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    role = require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_update_working_hours_settings")
    settings = {
        "opening_time": normalize_working_hours_time(payload.opening_time, "Opening time"),
        "closing_time": normalize_working_hours_time(payload.closing_time, "Closing time"),
        "enabled": bool(payload.enabled),
    }
    save_restaurant_settings(settings)
    logger.info("Working hours settings updated by role=%s settings=%s", role, settings)
    return {"message": "Working hours updated successfully.", "settings": settings}


@app.get("/api/admin/menu")
def get_admin_menu(x_admin_token: Optional[str] = Header(default=None)):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_get_menu")

    with SessionLocal() as db:
        items = db.query(MenuItem).order_by(MenuItem.category.asc(), MenuItem.id.asc()).all()
        return [serialize_menu_item(item) for item in items]


@app.post("/api/admin/menu")
def create_admin_menu_item(
    payload: AdminMenuUpsertRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_create_menu_item")
    validate_menu_fields(payload.name, payload.category, payload.category_group, payload.price)

    image_path = save_menu_image_from_data_url(payload.image_data_url, payload.image_filename)

    with SessionLocal() as db:
        item = MenuItem(
            category=payload.category.strip(),
            category_group=normalize_category_group(payload.category_group, payload.category),
            name=payload.name.strip(),
            price=payload.price,
            description=payload.description.strip(),
            image_path=image_path,
        )
        db.add(item)
        db.commit()
        db.refresh(item)
        return {"message": "Menu item created", "item": serialize_menu_item(item)}


@app.put("/api/admin/menu/{item_id}")
def update_admin_menu_item(
    item_id: int,
    payload: AdminMenuUpsertRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_update_menu_item")
    validate_menu_fields(payload.name, payload.category, payload.category_group, payload.price)

    with SessionLocal() as db:
        item = db.query(MenuItem).filter(MenuItem.id == item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Menu item not found")

        image_path = save_menu_image_from_data_url(payload.image_data_url, payload.image_filename)
        item.category = payload.category.strip()
        item.category_group = normalize_category_group(payload.category_group, payload.category)
        item.name = payload.name.strip()
        item.price = payload.price
        item.description = payload.description.strip()
        if image_path:
            item.image_path = image_path

        db.commit()
        db.refresh(item)
        return {"message": "Menu item updated", "item": serialize_menu_item(item)}


@app.delete("/api/admin/menu/{item_id}")
def delete_admin_menu_item(item_id: int, x_admin_token: Optional[str] = Header(default=None)):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_delete_menu_item")

    with SessionLocal() as db:
        item = db.query(MenuItem).filter(MenuItem.id == item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Menu item not found")

        db.delete(item)
        db.commit()
        return {"message": "Menu item deleted", "item_id": item_id}


@app.post("/api/admin/menu/{item_id}/availability")
def update_admin_menu_item_availability(
    item_id: int,
    payload: AdminMenuAvailabilityRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_update_menu_item_availability")

    with SessionLocal() as db:
        item = db.query(MenuItem).filter(MenuItem.id == item_id).first()
        if not item:
            raise HTTPException(status_code=404, detail="Menu item not found")

        item.is_available = payload.is_available
        db.commit()
        db.refresh(item)
        return {
            "message": "Menu item marked as available." if item.is_available else "Menu item marked as run out.",
            "item": serialize_menu_item(item),
        }


@app.post("/api/admin/menu/categories/rename")
def rename_admin_menu_category(
    payload: AdminCategoryRenameRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_rename_menu_category")

    current_category = payload.category.strip()
    new_category = payload.new_name.strip()
    if not current_category:
        raise HTTPException(status_code=400, detail="Category is required")
    if not new_category:
        raise HTTPException(status_code=400, detail="New category name is required")

    with SessionLocal() as db:
        items = db.query(MenuItem).filter(MenuItem.category == current_category).all()
        if not items:
            raise HTTPException(status_code=404, detail="Category not found")

        for item in items:
            item.category = new_category

        db.commit()
        return {
            "message": f"Category renamed to {new_category}.",
            "category": new_category,
            "updated_items": len(items),
        }


@app.post("/api/admin/menu/categories/remove")
def remove_admin_menu_category(
    payload: AdminCategoryRemoveRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF}, "admin_remove_menu_category")

    category = payload.category.strip()
    if not category:
        raise HTTPException(status_code=400, detail="Category is required")
    if category == "Others":
        raise HTTPException(status_code=400, detail="Others category cannot be removed")

    with SessionLocal() as db:
        items = db.query(MenuItem).filter(MenuItem.category == category).all()
        moved_count = len(items)
        for item in items:
            item.category = "Others"

        db.commit()
        return {
            "message": "Category removed. Foods were moved to Others." if moved_count else "Category removed.",
            "moved_to": "Others",
            "updated_items": moved_count,
        }


@app.get("/api/admin/orders")
def get_admin_orders(
    x_admin_token: Optional[str] = Header(default=None),
    status: Optional[str] = None,
    delivery_type: Optional[str] = None,
    date: Optional[str] = None,
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF, ROLE_CASHIER}, "admin_get_orders")

    with SessionLocal() as db:
        query = (
            db.query(Order)
            .options(selectinload(Order.items), joinedload(Order.user).joinedload(User.profile))
            .order_by(Order.created_at.desc(), Order.id.desc())
        )
        if delivery_type:
            query = query.filter(Order.delivery_type == delivery_type)
        if status:
            normalized_status = normalize_order_status(status)
            if normalized_status in TERMINAL_ORDER_STATUSES:
                query = query.filter(Order.status == normalized_status)
            elif normalized_status == "new":
                query = query.filter(Order.preparing_started_at.is_(None)).filter(Order.status.notin_(TERMINAL_ORDER_STATUSES))
            elif normalized_status == "preparing":
                threshold = datetime.now(RESTAURANT_TIMEZONE) - AUTO_DELIVERING_AFTER
                if delivery_type == "Pickup":
                    query = (
                        query.filter(Order.preparing_started_at.is_not(None))
                        .filter(Order.status.notin_(TERMINAL_ORDER_STATUSES))
                        .filter(Order.status != "ready_for_pickup")
                    )
                else:
                    query = (
                        query.filter(Order.preparing_started_at.is_not(None))
                        .filter(Order.preparing_started_at > threshold)
                        .filter(Order.status.notin_(TERMINAL_ORDER_STATUSES))
                    )
            elif normalized_status == "ready":
                query = query.filter(Order.status == "ready")
            elif normalized_status == "ready_for_pickup":
                query = query.filter(Order.status == "ready_for_pickup")
            elif normalized_status == "delivering":
                threshold = datetime.now(RESTAURANT_TIMEZONE) - AUTO_DELIVERING_AFTER
                query = (
                    query.filter(Order.preparing_started_at.is_not(None))
                    .filter(Order.preparing_started_at <= threshold)
                    .filter(Order.status.notin_(TERMINAL_ORDER_STATUSES))
                )

        if date:
            try:
                selected_date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid date format") from exc

            date_from = selected_date.replace(tzinfo=RESTAURANT_TIMEZONE)
            date_to = date_from + timedelta(days=1)
            query = query.filter(Order.created_at >= date_from, Order.created_at < date_to)

        orders = query.all()
        menu_item_metadata_map = build_menu_item_metadata_map(
            db,
            [item.product_id for order in orders for item in order.items],
        )
        return [serialize_admin_order_details(order, menu_item_metadata_map) for order in orders]


@app.get("/api/admin/orders/{order_id}")
def get_admin_order(order_id: str, x_admin_token: Optional[str] = Header(default=None)):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF, ROLE_CASHIER}, "admin_get_order")

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        menu_item_metadata_map = build_menu_item_metadata_map(db, [item.product_id for item in order.items])
        return serialize_admin_order_details(order, menu_item_metadata_map)


@app.get("/api/admin/summary/items")
def get_admin_summary_items(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    order_type: Optional[str] = None,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_get_summary")
    normalized_order_type = (order_type or "").strip().lower()
    allowed_final_statuses = ["completed", "served", "picked_up", "delivered", "paid"]
    logger.info(
        "Summary endpoint requested from_date=%s to_date=%s order_type=%s statuses=%s",
        from_date,
        to_date,
        normalized_order_type or "total",
        allowed_final_statuses,
    )

    with SessionLocal() as db:
        query = (
            db.query(
                OrderItem,
                Order.order_id,
                Order.status,
                Order.delivered_at,
                Order.order_type,
                MenuItem.category,
                MenuItem.category_group,
            )
            .join(Order, OrderItem.order_id == Order.id)
            .outerjoin(MenuItem, MenuItem.id == OrderItem.product_id)
            .filter(Order.status.in_(allowed_final_statuses))
        )

        if normalized_order_type:
            if normalized_order_type not in {"delivery", "pickup", "dine_in"}:
                raise HTTPException(status_code=400, detail="Invalid order_type filter")
            query = query.filter(Order.order_type == normalized_order_type)

        if from_date:
            try:
                parsed_from = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=RESTAURANT_TIMEZONE)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid from_date format") from exc
            query = query.filter(Order.delivered_at.is_not(None)).filter(Order.delivered_at >= parsed_from)

        if to_date:
            try:
                parsed_to = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=RESTAURANT_TIMEZONE) + timedelta(days=1)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid to_date format") from exc
            query = query.filter(Order.delivered_at.is_not(None)).filter(Order.delivered_at < parsed_to)

        rows = query.all()
        logger.info("Summary endpoint included row_count=%s", len(rows))

        summary_map: dict[tuple[Optional[int], str], dict] = {}
        included_order_ids: set[str] = set()
        for order_item, order_public_id, order_status, delivered_at, row_order_type, category, category_group in rows:
            food_name = (order_item.name or "").strip() or "Unknown Item"
            group_key = (order_item.product_id, food_name.lower())
            included_order_ids.add(order_public_id)
            logger.info(
                "Summary counting order_id=%s status=%s order_type=%s delivered_at=%s food=%s quantity=%s line_total=%s",
                order_public_id,
                order_status,
                row_order_type,
                delivered_at.isoformat() if delivered_at else None,
                food_name,
                order_item.quantity,
                order_item.line_total,
            )
            if group_key not in summary_map:
                summary_map[group_key] = {
                    "food_id": order_item.product_id,
                    "food_name": food_name,
                    "category": category or "Others",
                    "category_group": normalize_category_group(category_group, category),
                    "quantity_sold": 0,
                    "total_revenue": 0,
                }

            summary_map[group_key]["quantity_sold"] += int(order_item.quantity or 0)
            summary_map[group_key]["total_revenue"] += float(order_item.line_total or 0)
            logger.info(
                "Summary aggregated food=%s quantity_sold=%s total_revenue=%s",
                summary_map[group_key]["food_name"],
                summary_map[group_key]["quantity_sold"],
                summary_map[group_key]["total_revenue"],
            )

        summary_items = sorted(
            summary_map.values(),
            key=lambda item: (-item["quantity_sold"], item["food_name"].lower()),
        )
        total_revenue = sum(float(item["total_revenue"] or 0) for item in summary_items)
        logger.info(
            "Summary response order_type=%s included_orders=%s aggregated_items=%s total_revenue=%s",
            normalized_order_type or "total",
            sorted(included_order_ids),
            len(summary_items),
            total_revenue,
        )
        return summary_items


@app.post("/api/admin/orders/{order_id}/open")
def open_admin_order(
    order_id: str,
    kitchen_group: Optional[str] = None,
    x_admin_token: Optional[str] = Header(default=None),
):
    role = require_admin_roles(
        x_admin_token,
        {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF},
        "admin_open_order",
    )

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if kitchen_group:
            normalized_group = normalize_kitchen_group(kitchen_group)
            if role == ROLE_FAST_FOOD_CHEF and normalized_group != "fast_food":
                raise HTTPException(status_code=403, detail="Access denied")
            if role == ROLE_MILLIY_CHEF and normalized_group != "milliy_taom":
                raise HTTPException(status_code=403, detail="Access denied")
            group_items = get_order_group_items(order, normalized_group)
            logger.info(
                "Open endpoint routing order_id=%s kitchen_group=%s items=%s",
                order_id,
                normalized_group,
                [
                    {
                        "name": item.name,
                        "kitchen_group": item.kitchen_group,
                        "kitchen_status": item.kitchen_status,
                    }
                    for item in group_items
                ],
            )
            for item in group_items:
                if normalize_order_item_kitchen_status(item.kitchen_status) == "new":
                    item.kitchen_status = "preparing"
            recalculated_status = recalculate_order_kitchen_state(order)
            db.commit()
            db.refresh(order)
            logger.info(
                "Open endpoint updated order_id=%s kitchen_group=%s final_status=%s item_statuses=%s",
                order_id,
                normalized_group,
                recalculated_status,
                [
                    {
                        "name": item.name,
                        "kitchen_group": item.kitchen_group,
                        "kitchen_status": item.kitchen_status,
                    }
                    for item in order.items
                ],
            )
        else:
            stored_status = normalize_order_status(order.status)
            if stored_status not in TERMINAL_ORDER_STATUSES and order.preparing_started_at is None:
                order.preparing_started_at = datetime.now(RESTAURANT_TIMEZONE)
                db.commit()
                db.refresh(order)
        return {"message": "Order opened", "order": serialize_admin_order_details(order)}


@app.post("/api/admin/orders/{order_id}/ready")
def mark_admin_order_ready(
    order_id: str,
    kitchen_group: Optional[str] = None,
    x_admin_token: Optional[str] = Header(default=None),
):
    role = require_admin_roles(
        x_admin_token,
        {ROLE_MANAGER, ROLE_FAST_FOOD_CHEF, ROLE_MILLIY_CHEF},
        "admin_mark_order_ready",
    )
    logger.info("Ready endpoint hit for order %s", order_id)

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if kitchen_group:
            normalized_group = normalize_kitchen_group(kitchen_group)
            if role == ROLE_FAST_FOOD_CHEF and normalized_group != "fast_food":
                raise HTTPException(status_code=403, detail="Access denied")
            if role == ROLE_MILLIY_CHEF and normalized_group != "milliy_taom":
                raise HTTPException(status_code=403, detail="Access denied")
            logger.info("Ready endpoint kitchen-group action clicked order_id=%s kitchen_group=%s", order_id, normalized_group)
            group_items = get_order_group_items(order, normalized_group)
            if not group_items:
                raise HTTPException(status_code=400, detail="No kitchen items found for this group")
            pending_group_items = [
                item for item in group_items
                if normalize_order_item_kitchen_status(item.kitchen_status) != "ready"
            ]
            if not pending_group_items:
                raise HTTPException(status_code=400, detail="Kitchen items are already ready")
            if any(normalize_order_item_kitchen_status(item.kitchen_status) != "preparing" for item in pending_group_items):
                raise HTTPException(status_code=400, detail="Only preparing kitchen items can be marked as ready")

            for item in pending_group_items:
                item.kitchen_status = "ready"

            target_status = recalculate_order_kitchen_state(order)
            if target_status == "ready" and not is_pickup_order(order) and not is_dine_in_order(order):
                reset_ready_notification_tracking(order)
            else:
                order.ready_notification_sent = True
                order.ready_notification_attempts = 0
                order.ready_notification_last_error = None
                order.ready_notification_last_attempt_at = None
                order.ready_notification_next_attempt_at = None
            db.commit()
            db.refresh(order)
            logger.info(
                "Ready endpoint updated order_id=%s kitchen_group=%s final_status=%s item_statuses=%s",
                order_id,
                normalized_group,
                target_status,
                [
                    {
                        "name": item.name,
                        "kitchen_group": item.kitchen_group,
                        "kitchen_status": item.kitchen_status,
                    }
                    for item in order.items
                ],
            )

            if target_status == "ready_for_pickup":
                logger.info("Skipping courier Telegram notification for %s because order_type=pickup", order_id)
                return {
                    "message": "Order marked as ready for pickup.",
                    "notification_retrying": False,
                    "order": serialize_admin_order_details(order),
                }
            if is_dine_in_order(order):
                logger.info("Skipping courier Telegram notification for %s because order_type=dine_in", order_id)
                return {
                    "message": "Order marked as ready." if target_status == "ready" else "Kitchen items marked as ready.",
                    "notification_retrying": False,
                    "order": serialize_admin_order_details(order),
                }
            if target_status != "ready":
                logger.info("Skipping courier Telegram notification for %s because final_status=%s", order_id, target_status)
                return {
                    "message": "Kitchen items marked as ready.",
                    "notification_retrying": False,
                    "order": serialize_admin_order_details(order),
                }

        if is_dine_in_order(order):
            raise HTTPException(
                status_code=400,
                detail="Use the kitchen tabs to mark dine-in items as ready",
            )

        validate_ready_transition(order)
        target_status = get_ready_target_status(order)
        order.status = target_status
        if target_status == "ready" and not is_dine_in_order(order):
            reset_ready_notification_tracking(order)
        else:
            order.ready_notification_sent = True
            order.ready_notification_attempts = 0
            order.ready_notification_last_error = None
            order.ready_notification_last_attempt_at = None
            order.ready_notification_next_attempt_at = None
        if is_dine_in_order(order) and target_status == "ready":
            order.waiter_ready_acknowledged_at = None
        db.commit()
        db.refresh(order)
        logger.info("Order %s updated to %s", order_id, target_status)

        if target_status == "ready_for_pickup":
            return {
                "message": "Order marked as ready for pickup.",
                "notification_retrying": False,
                "order": serialize_admin_order_details(order),
            }
        if is_dine_in_order(order):
            return {
                "message": "Order marked as ready.",
                "notification_retrying": False,
                "order": serialize_admin_order_details(order),
            }

    notification_result = attempt_ready_notification_for_order(order_id, is_retry=False)
    logger.info(
        "Ready endpoint courier notification result order_id=%s sent=%s skipped=%s reason=%s",
        order_id,
        notification_result.get("sent"),
        notification_result.get("skipped"),
        notification_result.get("reason"),
    )

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if notification_result["sent"]:
            return {
                "message": "Order marked as ready. Group notification sent.",
                "notification_retrying": False,
                "order": serialize_admin_order_details(order),
            }

        logger.warning(
            "Initial ready notification failed for %s. background_retrying=True error=%s",
            order_id,
            notification_result.get("error"),
        )
        return {
            "message": "Order marked as ready. Group notification is retrying.",
            "notification_retrying": True,
            "order": serialize_admin_order_details(order),
        }


@app.post("/api/admin/orders/{order_id}/status")
def update_admin_order_status(
    order_id: str,
    payload: AdminStatusUpdateRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_update_order_status")

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        next_status = validate_admin_status_transition(order, payload.status)
        if is_dine_in_order(order) and next_status == "ready":
            if get_effective_order_status(order) != "ready":
                raise HTTPException(
                    status_code=400,
                    detail="All kitchen items must be ready before the order can be marked as ready",
                )
        logger.info(
            "Admin status update requested for order_id=%s current_status=%s next_status=%s payment_method=%s",
            order_id,
            get_effective_order_status(order),
            next_status,
            payload.payment_method,
        )
        order.status = next_status
        if payload.payment_method is not None:
            normalized_payment_method = payload.payment_method.strip()
            if normalized_payment_method:
                order.payment_method = normalized_payment_method
        if next_status == "preparing":
            if order.preparing_started_at is None:
                order.preparing_started_at = datetime.now(RESTAURANT_TIMEZONE)
        elif next_status == "cancelled":
            order.preparing_started_at = None
        if is_dine_in_order(order) and next_status == "ready":
            order.waiter_ready_acknowledged_at = None
        if next_status in {"delivered", "completed", "picked_up", "served"}:
            order.delivered_at = datetime.now(RESTAURANT_TIMEZONE)
        else:
            order.delivered_at = None
        db.commit()
        db.refresh(order)
        logger.info(
            "Admin status update succeeded for order_id=%s stored_status=%s payment_method=%s delivered_at=%s",
            order_id,
            order.status,
            order.payment_method,
            order.delivered_at.isoformat() if order.delivered_at else None,
        )
        return {"message": "Order status updated", "order": serialize_admin_order_details(order)}


@app.post("/api/admin/orders/{order_id}/mark-paid")
def mark_admin_order_paid(
    order_id: str,
    payload: MarkPaidRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_roles(x_admin_token, {ROLE_MANAGER, ROLE_CASHIER}, "admin_mark_order_paid")
    logger.info("Cashier mark-paid endpoint hit for order_id=%s payment_method=%s", order_id, payload.payment_method)

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        effective_status = get_effective_order_status(order)
        logger.info(
            "Cashier mark-paid order found order_id=%s effective_status=%s order_type=%s delivery_type=%s",
            order_id,
            effective_status,
            order.order_type,
            order.delivery_type,
        )

        payment_method = normalize_cashier_payment_method(payload.payment_method)

        if is_pickup_order(order):
            if effective_status != "ready_for_pickup":
                raise HTTPException(status_code=400, detail="Pickup order is not ready for payment")
            next_status = "picked_up"
        elif is_dine_in_order(order):
            if effective_status not in {"new", "preparing", "ready"}:
                raise HTTPException(status_code=400, detail="Dine-in order is not ready for payment")
            next_status = "served"
        else:
            if effective_status != "ready":
                raise HTTPException(status_code=400, detail="Order is not ready for payment")
            next_status = "completed"

        logger.info(
            "Cashier payment attempt order_id=%s order_type=%s status_before=%s payment_method=%s next_status=%s",
            order_id,
            order.order_type,
            effective_status,
            payment_method,
            next_status,
        )
        order.payment_method = payment_method
        order.status = next_status
        order.delivered_at = datetime.now(RESTAURANT_TIMEZONE)
        db.commit()
        db.refresh(order)
        logger.info(
            "Cashier payment success order_id=%s stored_status=%s payment_method=%s delivered_at=%s",
            order_id,
            order.status,
            order.payment_method,
            order.delivered_at.isoformat() if order.delivered_at else None,
        )
        return {"message": "Order marked as paid", "order": serialize_admin_order_details(order)}


@app.post("/api/waiter/orders/{order_id}/acknowledge")
def acknowledge_waiter_ready_order(order_id: str, x_waiter_token: Optional[str] = Header(default=None)):
    require_waiter_auth(x_waiter_token, "waiter_acknowledge_ready_order")

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        if not is_dine_in_order(order):
            raise HTTPException(status_code=400, detail="Only dine-in orders can be acknowledged")

        if get_effective_order_status(order) != "ready":
            return {"message": "Order is not awaiting waiter acknowledgement", "order": serialize_admin_order_details(order)}

        if not order.waiter_ready_acknowledged_at:
            order.waiter_ready_acknowledged_at = datetime.now(RESTAURANT_TIMEZONE)
            db.commit()
            db.refresh(order)

        return {"message": "Order acknowledgement saved", "order": serialize_admin_order_details(order)}


@app.delete("/api/admin/orders/{order_id}")
def delete_admin_order(order_id: str, x_admin_token: Optional[str] = Header(default=None)):
    require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_delete_order")

    with SessionLocal() as db:
        order = get_order_for_admin(db, order_id)
        db.delete(order)
        db.commit()
        logger.info("Admin deleted order %s", order_id)
        return {
            "message": "Order deleted",
            "order_id": order_id,
        }


@app.delete("/api/admin/orders")
def delete_all_admin_orders(x_admin_token: Optional[str] = Header(default=None)):
    require_admin_roles(x_admin_token, {ROLE_MANAGER}, "admin_delete_all_orders")

    with SessionLocal() as db:
        orders = db.query(Order).all()
        deleted_count = len(orders)
        for order in orders:
            db.delete(order)
        db.commit()
        logger.info("Admin deleted all orders. deleted_count=%s", deleted_count)
        return {
            "message": "All orders deleted",
            "deleted_count": deleted_count,
        }


@app.on_event("startup")
async def startup_ready_notification_retry_loop():
    global ready_notification_retry_task

    if ready_notification_retry_task and not ready_notification_retry_task.done():
        logger.info("Ready notification retry task already running. Skipping duplicate startup.")
        return

    ready_notification_retry_task = asyncio.create_task(ready_notification_retry_loop())
    logger.info("Ready notification retry task created")


@app.on_event("shutdown")
async def shutdown_ready_notification_retry_loop():
    global ready_notification_retry_task

    if not ready_notification_retry_task:
        return

    ready_notification_retry_task.cancel()
    try:
        await ready_notification_retry_task
    except asyncio.CancelledError:
        logger.info("Ready notification retry task stopped")
    finally:
        ready_notification_retry_task = None


app.mount("/static", StaticFiles(directory="webapp"), name="static")


@app.get("/")
def serve_webapp():
    return FileResponse("webapp/index.html")


@app.get("/admin")
def serve_admin_webapp():
    return FileResponse("webapp/admin.html")


@app.get("/waiter")
def serve_waiter_webapp():
    return FileResponse("webapp/waiter.html")
