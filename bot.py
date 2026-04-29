import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
    WebAppInfo,
)
from telegram.error import RetryAfter
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters

from database import Order, Profile, SessionLocal, User, init_db, normalize_order_status
from profile_access import (
    LOCATION_MAX_AGE,
    RESTAURANT_TIMEZONE,
    ensure_utc_datetime as shared_ensure_utc_datetime,
    is_location_fresh as shared_is_location_fresh,
    is_profile_app_ready as shared_is_profile_app_ready,
    is_profile_complete as shared_is_profile_complete,
    is_profile_verified as shared_is_profile_verified,
)


BOT_TOKEN = os.getenv("BOT_TOKEN") or "8524810543:AAHCihTyuTHCm5QmPiKelN6awOEhuvRxSLA"
WEB_APP_URL = os.getenv("WEB_APP_URL") or "https://cuculiform-unstirrable-marjory.ngrok-free.dev"
WORK_GROUP_CHAT_ID = int(os.getenv("WORK_GROUP_CHAT_ID", "-5290265926"))
VERIFIED_USERS_GROUP_CHAT_ID = os.getenv("VERIFIED_USERS_GROUP_CHAT_ID", "-5190237105")
DELIVERY_CALLBACK_PREFIX = "deliver:"
DELIVERED_CALLBACK_PREFIX = "delivered:"
ONBOARDING_WAITING_FOR_NAME = "waiting_for_name"
ONBOARDING_WAITING_FOR_CONTACT = "waiting_for_contact"
ONBOARDING_WAITING_FOR_LOCATION = "waiting_for_location"
NAME_ALLOWED_EXTRA_CHARS = {" ", "-", "'", "`", "’", "ʻ"}
COMMON_FAKE_NAME_PATTERNS = ("qwe", "asd", "zxc", "qaz", "wsx")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BOT_CLIENT = Bot(token=BOT_TOKEN)

ASK_LANGUAGE, ASK_NAME, ASK_CONTACT, ASK_LOCATION = range(4)

ONBOARDING_WAITING_FOR_LANGUAGE = "waiting_for_language"
LANGUAGE_OPTIONS = (
    ("O'zbek", "uz_latn"),
    ("English", "en"),
    ("Русский", "ru"),
    ("Ўзбек", "uz_cyrl"),
)
LANGUAGE_LABEL_TO_CODE = {label: code for label, code in LANGUAGE_OPTIONS}
LANGUAGE_CODE_TO_LABEL = {code: label for label, code in LANGUAGE_OPTIONS}

TEXTS = {
    "en": {
        "choose_language": "Please choose your language.",
        "invalid_language": "Please choose a language using the keyboard buttons.",
        "language_updated": "Language updated to English.",
        "change_language_hint": "Use /language anytime to change your language.",
        "enter_name": "Please enter your name.",
        "invalid_name": "Please enter a real name using 2 to 25 letters.",
        "share_contact": "Please share your phone number.",
        "share_contact_prompt": "Please share your phone number.",
        "share_contact_button": "Share Contact",
        "share_contact_required": "Please use the Share Contact button.",
        "share_own_contact": "Please share your own contact using the Telegram button.",
        "share_location": "Please share your current location to complete verification.",
        "share_location_prompt": "Please share your current location to complete verification.",
        "location_expired": "Your saved location is older than 24 hours. Please share your current location to continue.",
        "share_location_refresh_prompt": "Your saved location is older than 24 hours. Please share your current location to continue.",
        "share_location_button": "Share Current Location",
        "share_location_required": "Please use the Share Current Location button.",
        "location_required": "Location is required before you can open the app.",
        "verified_success": "Welcome, {name}! You are verified.",
        "verification_complete": "Welcome, {name}! You are verified.",
        "location_refreshed": "Welcome, {name}! Your location has been refreshed.",
        "welcome_verified": "Welcome, {name}! You are verified.",
        "open_app_ready": "Food delivery app is ready.",
        "open_app_button": "Open Food Delivery App",
        "change_language_command_ack": "Choose your new language.",
        "language_selection_saved": "Language saved.",
    },
    "uz_latn": {
        "choose_language": "Tilni tanlang.",
        "invalid_language": "Iltimos, tilni tugmalar orqali tanlang.",
        "language_updated": "Til O'zbek tiliga o'zgartirildi.",
        "change_language_hint": "Tilni o'zgartirish uchun istalgan payt /language buyrug'idan foydalaning.",
        "enter_name": "Ismingizni kiriting",
        "invalid_name": "Iltimos, 2 dan 25 tagacha harfdan iborat haqiqiy ism kiriting.",
        "share_contact": "Iltimos, telefon raqamingizni yuboring.",
        "share_contact_prompt": "Iltimos, telefon raqamingizni yuboring.",
        "share_contact_button": "Kontaktni yuborish",
        "share_contact_required": "Iltimos, Kontaktni yuborish tugmasidan foydalaning.",
        "share_own_contact": "Iltimos, Telegram tugmasi orqali o'zingizning kontaktingizni yuboring.",
        "share_location": "Tasdiqlashni yakunlash uchun joriy joylashuvingizni yuboring.",
        "share_location_prompt": "Tasdiqlashni yakunlash uchun joriy joylashuvingizni yuboring.",
        "location_expired": "Saqlangan joylashuvingiz 24 soatdan eski. Davom etish uchun joriy joylashuvingizni yuboring.",
        "share_location_refresh_prompt": "Saqlangan joylashuvingiz 24 soatdan eski. Davom etish uchun joriy joylashuvingizni yuboring.",
        "share_location_button": "Joriy joylashuvni yuborish",
        "share_location_required": "Iltimos, Joriy joylashuvni yuborish tugmasidan foydalaning.",
        "location_required": "Ilovaga kirishdan oldin joylashuv yuborilishi shart.",
        "verified_success": "Xush kelibsiz, {name}! Siz tasdiqlandingiz.",
        "verification_complete": "Xush kelibsiz, {name}! Siz tasdiqlandingiz.",
        "location_refreshed": "Xush kelibsiz, {name}! Joylashuvingiz yangilandi.",
        "welcome_verified": "Xush kelibsiz, {name}! Siz tasdiqlandingiz.",
        "open_app_ready": "Oziq-ovqat yetkazib berish ilovasi tayyor.",
        "open_app_button": "Oziq-ovqat yetkazib berish ilovasini ochish",
        "change_language_command_ack": "Yangi tilni tanlang.",
        "language_selection_saved": "Til saqlandi.",
    },
    "ru": {
        "choose_language": "Пожалуйста, выберите язык.",
        "invalid_language": "Пожалуйста, выберите язык кнопками клавиатуры.",
        "language_updated": "Язык изменён на русский.",
        "change_language_hint": "Чтобы сменить язык позже, используйте команду /language.",
        "enter_name": "Введите ваше имя",
        "invalid_name": "Пожалуйста, введите настоящее имя из 2–25 букв.",
        "share_contact": "Пожалуйста, отправьте свой номер телефона.",
        "share_contact_prompt": "Пожалуйста, отправьте свой номер телефона.",
        "share_contact_button": "Отправить контакт",
        "share_contact_required": "Пожалуйста, используйте кнопку отправки контакта.",
        "share_own_contact": "Пожалуйста, отправьте именно свой контакт через кнопку Telegram.",
        "share_location": "Пожалуйста, отправьте вашу текущую геолокацию, чтобы завершить проверку.",
        "share_location_prompt": "Пожалуйста, отправьте вашу текущую геолокацию, чтобы завершить проверку.",
        "location_expired": "Ваша сохранённая геолокация старше 24 часов. Пожалуйста, отправьте текущую геолокацию, чтобы продолжить.",
        "share_location_refresh_prompt": "Ваша сохранённая геолокация старше 24 часов. Пожалуйста, отправьте текущую геолокацию, чтобы продолжить.",
        "share_location_button": "Отправить геолокацию",
        "share_location_required": "Пожалуйста, используйте кнопку отправки геолокации.",
        "location_required": "Перед открытием приложения нужно отправить геолокацию.",
        "verified_success": "Добро пожаловать, {name}! Вы подтверждены.",
        "verification_complete": "Добро пожаловать, {name}! Вы подтверждены.",
        "location_refreshed": "Добро пожаловать, {name}! Ваша геолокация обновлена.",
        "welcome_verified": "Добро пожаловать, {name}! Вы подтверждены.",
        "open_app_ready": "Приложение доставки еды готово.",
        "open_app_button": "Открыть приложение доставки еды",
        "change_language_command_ack": "Выберите новый язык.",
        "language_selection_saved": "Язык сохранён.",
    },
    "uz_cyrl": {
        "choose_language": "Тилни танланг.",
        "invalid_language": "Илтимос, тилни тугмалар орқали танланг.",
        "language_updated": "Тил Ўзбек тилига ўзгартирилди.",
        "change_language_hint": "Тилни ўзгартириш учун исталган пайт /language буйруғидан фойдаланинг.",
        "enter_name": "Исмингизни киритинг",
        "invalid_name": "Илтимос, 2–25 ҳарфдан иборат ҳақиқий исм киритинг.",
        "share_contact": "Илтимос, телефон рақамингизни юборинг.",
        "share_contact_prompt": "Илтимос, телефон рақамингизни юборинг.",
        "share_contact_button": "Контактни юбориш",
        "share_contact_required": "Илтимос, Контактни юбориш тугмасидан фойдаланинг.",
        "share_own_contact": "Илтимос, Telegram тугмаси орқали ўз контактигизни юборинг.",
        "share_location": "Тасдиқлашни якунлаш учун жорий жойлашувингизни юборинг.",
        "share_location_prompt": "Тасдиқлашни якунлаш учун жорий жойлашувингизни юборинг.",
        "location_expired": "Сақланган жойлашувингиз 24 соатдан эски. Давом этиш учун жорий жойлашувингизни юборинг.",
        "share_location_refresh_prompt": "Сақланган жойлашувингиз 24 соатдан эски. Давом этиш учун жорий жойлашувингизни юборинг.",
        "share_location_button": "Жорий жойлашувни юбориш",
        "share_location_required": "Илтимос, Жорий жойлашувни юбориш тугмасидан фойдаланинг.",
        "location_required": "Иловани очишдан олдин жойлашув юборилиши шарт.",
        "verified_success": "Хуш келибсиз, {name}! Сиз тасдиқландингиз.",
        "verification_complete": "Хуш келибсиз, {name}! Сиз тасдиқландингиз.",
        "location_refreshed": "Хуш келибсиз, {name}! Жойлашувингиз янгиланди.",
        "welcome_verified": "Хуш келибсиз, {name}! Сиз тасдиқландингиз.",
        "open_app_ready": "Овқат етказиб бериш иловаси тайёр.",
        "open_app_button": "Овқат етказиб бериш иловасини очиш",
        "change_language_command_ack": "Янги тилни танланг.",
        "language_selection_saved": "Тил сақланди.",
    },
}


def get_text(lang: str | None, key: str, **kwargs) -> str:
    language = lang if lang in TEXTS else "en"
    template = TEXTS.get(language, {}).get(key) or TEXTS["en"].get(key) or key
    return template.format(**kwargs)


def normalize_bot_language(value: str | None) -> str:
    if value in LANGUAGE_CODE_TO_LABEL:
        return value
    return "en"


def build_language_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("O'zbek"), KeyboardButton("English")],
            [KeyboardButton("Русский"), KeyboardButton("Ўзбек")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        print("CHAT ID:", update.effective_chat.id)


def get_order_map_url(order: dict) -> str:
    latitude = order.get("latitude")
    longitude = order.get("longitude")
    if latitude is not None and longitude is not None:
        map_url = (
            f"https://yandex.com/maps/?ll={longitude},{latitude}"
            f"&z=16&pt={longitude},{latitude},pm2rdm"
        )
        logger.info(
            "Generated Yandex map URL lat=%s lng=%s url=%s",
            latitude,
            longitude,
            map_url,
        )
        return map_url

    location_text = order.get("resolved_address") or order.get("address") or ""
    if not location_text:
        return ""
    map_url = f"https://yandex.com/maps/?text={quote_plus(location_text)}"
    logger.info(
        "Generated Yandex map URL from address lat=%s lng=%s url=%s",
        latitude,
        longitude,
        map_url,
    )
    return map_url


def build_open_app_markup(lang: str | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text=get_text(lang, "open_app_button"),
                    web_app=WebAppInfo(url=WEB_APP_URL),
                )
            ]
        ]
    )


def is_profile_complete(profile: Profile | None) -> bool:
    return shared_is_profile_complete(profile)


def is_profile_verified(profile: Profile | None) -> bool:
    return shared_is_profile_verified(profile)


def is_location_fresh(profile: Profile | None) -> bool:
    return shared_is_location_fresh(profile)


def ensure_utc_datetime(value: datetime) -> datetime:
    return shared_ensure_utc_datetime(value)


def get_first_name(name: str) -> str:
    parts = [part for part in (name or "").strip().split() if part]
    return parts[0] if parts else "there"


def is_repeated_pattern(value: str) -> bool:
    if len(value) < 4:
        return False
    for size in range(1, (len(value) // 2) + 1):
        if len(value) % size == 0:
            fragment = value[:size]
            if fragment * (len(value) // size) == value:
                return True
    return False


def validate_full_name(value: str) -> tuple[bool, str]:
    text = " ".join((value or "").strip().split())
    if len(text) < 2 or len(text) > 25:
        return False, "invalid_name"

    if not all(char.isalpha() or char in NAME_ALLOWED_EXTRA_CHARS for char in text):
        return False, "invalid_name"

    letters_only = "".join(char.lower() for char in text if char.isalpha())
    if len(letters_only) < 2:
        return False, "invalid_name"
    if len(set(letters_only)) <= 2:
        return False, "invalid_name"

    if re.search(r"(.)\1{2,}", letters_only):
        return False, "invalid_name"

    if is_repeated_pattern(letters_only):
        return False, "invalid_name"

    lowered_parts = [part.lower() for part in re.split(r"[\s\-]+", text) if part]
    if any(part in COMMON_FAKE_NAME_PATTERNS for part in lowered_parts):
        return False, "invalid_name"

    if len(lowered_parts) == 1 and re.fullmatch(r"[a-z]+", lowered_parts[0]) and text == text.lower() and len(lowered_parts[0]) >= 5:
        return False, "invalid_name"

    return True, text


def sync_profile_verification(telegram_user_id: str, telegram_username: str | None = None) -> Profile | None:
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == telegram_user_id).first()
        if not user:
            return None

        profile = db.query(Profile).filter(Profile.user_id == user.id).first()
        if not profile:
            return None

        if telegram_username is not None:
            profile.telegram_username = telegram_username
        if not profile.preferred_language:
            profile.preferred_language = "en"
        should_be_verified = is_profile_complete(profile)
        if profile.verified != should_be_verified:
            profile.verified = should_be_verified
            profile.verified_at = datetime.now(RESTAURANT_TIMEZONE) if should_be_verified else None
            if should_be_verified and not profile.last_location_at:
                profile.last_location_at = profile.verified_at
        db.commit()
        db.refresh(profile)

        return profile


def get_profile_by_telegram_user_id(telegram_user_id: str) -> Profile | None:
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == telegram_user_id).first()
        if not user:
            return None
        return db.query(Profile).filter(Profile.user_id == user.id).first()


def get_profile_language(profile: Profile | None) -> str:
    if not profile:
        return "en"
    return normalize_bot_language(profile.preferred_language)


def get_context_language(context: ContextTypes.DEFAULT_TYPE, profile: Profile | None = None) -> str:
    pending_language = context.user_data.get("preferred_language")
    if pending_language:
        return normalize_bot_language(pending_language)
    return get_profile_language(profile)


def save_preferred_language(
    telegram_user_id: str,
    preferred_language: str,
    telegram_username: str | None = None,
) -> None:
    language_code = normalize_bot_language(preferred_language)
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == telegram_user_id).first()
        if not user:
            user = User(telegram_user_id=telegram_user_id)
            db.add(user)
            db.flush()

        profile = db.query(Profile).filter(Profile.user_id == user.id).first()
        if not profile:
            profile = Profile(user_id=user.id, name="", phone="", language="English", preferred_language=language_code)
            db.add(profile)

        profile.preferred_language = language_code
        if telegram_username is not None:
            profile.telegram_username = telegram_username
        db.commit()


def get_courier_identity(telegram_user) -> tuple[str, str | None]:
    if not telegram_user:
        return "Courier", None

    telegram_user_id = str(telegram_user.id)
    courier_name = (
        telegram_user.full_name
        or telegram_user.username
        or telegram_user.first_name
        or "Courier"
    )
    courier_phone = None

    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == telegram_user_id).first()
        if user:
            profile = db.query(Profile).filter(Profile.user_id == user.id).first()
            if profile and profile.phone and profile.phone.strip():
                courier_phone = profile.phone.strip()

    logger.info(
        "Resolved courier identity for telegram_user_id=%s name=%s phone_present=%s",
        telegram_user_id,
        courier_name,
        bool(courier_phone),
    )
    return courier_name, courier_phone


def save_profile_from_bot(
    telegram_user_id: str,
    *,
    name: str,
    phone: str,
    preferred_language: str | None = None,
    telegram_username: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    readable_address: str | None = None,
) -> dict:
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_user_id == telegram_user_id).first()
        if not user:
            user = User(telegram_user_id=telegram_user_id)
            db.add(user)
            db.flush()

        profile = db.query(Profile).filter(Profile.user_id == user.id).first()
        if not profile:
            profile = Profile(user_id=user.id, name="", phone="", language="English", preferred_language="en")
            db.add(profile)

        profile.name = name
        profile.phone = phone
        if preferred_language is not None:
            profile.preferred_language = normalize_bot_language(preferred_language)
        profile.telegram_username = telegram_username
        had_existing_location = profile.latitude is not None and profile.longitude is not None
        previous_latitude = profile.latitude
        previous_longitude = profile.longitude
        profile.latitude = latitude
        profile.longitude = longitude
        profile.readable_address = readable_address
        location_shared = latitude is not None and longitude is not None
        location_changed = (
            location_shared
            and (
                not had_existing_location
                or previous_latitude != latitude
                or previous_longitude != longitude
            )
        )
        location_updated = location_shared and (
            location_changed or had_existing_location
        )
        if latitude is not None and longitude is not None:
            profile.last_location_at = datetime.now(RESTAURANT_TIMEZONE)
            logger.info(
                "Bot location timestamp refreshed telegram_user_id=%s last_location_at=%s latitude=%s longitude=%s",
                telegram_user_id,
                profile.last_location_at.isoformat(),
                latitude,
                longitude,
            )
        was_verified = bool(profile.verified)
        profile.verified = is_profile_complete(profile)
        if profile.verified and not profile.verified_at:
            profile.verified_at = datetime.now(RESTAURANT_TIMEZONE)
        if not profile.verified:
            profile.verified_at = None
        db.commit()
        return {
            "became_verified": profile.verified and not was_verified,
            "location_updated": location_updated,
            "last_location_at": profile.last_location_at,
        }


async def send_verified_user_notification(
    *,
    telegram_user_id: str,
    name: str,
    phone: str,
    telegram_username: str | None,
    latitude: float | None,
    longitude: float | None,
    readable_address: str | None,
    verified_at: datetime | None = None,
) -> None:
    if not VERIFIED_USERS_GROUP_CHAT_ID:
        logger.info("VERIFIED_USERS_GROUP_CHAT_ID is not set. Skipping verified-user notification.")
        return

    location_text = (
        f"{latitude}, {longitude}"
        if latitude is not None and longitude is not None
        else "No location provided"
    )
    message = "\n".join(
        [
            "USER VERIFIED",
            f"Name: {name}",
            f"Phone: {phone}",
            f"Telegram Username: @{telegram_username}" if telegram_username else "Telegram Username: -",
            f"Telegram User ID: {telegram_user_id}",
            f"Location: {location_text}",
            f"Readable Address: {readable_address or '-'}",
            f"Verified At: {(verified_at or datetime.now(timezone.utc)).isoformat()}",
        ]
    )
    try:
        await BOT_CLIENT.send_message(chat_id=int(VERIFIED_USERS_GROUP_CHAT_ID), text=message)
        logger.info("Verified-user notification sent for telegram_user_id=%s", telegram_user_id)
    except Exception as exc:
        logger.exception(
            "Failed to send verified-user notification for telegram_user_id=%s: %s",
            telegram_user_id,
            exc,
        )


async def send_location_updated_notification(
    *,
    telegram_user_id: str,
    name: str,
    phone: str,
    latitude: float | None,
    longitude: float | None,
    refreshed_at: datetime | None,
) -> None:
    if not VERIFIED_USERS_GROUP_CHAT_ID:
        logger.info("VERIFIED_USERS_GROUP_CHAT_ID is not set. Skipping location-updated notification.")
        return

    map_url = ""
    if latitude is not None and longitude is not None:
        map_url = get_order_map_url({"latitude": latitude, "longitude": longitude})
    refresh_time = (refreshed_at or datetime.now(RESTAURANT_TIMEZONE)).astimezone(RESTAURANT_TIMEZONE)
    message_lines = [
        "Location updated:",
        f"Name: {name or '-'}",
        f"Phone: {phone or '-'}",
        f"Telegram ID: {telegram_user_id}",
        f"Location: {latitude}, {longitude}" if latitude is not None and longitude is not None else "Location: -",
    ]
    if map_url:
        message_lines.append(f"Map: {map_url}")
    message_lines.append(f"Time: {refresh_time.strftime('%Y-%m-%d %H:%M')}")
    message = "\n".join(message_lines)

    try:
        await BOT_CLIENT.send_message(chat_id=int(VERIFIED_USERS_GROUP_CHAT_ID), text=message)
        logger.info(
            "Location-updated notification sent telegram_user_id=%s latitude=%s longitude=%s refreshed_at=%s",
            telegram_user_id,
            latitude,
            longitude,
            refresh_time.isoformat(),
        )
    except Exception as exc:
        logger.exception(
            "Failed to send location-updated notification for telegram_user_id=%s: %s",
            telegram_user_id,
            exc,
        )


def build_courier_message(order: dict, status_label: str) -> str:
    items_text = "\n".join(
        f"- {item['name']} x{item['quantity']}"
        for item in order.get("items", [])
    ) or "- No items"

    location_text = order.get("resolved_address") or order.get("address") or "No address"
    phone_text = order.get("phone") or "-"
    customer_name = order.get("customer_name") or "Unknown customer"
    map_url = order.get("map_url") or get_order_map_url(order)

    lines = [
        "COURIER ORDER",
        f"Order: {order.get('order_id', '-')}",
        f"Status: {status_label}",
        f"Customer: {customer_name}",
        f"Phone: {phone_text}",
        f"Location: {location_text}",
        "Items:",
        items_text,
    ]

    if map_url:
        lines.append(f"Map: {map_url}")

    return "\n".join(lines)


def build_ready_order_message(order: dict) -> str:
    return build_courier_message(order, "READY")


def build_ready_order_reply_markup(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="Start Delivering",
                    callback_data=f"{DELIVERY_CALLBACK_PREFIX}{order_id}",
                )
            ]
        ]
    )


def build_delivered_reply_markup(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="Delivered",
                    callback_data=f"{DELIVERED_CALLBACK_PREFIX}{order_id}",
                )
            ]
        ]
    )


async def send_ready_order_to_group_result(order: dict) -> dict:
    order_id = order.get("order_id")
    if not order_id:
        logger.error("Ready-order payload is missing order_id. Message was not sent.")
        return {
            "sent": False,
            "error": "Ready-order payload is missing order_id.",
            "retry_after": None,
        }

    try:
        logger.info("send_ready_order_to_group called for %s", order_id)
        await BOT_CLIENT.send_message(
            chat_id=WORK_GROUP_CHAT_ID,
            text=build_ready_order_message(order),
            reply_markup=build_ready_order_reply_markup(order_id),
        )
        logger.info("Ready-order message sent to work group for %s", order_id)
        return {
            "sent": True,
            "error": None,
            "retry_after": None,
        }
    except RetryAfter as exc:
        retry_after = float(exc.retry_after or 0)
        logger.warning(
            "Telegram rate-limited ready-order message for %s. retry_after=%s",
            order_id,
            retry_after,
        )
        return {
            "sent": False,
            "error": str(exc),
            "retry_after": retry_after,
        }
    except Exception as exc:
        logger.exception("Failed to send ready-order message for %s: %s", order_id, exc)
        retry_after = getattr(exc, "retry_after", None)
        return {
            "sent": False,
            "error": str(exc),
            "retry_after": float(retry_after) if retry_after else None,
        }


async def send_ready_order_to_group(order: dict) -> bool:
    result = await send_ready_order_to_group_result(order)
    return bool(result["sent"])


def send_ready_order_to_group_sync(order: dict) -> bool:
    return asyncio.run(send_ready_order_to_group(order))


def send_ready_order_to_group_result_sync(order: dict) -> dict:
    return asyncio.run(send_ready_order_to_group_result(order))


def mark_order_delivering(order_id: str, courier_name: str, courier_phone: str | None = None) -> tuple[bool, str]:
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if not order:
            logger.error("Callback requested missing order %s", order_id)
            return False, "Order not found."

        if (order.delivery_type or "").strip().lower() == "pickup":
            logger.warning("Pickup order %s was blocked from delivery callback flow", order_id)
            return False, "Pickup orders do not enter delivery flow."

        normalized_status = normalize_order_status(order.status)
        if normalized_status == "delivering":
            return True, "Order is already delivering."
        if normalized_status != "ready":
            logger.error("Order %s cannot move to delivering from status %s", order_id, normalized_status)
            return False, f"Order cannot move to delivering from {normalized_status}."

        logger.info("Attempting to move order %s to delivering", order_id)
        order.status = "delivering"
        order.courier_name = courier_name
        order.courier_phone = courier_phone
        db.commit()
        logger.info(
            "Order %s moved to delivering from Telegram callback with courier_name=%s phone_present=%s",
            order_id,
            courier_name,
            bool(courier_phone),
        )
        return True, "Order marked as delivering."


def mark_order_delivered(order_id: str) -> tuple[bool, str]:
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if not order:
            logger.error("Delivered callback requested missing order %s", order_id)
            return False, "Order not found."

        normalized_status = normalize_order_status(order.status)
        if normalized_status == "delivered":
            return True, "Order is already delivered."
        if normalized_status != "delivering":
            logger.error("Order %s cannot move to delivered from status %s", order_id, normalized_status)
            return False, f"Order cannot move to delivered from {normalized_status}."

        logger.info("Attempting to move order %s to delivered", order_id)
        order.status = "delivered"
        order.delivered_at = datetime.now(timezone.utc)
        db.commit()
        logger.info("Order %s moved to delivered from Telegram callback", order_id)
        return True, "Order marked as delivered."


async def prompt_for_contact(update: Update, lang: str):
    contact_keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton(get_text(lang, "share_contact_button"), request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(
        get_text(lang, "share_contact"),
        reply_markup=contact_keyboard,
    )


async def prompt_for_required_location(update: Update, lang: str, refresh_only: bool = False):
    location_keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton(get_text(lang, "share_location_button"), request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(
        get_text(lang, "location_expired" if refresh_only else "share_location"),
        reply_markup=location_keyboard,
    )


async def prompt_for_language_selection(update: Update, message: str):
    await update.message.reply_text(
        message,
        reply_markup=build_language_keyboard(),
    )


async def start_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    context.user_data.setdefault("onboarding", {})
    context.user_data["preferred_language"] = lang
    await update.message.reply_text(get_text(lang, "enter_name"), reply_markup=ReplyKeyboardRemove())


async def finish_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_user_id = str(update.effective_user.id)
    telegram_username = update.effective_user.username if update.effective_user else None
    onboarding_data = context.user_data.get("onboarding", {})
    lang = normalize_bot_language(onboarding_data.get("preferred_language") or context.user_data.get("preferred_language"))
    save_result = save_profile_from_bot(
        telegram_user_id,
        name=onboarding_data.get("name", ""),
        phone=onboarding_data.get("phone", ""),
        preferred_language=lang,
        telegram_username=telegram_username,
        latitude=onboarding_data.get("latitude"),
        longitude=onboarding_data.get("longitude"),
        readable_address=onboarding_data.get("readable_address"),
    )
    became_verified = bool(save_result.get("became_verified"))
    location_updated = bool(save_result.get("location_updated"))
    refreshed_at = save_result.get("last_location_at")
    refreshed_only = bool(onboarding_data.get("refresh_only"))
    context.user_data["onboarding"] = {}

    if location_updated:
        await send_location_updated_notification(
            telegram_user_id=telegram_user_id,
            name=onboarding_data.get("name", ""),
            phone=onboarding_data.get("phone", ""),
            latitude=onboarding_data.get("latitude"),
            longitude=onboarding_data.get("longitude"),
            refreshed_at=refreshed_at,
        )

    if became_verified:
        await send_verified_user_notification(
            telegram_user_id=telegram_user_id,
            name=onboarding_data.get("name", ""),
            phone=onboarding_data.get("phone", ""),
            telegram_username=telegram_username,
            latitude=onboarding_data.get("latitude"),
            longitude=onboarding_data.get("longitude"),
            readable_address=onboarding_data.get("readable_address"),
            verified_at=datetime.now(RESTAURANT_TIMEZONE),
        )

    await update.message.reply_text(
        get_text(
            lang,
            "location_refreshed" if refreshed_only else "verified_success",
            name=get_first_name(onboarding_data.get("name", "")),
        ),
        reply_markup=ReplyKeyboardRemove(),
    )
    await update.message.reply_text(
        get_text(lang, "open_app_ready"),
        reply_markup=build_open_app_markup(lang),
    )
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_user_id = str(update.effective_user.id)
    existing_profile = sync_profile_verification(
        telegram_user_id,
        update.effective_user.username if update.effective_user else None,
    )
    lang = get_profile_language(existing_profile)
    readiness = shared_is_profile_app_ready(existing_profile)
    profile_verified = readiness["verified"]
    location_fresh = readiness["location_fresh"]
    logger.info(
        "/start profile state user_id=%s exists=%s verified=%s location_fresh=%s app_ready=%s last_location_at=%s complete=%s",
        telegram_user_id,
        bool(existing_profile),
        profile_verified,
        location_fresh,
        readiness["app_ready"],
        existing_profile.last_location_at.isoformat() if existing_profile and existing_profile.last_location_at else None,
        is_profile_complete(existing_profile),
    )
    if readiness["app_ready"]:
        context.user_data["preferred_language"] = lang
        logger.info("Skipping onboarding for %s because profile is verified", telegram_user_id)
        await update.message.reply_text(
            get_text(lang, "welcome_verified", name=get_first_name(existing_profile.name or "")),
            reply_markup=build_open_app_markup(lang),
        )
        return ConversationHandler.END

    if profile_verified and readiness["location_refresh_required"]:
        logger.info("Requesting fresh location for %s because stored location is stale", telegram_user_id)
        context.user_data["onboarding"] = {
            "name": existing_profile.name,
            "phone": existing_profile.phone,
            "refresh_only": True,
            "readable_address": existing_profile.readable_address,
            "preferred_language": lang,
        }
        context.user_data["preferred_language"] = lang
        await prompt_for_required_location(update, lang, True)
        return ASK_LOCATION

    if not existing_profile or not existing_profile.preferred_language:
        context.user_data["onboarding"] = {}
        await prompt_for_language_selection(
            update,
            "Please choose your language.\n\nTilni tanlang.\n\nПожалуйста, выберите язык.\n\nТилни танланг.",
        )
        return ASK_LANGUAGE

    logger.info("Starting onboarding for %s because profile is missing or incomplete", telegram_user_id)
    context.user_data["onboarding"] = {"preferred_language": lang}
    await start_name_step(update, context, lang)
    return ASK_NAME


async def change_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_user_id = str(update.effective_user.id)
    profile = get_profile_by_telegram_user_id(telegram_user_id)
    lang = get_profile_language(profile)
    context.user_data["language_change_only"] = True
    await prompt_for_language_selection(update, get_text(lang, "change_language_command_ack"))
    return ASK_LANGUAGE


async def choose_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    selected_label = (update.message.text or "").strip()
    lang = LANGUAGE_LABEL_TO_CODE.get(selected_label)
    if not lang:
        current_lang = normalize_bot_language(context.user_data.get("preferred_language"))
        await update.message.reply_text(
            get_text(current_lang, "invalid_language"),
            reply_markup=build_language_keyboard(),
        )
        return ASK_LANGUAGE

    telegram_user_id = str(update.effective_user.id)
    telegram_username = update.effective_user.username if update.effective_user else None
    save_preferred_language(telegram_user_id, lang, telegram_username)
    context.user_data["preferred_language"] = lang
    context.user_data.setdefault("onboarding", {})["preferred_language"] = lang

    if context.user_data.pop("language_change_only", False):
        profile = sync_profile_verification(telegram_user_id, telegram_username)
        readiness = shared_is_profile_app_ready(profile)
        await update.message.reply_text(get_text(lang, "language_updated"), reply_markup=ReplyKeyboardRemove())
        if profile and readiness["app_ready"]:
            await update.message.reply_text(
                get_text(lang, "open_app_ready"),
                reply_markup=build_open_app_markup(lang),
            )
            return ConversationHandler.END
        if profile and readiness["verified"] and readiness["location_refresh_required"]:
            context.user_data["onboarding"] = {
                "name": profile.name,
                "phone": profile.phone,
                "refresh_only": True,
                "readable_address": profile.readable_address,
                "preferred_language": lang,
            }
            await prompt_for_required_location(update, lang, True)
            return ASK_LOCATION
        context.user_data["onboarding"] = {"preferred_language": lang}
        await start_name_step(update, context, lang)
        return ASK_NAME

    await update.message.reply_text(get_text(lang, "language_selection_saved"), reply_markup=ReplyKeyboardRemove())
    await start_name_step(update, context, lang)
    return ASK_NAME


async def handle_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = normalize_bot_language(context.user_data.get("preferred_language"))
    text = (update.message.text or "").strip()
    logger.info("handle_name triggered user_id=%s raw_text=%s", update.effective_user.id if update.effective_user else "-", text)
    is_valid, result = validate_full_name(text)
    if not is_valid:
        logger.info("handle_name validation_failed user_id=%s reason=%s", update.effective_user.id if update.effective_user else "-", result)
        await update.message.reply_text(get_text(lang, result))
        logger.info("handle_name reply_sent invalid user_id=%s", update.effective_user.id if update.effective_user else "-")
        return ASK_NAME

    logger.info("handle_name validation_passed user_id=%s parsed_name=%s", update.effective_user.id if update.effective_user else "-", result)
    context.user_data.setdefault("onboarding", {})["name"] = result
    await prompt_for_contact(update, lang)
    logger.info("handle_name reply_sent next_state=ASK_CONTACT user_id=%s", update.effective_user.id if update.effective_user else "-")
    return ASK_CONTACT


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = normalize_bot_language(context.user_data.get("preferred_language"))
    contact = update.message.contact
    if not contact:
        await update.message.reply_text(get_text(lang, "share_contact_required"))
        return ASK_CONTACT
    if contact.user_id and update.effective_user and contact.user_id != update.effective_user.id:
        await update.message.reply_text(get_text(lang, "share_own_contact"))
        return ASK_CONTACT

    context.user_data.setdefault("onboarding", {})["phone"] = contact.phone_number
    await prompt_for_required_location(update, lang)
    return ASK_LOCATION


async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = normalize_bot_language(context.user_data.get("preferred_language"))
    location = update.message.location
    if not location:
        await update.message.reply_text(get_text(lang, "share_location_required"))
        return ASK_LOCATION

    onboarding_data = context.user_data.setdefault("onboarding", {})
    onboarding_data["latitude"] = location.latitude
    onboarding_data["longitude"] = location.longitude
    return await finish_onboarding(update, context)


async def handle_skip_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip().lower() != "skip":
        return ASK_LOCATION

    lang = normalize_bot_language(context.user_data.get("preferred_language"))
    await update.message.reply_text(get_text(lang, "location_required"))
    await prompt_for_required_location(update, lang, bool(context.user_data.get("onboarding", {}).get("refresh_only")))
    return ASK_LOCATION


async def handle_start_delivering_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.error("Received callback update without callback_query payload")
        return

    callback_data = query.data or ""
    logger.info("Delivery callback received: %s", callback_data)
    if not callback_data.startswith(DELIVERY_CALLBACK_PREFIX):
        logger.error("Invalid callback data received: %s", callback_data)
        await query.answer("Invalid action.", show_alert=True)
        return

    order_id = callback_data.removeprefix(DELIVERY_CALLBACK_PREFIX).strip()
    if not order_id:
        logger.error("Callback data is missing order id: %s", callback_data)
        await query.answer("Invalid order reference.", show_alert=True)
        return

    courier_name, courier_phone = get_courier_identity(query.from_user)
    logger.info(
        "Start delivering callback parsed for order=%s courier_name=%s phone_present=%s",
        order_id,
        courier_name,
        bool(courier_phone),
    )
    success, message = mark_order_delivering(order_id, courier_name, courier_phone)
    await query.answer(message, show_alert=not success)
    if not success:
        return

    updated_text = build_courier_message(
        {"order_id": order_id, **extract_order_details_from_message(query.message.text)},
        "DELIVERING",
    ) + f"\n\nDelivery started by: {courier_name}"
    try:
        await query.edit_message_text(
            updated_text,
            reply_markup=build_delivered_reply_markup(order_id),
        )
        logger.info("Telegram message updated to Delivered button for %s", order_id)
    except Exception as exc:
        logger.exception("Failed to update Telegram message for %s: %s", order_id, exc)


def extract_order_details_from_message(message_text: str) -> dict:
    details = {
        "customer_name": "Unknown customer",
        "phone": "-",
        "address": "No address",
        "items": [],
    }
    if not message_text:
        return details

    lines = message_text.splitlines()
    items_started = False
    for line in lines:
        if line.startswith("Customer: "):
            details["customer_name"] = line.removeprefix("Customer: ").strip()
        elif line.startswith("Phone: "):
            details["phone"] = line.removeprefix("Phone: ").strip()
        elif line.startswith("Location: "):
            details["address"] = line.removeprefix("Location: ").strip()
        elif line == "Items:":
            items_started = True
        elif items_started and line.startswith("- "):
            item_line = line.removeprefix("- ").strip()
            if " x" in item_line:
                name, quantity = item_line.rsplit(" x", 1)
                details["items"].append({"name": name, "quantity": quantity})
        elif items_started and not line.startswith("- "):
            items_started = False

    return details


async def handle_delivered_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.error("Received delivered callback without callback_query payload")
        return

    callback_data = query.data or ""
    logger.info("Delivered callback received: %s", callback_data)
    if not callback_data.startswith(DELIVERED_CALLBACK_PREFIX):
        logger.error("Invalid delivered callback data received: %s", callback_data)
        await query.answer("Invalid action.", show_alert=True)
        return

    order_id = callback_data.removeprefix(DELIVERED_CALLBACK_PREFIX).strip()
    if not order_id:
        logger.error("Delivered callback data is missing order id: %s", callback_data)
        await query.answer("Invalid order reference.", show_alert=True)
        return

    success, message = mark_order_delivered(order_id)
    await query.answer(message, show_alert=not success)
    if not success:
        return

    delivered_by = query.from_user.full_name if query.from_user else "Courier"
    updated_text = build_courier_message(
        {"order_id": order_id, **extract_order_details_from_message(query.message.text)},
        "DELIVERED",
    ) + f"\n\nDelivered by: {delivered_by}"
    try:
        await query.edit_message_text(updated_text)
        logger.info("Telegram message finalized as delivered for %s", order_id)
    except Exception as exc:
        logger.exception("Failed to finalize Telegram message for %s: %s", order_id, exc)


def main():
    if BOT_TOKEN == "local-dev-bot-token":
        print("BOT_TOKEN is not set. Set BOT_TOKEN to run the Telegram bot.")
        return

    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    onboarding_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("language", change_language),
        ],
        states={
            ASK_LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_language)],
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_name)],
            ASK_CONTACT: [MessageHandler((filters.CONTACT | filters.TEXT) & ~filters.COMMAND, handle_contact)],
            ASK_LOCATION: [
                MessageHandler(filters.TEXT & filters.Regex(r"^(Skip|skip)$"), handle_skip_location),
                MessageHandler((filters.LOCATION | filters.TEXT) & ~filters.COMMAND, handle_location),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("language", change_language),
        ],
        allow_reentry=True,
    )
    application.add_handler(onboarding_handler)
    application.add_handler(CallbackQueryHandler(handle_start_delivering_callback, pattern=f"^{DELIVERY_CALLBACK_PREFIX}"))
    application.add_handler(CallbackQueryHandler(handle_delivered_callback, pattern=f"^{DELIVERED_CALLBACK_PREFIX}"))
    application.add_handler(MessageHandler(filters.ALL, get_chat_id))
    print("Bot is running...")
    application.run_polling()


if __name__ == "__main__":
    main()
