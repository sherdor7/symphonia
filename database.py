from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


DATABASE_URL = "sqlite:///./food_delivery.db"
TASHKENT_TZ = ZoneInfo("Asia/Tashkent")

LEGACY_MENU_ITEMS = {
    "Sets": [
        {"id": 1, "name": "Family Set", "price": 120000, "description": "Big combo for family"},
        {"id": 2, "name": "Chicken Set", "price": 85000, "description": "Chicken combo set"},
    ],
    "Burgers": [
        {"id": 3, "name": "Cheeseburger", "price": 28000, "description": "Beef, cheese, sauce"},
        {"id": 4, "name": "Double Burger", "price": 39000, "description": "Double beef patty"},
    ],
    "Shawarma": [
        {"id": 5, "name": "Chicken Shawarma", "price": 24000, "description": "Lavash, chicken, veggies"},
        {"id": 6, "name": "Beef Shawarma", "price": 27000, "description": "Lavash, beef, veggies"},
    ],
    "Samsa": [
        {"id": 7, "name": "Beef Samsa", "price": 10000, "description": "Traditional samsa"},
    ],
    "Hot Meals": [
        {"id": 8, "name": "Pilaf", "price": 35000, "description": "Uzbek plov"},
    ],
    "Snacks": [
        {"id": 9, "name": "French Fries", "price": 15000, "description": "Crispy fries"},
    ],
    "Sauces": [
        {"id": 10, "name": "Garlic Sauce", "price": 5000, "description": "Creamy garlic sauce"},
    ],
    "Salads": [
        {"id": 11, "name": "Caesar Salad", "price": 26000, "description": "Chicken caesar"},
    ],
    "Drinks": [
        {"id": 12, "name": "Coca-Cola 0.5L", "price": 9000, "description": "Cold drink"},
        {"id": 13, "name": "Water", "price": 4000, "description": "Still water"},
    ],
    "Others": [
        {"id": 14, "name": "Dessert", "price": 18000, "description": "Sweet dessert"},
    ],
}

ORDER_STATUSES = (
    "new",
    "preparing",
    "ready",
    "ready_for_pickup",
    "delivering",
    "delivered",
    "completed",
    "picked_up",
    "served",
    "cancelled",
)
DEFAULT_ORDER_STATUS = "new"
FINAL_ORDER_STATUSES = ("delivered", "cancelled")
TERMINAL_ORDER_STATUSES = ("delivered", "completed", "picked_up", "served", "cancelled")
LEGACY_STATUS_MAP = {
    "pending": "new",
    "accepted": "new",
    "new": "new",
    "preparing": "preparing",
    "ready": "ready",
    "ready for pickup": "ready_for_pickup",
    "ready_for_pickup": "ready_for_pickup",
    "delivering": "delivering",
    "delivered": "delivered",
    "completed": "completed",
    "picked up": "picked_up",
    "picked_up": "picked_up",
    "served": "served",
    "cancelled": "cancelled",
}


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    telegram_user_id: Mapped[str] = mapped_column(String, unique=True, index=True)

    profile: Mapped[Optional["Profile"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        uselist=False,
    )
    cart_items: Mapped[list["CartItem"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )
    orders: Mapped[list["Order"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )


class Profile(Base):
    __tablename__ = "profiles"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), primary_key=True)
    name: Mapped[str] = mapped_column(String, default="")
    phone: Mapped[str] = mapped_column(String, default="")
    language: Mapped[str] = mapped_column(String, default="English")
    preferred_language: Mapped[str] = mapped_column(String, default="en")
    telegram_username: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    readable_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_location_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="profile")


class CartItem(Base):
    __tablename__ = "cart_items"
    __table_args__ = (UniqueConstraint("user_id", "product_id", name="uq_cart_user_product"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    product_id: Mapped[int] = mapped_column(Integer, index=True)
    quantity: Mapped[int] = mapped_column(Integer)

    user: Mapped[User] = relationship(back_populates="cart_items")


class MenuItem(Base):
    __tablename__ = "menu_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    category: Mapped[str] = mapped_column(String, default="Others", index=True)
    category_group: Mapped[str] = mapped_column(String, default="fast_food", index=True)
    is_available: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    name: Mapped[str] = mapped_column(String, index=True)
    price: Mapped[float] = mapped_column(Float)
    description: Mapped[str] = mapped_column(String, default="")
    image_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(TASHKENT_TZ),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(TASHKENT_TZ),
        onupdate=lambda: datetime.now(TASHKENT_TZ),
    )


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    order_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    total: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String, default=DEFAULT_ORDER_STATUS, index=True)
    order_type: Mapped[str] = mapped_column(String, default="delivery", index=True)
    delivery_type: Mapped[str] = mapped_column(String)
    table_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    waiter_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    address: Mapped[str] = mapped_column(String)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolved_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    payment_method: Mapped[str] = mapped_column(String)
    comment: Mapped[str] = mapped_column(String, default="")
    courier_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    courier_phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ready_notification_sent: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ready_notification_attempts: Mapped[int] = mapped_column(Integer, default=0)
    ready_notification_last_error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ready_notification_last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ready_notification_next_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    waiter_ready_acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    preparing_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    after_hours: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(TASHKENT_TZ),
        index=True,
    )

    user: Mapped[User] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
    )


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True)
    product_id: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[int] = mapped_column(Integer)
    line_total: Mapped[float] = mapped_column(Float)
    kitchen_group: Mapped[str] = mapped_column(String, default="fast_food", index=True)
    kitchen_status: Mapped[str] = mapped_column(String, default="new", index=True)
    is_drink: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    order: Mapped[Order] = relationship(back_populates="items")


engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def normalize_order_status(value: Optional[str]) -> str:
    if not value:
        return DEFAULT_ORDER_STATUS

    normalized = LEGACY_STATUS_MAP.get(value.strip().lower())
    return normalized or DEFAULT_ORDER_STATUS


def backfill_order_statuses():
    with engine.begin() as connection:
        rows = connection.execute(text("SELECT id, status FROM orders")).mappings().all()
        for row in rows:
            normalized = normalize_order_status(row["status"])
            if normalized != row["status"]:
                connection.execute(
                    text("UPDATE orders SET status = :status WHERE id = :id"),
                    {"status": normalized, "id": row["id"]},
                )


def ensure_preparing_started_at_column():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "preparing_started_at" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN preparing_started_at DATETIME"))


def ensure_delivered_at_column():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "delivered_at" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN delivered_at DATETIME"))


def ensure_after_hours_column():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "after_hours" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN after_hours BOOLEAN DEFAULT 0"))


def ensure_courier_columns():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "courier_name" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN courier_name VARCHAR"))
        if "courier_phone" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN courier_phone VARCHAR"))


def ensure_ready_notification_columns():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "ready_notification_sent" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN ready_notification_sent BOOLEAN DEFAULT 0"))
        if "ready_notification_attempts" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN ready_notification_attempts INTEGER DEFAULT 0"))
        if "ready_notification_last_error" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN ready_notification_last_error VARCHAR"))
        if "ready_notification_last_attempt_at" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN ready_notification_last_attempt_at DATETIME"))
        if "ready_notification_next_attempt_at" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN ready_notification_next_attempt_at DATETIME"))
        if "waiter_ready_acknowledged_at" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN waiter_ready_acknowledged_at DATETIME"))


def ensure_profile_location_columns():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(profiles)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "latitude" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN latitude FLOAT"))
        if "longitude" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN longitude FLOAT"))
        if "preferred_language" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN preferred_language VARCHAR DEFAULT 'en'"))
        if "telegram_username" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN telegram_username VARCHAR"))
        if "readable_address" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN readable_address VARCHAR"))
        if "verified" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN verified BOOLEAN DEFAULT 0"))
        if "verified_at" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN verified_at DATETIME"))
        if "last_location_at" not in column_names:
            connection.execute(text("ALTER TABLE profiles ADD COLUMN last_location_at DATETIME"))


def backfill_profile_verification():
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT user_id, name, phone, latitude, longitude, verified, verified_at, last_location_at
                FROM profiles
                """
            )
        ).mappings().all()
        for row in rows:
            is_complete = bool((row["name"] or "").strip()) and bool((row["phone"] or "").strip())
            is_complete = is_complete and row["latitude"] is not None and row["longitude"] is not None
            if is_complete and not row["verified"]:
                connection.execute(
                    text(
                        """
                        UPDATE profiles
                        SET verified = 1,
                            verified_at = COALESCE(verified_at, CURRENT_TIMESTAMP)
                        WHERE user_id = :user_id
                        """
                    ),
                    {"user_id": row["user_id"]},
                )


def backfill_profile_languages():
    with engine.begin() as connection:
        rows = connection.execute(
            text("SELECT user_id, language, preferred_language FROM profiles")
        ).mappings().all()
        for row in rows:
            if row["preferred_language"]:
                continue

            app_language = (row["language"] or "").strip().lower()
            if app_language == "russian":
                preferred_language = "ru"
            elif app_language == "uzbek":
                preferred_language = "uz_latn"
            else:
                preferred_language = "en"

            connection.execute(
                text("UPDATE profiles SET preferred_language = :preferred_language WHERE user_id = :user_id"),
                {"preferred_language": preferred_language, "user_id": row["user_id"]},
            )
            if is_complete and not row["last_location_at"]:
                connection.execute(
                    text(
                        """
                        UPDATE profiles
                        SET last_location_at = COALESCE(verified_at, CURRENT_TIMESTAMP)
                        WHERE user_id = :user_id
                        """
                    ),
                    {"user_id": row["user_id"]},
                )
            if not is_complete and row["verified"]:
                connection.execute(
                    text(
                        """
                        UPDATE profiles
                        SET verified = 0,
                            verified_at = NULL
                        WHERE user_id = :user_id
                        """
                    ),
                    {"user_id": row["user_id"]},
                )
            if not is_complete and row["last_location_at"]:
                connection.execute(
                    text(
                        """
                        UPDATE profiles
                        SET last_location_at = NULL
                        WHERE user_id = :user_id
                        """
                    ),
                    {"user_id": row["user_id"]},
                )


def ensure_location_columns():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(orders)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "latitude" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN latitude FLOAT"))
        if "longitude" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN longitude FLOAT"))
        if "resolved_address" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN resolved_address VARCHAR"))
        if "order_type" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN order_type VARCHAR DEFAULT 'delivery'"))
        if "table_number" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN table_number INTEGER"))
        if "waiter_name" not in column_names:
            connection.execute(text("ALTER TABLE orders ADD COLUMN waiter_name VARCHAR"))


def ensure_menu_category_group_column():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(menu_items)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "category_group" not in column_names:
            connection.execute(text("ALTER TABLE menu_items ADD COLUMN category_group VARCHAR DEFAULT 'fast_food'"))
        if "is_available" not in column_names:
            connection.execute(text("ALTER TABLE menu_items ADD COLUMN is_available BOOLEAN DEFAULT 1"))


def ensure_order_item_kitchen_columns():
    with engine.begin() as connection:
        columns = connection.execute(text("PRAGMA table_info(order_items)")).mappings().all()
        column_names = {column["name"] for column in columns}
        if "kitchen_group" not in column_names:
            connection.execute(text("ALTER TABLE order_items ADD COLUMN kitchen_group VARCHAR DEFAULT 'fast_food'"))
        if "kitchen_status" not in column_names:
            connection.execute(text("ALTER TABLE order_items ADD COLUMN kitchen_status VARCHAR DEFAULT 'new'"))
        if "is_drink" not in column_names:
            connection.execute(text("ALTER TABLE order_items ADD COLUMN is_drink BOOLEAN DEFAULT 0"))


def backfill_menu_category_groups():
    with engine.begin() as connection:
        rows = connection.execute(text("SELECT id, category, category_group FROM menu_items")).mappings().all()
        for row in rows:
            if row["category_group"]:
                continue

            category = (row["category"] or "").strip()
            category_group = "milliy_taom" if category in {"Hot Meals", "Samsa"} else "fast_food"
            connection.execute(
                text("UPDATE menu_items SET category_group = :category_group WHERE id = :id"),
                {"category_group": category_group, "id": row["id"]},
            )


def backfill_order_item_kitchen_fields():
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    order_items.id,
                    order_items.kitchen_group,
                    order_items.kitchen_status,
                    order_items.is_drink,
                    menu_items.category AS menu_category,
                    menu_items.category_group AS menu_category_group,
                    orders.status AS order_status,
                    orders.preparing_started_at
                FROM order_items
                LEFT JOIN menu_items ON menu_items.id = order_items.product_id
                LEFT JOIN orders ON orders.id = order_items.order_id
                """
            )
        ).mappings().all()
        for row in rows:
            category = (row["menu_category"] or "").strip()
            category_group = row["menu_category_group"] or ("milliy_taom" if category in {"Hot Meals", "Samsa"} else "fast_food")
            is_drink = int(category.lower() == "drinks")

            kitchen_status = row["kitchen_status"]
            if not kitchen_status:
                normalized_status = normalize_order_status(row["order_status"])
                if is_drink:
                    kitchen_status = "ready"
                elif normalized_status in TERMINAL_ORDER_STATUSES or normalized_status in {"ready", "ready_for_pickup"}:
                    kitchen_status = "ready"
                elif normalized_status in {"preparing", "delivering"} or row["preparing_started_at"]:
                    kitchen_status = "preparing"
                else:
                    kitchen_status = "new"

            connection.execute(
                text(
                    """
                    UPDATE order_items
                    SET kitchen_group = :kitchen_group,
                        kitchen_status = :kitchen_status,
                        is_drink = :is_drink
                    WHERE id = :id
                    """
                ),
                {
                    "id": row["id"],
                    "kitchen_group": category_group,
                    "kitchen_status": kitchen_status,
                    "is_drink": is_drink,
                },
            )


def backfill_order_types():
    with engine.begin() as connection:
        rows = connection.execute(text("SELECT id, delivery_type, order_type FROM orders")).mappings().all()
        for row in rows:
            if row["order_type"]:
                continue

            delivery_type = (row["delivery_type"] or "").strip().lower()
            order_type = "pickup" if delivery_type == "pickup" else "delivery"
            connection.execute(
                text("UPDATE orders SET order_type = :order_type WHERE id = :id"),
                {"order_type": order_type, "id": row["id"]},
            )


def backfill_preparing_started_at():
    with engine.begin() as connection:
        rows = connection.execute(
            text("SELECT id, status, created_at, preparing_started_at FROM orders")
        ).mappings().all()
        for row in rows:
            normalized = normalize_order_status(row["status"])
            if normalized in ("preparing", "ready", "delivering") and not row["preparing_started_at"]:
                connection.execute(
                    text("UPDATE orders SET preparing_started_at = :preparing_started_at WHERE id = :id"),
                    {"preparing_started_at": row["created_at"], "id": row["id"]},
                )


def backfill_delivered_at():
    with engine.begin() as connection:
        rows = connection.execute(
            text("SELECT id, status, created_at, delivered_at FROM orders")
        ).mappings().all()
        for row in rows:
            normalized = normalize_order_status(row["status"])
            if normalized == "delivered" and not row["delivered_at"]:
                connection.execute(
                    text("UPDATE orders SET delivered_at = :delivered_at WHERE id = :id"),
                    {"delivered_at": row["created_at"], "id": row["id"]},
                )


def backfill_ready_notification_sent():
    with engine.begin() as connection:
        rows = connection.execute(
            text("SELECT id, status, ready_notification_sent FROM orders")
        ).mappings().all()
        for row in rows:
            normalized = normalize_order_status(row["status"])
            if normalized != "ready" and not row["ready_notification_sent"]:
                connection.execute(
                    text("UPDATE orders SET ready_notification_sent = 1 WHERE id = :id"),
                    {"id": row["id"]},
                )


def seed_menu_items():
    with SessionLocal() as db:
        existing_item = db.query(MenuItem.id).first()
        if existing_item:
            return

        for category, items in LEGACY_MENU_ITEMS.items():
            for item in items:
                db.add(
                    MenuItem(
                        id=item["id"],
                        category=category,
                        category_group="milliy_taom" if category in {"Hot Meals", "Samsa"} else "fast_food",
                        name=item["name"],
                        price=item["price"],
                        description=item["description"],
                    )
                )

        db.commit()


def init_db():
    Base.metadata.create_all(bind=engine)
    ensure_preparing_started_at_column()
    ensure_delivered_at_column()
    ensure_after_hours_column()
    ensure_courier_columns()
    ensure_ready_notification_columns()
    ensure_profile_location_columns()
    ensure_location_columns()
    ensure_menu_category_group_column()
    ensure_order_item_kitchen_columns()
    backfill_order_statuses()
    backfill_preparing_started_at()
    backfill_delivered_at()
    backfill_ready_notification_sent()
    backfill_profile_verification()
    backfill_profile_languages()
    backfill_order_types()
    backfill_menu_category_groups()
    backfill_order_item_kitchen_fields()
    seed_menu_items()
