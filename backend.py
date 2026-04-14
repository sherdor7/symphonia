from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from uuid import uuid4
from typing import Optional
from init_data_py import InitData
import os

BOT_TOKEN = os.getenv("8524810543:AAHCihTyuTHCm5QmPiKelN6awOEhuvRxSLA")

app = FastAPI(title="Food Delivery Telegram Mini App")

# -----------------------------
# In-memory demo storage
# -----------------------------
MENU = {
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

USERS = {
    # user_id: {
    #   "cart": [],
    #   "orders": [],
    #   "profile": {...}
    # }
}

# -----------------------------
# Models
# -----------------------------
class AddToCartRequest(BaseModel):
    product_id: int
    quantity: int = 1


class UpdateCartRequest(BaseModel):
    product_id: int
    quantity: int


class CheckoutRequest(BaseModel):
    delivery_type: str
    address: str
    payment_method: str
    comment: str = ""


class ProfileUpdateRequest(BaseModel):
    name: str
    phone: str
    language: str = "English"


# -----------------------------
# Helpers
# -----------------------------
def get_telegram_user_id(x_telegram_init_data: Optional[str]) -> str:
    if not x_telegram_init_data:
        # Local browser fallback for testing outside Telegram
        return "demo-user-1"

    try:
        parsed = InitData.parse(x_telegram_init_data)

        if not parsed.validate(BOT_TOKEN):
            raise HTTPException(status_code=401, detail="Invalid Telegram init data")

        if not parsed.user:
            raise HTTPException(status_code=401, detail="Telegram user not found")

        return str(parsed.user.id)

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Failed to parse Telegram init data")


def get_or_create_user(user_id: str):
    if user_id not in USERS:
        USERS[user_id] = {
            "cart": [],
            "orders": [],
            "profile": {
                "name": "",
                "phone": "",
                "language": "English",
                "addresses": [],
                "payment_methods": [],
            },
        }
    return USERS[user_id]


def find_product(product_id: int):
    for category, items in MENU.items():
        for item in items:
            if item["id"] == product_id:
                product = dict(item)
                product["category"] = category
                return product
    return None


def calculate_cart(cart):
    detailed = []
    total = 0

    for item in cart:
        product = find_product(item["product_id"])
        if product:
            line_total = product["price"] * item["quantity"]
            total += line_total
            detailed.append(
                {
                    "product_id": product["id"],
                    "name": product["name"],
                    "price": product["price"],
                    "quantity": item["quantity"],
                    "line_total": line_total,
                }
            )

    return {"items": detailed, "total": total}


# -----------------------------
# API routes
# -----------------------------
@app.get("/api/menu")
def get_menu():
    return MENU


@app.post("/api/cart/add")
def add_to_cart(
    payload: AddToCartRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)

    product = find_product(payload.product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if payload.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

    existing = next((x for x in user["cart"] if x["product_id"] == payload.product_id), None)
    if existing:
        existing["quantity"] += payload.quantity
    else:
        user["cart"].append(
            {
                "product_id": payload.product_id,
                "quantity": payload.quantity,
            }
        )

    return {"message": "Added to cart", "user_id": user_id, "cart": calculate_cart(user["cart"])}


@app.get("/api/cart/me")
def get_cart(x_telegram_init_data: Optional[str] = Header(default=None)):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)
    return calculate_cart(user["cart"])


@app.post("/api/cart/update")
def update_cart(
    payload: UpdateCartRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)

    for item in user["cart"]:
        if item["product_id"] == payload.product_id:
            if payload.quantity <= 0:
                user["cart"] = [x for x in user["cart"] if x["product_id"] != payload.product_id]
            else:
                item["quantity"] = payload.quantity
            return {"message": "Cart updated", "cart": calculate_cart(user["cart"])}

    raise HTTPException(status_code=404, detail="Product not found in cart")


@app.post("/api/checkout")
def checkout(
    payload: CheckoutRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)

    if not user["cart"]:
        raise HTTPException(status_code=400, detail="Cart is empty")

    cart_data = calculate_cart(user["cart"])
    order_id = str(uuid4())[:8]

    order = {
        "order_id": order_id,
        "items": cart_data["items"],
        "total": cart_data["total"],
        "status": "Accepted",
        "delivery_type": payload.delivery_type,
        "address": payload.address,
        "payment_method": payload.payment_method,
        "comment": payload.comment,
    }

    user["orders"].insert(0, order)
    user["cart"] = []

    return {"message": "Order placed successfully", "order": order}


@app.get("/api/orders/me")
def get_orders(x_telegram_init_data: Optional[str] = Header(default=None)):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)
    return user["orders"]


@app.get("/api/profile/me")
def get_profile(x_telegram_init_data: Optional[str] = Header(default=None)):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)
    return user["profile"]


@app.post("/api/profile/update")
def update_profile(
    payload: ProfileUpdateRequest,
    x_telegram_init_data: Optional[str] = Header(default=None),
):
    user_id = get_telegram_user_id(x_telegram_init_data)
    user = get_or_create_user(user_id)
    user["profile"]["name"] = payload.name
    user["profile"]["phone"] = payload.phone
    user["profile"]["language"] = payload.language
    return {"message": "Profile updated", "profile": user["profile"]}


# -----------------------------
# Static web app
# -----------------------------
app.mount("/static", StaticFiles(directory="webapp"), name="static")


@app.get("/")
def serve_webapp():
    return FileResponse("webapp/index.html")
