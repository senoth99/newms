import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request


app = FastAPI(title="MoySklad Telegram Notifier")


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _format_datetime(value: Optional[str]) -> str:
    if not value:
        return "не указана"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def _format_money(value: Optional[int]) -> str:
    if value is None:
        return "не указана"
    return f"{value / 100:.2f}"


def _get_attribute_value(order: Dict[str, Any], attribute_name: str) -> Optional[Any]:
    attributes = order.get("attributes", [])
    if not isinstance(attributes, list):
        return None
    name_normalized = attribute_name.casefold()
    for attribute in attributes:
        if attribute.get("name", "").casefold() == name_normalized:
            return attribute.get("value")
    return None


def _format_attribute_money(value: Optional[Any]) -> str:
    if value is None:
        return "не указана"
    if isinstance(value, int):
        return _format_money(value)
    if isinstance(value, float):
        return _format_money(int(value))
    return str(value)


def _moysklad_headers() -> Dict[str, str]:
    token = os.getenv("MS_TOKEN")
    basic_token = os.getenv("MS_BASIC_TOKEN")
    if basic_token:
        return {"Authorization": f"Basic {basic_token}"}
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def fetch_order_details(href: str) -> Dict[str, Any]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    response = requests.get(href, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_order_positions(href: str) -> List[Dict[str, Any]]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    response = requests.get(href, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json().get("rows", [])


def _format_positions(positions: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for position in positions:
        assortment = position.get("assortment", {})
        name = assortment.get("name") or "Товар"
        quantity = position.get("quantity") or 0
        price = _format_money(position.get("price"))
        lines.append(f"{name} - {quantity} шт. - {price} руб.")
    if not lines:
        return "нет позиций"
    return "\n".join(lines)


def build_message(order: Dict[str, Any]) -> str:
    agent = order.get("agent", {}).get("name") or "не указан"
    state = order.get("state", {}).get("name") or "не указан"
    moment = _format_datetime(order.get("moment"))
    name = order.get("name") or "без номера"
    sum_value = _format_money(order.get("sum"))
    description = order.get("description") or "нет"
    href = order.get("meta", {}).get("href") or "нет"
    site = order.get("source", {}).get("name") or _get_attribute_value(order, "сайт") or "не указан"
    order_id = order.get("name") or order.get("id") or "не указан"
    recipient = (
        order.get("shipmentAddressFull", {}).get("recipient")
        or _get_attribute_value(order, "получатель")
        or agent
    )
    phone = (
        order.get("phone")
        or order.get("agent", {}).get("phone")
        or _get_attribute_value(order, "телефон")
        or "не указан"
    )
    email = (
        order.get("email")
        or order.get("agent", {}).get("email")
        or _get_attribute_value(order, "email")
        or "не указан"
    )
    telegram = _get_attribute_value(order, "telegram") or _get_attribute_value(order, "телеграм") or "не указан"
    delivery_method = (
        _get_attribute_value(order, "способ доставки")
        or order.get("shipmentAddressFull", {}).get("comment")
        or "не указан"
    )
    address = (
        order.get("shipmentAddress")
        or order.get("shipmentAddressFull", {}).get("address")
        or "не указан"
    )
    delivery_link = _get_attribute_value(order, "ссылка на доставку") or "не указана"
    track_number = _get_attribute_value(order, "трек-номер") or "не указан"
    delivery_cost = _format_attribute_money(_get_attribute_value(order, "стоимость доставки"))
    promo_code = _get_attribute_value(order, "промокод") or "не указан"

    positions_meta = order.get("positions", {}).get("meta", {}).get("href")
    positions = order.get("positions", {}).get("rows") or []
    if positions_meta and not positions:
        positions = fetch_order_positions(positions_meta)
    positions_text = _format_positions(positions)

    return (
        f"📦 Заказ с \"{site}\" ({state})\n"
        f"ID заказа: {order_id}\n\n"
        f"👤 Получатель: {recipient}\n"
        f"📞 Номер телефона: {phone}\n"
        f"📧 Email: {email}\n"
        f"Telegram (telegram): {telegram}\n"
        f"Способ доставки: {delivery_method}\n\n"
        f"🏠 Адрес доставки: {address}\n"
        f"Ссылка на доставку: {delivery_link}\n"
        f"Трек-номер: {track_number}\n\n"
        "Состав заказа:\n"
        f"{positions_text}\n\n"
        f"Стоимость доставки: {delivery_cost} руб.\n\n"
        f"Промокод: {promo_code}\n\n"
        f"Сумма заказа: {sum_value} руб.\n\n"
        f"Комментарий: {description}\n"
        f"Создан: {moment}\n"
        f"Ссылка: {href}"
    )


def send_telegram_message(text: str) -> None:
    bot_token = _get_env("TG_BOT_TOKEN")
    chat_id = _get_env("TG_CHAT_ID")

    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10,
    )
    response.raise_for_status()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook/moysklad")
async def moysklad_webhook(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    events: List[Dict[str, Any]] = payload.get("events", [])
    if not events:
        raise HTTPException(status_code=400, detail="No events in payload")

    notified: List[str] = []
    for event in events:
        meta = event.get("meta", {})
        if meta.get("type") != "customerorder":
            continue
        href = meta.get("href")
        if not href:
            continue

        try:
            order = fetch_order_details(href)
            message = build_message(order)
            send_telegram_message(message)
            notified.append(order.get("name") or href)
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not notified:
        return {"status": "ignored"}

    return {"status": "sent", "orders": notified}
