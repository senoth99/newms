import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from html import escape
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

import anyio
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse


app = FastAPI(title="MoySklad Telegram Notifier")


CACHE_PATH = "/tmp/orders_cache.json"
CACHE_TTL_SECONDS = 300

CACHE_LOCK = threading.Lock()
REFRESH_LOCK = asyncio.Lock()
SUBSCRIBERS_LOCK = asyncio.Lock()
SUBSCRIBERS: List[asyncio.Queue[str]] = []

logger = logging.getLogger("moysklad")
logging.basicConfig(level=logging.INFO)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _get_state_name(order: Dict[str, Any]) -> str:
    state_info = order.get("state", {})
    state = state_info.get("name")
    if not state:
        state_href = state_info.get("meta", {}).get("href")
        if state_href:
            state = fetch_entity(state_href).get("name")
    return state or "не указан"


def _get_agent_details(order: Dict[str, Any]) -> Dict[str, Optional[str]]:
    agent_info = order.get("agent", {})
    agent = agent_info.get("name")
    agent_phone = agent_info.get("phone")
    agent_email = agent_info.get("email")
    agent_href = agent_info.get("meta", {}).get("href")
    if agent_href and (not agent or not agent_phone or not agent_email):
        agent_details = fetch_entity(agent_href)
        agent = agent or agent_details.get("name")
        agent_phone = agent_phone or agent_details.get("phone")
        agent_email = agent_email or agent_details.get("email")
    return {
        "agent": agent or "не указан",
        "agent_phone": agent_phone,
        "agent_email": agent_email,
    }


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


def _get_delivery_method(order: Dict[str, Any]) -> str:
    delivery_service = order.get("shipmentAddressFull", {}).get("deliveryService")
    shipment_method = order.get("shipmentAddressFull", {}).get("shipmentMethod")
    delivery_method = _get_attribute_value(order, "способ доставки")
    if not delivery_method:
        if isinstance(delivery_service, dict):
            delivery_method = delivery_service.get("name")
        elif delivery_service:
            delivery_method = str(delivery_service)
    if not delivery_method:
        if isinstance(shipment_method, dict):
            delivery_method = shipment_method.get("name")
        elif shipment_method:
            delivery_method = str(shipment_method)
    return delivery_method or "не указан"


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


def fetch_entity(href: str) -> Dict[str, Any]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    response = requests.get(href, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_assortment_name(href: str) -> Optional[str]:
    return fetch_entity(href).get("name")


def _moysklad_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_customer_orders(limit: int = 100, max_days: int = 7) -> List[Dict[str, Any]]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    since = datetime.now(timezone.utc) - timedelta(days=max_days)
    filter_since = f"moment>={_moysklad_datetime(since)}"

    orders: List[Dict[str, Any]] = []
    offset = 0
    while True:
        response = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder",
            headers=headers,
            params={
                "limit": limit,
                "offset": offset,
                "expand": "state",
                "filter": filter_since,
            },
            timeout=10,
        )
        response.raise_for_status()
        rows = response.json().get("rows", [])
        orders.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
    return orders


def _format_positions(positions: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for position in positions:
        assortment = position.get("assortment", {})
        name = assortment.get("name")
        if not name:
            assortment_href = assortment.get("meta", {}).get("href")
            if assortment_href:
                name = fetch_assortment_name(assortment_href)
        name = name or "Товар"
        quantity = position.get("quantity") or 0
        if isinstance(quantity, float) and quantity.is_integer():
            quantity = int(quantity)
        price = _format_money(position.get("price"))
        lines.append(f"{name} - {quantity} шт. - {price} руб.")
    if not lines:
        return "нет позиций"
    return "\n".join(lines)


def _order_link(order: Dict[str, Any]) -> str:
    order_id = order.get("id")
    return (
        f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"
        if order_id
        else order.get("meta", {}).get("href")
    ) or "нет"


def is_cdek_state(state: str) -> bool:
    return "сдек" in state.casefold()


def is_new_order(state: str) -> bool:
    state_value = state.casefold()
    return any(
        word in state_value
        for word in [
            "нов",
            "принят",
            "оплачен",
            "обработ",
        ]
    )


def build_message(order: Dict[str, Any]) -> str:
    agent_details = _get_agent_details(order)
    agent = agent_details["agent"]
    agent_phone = agent_details["agent_phone"]
    agent_email = agent_details["agent_email"]

    state = _get_state_name(order)
    moment = _format_datetime(order.get("moment"))
    name = order.get("name") or "без номера"
    sum_value = _format_money(order.get("sum"))
    description = (
        order.get("description")
        or order.get("shipmentAddressFull", {}).get("comment")
        or _get_attribute_value(order, "комментарий")
        or "нет"
    )
    order_id = order.get("id") or "не указан"
    order_link = (
        f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"
        if order_id != "не указан"
        else order.get("meta", {}).get("href")
    ) or "нет"
    recipient = (
        order.get("shipmentAddressFull", {}).get("recipient")
        or _get_attribute_value(order, "получатель")
        or agent
    )
    phone = (
        order.get("phone")
        or agent_phone
        or _get_attribute_value(order, "телефон")
        or "не указан"
    )
    email = (
        order.get("email")
        or agent_email
        or _get_attribute_value(order, "email")
        or "не указан"
    )
    delivery_method = _get_delivery_method(order)
    address = (
        order.get("shipmentAddress")
        or order.get("shipmentAddressFull", {}).get("address")
        or "не указан"
    )
    delivery_link = _get_attribute_value(order, "ссылка на доставку") or "не указана"
    track_number = _get_attribute_value(order, "трек-номер") or "не указан"

    positions_meta = order.get("positions", {}).get("meta", {}).get("href")
    positions = order.get("positions", {}).get("rows") or []
    if positions_meta and not positions:
        positions = fetch_order_positions(positions_meta)
    positions_text = _format_positions(positions)

    return (
        f"📦 {state}\n"
        f"ID заказа: {name}\n\n"
        f"👤 Получатель: {recipient}\n"
        f"📞 Номер телефона: {phone}\n"
        f"📧 Email: {email}\n"
        f"Способ доставки: {delivery_method}\n\n"
        f"🏠 Адрес доставки: {address}\n"
        f"Ссылка на доставку: {delivery_link}\n"
        f"Трек-номер: {track_number}\n\n"
        "Состав заказа:\n"
        f"{positions_text}\n\n"
        f"Сумма заказа: {sum_value} руб.\n\n"
        f"Комментарий: {description}\n"
        f"Создан: {moment}\n"
        f"Ссылка: {order_link}"
    )


def build_cdek_message(order: Dict[str, Any]) -> str:
    agent_details = _get_agent_details(order)
    agent = agent_details["agent"]
    agent_phone = agent_details["agent_phone"]
    state = _get_state_name(order)
    name = order.get("name") or "без номера"
    order_id = order.get("id") or "не указан"
    order_link = (
        f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"
        if order_id != "не указан"
        else order.get("meta", {}).get("href")
    ) or "нет"
    recipient = (
        order.get("shipmentAddressFull", {}).get("recipient")
        or _get_attribute_value(order, "получатель")
        or agent
    )
    phone = (
        order.get("phone")
        or agent_phone
        or _get_attribute_value(order, "телефон")
        or "не указан"
    )
    address = (
        order.get("shipmentAddress")
        or order.get("shipmentAddressFull", {}).get("address")
        or "не указан"
    )
    delivery_link = _get_attribute_value(order, "ссылка на доставку") or "не указана"
    track_number = _get_attribute_value(order, "трек-номер") or "не указан"

    return (
        f"🚚 {state}\n"
        f"ID заказа: {name}\n\n"
        f"👤 Получатель: {recipient}\n"
        f"📞 Номер телефона: {phone}\n"
        f"🏠 Адрес доставки: {address}\n"
        f"Ссылка на доставку: {delivery_link}\n"
        f"Трек-номер: {track_number}\n"
        f"Ссылка: {order_link}"
    )


def send_telegram_message(text: str) -> None:
    bot_token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    if not bot_token or not chat_id:
        logger.warning("Telegram env vars missing, skipping send")
        return

    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10,
    )
    response.raise_for_status()


def _serialize_order(order: Dict[str, Any]) -> Dict[str, Any]:
    state_name = _get_state_name(order)
    agent_info = order.get("agent")
    recipient = None
    if isinstance(agent_info, dict):
        recipient = agent_info.get("name")
    shipment_full = order.get("shipmentAddressFull")
    city = None
    if isinstance(shipment_full, dict):
        city = shipment_full.get("city") or shipment_full.get("region")
    return {
        "id": order.get("id") or "",
        "name": order.get("name") or "без номера",
        "state": state_name,
        "moment": order.get("moment"),
        "sum": order.get("sum"),
        "city": city,
        "recipient": recipient,
        "link": _order_link(order),
    }


def _stats_from_orders(orders: List[Dict[str, Any]]) -> Dict[str, int]:
    stats = {"new_orders": 0, "cdek_orders": 0, "total_orders": len(orders)}
    for order in orders:
        state = str(order.get("state") or "")
        if is_cdek_state(state):
            stats["cdek_orders"] += 1
            continue
        if is_new_order(state):
            stats["new_orders"] += 1
    return stats


def _cache_payload(orders: List[Dict[str, Any]], updated_at: Optional[str] = None) -> Dict[str, Any]:
    updated = updated_at or _now_iso()
    stats = _stats_from_orders(orders)
    return {
        "updated_at": updated,
        "ttl_seconds": CACHE_TTL_SECONDS,
        "stats": stats,
        "orders": orders,
    }


def _ensure_cache_dir() -> None:
    cache_dir = os.path.dirname(CACHE_PATH)
    if not cache_dir:
        return
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError as exc:
        logger.warning("Failed to ensure cache directory %s: %s", cache_dir, exc)


def _load_cache_unlocked() -> Optional[Dict[str, Any]]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        logger.warning("Failed to decode cache: %s", exc)
        return None


def _write_cache_unlocked(cache: Dict[str, Any]) -> None:
    _ensure_cache_dir()
    with NamedTemporaryFile("w", delete=False, dir=os.path.dirname(CACHE_PATH), encoding="utf-8") as handle:
        json.dump(cache, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
        temp_name = handle.name
    os.replace(temp_name, CACHE_PATH)


def read_cache() -> Optional[Dict[str, Any]]:
    with CACHE_LOCK:
        return _load_cache_unlocked()


def write_cache(cache: Dict[str, Any]) -> None:
    with CACHE_LOCK:
        _write_cache_unlocked(cache)


def update_cache_with_order(order_payload: Dict[str, Any]) -> Dict[str, Any]:
    with CACHE_LOCK:
        cache = _load_cache_unlocked() or _cache_payload([])
        orders = cache.get("orders", [])
        order_id = order_payload.get("id")
        updated_orders: List[Dict[str, Any]] = []
        replaced = False
        for existing in orders:
            if order_id and existing.get("id") == order_id:
                updated_orders.append(order_payload)
                replaced = True
            else:
                updated_orders.append(existing)
        if not replaced:
            updated_orders.append(order_payload)
        cache = _cache_payload(updated_orders)
        _write_cache_unlocked(cache)
    return cache


def build_cache_from_orders(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    serialized = [_serialize_order(order) for order in orders]
    return _cache_payload(serialized)


def cache_is_stale(cache: Dict[str, Any]) -> bool:
    updated_at = cache.get("updated_at")
    ttl = cache.get("ttl_seconds", CACHE_TTL_SECONDS)
    if not updated_at:
        return True
    try:
        parsed = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
    except ValueError:
        return True
    now = datetime.now(timezone.utc)
    return (now - parsed).total_seconds() > int(ttl)


def _log_stats(cache: Dict[str, Any]) -> None:
    stats = cache.get("stats", {})
    updated_at = cache.get("updated_at")
    logger.info(
        "[STATS] total=%s new=%s cdek=%s updated_at=%s",
        stats.get("total_orders"),
        stats.get("new_orders"),
        stats.get("cdek_orders"),
        updated_at,
    )


def _event_payload(cache: Dict[str, Any]) -> str:
    payload = {
        "updated_at": cache.get("updated_at"),
        "ttl_seconds": cache.get("ttl_seconds", CACHE_TTL_SECONDS),
        "stats": cache.get("stats", {}),
        "orders": cache.get("orders", []),
        "stale": cache_is_stale(cache),
    }
    return json.dumps(payload, ensure_ascii=False)


def _safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")


def _render_landing_page(
    new_orders: int,
    cdek_orders: int,
    orders: List[Dict[str, Any]],
    updated_at: Optional[str],
    stale: bool,
    has_cache: bool,
) -> str:
    updated_text = _format_datetime(updated_at) if updated_at else "не обновлялось"
    status_text = "Данные загружаются" if not has_cache else "Данные обновлены"
    warning_block = ""
    if stale and has_cache:
        warning_block = """
        <div class="warning">
            Данные устарели. Нажмите «Обновить», чтобы подтянуть актуальные значения.
        </div>
        """
    empty_block = ""
    if not has_cache:
        empty_block = """
        <div class="warning">
            Данные загружаются. Попробуйте обновить позже.
        </div>
        """
    initial_payload = _safe_json_dumps(
        {
            "updated_at": updated_at,
            "ttl_seconds": CACHE_TTL_SECONDS,
            "stats": {"new_orders": new_orders, "cdek_orders": cdek_orders},
            "orders": orders,
            "stale": stale,
        }
    )
    return f"""
    <!doctype html>
    <html lang="ru">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>CASHER OPS DASHBOARD</title>
            <style>
                :root {{
                    color-scheme: dark;
                    font-family: "Inter", "Segoe UI", "Helvetica Neue", Arial, sans-serif;
                }}
                body {{
                    margin: 0;
                    background: radial-gradient(circle at top, #0f3d2e 0%, #0b0f0d 45%, #000000 100%);
                    color: #e5e5e5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 32px 20px 64px;
                }}
                h1 {{
                    font-size: clamp(24px, 4vw, 36px);
                    margin-bottom: 8px;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                }}
                .subtitle {{
                    color: #c6c6c6;
                    margin-bottom: 16px;
                    max-width: 680px;
                }}
                .meta {{
                    display: flex;
                    flex-wrap: wrap;
                    align-items: center;
                    gap: 12px 24px;
                    margin-bottom: 24px;
                    font-size: 13px;
                    color: #c6c6c6;
                }}
                .meta span {{
                    display: inline-flex;
                    gap: 6px;
                    align-items: center;
                }}
                .status-pill {{
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                    padding: 6px 12px;
                    border-radius: 999px;
                    border: 1px solid rgba(198, 198, 198, 0.3);
                    background: rgba(15, 61, 46, 0.4);
                    box-shadow: 0 0 18px rgba(35, 255, 180, 0.15);
                }}
                .refresh-button {{
                    border: 1px solid rgba(198, 198, 198, 0.4);
                    background: rgba(15, 61, 46, 0.6);
                    color: #e5e5e5;
                    font-weight: 600;
                    padding: 10px 18px;
                    border-radius: 999px;
                    cursor: pointer;
                    transition: box-shadow 0.2s ease, transform 0.2s ease;
                }}
                .refresh-button:disabled {{
                    opacity: 0.6;
                    cursor: progress;
                }}
                .refresh-button:not(:disabled):hover {{
                    box-shadow: 0 0 18px rgba(35, 255, 180, 0.35);
                    transform: translateY(-1px);
                }}
                .grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                    gap: 18px;
                }}
                .card {{
                    background: rgba(11, 15, 13, 0.7);
                    border-radius: 18px;
                    padding: 26px;
                    box-shadow: 0 0 24px rgba(35, 255, 180, 0.1);
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    cursor: pointer;
                    border: 1px solid rgba(198, 198, 198, 0.2);
                    position: relative;
                    overflow: hidden;
                }}
                .card:hover {{
                    border-color: rgba(198, 198, 198, 0.5);
                }}
                .card.kpi-alert::after {{
                    content: "";
                    position: absolute;
                    inset: -2px;
                    border-radius: 20px;
                    border: 1px solid rgba(198, 198, 198, 0.7);
                    box-shadow: 0 0 26px rgba(35, 255, 180, 0.6);
                    animation: blink 0.45s ease-in-out 2;
                }}
                .value {{
                    font-size: clamp(36px, 6vw, 52px);
                    font-weight: 700;
                    color: #e5e5e5;
                }}
                .label {{
                    font-size: 14px;
                    letter-spacing: 0.12em;
                    text-transform: uppercase;
                    color: #c6c6c6;
                }}
                .filters {{
                    margin-top: 28px;
                    display: flex;
                    flex-wrap: wrap;
                    gap: 16px;
                    align-items: center;
                }}
                .filter-group {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                    align-items: center;
                }}
                .filter-label {{
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.12em;
                    color: #c6c6c6;
                }}
                .filter-button {{
                    border: 1px solid rgba(198, 198, 198, 0.3);
                    background: rgba(11, 15, 13, 0.7);
                    color: #e5e5e5;
                    padding: 8px 14px;
                    border-radius: 999px;
                    cursor: pointer;
                    font-size: 13px;
                }}
                .filter-button.active {{
                    border-color: rgba(35, 255, 180, 0.8);
                    box-shadow: 0 0 16px rgba(35, 255, 180, 0.35);
                }}
                .reset-button {{
                    border: 1px solid rgba(198, 198, 198, 0.4);
                    background: transparent;
                    color: #c6c6c6;
                    padding: 8px 16px;
                    border-radius: 999px;
                    cursor: pointer;
                    font-size: 13px;
                }}
                .orders {{
                    margin-top: 28px;
                    display: grid;
                    gap: 16px;
                }}
                .order-card {{
                    border: 1px solid rgba(198, 198, 198, 0.2);
                    border-radius: 18px;
                    padding: 18px 20px;
                    background: rgba(0, 0, 0, 0.5);
                    display: grid;
                    gap: 12px;
                    cursor: pointer;
                    transition: border 0.2s ease, box-shadow 0.2s ease;
                }}
                .order-card:hover {{
                    border-color: rgba(198, 198, 198, 0.45);
                    box-shadow: 0 0 24px rgba(35, 255, 180, 0.15);
                }}
                .order-card.new-flash {{
                    animation: glow 0.5s ease-in-out 1;
                }}
                .order-header {{
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: space-between;
                    align-items: center;
                    gap: 12px;
                }}
                .order-number {{
                    font-size: 18px;
                    font-weight: 600;
                }}
                .status-badge {{
                    padding: 4px 10px;
                    border-radius: 999px;
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.08em;
                    border: 1px solid transparent;
                }}
                .status-new {{
                    color: #9cffd6;
                    border-color: rgba(156, 255, 214, 0.4);
                }}
                .status-paid {{
                    color: #23ffb4;
                    border-color: rgba(35, 255, 180, 0.5);
                }}
                .status-cdek {{
                    color: #e5e5e5;
                    border-color: rgba(229, 229, 229, 0.5);
                }}
                .status-warning {{
                    color: #ffd166;
                    border-color: rgba(255, 209, 102, 0.5);
                }}
                .status-error {{
                    color: #ff5f5f;
                    border-color: rgba(255, 95, 95, 0.5);
                }}
                .order-meta {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                    gap: 8px 16px;
                    font-size: 13px;
                    color: #c6c6c6;
                }}
                .order-meta span {{
                    display: block;
                }}
                .order-actions {{
                    display: flex;
                    justify-content: flex-end;
                }}
                .order-link {{
                    border: 1px solid rgba(198, 198, 198, 0.4);
                    background: rgba(15, 61, 46, 0.5);
                    color: #e5e5e5;
                    padding: 8px 16px;
                    border-radius: 999px;
                    font-size: 13px;
                    text-decoration: none;
                }}
                .empty-state {{
                    padding: 24px;
                    border-radius: 16px;
                    border: 1px dashed rgba(198, 198, 198, 0.3);
                    text-align: center;
                    color: #c6c6c6;
                }}
                .warning {{
                    margin-top: 24px;
                    padding: 14px 18px;
                    border-radius: 14px;
                    border: 1px solid rgba(255, 209, 102, 0.6);
                    background: rgba(255, 209, 102, 0.12);
                    color: #ffd166;
                    font-size: 13px;
                }}
                .warning strong {{
                    color: #ffe5a6;
                }}
                .stale {{
                    color: #ffd166;
                }}
                @keyframes glow {{
                    0% {{ box-shadow: 0 0 0 rgba(35, 255, 180, 0.0); }}
                    50% {{ box-shadow: 0 0 28px rgba(35, 255, 180, 0.6); }}
                    100% {{ box-shadow: 0 0 0 rgba(35, 255, 180, 0.0); }}
                }}
                @keyframes blink {{
                    0%, 100% {{ opacity: 1; }}
                    50% {{ opacity: 0.3; }}
                }}
                @media (max-width: 768px) {{
                    .container {{
                        padding: 24px 16px 48px;
                    }}
                    .order-actions {{
                        justify-content: flex-start;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>CASHER OPS DASHBOARD</h1>
                <div class="subtitle">
                    Операционный контроль заказов CASHER в реальном времени: новые заявки,
                    статусы и визуальный контроль.
                </div>
                <div class="meta">
                    <span>Обновлено: <strong id="updated-at">{escape(updated_text)}</strong></span>
                    <span class="status-pill">Статус: <strong id="status-text">{escape(status_text)}</strong></span>
                    <button class="refresh-button" id="refresh-button" type="button">Обновить</button>
                </div>
                <div class="grid">
                    <button class="card" type="button" id="kpi-new-orders">
                        <div class="value" id="new-orders-count">{new_orders}</div>
                        <div class="label">НОВЫЕ ЗАКАЗЫ</div>
                    </button>
                    <button class="card" type="button" id="kpi-cdek-orders">
                        <div class="value" id="cdek-orders-count">{cdek_orders}</div>
                        <div class="label">ОТПРАВЛЕНО СДЕК</div>
                    </button>
                </div>
                <div class="filters">
                    <div class="filter-group" data-filter-group="period">
                        <span class="filter-label">Период</span>
                        <button class="filter-button active" type="button" data-period="today">Сегодня</button>
                        <button class="filter-button" type="button" data-period="three_days">3 дня</button>
                        <button class="filter-button" type="button" data-period="week">7 дней</button>
                    </div>
                    <div class="filter-group" data-filter-group="status">
                        <span class="filter-label">Статус</span>
                        <button class="filter-button active" type="button" data-status="all">Все</button>
                        <button class="filter-button" type="button" data-status="new">Новые</button>
                        <button class="filter-button" type="button" data-status="cdek">СДЭК</button>
                    </div>
                    <button class="reset-button" id="reset-filters" type="button">Сбросить</button>
                </div>
                <div class="orders" id="orders-list">
                    <div class="empty-state">Загрузка данных...</div>
                </div>
                {warning_block}
                {empty_block}
            </div>
            <script>
                const kpiNewOrders = document.getElementById('kpi-new-orders');
                const kpiCdekOrders = document.getElementById('kpi-cdek-orders');
                const refreshButton = document.getElementById('refresh-button');
                const statusText = document.getElementById('status-text');
                const updatedAt = document.getElementById('updated-at');
                const newOrdersCount = document.getElementById('new-orders-count');
                const cdekOrdersCount = document.getElementById('cdek-orders-count');
                const ordersList = document.getElementById('orders-list');
                const resetFilters = document.getElementById('reset-filters');
                const periodButtons = document.querySelectorAll('[data-period]');
                const statusButtons = document.querySelectorAll('[data-status]');

                const initialPayload = {initial_payload};
                let currentPayload = initialPayload;
                let knownOrderIds = new Set((initialPayload.orders || []).map((order) => order.id));
                let activeFilters = {{
                    period: 'today',
                    status: 'all',
                }};

                const formatDate = (value) => {{
                    if (!value) return 'не указана';
                    return value.replace('T', ' ').split('.')[0];
                }};

                const getStatusClass = (state) => {{
                    const value = (state || '').toLowerCase();
                    if (value.includes('ошиб') || value.includes('api')) return 'status-error';
                    if (value.includes('оплачен')) return 'status-paid';
                    if (value.includes('сдек')) return 'status-cdek';
                    if (value.includes('нов') || value.includes('принят') || value.includes('обработ')) return 'status-new';
                    if (value.includes('проблем')) return 'status-warning';
                    return 'status-warning';
                }};

                const isNewOrder = (state) => {{
                    const value = (state || '').toLowerCase();
                    return (
                        value.includes('нов') ||
                        value.includes('принят') ||
                        value.includes('оплачен') ||
                        value.includes('обработ')
                    ) && !value.includes('сдек');
                }};

                const filterByPeriod = (orders) => {{
                    const now = new Date();
                    return orders.filter((order) => {{
                        if (!order.moment) return false;
                        const orderDate = new Date(order.moment);
                        if (activeFilters.period === 'today') {{
                            return (
                                orderDate.getDate() === now.getDate() &&
                                orderDate.getMonth() === now.getMonth() &&
                                orderDate.getFullYear() === now.getFullYear()
                            );
                        }}
                        const diffDays = (now - orderDate) / (1000 * 60 * 60 * 24);
                        if (activeFilters.period === 'three_days') {{
                            return diffDays <= 3;
                        }}
                        return diffDays <= 7;
                    }});
                }};

                const filterByStatus = (orders) => {{
                    if (activeFilters.status === 'all') return orders;
                    if (activeFilters.status === 'cdek') {{
                        return orders.filter((order) => (order.state || '').toLowerCase().includes('сдек'));
                    }}
                    if (activeFilters.status === 'new') {{
                        return orders.filter((order) => isNewOrder(order.state));
                    }}
                    return orders;
                }};

                const renderOrders = (orders, highlightedIds = new Set()) => {{
                    if (!orders.length) {{
                        ordersList.innerHTML = '<div class="empty-state">Нет заказов по выбранным фильтрам</div>';
                        return;
                    }}
                    ordersList.innerHTML = orders.map((order) => {{
                        const statusClass = getStatusClass(order.state);
                        const isHighlighted = highlightedIds.has(order.id);
                        return `
                            <div class="order-card ${{statusClass}} ${{isHighlighted ? 'new-flash' : ''}}" data-link="${{order.link || '#'}}">
                                <div class="order-header">
                                    <div class="order-number">${{order.name || 'без номера'}}</div>
                                    <span class="status-badge ${{statusClass}}">${{order.state || 'не указан'}}</span>
                                </div>
                                <div class="order-meta">
                                    <span>Дата: ${{formatDate(order.moment)}}</span>
                                    <span>Город: ${{order.city || 'не указан'}}</span>
                                    <span>Получатель: ${{order.recipient || 'не указан'}}</span>
                                </div>
                                <div class="order-actions">
                                    <a class="order-link" href="${{order.link || '#'}}" target="_blank" rel="noreferrer">Открыть в МойСклад</a>
                                </div>
                            </div>
                        `;
                    }}).join('');

                    document.querySelectorAll('.order-card').forEach((card) => {{
                        card.addEventListener('click', (event) => {{
                            if (event.target.closest('a')) return;
                            const link = card.getAttribute('data-link');
                            if (link) {{
                                window.open(link, '_blank', 'noreferrer');
                            }}
                        }});
                    }});
                }};

                const applyFilters = (orders, highlightedIds) => {{
                    const filtered = filterByStatus(filterByPeriod(orders));
                    const sorted = filtered.sort((a, b) => {{
                        const aIsNew = isNewOrder(a.state) ? 1 : 0;
                        const bIsNew = isNewOrder(b.state) ? 1 : 0;
                        if (aIsNew !== bIsNew) {{
                            return bIsNew - aIsNew;
                        }}
                        const aTime = a.moment ? new Date(a.moment).getTime() : 0;
                        const bTime = b.moment ? new Date(b.moment).getTime() : 0;
                        return bTime - aTime;
                    }});
                    renderOrders(sorted, highlightedIds);
                }};

                const updateKpi = (payload, previousNewCount = 0) => {{
                    const newCount = payload.stats?.new_orders ?? 0;
                    newOrdersCount.textContent = newCount;
                    cdekOrdersCount.textContent = payload.stats?.cdek_orders ?? 0;
                    if (payload.updated_at) {{
                        updatedAt.textContent = formatDate(payload.updated_at);
                    }}
                    statusText.textContent = payload.stale ? 'Данные устарели' : 'Данные обновлены';
                    statusText.classList.toggle('stale', Boolean(payload.stale));

                    if (newCount > previousNewCount) {{
                        kpiNewOrders.classList.add('kpi-alert');
                        setTimeout(() => kpiNewOrders.classList.remove('kpi-alert'), 900);
                    }}
                }};

                const updateFromPayload = (payload) => {{
                    if (!payload || !payload.stats) return;
                    const previousNewCount = currentPayload.stats?.new_orders ?? 0;
                    updateKpi(payload, previousNewCount);

                    const orders = payload.orders || [];
                    const newIds = new Set(orders.map((order) => order.id));
                    const highlightedIds = new Set();
                    newIds.forEach((id) => {{
                        if (id && !knownOrderIds.has(id)) {{
                            highlightedIds.add(id);
                        }}
                    }});
                    knownOrderIds = newIds;
                    currentPayload = payload;
                    applyFilters(orders, highlightedIds);
                }};

                const setActiveButton = (buttons, activeValue, dataAttr) => {{
                    buttons.forEach((button) => {{
                        const value = button.getAttribute(dataAttr);
                        button.classList.toggle('active', value === activeValue);
                    }});
                }};

                periodButtons.forEach((button) => {{
                    button.addEventListener('click', () => {{
                        activeFilters.period = button.getAttribute('data-period');
                        setActiveButton(periodButtons, activeFilters.period, 'data-period');
                        applyFilters(currentPayload.orders || [], new Set());
                    }});
                }});

                statusButtons.forEach((button) => {{
                    button.addEventListener('click', () => {{
                        activeFilters.status = button.getAttribute('data-status');
                        setActiveButton(statusButtons, activeFilters.status, 'data-status');
                        applyFilters(currentPayload.orders || [], new Set());
                    }});
                }});

                resetFilters.addEventListener('click', () => {{
                    activeFilters = {{ period: 'today', status: 'all' }};
                    setActiveButton(periodButtons, activeFilters.period, 'data-period');
                    setActiveButton(statusButtons, activeFilters.status, 'data-status');
                    applyFilters(currentPayload.orders || [], new Set());
                }});

                kpiNewOrders.addEventListener('click', () => {{
                    activeFilters.status = 'new';
                    setActiveButton(statusButtons, activeFilters.status, 'data-status');
                    applyFilters(currentPayload.orders || [], new Set());
                    ordersList.scrollIntoView({{ behavior: 'smooth' }});
                }});

                kpiCdekOrders.addEventListener('click', () => {{
                    activeFilters.status = 'cdek';
                    setActiveButton(statusButtons, activeFilters.status, 'data-status');
                    applyFilters(currentPayload.orders || [], new Set());
                    ordersList.scrollIntoView({{ behavior: 'smooth' }});
                }});

                refreshButton.addEventListener('click', async () => {{
                    refreshButton.disabled = true;
                    statusText.textContent = 'Обновляем...';
                    try {{
                        const response = await fetch('/refresh', {{ method: 'POST' }});
                        const payload = await response.json();
                        if (payload.updated_at) {{
                            updatedAt.textContent = payload.updated_at.replace('T', ' ').split('.')[0];
                        }}
                        statusText.textContent = 'Данные обновлены';
                    }} catch (error) {{
                        statusText.textContent = 'Ошибка обновления';
                    }} finally {{
                        refreshButton.disabled = false;
                    }}
                }});

                updateFromPayload(initialPayload);

                const eventSource = new EventSource('/events');
                eventSource.onmessage = (event) => {{
                    try {{
                        const payload = JSON.parse(event.data);
                        updateFromPayload(payload);
                    }} catch (error) {{
                        console.warn('Failed to parse event', error);
                    }}
                }};
            </script>
        </body>
    </html>
    """


async def broadcast_event(cache: Dict[str, Any]) -> None:
    payload = _event_payload(cache)
    async with SUBSCRIBERS_LOCK:
        for queue in list(SUBSCRIBERS):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning("Dropping SSE event for slow client")


async def refresh_cache(reason: str) -> Optional[Dict[str, Any]]:
    async with REFRESH_LOCK:
        try:
            orders = await anyio.to_thread.run_sync(fetch_customer_orders)
            cache = await anyio.to_thread.run_sync(build_cache_from_orders, orders)
            await anyio.to_thread.run_sync(write_cache, cache)
            _log_stats(cache)
            await broadcast_event(cache)
            logger.info("Cache refreshed: %s", reason)
            return cache
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to refresh cache: %s", exc)
            return None


async def auto_refresh_loop() -> None:
    await asyncio.sleep(60)
    while True:
        try:
            cache = await anyio.to_thread.run_sync(read_cache)
            if cache is None or cache_is_stale(cache):
                await refresh_cache("ttl")
        except Exception as exc:  # noqa: BLE001
            logger.exception("Auto refresh failed: %s", exc)
        await asyncio.sleep(60)


async def _process_webhook_event(href: str) -> None:
    try:
        order = await anyio.to_thread.run_sync(fetch_order_details, href)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch order details: %s", exc)
        return

    cache: Optional[Dict[str, Any]] = None
    try:
        order_payload = await anyio.to_thread.run_sync(_serialize_order, order)
        cache = await anyio.to_thread.run_sync(update_cache_with_order, order_payload)
        _log_stats(cache)
        await broadcast_event(cache)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to update cache for webhook: %s", exc)

    try:
        state_name = await anyio.to_thread.run_sync(_get_state_name, order)
        if is_cdek_state(state_name):
            message = await anyio.to_thread.run_sync(build_cdek_message, order)
        else:
            message = await anyio.to_thread.run_sync(build_message, order)
        await anyio.to_thread.run_sync(send_telegram_message, message)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to send Telegram notification: %s", exc)


@app.on_event("startup")
async def startup_event() -> None:
    _ensure_cache_dir()
    await refresh_cache("startup")
    asyncio.create_task(auto_refresh_loop())


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def landing() -> HTMLResponse:
    counts = {"new_orders": 0, "cdek_orders": 0}
    orders: List[Dict[str, Any]] = []
    updated_at: Optional[str] = None
    stale = False
    has_cache = False

    try:
        cache = read_cache()
        if cache:
            has_cache = True
            updated_at = cache.get("updated_at")
            stale = cache_is_stale(cache)
            stats = cache.get("stats", {})
            counts = {
                "new_orders": int(stats.get("new_orders", 0)),
                "cdek_orders": int(stats.get("cdek_orders", 0)),
            }
            orders = sorted(
                cache.get("orders", []),
                key=lambda order: order.get("moment") or "",
                reverse=True,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read cache: %s", exc)

    html = _render_landing_page(
        new_orders=counts["new_orders"],
        cdek_orders=counts["cdek_orders"],
        orders=orders,
        updated_at=updated_at,
        stale=stale,
        has_cache=has_cache,
    )
    return HTMLResponse(content=html, status_code=200)


@app.post("/refresh")
async def refresh() -> JSONResponse:
    cache = await refresh_cache("manual")
    if not cache:
        cache = await anyio.to_thread.run_sync(read_cache)
    return JSONResponse({"status": "ok", "updated_at": cache.get("updated_at") if cache else None})


@app.get("/events")
async def events() -> StreamingResponse:
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=10)
    async with SUBSCRIBERS_LOCK:
        SUBSCRIBERS.append(queue)

    async def event_stream() -> Any:
        try:
            cache = await anyio.to_thread.run_sync(read_cache)
            if cache:
                yield f"data: {_event_payload(cache)}\n\n"
            while True:
                payload = await queue.get()
                yield f"data: {payload}\n\n"
        except asyncio.CancelledError:
            raise
        finally:
            async with SUBSCRIBERS_LOCK:
                if queue in SUBSCRIBERS:
                    SUBSCRIBERS.remove(queue)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/webhook/moysklad")
async def moysklad_webhook(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Invalid webhook payload: %s", exc)
        return JSONResponse({"status": "ok"})

    events: List[Dict[str, Any]] = payload.get("events", []) if isinstance(payload, dict) else []
    if not events:
        logger.info("Webhook received without events")
        return JSONResponse({"status": "ok"})

    for event in events:
        meta = event.get("meta", {})
        if meta.get("type") != "customerorder":
            continue
        href = meta.get("href")
        if not href:
            continue
        asyncio.create_task(_process_webhook_event(href))

    return JSONResponse({"status": "ok"})
