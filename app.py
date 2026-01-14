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
MSK_TZ = timezone(timedelta(hours=3))

CACHE_LOCK = threading.Lock()
REFRESH_LOCK = asyncio.Lock()
SUBSCRIBERS_LOCK = asyncio.Lock()
SUBSCRIBERS: List[asyncio.Queue[str]] = []

logger = logging.getLogger("moysklad")
logging.basicConfig(level=logging.INFO)


def _now_iso() -> str:
    return datetime.now(MSK_TZ).isoformat()


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MSK_TZ)
    return parsed.astimezone(MSK_TZ)


def _msk_now() -> datetime:
    return datetime.now(MSK_TZ)


def _msk_day_start(value: Optional[datetime] = None) -> datetime:
    current = value or _msk_now()
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def _msk_millis(value: Optional[str]) -> Optional[int]:
    parsed = _parse_datetime(value)
    if not parsed:
        return None
    return int(parsed.timestamp() * 1000)


def _format_datetime(value: Optional[str]) -> str:
    if not value:
        return "не указана"
    parsed = _parse_datetime(value)
    if not parsed:
        return value
    return parsed.strftime("%d.%m.%Y")


def _format_datetime_with_time(value: Optional[str]) -> str:
    if not value:
        return "не указана"
    parsed = _parse_datetime(value)
    if not parsed:
        return value
    return parsed.strftime("%d.%m.%Y %H:%M")


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


def _get_attribute_first(order: Dict[str, Any], *attribute_names: str) -> Optional[Any]:
    for name in attribute_names:
        value = _get_attribute_value(order, name)
        if value:
            return value
    return None


def _normalize_text(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_city_from_address(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    candidate = address.split(",")[0].strip()
    return candidate or None


def _compose_shipment_address(shipment_full: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(shipment_full, dict):
        return None
    address = _normalize_text(shipment_full.get("address"))
    if address:
        return address
    parts: List[str] = []
    for key in ("postalCode", "country", "region", "city"):
        value = _normalize_text(shipment_full.get(key))
        if value:
            parts.append(value)
    street = _normalize_text(shipment_full.get("street"))
    house = _normalize_text(shipment_full.get("house"))
    apartment = _normalize_text(shipment_full.get("apartment"))
    street_parts: List[str] = []
    if street:
        street_parts.append(street)
    if house:
        street_parts.append(f"д. {house}")
    if apartment:
        street_parts.append(f"кв. {apartment}")
    if street_parts:
        parts.append(", ".join(street_parts))
    for key in ("addInfo", "comment"):
        value = _normalize_text(shipment_full.get(key))
        if value:
            parts.append(value)
    if not parts:
        return None
    return ", ".join(parts)


def _format_attribute_money(value: Optional[Any]) -> str:
    if value is None:
        return "не указана"
    if isinstance(value, int):
        return _format_money(value)
    if isinstance(value, float):
        return _format_money(int(value))
    return str(value)


def _first_non_empty(*values: Optional[Any]) -> Optional[str]:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return None


def _extract_delivery_method(value: Optional[Any]) -> Optional[str]:
    if isinstance(value, dict):
        name = value.get("name")
        return _normalize_text(name)
    return _normalize_text(value)


def extract_order_full_data(order: Dict[str, Any]) -> Dict[str, Any]:
    agent_details = _get_agent_details(order)
    shipment_full = order.get("shipmentAddressFull")
    shipment_full_data = shipment_full if isinstance(shipment_full, dict) else {}

    delivery_address_attribute = _normalize_text(_get_attribute_value(order, "адрес доставки"))

    recipient = _first_non_empty(
        shipment_full_data.get("recipient"),
        _get_attribute_value(order, "получатель"),
        order.get("recipient"),
        agent_details.get("agent"),
    ) or "не указан"

    phone = _first_non_empty(
        shipment_full_data.get("phone"),
        _get_attribute_value(order, "телефон"),
        order.get("phone"),
        agent_details.get("agent_phone"),
    ) or "не указан"

    email = _first_non_empty(
        shipment_full_data.get("email"),
        _get_attribute_value(order, "email"),
        order.get("email"),
        agent_details.get("agent_email"),
    ) or "не указан"

    address = _first_non_empty(
        _compose_shipment_address(shipment_full_data),
        delivery_address_attribute,
        _get_attribute_first(
            order,
            "адрес",
            "адрес доставки",
            "адрес получателя",
            "адрес доставки получателя",
            "address",
        ),
        order.get("shipmentAddress"),
        order.get("address"),
    ) or "не указан"

    city = _first_non_empty(
        shipment_full_data.get("city"),
        shipment_full_data.get("settlement"),
        shipment_full_data.get("region"),
        _extract_city_from_address(_normalize_text(shipment_full_data.get("address"))),
        _extract_city_from_address(delivery_address_attribute),
        _get_attribute_first(
            order,
            "город",
            "город доставки",
            "населенный пункт",
            "city",
        ),
        _extract_city_from_address(_normalize_text(order.get("shipmentAddress"))),
    ) or "не указан"

    delivery_method = _first_non_empty(
        _extract_delivery_method(shipment_full_data.get("deliveryService")),
        _extract_delivery_method(shipment_full_data.get("shipmentMethod")),
        _get_attribute_value(order, "способ доставки"),
        order.get("deliveryMethod"),
        order.get("shipmentMethod"),
    ) or "не указан"

    comment = _first_non_empty(
        shipment_full_data.get("comment"),
        shipment_full_data.get("addInfo"),
        _get_attribute_value(order, "комментарий"),
        order.get("description"),
    ) or "не указан"

    moment_display = _format_datetime(order.get("moment"))

    return {
        "state": _get_state_name(order),
        "name": order.get("name") or "без номера",
        "sum": order.get("sum"),
        "recipient": recipient,
        "phone": phone,
        "email": email,
        "delivery_method": delivery_method,
        "city": city,
        "address": address,
        "comment": comment,
        "moment": moment_display,
        "link": _order_link(order),
    }


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
    return value.astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S")


def fetch_customer_orders(limit: int = 100, max_days: int = 7) -> List[Dict[str, Any]]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    since = _msk_now() - timedelta(days=max_days)
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
    fields = extract_order_full_data(order)
    state = fields["state"]
    moment = fields["moment"]
    name = fields["name"]
    sum_value = _format_money(fields["sum"])
    description = fields["comment"]
    order_link = fields["link"]
    recipient = fields["recipient"]
    phone = fields["phone"]
    email = fields["email"]
    delivery_method = fields["delivery_method"]
    address = fields["address"]
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
    fields = extract_order_full_data(order)
    state = fields["state"]
    name = fields["name"]
    order_link = fields["link"]
    recipient = fields["recipient"]
    phone = fields["phone"]
    address = fields["address"]
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
    fields = extract_order_full_data(order)
    return {
        "id": order.get("id") or "",
        "name": fields["name"],
        "state": fields["state"],
        "moment": fields["moment"],
        "moment_ms": _msk_millis(order.get("moment")),
        "sum": fields["sum"],
        "sum_display": f"{_format_money(fields['sum'])} руб.",
        "city": fields["city"],
        "recipient": fields["recipient"],
        "phone": fields["phone"],
        "email": fields["email"],
        "delivery_method": fields["delivery_method"],
        "address": fields["address"],
        "comment": fields["comment"],
        "link": fields["link"],
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
    now_msk = _msk_now()
    today_start = _msk_day_start(now_msk)
    stats = _stats_from_orders(orders)
    return {
        "updated_at": updated,
        "updated_at_display": _format_datetime_with_time(updated),
        "server_msk_now_ms": int(now_msk.timestamp() * 1000),
        "server_msk_today_start_ms": int(today_start.timestamp() * 1000),
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
    parsed = _parse_datetime(str(updated_at))
    if not parsed:
        return True
    now = _msk_now()
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
    updated_at = cache.get("updated_at")
    updated_at_display = cache.get("updated_at_display")
    if updated_at and not updated_at_display:
        updated_at_display = _format_datetime_with_time(str(updated_at))
    now_msk = _msk_now()
    server_now_ms = int(now_msk.timestamp() * 1000)
    server_today_start_ms = int(_msk_day_start(now_msk).timestamp() * 1000)
    payload = {
        "updated_at": updated_at,
        "updated_at_display": updated_at_display,
        "server_msk_now_ms": server_now_ms,
        "server_msk_today_start_ms": server_today_start_ms,
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
    updated_text = _format_datetime_with_time(updated_at) if updated_at else "не обновлялось"
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
            "updated_at_display": updated_text if updated_at else None,
            "server_msk_now_ms": int(_msk_now().timestamp() * 1000),
            "server_msk_today_start_ms": int(_msk_day_start().timestamp() * 1000),
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
                    --matte-green: #00f5a0;
                    --matte-black: #050706;
                    --matte-surface: rgba(12, 18, 15, 0.82);
                    --matte-surface-strong: rgba(12, 18, 15, 0.95);
                    --matte-white: #f7f7f5;
                    --matte-muted: #8c9a92;
                    --matte-border: rgba(247, 247, 245, 0.12);
                    --neon-green: #4cffb2;
                    --accent-blue: #5cc8ff;
                    --accent-purple: #8f6bff;
                    --accent-warning: #f1e3b1;
                    --glass-border: rgba(247, 247, 245, 0.08);
                }}
                body {{
                    margin: 0;
                    background:
                        radial-gradient(circle at top left, rgba(92, 200, 255, 0.18) 0%, transparent 45%),
                        radial-gradient(circle at 40% 0, rgba(76, 255, 178, 0.15) 0%, transparent 50%),
                        radial-gradient(circle at 90% 10%, rgba(143, 107, 255, 0.18) 0%, transparent 55%),
                        linear-gradient(140deg, #050806 0%, #090f0c 45%, #020403 100%);
                    color: var(--matte-white);
                    background-attachment: fixed;
                }}
                body,
                .glitter {{
                    will-change: opacity;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 32px 20px 64px;
                    position: relative;
                    z-index: 1;
                }}
                .hero {{
                    display: grid;
                    grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr);
                    gap: 24px;
                    margin-bottom: 32px;
                    align-items: stretch;
                }}
                .hero-panel {{
                    background: var(--matte-surface);
                    border: 1px solid var(--glass-border);
                    border-radius: 22px;
                    padding: 26px 28px;
                    box-shadow: 0 18px 40px rgba(0, 0, 0, 0.45);
                    backdrop-filter: blur(16px);
                }}
                .hero-eyebrow {{
                    font-size: 12px;
                    letter-spacing: 0.35em;
                    text-transform: uppercase;
                    color: var(--matte-muted);
                }}
                h1 {{
                    font-size: clamp(24px, 4vw, 36px);
                    margin: 10px 0 12px;
                    letter-spacing: 0.12em;
                    text-transform: uppercase;
                }}
                .subtitle {{
                    color: var(--matte-muted);
                    margin-bottom: 16px;
                    max-width: 680px;
                }}
                .note {{
                    margin-top: 16px;
                    font-size: 12px;
                    color: rgba(247, 247, 245, 0.7);
                    letter-spacing: 0.12em;
                    text-transform: uppercase;
                }}
                .meta {{
                    display: flex;
                    flex-wrap: wrap;
                    align-items: center;
                    gap: 12px 24px;
                    font-size: 13px;
                    color: var(--matte-muted);
                }}
                .meta span strong {{
                    color: var(--matte-white);
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
                    border: 1px solid rgba(92, 200, 255, 0.35);
                    background: rgba(7, 15, 19, 0.6);
                }}
                .refresh-button {{
                    border: 1px solid rgba(76, 255, 178, 0.65);
                    background: linear-gradient(120deg, rgba(76, 255, 178, 0.35), rgba(92, 200, 255, 0.35));
                    color: #04110a;
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
                    box-shadow: 0 12px 24px rgba(76, 255, 178, 0.2);
                    transform: translateY(-1px);
                }}
                .kpi-grid {{
                    margin-top: 8px;
                }}
                .grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                    gap: 18px;
                }}
                .grid.compact {{
                    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                }}
                .card {{
                    background: var(--matte-surface);
                    border-radius: 18px;
                    padding: 26px;
                    box-shadow: 0 12px 30px rgba(0, 0, 0, 0.4);
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    cursor: pointer;
                    border: 1px solid var(--matte-border);
                    position: relative;
                    overflow: hidden;
                }}
                .card:hover {{
                    border-color: rgba(76, 255, 178, 0.6);
                }}
                .card.kpi-alert {{
                    box-shadow: 0 0 18px rgba(57, 255, 136, 0.35);
                    animation: blink 0.175s ease-in-out 1;
                }}
                .card .value.small {{
                    font-size: clamp(22px, 4vw, 30px);
                    text-shadow: none;
                }}
                .card .value.small::after {{
                    width: 28px;
                    opacity: 0.5;
                }}
                .card .value.secondary {{
                    font-size: 18px;
                    font-weight: 600;
                    color: rgba(247, 247, 245, 0.82);
                    text-shadow: none;
                }}
                .card .meta-hint {{
                    font-size: 11px;
                    letter-spacing: 0.1em;
                    text-transform: uppercase;
                    color: var(--matte-muted);
                }}
                .sales-section {{
                    margin-top: 20px;
                }}
                .chart-card {{
                    background: rgba(10, 16, 13, 0.88);
                    border-radius: 20px;
                    border: 1px solid var(--matte-border);
                    padding: 20px 22px 24px;
                    box-shadow: 0 12px 30px rgba(0, 0, 0, 0.35);
                }}
                .chart-header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 16px;
                }}
                .chart-title {{
                    font-size: 14px;
                    letter-spacing: 0.18em;
                    text-transform: uppercase;
                    color: var(--matte-muted);
                }}
                .chart-meta {{
                    font-size: 12px;
                    color: rgba(247, 247, 245, 0.7);
                }}
                .weekly-chart {{
                    display: grid;
                    grid-template-columns: repeat(7, minmax(0, 1fr));
                    gap: 14px;
                    align-items: end;
                    min-height: 180px;
                }}
                .chart-bar {{
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    gap: 6px;
                }}
                .chart-bar-fill {{
                    width: 100%;
                    border-radius: 999px 999px 12px 12px;
                    background: linear-gradient(180deg, rgba(92, 200, 255, 0.85), rgba(76, 255, 178, 0.4));
                    box-shadow: 0 8px 18px rgba(92, 200, 255, 0.2);
                    min-height: 10px;
                    transition: height 0.3s ease;
                }}
                .chart-bar-label {{
                    font-size: 11px;
                    color: rgba(247, 247, 245, 0.55);
                    letter-spacing: 0.08em;
                }}
                .chart-bar-value {{
                    font-size: 12px;
                    color: var(--matte-white);
                }}
                .value {{
                    font-size: clamp(36px, 6vw, 52px);
                    font-weight: 700;
                    color: var(--matte-white);
                    text-shadow: 0 0 12px rgba(76, 255, 178, 0.2);
                    position: relative;
                }}
                .value::after {{
                    content: "";
                    display: block;
                    width: 40px;
                    height: 2px;
                    margin-top: 8px;
                    background: rgba(76, 255, 178, 0.4);
                    box-shadow: 0 0 8px rgba(76, 255, 178, 0.35);
                    border-radius: 999px;
                }}
                .label {{
                    font-size: 14px;
                    letter-spacing: 0.12em;
                    text-transform: uppercase;
                    color: var(--matte-muted);
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
                    background: var(--matte-surface);
                    border: 1px solid var(--glass-border);
                    border-radius: 999px;
                    padding: 8px 12px;
                }}
                .filter-label {{
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.12em;
                    color: var(--matte-muted);
                }}
                .filter-button {{
                    border: 1px solid var(--matte-border);
                    background: var(--matte-surface-strong);
                    color: var(--matte-white);
                    padding: 8px 14px;
                    border-radius: 999px;
                    cursor: pointer;
                    font-size: 13px;
                }}
                .filter-button,
                .filter-button:hover,
                .filter-button:active,
                .filter-button:focus,
                .filter-button:focus-visible,
                .filter-button.active,
                .reset-button,
                .reset-button:hover,
                .reset-button:active,
                .reset-button:focus,
                .reset-button:focus-visible {{
                    background: var(--matte-surface-strong);
                    box-shadow: none;
                    outline: none;
                }}
                .filter-button.active {{
                    border-color: rgba(92, 200, 255, 0.6);
                    box-shadow: 0 6px 16px rgba(92, 200, 255, 0.18);
                }}
                .reset-button {{
                    border: 1px solid rgba(1, 50, 32, 0.4);
                    color: var(--matte-muted);
                    padding: 8px 16px;
                    border-radius: 999px;
                    cursor: pointer;
                    font-size: 13px;
                }}
                .orders {{
                    margin-top: 28px;
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
                    gap: 22px;
                }}
                .order-card {{
                    border: 1px solid var(--matte-border);
                    border-radius: 18px;
                    padding: 20px 22px 22px;
                    background: rgba(10, 16, 13, 0.88);
                    display: grid;
                    gap: 16px;
                    cursor: pointer;
                    transition: border 0.2s ease, box-shadow 0.2s ease;
                }}
                .order-card:hover {{
                    border-color: rgba(92, 200, 255, 0.45);
                    box-shadow: 0 16px 26px rgba(0, 0, 0, 0.32);
                }}
                .order-card.new-order {{
                    border-color: rgba(57, 255, 136, 0.4);
                    box-shadow: 0 0 14px rgba(57, 255, 136, 0.18);
                }}
                .order-card.new-flash {{
                    box-shadow: 0 0 22px rgba(57, 255, 136, 0.25);
                }}
                .order-header {{
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: space-between;
                    align-items: center;
                    gap: 12px;
                }}
                .order-header-actions {{
                    display: inline-flex;
                    align-items: center;
                    gap: 12px;
                }}
                .order-number {{
                    font-size: 18px;
                    font-weight: 600;
                }}
                .order-primary {{
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }}
                .order-meta-primary {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 14px 18px;
                    font-size: 13px;
                    color: var(--matte-muted);
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
                    color: #dff6ea;
                    border-color: rgba(223, 246, 234, 0.35);
                }}
                .status-paid {{
                    color: #ecf7ff;
                    border-color: rgba(236, 247, 255, 0.4);
                }}
                .status-cdek {{
                    color: #f5f5f5;
                    border-color: rgba(245, 245, 245, 0.35);
                }}
                .status-warning {{
                    color: #f1e3b1;
                    border-color: rgba(241, 227, 177, 0.4);
                }}
                .status-error {{
                    color: #f1b8b8;
                    border-color: rgba(241, 184, 184, 0.45);
                }}
                .order-meta {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                    gap: 14px 18px;
                    font-size: 13px;
                    color: var(--matte-muted);
                }}
                .order-summary {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 12px 18px;
                    font-size: 13px;
                    color: var(--matte-muted);
                }}
                .order-details {{
                    display: none;
                    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                    gap: 14px 18px;
                    font-size: 13px;
                    color: var(--matte-muted);
                    border-top: 1px dashed rgba(247, 247, 245, 0.08);
                    padding-top: 12px;
                }}
                .order-card.expanded .order-details {{
                    display: grid;
                }}
                .order-toggle {{
                    font-size: 11px;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                    color: rgba(247, 247, 245, 0.55);
                }}
                .meta-item {{
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                }}
                .meta-label {{
                    font-size: 11px;
                    text-transform: uppercase;
                    letter-spacing: 0.08em;
                    color: rgba(247, 247, 245, 0.55);
                }}
                .meta-value {{
                    font-size: 13px;
                    color: var(--matte-white);
                }}
                .order-notes {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                    gap: 16px 20px;
                }}
                .order-actions {{
                    display: flex;
                    justify-content: flex-end;
                }}
                .order-link {{
                    border: 1px solid rgba(92, 200, 255, 0.4);
                    background: rgba(7, 15, 19, 0.6);
                    color: var(--matte-white);
                    padding: 8px 16px;
                    border-radius: 999px;
                    font-size: 13px;
                    text-decoration: none;
                }}
                .glitter {{
                    position: fixed;
                    inset: 0;
                    pointer-events: none;
                    background-image:
                        radial-gradient(circle at 20% 30%, rgba(247, 247, 245, 0.08) 0, transparent 2px),
                        radial-gradient(circle at 70% 20%, rgba(247, 247, 245, 0.06) 0, transparent 2px),
                        radial-gradient(circle at 40% 80%, rgba(247, 247, 245, 0.06) 0, transparent 2px),
                        radial-gradient(circle at 80% 60%, rgba(247, 247, 245, 0.05) 0, transparent 2px);
                    opacity: 0.16;
                    mix-blend-mode: screen;
                    animation: glitter 3s ease-in-out infinite;
                }}
                .glitter::after {{
                    content: "";
                    position: absolute;
                    inset: 0;
                    background-image:
                        radial-gradient(circle at 10% 50%, rgba(247, 247, 245, 0.06) 0, transparent 2px),
                        radial-gradient(circle at 90% 40%, rgba(247, 247, 245, 0.06) 0, transparent 2px);
                    opacity: 0.3;
                    animation: glitter 4s ease-in-out infinite reverse;
                }}
                .empty-state {{
                    padding: 24px;
                    border-radius: 16px;
                    border: 1px dashed rgba(1, 50, 32, 0.35);
                    text-align: center;
                    color: var(--matte-muted);
                }}
                .warning {{
                    margin-top: 24px;
                    padding: 14px 18px;
                    border-radius: 14px;
                    border: 1px solid rgba(243, 227, 176, 0.45);
                    background: rgba(15, 21, 18, 0.7);
                    color: #f1e3b1;
                    font-size: 13px;
                }}
                .warning strong {{
                    color: #f6efd5;
                }}
                .stale {{
                    color: #f1e3b1;
                }}
                @keyframes blink {{
                    0%, 100% {{ opacity: 1; }}
                    50% {{ opacity: 0.3; }}
                }}
                @keyframes glitter {{
                    0% {{ opacity: 0.25; }}
                    50% {{ opacity: 0.5; }}
                    100% {{ opacity: 0.25; }}
                }}
                @media (max-width: 768px) {{
                    .container {{
                        padding: 24px 16px 48px;
                    }}
                    .hero {{
                        grid-template-columns: 1fr;
                    }}
                    .order-actions {{
                        justify-content: flex-start;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="glitter" aria-hidden="true"></div>
            <div class="container">
                <section class="hero">
                    <div class="hero-panel">
                        <div class="hero-eyebrow">Live Ops</div>
                        <h1>CASHER OPS DASHBOARD</h1>
                        <div class="subtitle">
                            Операционный контроль заказов CASHER в реальном времени: новые заявки,
                            статусы и визуальный контроль.
                        </div>
                        <div class="note">Данные загружаются максимум за последние 7 дней.</div>
                    </div>
                    <div class="hero-panel">
                        <div class="meta">
                            <span>Обновлено: <strong id="updated-at">{escape(updated_text)}</strong></span>
                            <span class="status-pill">Статус: <strong id="status-text">{escape(status_text)}</strong></span>
                            <button class="refresh-button" id="refresh-button" type="button">Обновить</button>
                        </div>
                    </div>
                </section>
                <div class="grid kpi-grid">
                    <button class="card" type="button" id="kpi-new-orders">
                        <div class="value" id="new-orders-count">{new_orders}</div>
                        <div class="label">НОВЫЕ ЗАКАЗЫ</div>
                    </button>
                    <button class="card" type="button" id="kpi-cdek-orders">
                        <div class="value" id="cdek-orders-count">{cdek_orders}</div>
                        <div class="label">ОТПРАВЛЕНО СДЕК</div>
                    </button>
                </div>
                <div class="sales-section">
                    <div class="grid compact">
                        <div class="card" id="daily-sales-card">
                            <div class="label">Выручка за сутки</div>
                            <div class="value small" id="daily-sales-sum">0 руб.</div>
                            <div class="value secondary" id="daily-sales-count">0 заказов</div>
                            <div class="meta-hint">Сумма и количество продаж</div>
                        </div>
                        <div class="card" id="weekly-sales-card">
                            <div class="label">Выручка за 7 дней</div>
                            <div class="value small" id="weekly-sales-sum">0 руб.</div>
                            <div class="value secondary" id="weekly-sales-count">0 заказов</div>
                            <div class="meta-hint">Сумма и количество продаж</div>
                        </div>
                    </div>
                    <div class="chart-card">
                        <div class="chart-header">
                            <div class="chart-title">Продажи за неделю</div>
                            <div class="chart-meta" id="weekly-sales-meta">Последние 7 дней</div>
                        </div>
                        <div class="weekly-chart" id="weekly-sales-chart"></div>
                    </div>
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
                const dailySalesSum = document.getElementById('daily-sales-sum');
                const dailySalesCount = document.getElementById('daily-sales-count');
                const weeklySalesSum = document.getElementById('weekly-sales-sum');
                const weeklySalesCount = document.getElementById('weekly-sales-count');
                const weeklySalesChart = document.getElementById('weekly-sales-chart');
                const weeklySalesMeta = document.getElementById('weekly-sales-meta');
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
                    if (value.includes('.')) return value;
                    const match = value.match(/(\\d{{4}})-(\\d{{2}})-(\\d{{2}})/);
                    if (match) {{
                        return `${{match[3]}}.${{match[2]}}.${{match[1]}}`;
                    }}
                    return value;
                }};

                const formatMoney = (value) => {{
                    const formatter = new Intl.NumberFormat('ru-RU', {{
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                    }});
                    return `${{formatter.format(value)}} руб.`;
                }};

                const formatOrderCount = (value) => {{
                    const formatter = new Intl.NumberFormat('ru-RU');
                    const mod10 = value % 10;
                    const mod100 = value % 100;
                    let suffix = 'заказов';
                    if (mod10 === 1 && mod100 !== 11) {{
                        suffix = 'заказ';
                    }} else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) {{
                        suffix = 'заказа';
                    }}
                    return `${{formatter.format(value)}} ${{suffix}}`;
                }};

                const formatDayLabel = (ms) => {{
                    return new Date(ms).toLocaleDateString('ru-RU', {{
                        day: '2-digit',
                        month: '2-digit',
                        timeZone: 'Europe/Moscow',
                    }});
                }};

                const getMskNowMs = () => {{
                    const localNow = new Date();
                    const utcMs = localNow.getTime() + localNow.getTimezoneOffset() * 60000;
                    return utcMs + 3 * 60 * 60 * 1000;
                }};

                const getMskTodayStartMs = () => {{
                    const nowMs =
                        currentPayload?.server_msk_now_ms && !currentPayload?.stale
                            ? currentPayload.server_msk_now_ms
                            : getMskNowMs();
                    const mskDate = new Date(nowMs);
                    mskDate.setUTCHours(0, 0, 0, 0);
                    return mskDate.getTime();
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
                    return orders.filter((order) => {{
                        if (!order.moment_ms) return false;
                        const orderTime = order.moment_ms;
                        const nowTime = currentPayload.server_msk_now_ms;
                        const todayStart = getMskTodayStartMs();
                        if (activeFilters.period === 'today') {{
                            return todayStart ? orderTime >= todayStart : true;
                        }}
                        if (!nowTime) return true;
                        const diffDays = (nowTime - orderTime) / (1000 * 60 * 60 * 24);
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

                const buildWeeklySeries = (orders) => {{
                    const dayMs = 24 * 60 * 60 * 1000;
                    const todayStart = getMskTodayStartMs();
                    const startMs = todayStart - 6 * dayMs;
                    const series = Array.from({{ length: 7 }}, (_, index) => {{
                        const dayStart = startMs + index * dayMs;
                        return {{
                            label: formatDayLabel(dayStart),
                            sum: 0,
                            count: 0,
                        }};
                    }});
                    orders.forEach((order) => {{
                        const orderTime = order.moment_ms;
                        if (!orderTime) return;
                        const index = Math.floor((orderTime - startMs) / dayMs);
                        if (index < 0 || index > 6) return;
                        const sum = typeof order.sum === 'number' ? order.sum : 0;
                        series[index].sum += sum;
                        series[index].count += 1;
                    }});
                    return series;
                }};

                const renderWeeklyChart = (series) => {{
                    if (!weeklySalesChart) return;
                    if (!series.length) {{
                        weeklySalesChart.innerHTML = '<div class="empty-state">Нет данных для графика</div>';
                        return;
                    }}
                    const maxSum = Math.max(...series.map((item) => item.sum));
                    weeklySalesChart.innerHTML = series
                        .map((item) => {{
                            const height = maxSum ? Math.round((item.sum / maxSum) * 100) : 0;
                            const sumRub = item.sum / 100;
                            return `
                                <div class="chart-bar">
                                    <div class="chart-bar-fill" style="height: ${{height}}%"></div>
                                    <div class="chart-bar-value">${{formatMoney(sumRub)}}</div>
                                    <div class="chart-bar-label">${{item.label}}</div>
                                </div>
                            `;
                        }})
                        .join('');
                }};

                const updateSalesStats = (payload) => {{
                    const orders = payload.orders || [];
                    const series = buildWeeklySeries(orders);
                    const daily = series[series.length - 1] || {{ sum: 0, count: 0 }};
                    const weekly = series.reduce(
                        (acc, item) => {{
                            acc.sum += item.sum;
                            acc.count += item.count;
                            return acc;
                        }},
                        {{ sum: 0, count: 0 }}
                    );
                    if (dailySalesSum) {{
                        dailySalesSum.textContent = formatMoney(daily.sum / 100);
                    }}
                    if (dailySalesCount) {{
                        dailySalesCount.textContent = formatOrderCount(daily.count);
                    }}
                    if (weeklySalesSum) {{
                        weeklySalesSum.textContent = formatMoney(weekly.sum / 100);
                    }}
                    if (weeklySalesCount) {{
                        weeklySalesCount.textContent = formatOrderCount(weekly.count);
                    }}
                    if (weeklySalesMeta) {{
                        weeklySalesMeta.textContent = `Всего ${{formatOrderCount(weekly.count)}}`;
                    }}
                    renderWeeklyChart(series);
                }};

                const renderOrders = (orders, highlightedIds = new Set()) => {{
                    if (!orders.length) {{
                        ordersList.innerHTML = '<div class="empty-state">Нет заказов по выбранным фильтрам</div>';
                        return;
                    }}
                    ordersList.innerHTML = orders.map((order) => {{
                        const statusClass = getStatusClass(order.state);
                        const isHighlighted = highlightedIds.has(order.id);
                        const isNew = isNewOrder(order.state);
                        return `
                            <div class="order-card ${{isNew ? 'new-order' : ''}} ${{isHighlighted ? 'new-flash' : ''}}" data-order-id="${{order.id || ''}}">
                                <div class="order-header">
                                    <div class="order-primary">
                                        <div class="order-number">${{order.name || 'без номера'}}</div>
                                        <div class="order-summary">
                                            <div class="meta-item">
                                                <span class="meta-label">Дата</span>
                                                <span class="meta-value">${{formatDate(order.moment)}}</span>
                                            </div>
                                            <div class="meta-item">
                                                <span class="meta-label">Город</span>
                                                <span class="meta-value">${{order.city || 'не указан'}}</span>
                                            </div>
                                            <div class="meta-item">
                                                <span class="meta-label">Доставка</span>
                                                <span class="meta-value">${{order.delivery_method || 'не указан'}}</span>
                                            </div>
                                            <div class="meta-item">
                                                <span class="meta-label">Сумма</span>
                                                <span class="meta-value">${{order.sum_display || 'не указана'}}</span>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="order-header-actions">
                                        <span class="order-toggle" data-order-toggle>Подробнее</span>
                                        <span class="status-badge ${{statusClass}}">${{order.state || 'не указан'}}</span>
                                    </div>
                                </div>
                                <div class="order-details">
                                    <div class="meta-item">
                                        <span class="meta-label">Получатель</span>
                                        <span class="meta-value">${{order.recipient || 'не указан'}}</span>
                                    </div>
                                    <div class="meta-item">
                                        <span class="meta-label">Телефон</span>
                                        <span class="meta-value">${{order.phone || 'не указан'}}</span>
                                    </div>
                                    <div class="meta-item">
                                        <span class="meta-label">Email</span>
                                        <span class="meta-value">${{order.email || 'не указан'}}</span>
                                    </div>
                                    <div class="meta-item">
                                        <span class="meta-label">Адрес</span>
                                        <span class="meta-value">${{order.address || 'не указан'}}</span>
                                    </div>
                                    <div class="meta-item">
                                        <span class="meta-label">Комментарий</span>
                                        <span class="meta-value">${{order.comment || 'не указан'}}</span>
                                    </div>
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
                            card.classList.toggle('expanded');
                            const toggle = card.querySelector('[data-order-toggle]');
                            if (toggle) {{
                                toggle.textContent = card.classList.contains('expanded') ? 'Скрыть' : 'Подробнее';
                            }}
                        }});
                    }});

                    if (highlightedIds.size) {{
                        setTimeout(() => {{
                            document.querySelectorAll('.order-card.new-flash').forEach((card) => {{
                                card.classList.remove('new-flash');
                            }});
                        }}, 450);
                    }}
                }};

                const applyFilters = (orders, highlightedIds) => {{
                    const filtered = filterByStatus(filterByPeriod(orders));
                    const sorted = filtered.sort((a, b) => {{
                        const aTime = a.moment_ms || 0;
                        const bTime = b.moment_ms || 0;
                        return bTime - aTime;
                    }});
                    renderOrders(sorted, highlightedIds);
                }};

                const updateKpi = (payload, previousNewCount = 0) => {{
                    const newCount = payload.stats?.new_orders ?? 0;
                    newOrdersCount.textContent = newCount;
                    cdekOrdersCount.textContent = payload.stats?.cdek_orders ?? 0;
                    if (payload.updated_at_display) {{
                        updatedAt.textContent = payload.updated_at_display;
                    }}
                    statusText.textContent = payload.stale ? 'Данные устарели' : 'Данные обновлены';
                    statusText.classList.toggle('stale', Boolean(payload.stale));

                    if (newCount > previousNewCount) {{
                        kpiNewOrders.classList.add('kpi-alert');
                        setTimeout(() => kpiNewOrders.classList.remove('kpi-alert'), 450);
                    }}
                }};

                const updateFromPayload = (payload) => {{
                    if (!payload || !payload.stats) return;
                    const previousNewCount = currentPayload.stats?.new_orders ?? 0;
                    updateKpi(payload, previousNewCount);
                    updateSalesStats(payload);

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
                        if (payload.updated_at_display) {{
                            updatedAt.textContent = payload.updated_at_display;
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
                key=lambda order: order.get("moment_ms") or 0,
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
    return JSONResponse(
        {
            "status": "ok",
            "updated_at": cache.get("updated_at") if cache else None,
            "updated_at_display": cache.get("updated_at_display") if cache else None,
        }
    )


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
