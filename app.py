import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timezone
from html import escape
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

import anyio
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse


app = FastAPI(title="MoySklad Telegram Notifier")


CACHE_PATH = "/data/orders_cache.json"
CACHE_TTL_SECONDS = 300

CACHE_LOCK = threading.Lock()
REFRESH_LOCK = asyncio.Lock()
SUBSCRIBERS_LOCK = asyncio.Lock()
SUBSCRIBERS: List[asyncio.Queue[str]] = []

logger = logging.getLogger("moysklad")
logging.basicConfig(level=logging.INFO)


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


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


def fetch_customer_orders(limit: int = 100) -> List[Dict[str, Any]]:
    headers = _moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    orders: List[Dict[str, Any]] = []
    offset = 0
    while True:
        response = requests.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder",
            headers=headers,
            params={"limit": limit, "offset": offset, "expand": "state"},
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
    bot_token = _get_env("TG_BOT_TOKEN")
    chat_id = _get_env("TG_CHAT_ID")

    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10,
    )
    response.raise_for_status()


def _serialize_order(order: Dict[str, Any]) -> Dict[str, Any]:
    state_name = _get_state_name(order)
    return {
        "id": order.get("id") or "",
        "name": order.get("name") or "без номера",
        "state": state_name,
        "moment": order.get("moment"),
        "sum": order.get("sum"),
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
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)


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


def _order_row_cached(order: Dict[str, Any]) -> Dict[str, str]:
    return {
        "name": escape(str(order.get("name") or "без номера")),
        "state": escape(str(order.get("state") or "не указан")),
        "moment": escape(_format_datetime(order.get("moment"))),
        "total": escape(_format_money(order.get("sum"))),
        "link": escape(str(order.get("link") or "нет")),
    }


def _render_table_rows(rows: List[Dict[str, str]]) -> str:
    if not rows:
        return """
        <tr class="empty-row">
            <td colspan="4">Нет заказов</td>
        </tr>
        """
    return "\n".join(
        f"""
        <tr>
            <td><a href="{row["link"]}" target="_blank" rel="noreferrer">{row["name"]}</a></td>
            <td>{row["state"]}</td>
            <td>{row["moment"]}</td>
            <td>{row["total"]}</td>
        </tr>
        """
        for row in rows
    )


def _render_landing_page(
    new_orders: int,
    cdek_orders: int,
    new_order_rows: List[Dict[str, str]],
    cdek_order_rows: List[Dict[str, str]],
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
    return f"""
    <!doctype html>
    <html lang="ru">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>Статистика заказов</title>
            <style>
                :root {{
                    color-scheme: light;
                    font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
                }}
                body {{
                    margin: 0;
                    background: #013220;
                    color: #c6c6c6;
                }}
                .container {{
                    max-width: 960px;
                    margin: 0 auto;
                    padding: 48px 24px 64px;
                }}
                h1 {{
                    font-size: 32px;
                    margin-bottom: 12px;
                }}
                .subtitle {{
                    color: #c6c6c6;
                    margin-bottom: 8px;
                }}
                .meta {{
                    display: flex;
                    flex-wrap: wrap;
                    align-items: center;
                    gap: 16px;
                    margin-bottom: 32px;
                    font-size: 14px;
                }}
                .meta span {{
                    display: inline-flex;
                    gap: 6px;
                    align-items: center;
                }}
                .refresh-button {{
                    border: none;
                    background: #c6c6c6;
                    color: #013220;
                    font-weight: 600;
                    padding: 10px 16px;
                    border-radius: 999px;
                    cursor: pointer;
                }}
                .refresh-button:disabled {{
                    opacity: 0.6;
                    cursor: progress;
                }}
                .grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                    gap: 24px;
                }}
                .card {{
                    background: #c6c6c6;
                    border-radius: 16px;
                    padding: 28px;
                    box-shadow: 0 10px 30px rgba(1, 50, 32, 0.4);
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    color: #013220;
                    cursor: pointer;
                    border: 2px solid transparent;
                }}
                .card:focus,
                .card:hover {{
                    border-color: #013220;
                }}
                .value {{
                    font-size: 48px;
                    font-weight: 700;
                }}
                .label {{
                    font-size: 14px;
                    letter-spacing: 0.12em;
                    text-transform: uppercase;
                    color: #013220;
                }}
                .tables {{
                    margin-top: 32px;
                    display: grid;
                    gap: 24px;
                }}
                .table-card {{
                    background: #c6c6c6;
                    border-radius: 16px;
                    padding: 20px;
                    color: #013220;
                    display: none;
                }}
                .table-card.active {{
                    display: block;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 12px;
                }}
                th,
                td {{
                    padding: 12px 10px;
                    text-align: left;
                    border-bottom: 1px solid rgba(1, 50, 32, 0.2);
                    font-size: 14px;
                }}
                th {{
                    text-transform: uppercase;
                    font-size: 12px;
                    letter-spacing: 0.08em;
                }}
                a {{
                    color: #013220;
                    font-weight: 600;
                    text-decoration: none;
                }}
                .empty-row td {{
                    text-align: center;
                    font-style: italic;
                }}
                .warning {{
                    margin-top: 24px;
                    padding: 16px 20px;
                    border-radius: 12px;
                    background: #ffd166;
                    color: #013220;
                    font-size: 14px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Лендинг статистики заказов</h1>
                <div class="subtitle">
                    Здесь отображаются новые заказы и заказы, собранные в СДЭК.
                </div>
                <div class="meta">
                    <span>Обновлено: <strong id="updated-at">{escape(updated_text)}</strong></span>
                    <span>Статус: <strong id="status-text">{escape(status_text)}</strong></span>
                    <button class="refresh-button" id="refresh-button" type="button">Обновить</button>
                </div>
                <div class="grid">
                    <button class="card" type="button" data-target="new-orders-table">
                        <div class="value" id="new-orders-count">{new_orders}</div>
                        <div class="label">НОВЫЕ ЗАКАЗЫ</div>
                    </button>
                    <button class="card" type="button" data-target="cdek-orders-table">
                        <div class="value" id="cdek-orders-count">{cdek_orders}</div>
                        <div class="label">ОТПРАВЛЕНО СДЕК</div>
                    </button>
                </div>
                <div class="tables">
                    <div class="table-card" id="new-orders-table">
                        <h2>Новые заказы</h2>
                        <table>
                            <thead>
                                <tr>
                                    <th>Заказ</th>
                                    <th>Статус</th>
                                    <th>Дата</th>
                                    <th>Сумма</th>
                                </tr>
                            </thead>
                            <tbody id="new-orders-body">
                                {_render_table_rows(new_order_rows)}
                            </tbody>
                        </table>
                    </div>
                    <div class="table-card" id="cdek-orders-table">
                        <h2>Отправлено СДЭК</h2>
                        <table>
                            <thead>
                                <tr>
                                    <th>Заказ</th>
                                    <th>Статус</th>
                                    <th>Дата</th>
                                    <th>Сумма</th>
                                </tr>
                            </thead>
                            <tbody id="cdek-orders-body">
                                {_render_table_rows(cdek_order_rows)}
                            </tbody>
                        </table>
                    </div>
                </div>
                {warning_block}
                {empty_block}
            </div>
            <script>
                const cards = document.querySelectorAll('[data-target]');
                const refreshButton = document.getElementById('refresh-button');
                const statusText = document.getElementById('status-text');
                const updatedAt = document.getElementById('updated-at');
                const newOrdersCount = document.getElementById('new-orders-count');
                const cdekOrdersCount = document.getElementById('cdek-orders-count');
                const newOrdersBody = document.getElementById('new-orders-body');
                const cdekOrdersBody = document.getElementById('cdek-orders-body');

                const renderRows = (rows) => {{
                    if (!rows.length) {{
                        return '<tr class="empty-row"><td colspan="4">Нет заказов</td></tr>';
                    }}
                    return rows.map((row) => `
                        <tr>
                            <td><a href="${{row.link}}" target="_blank" rel="noreferrer">${{row.name}}</a></td>
                            <td>${{row.state}}</td>
                            <td>${{row.moment}}</td>
                            <td>${{row.total}}</td>
                        </tr>
                    `).join('');
                }};

                const updateFromPayload = (payload) => {{
                    if (!payload || !payload.stats) return;
                    newOrdersCount.textContent = payload.stats.new_orders ?? 0;
                    cdekOrdersCount.textContent = payload.stats.cdek_orders ?? 0;
                    if (payload.updated_at) {{
                        updatedAt.textContent = payload.updated_at.replace('T', ' ').split('.')[0];
                    }}
                    statusText.textContent = payload.stale ? 'Данные устарели' : 'Данные обновлены';
                    const orders = payload.orders || [];
                    const newOrders = orders.filter((order) => {{
                        const state = (order.state || '').toLowerCase();
                        return state.includes('сдек') === false && (
                            state.includes('нов') ||
                            state.includes('принят') ||
                            state.includes('оплачен') ||
                            state.includes('обработ')
                        );
                    }});
                    const cdekOrders = orders.filter((order) => (order.state || '').toLowerCase().includes('сдек'));
                    newOrdersBody.innerHTML = renderRows(newOrders.map((order) => ({{
                        name: order.name || 'без номера',
                        state: order.state || 'не указан',
                        moment: order.moment ? order.moment.replace('T', ' ').split('.')[0] : 'не указана',
                        total: order.sum ? (order.sum / 100).toFixed(2) : 'не указана',
                        link: order.link || '#',
                    }})));
                    cdekOrdersBody.innerHTML = renderRows(cdekOrders.map((order) => ({{
                        name: order.name || 'без номера',
                        state: order.state || 'не указан',
                        moment: order.moment ? order.moment.replace('T', ' ').split('.')[0] : 'не указана',
                        total: order.sum ? (order.sum / 100).toFixed(2) : 'не указана',
                        link: order.link || '#',
                    }})));
                }};

                cards.forEach((card) => {{
                    card.addEventListener('click', () => {{
                        const targetId = card.getAttribute('data-target');
                        if (!targetId) return;
                        const target = document.getElementById(targetId);
                        if (!target) return;
                        const isActive = target.classList.contains('active');
                        document.querySelectorAll('.table-card').forEach((table) => {{
                            table.classList.remove('active');
                        }});
                        if (!isActive) {{
                            target.classList.add('active');
                        }}
                    }});
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
    asyncio.create_task(auto_refresh_loop())


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def landing() -> HTMLResponse:
    new_order_rows: List[Dict[str, str]] = []
    cdek_order_rows: List[Dict[str, str]] = []
    counts = {"new_orders": 0, "cdek_orders": 0}
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
            orders = cache.get("orders", [])
            new_orders = [
                _order_row_cached(order)
                for order in orders
                if is_new_order(str(order.get("state") or ""))
                and not is_cdek_state(str(order.get("state") or ""))
            ]
            cdek_orders = [
                _order_row_cached(order)
                for order in orders
                if is_cdek_state(str(order.get("state") or ""))
            ]
            new_order_rows = sorted(new_orders, key=lambda row: row["moment"], reverse=True)
            cdek_order_rows = sorted(cdek_orders, key=lambda row: row["moment"], reverse=True)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read cache: %s", exc)

    html = _render_landing_page(
        new_orders=counts["new_orders"],
        cdek_orders=counts["cdek_orders"],
        new_order_rows=new_order_rows,
        cdek_order_rows=cdek_order_rows,
        updated_at=updated_at,
        stale=stale,
        has_cache=has_cache,
    )
    return HTMLResponse(content=html, status_code=200)


@app.post("/refresh")
async def refresh() -> JSONResponse:
    cache = await refresh_cache("manual")
    if cache:
        return JSONResponse({"status": "ok", "updated_at": cache.get("updated_at")})
    return JSONResponse({"status": "error", "updated_at": None})


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
