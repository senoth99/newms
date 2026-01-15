import asyncio
import logging
import os
import threading
from html import escape
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional

import anyio
import orjson
import pendulum
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, ORJSONResponse, StreamingResponse
from pydantic import BaseModel, Field


app = FastAPI(title="MoySklad Telegram Notifier")

CACHE_PATH = "/tmp/orders_cache.json"
CACHE_TTL_SECONDS = 300
MSK_TZ = pendulum.timezone("Europe/Moscow")
EMPTY_VALUE = "—"

CACHE_LOCK = threading.Lock()
ENTITY_CACHE_LOCK = threading.Lock()
UPDATE_LOCK = asyncio.Lock()
SUBSCRIBERS_LOCK = asyncio.Lock()
SUBSCRIBERS: List[asyncio.Queue[str]] = []
ENTITY_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}

logger = logging.getLogger("moysklad")
logging.basicConfig(level=logging.INFO)


class OrderDTO(BaseModel):
    id: str
    name: str
    state: str
    moment: str
    moment_ms: int
    sum: int
    sum_display: str
    recipient: str
    phone: Optional[str]
    email: Optional[str]
    delivery_method: str
    city: str
    address: str
    comment: str
    link: str
    day_key: Optional[str]
    day_label: str

    class Config:
        frozen = True


def msk_now() -> pendulum.DateTime:
    return pendulum.now(MSK_TZ)


def parse_msk(value: Optional[Any]) -> Optional[pendulum.DateTime]:
    if not value:
        return None
    try:
        return pendulum.parse(str(value)).in_timezone(MSK_TZ)
    except Exception:
        return None


def format_msk(value: Optional[pendulum.DateTime]) -> str:
    if not value:
        return EMPTY_VALUE
    return value.format("YYYY-MM-DD HH:mm")


def format_money(value: Optional[int]) -> str:
    if value is None:
        return EMPTY_VALUE
    return f"{value / 100:,.2f}".replace(",", " ")


def msk_day_start(value: Optional[pendulum.DateTime] = None) -> pendulum.DateTime:
    return (value or msk_now()).start_of("day")


def msk_day_labels(days: int = 7) -> List[Dict[str, str]]:
    today = msk_day_start()
    start = today.subtract(days=days - 1)
    return [
        {"key": start.add(days=offset).format("YYYY-MM-DD"), "label": start.add(days=offset).format("DD.MM")}
        for offset in range(days)
    ]


def moysklad_headers() -> Dict[str, str]:
    token = os.getenv("MS_TOKEN")
    basic_token = os.getenv("MS_BASIC_TOKEN")
    if basic_token:
        logger.info("Using MoySklad Basic auth")
        return {"Authorization": f"Basic {basic_token}"}
    if token:
        logger.info("Using MoySklad Bearer token auth")
        return {"Authorization": f"Bearer {token}"}
    logger.warning("MoySklad auth token is missing")
    return {}


def store_href_from_env() -> Optional[str]:
    store_href = os.getenv("MS_STORE_HREF")
    if store_href:
        return store_href
    store_id = os.getenv("MS_STORE_ID")
    if not store_id:
        return None
    return f"https://api.moysklad.ru/api/remap/1.2/entity/store/{store_id}"


def order_matches_store(order: Dict[str, Any], store_href: Optional[str]) -> bool:
    if not store_href:
        return True
    store_info = order.get("store")
    if not isinstance(store_info, dict):
        logger.warning("Order %s has no store info; skipping strict store filter", order.get("id"))
        return True
    href = store_info.get("meta", {}).get("href") or store_info.get("href")
    if not href:
        logger.warning("Order %s store href missing; skipping strict store filter", order.get("id"))
        return True
    if href != store_href:
        logger.info("Order %s skipped by store filter", order.get("id"))
        return False
    return True


def fetch_entity(href: str) -> Optional[Dict[str, Any]]:
    if not href:
        return None
    with ENTITY_CACHE_LOCK:
        if href in ENTITY_CACHE:
            return ENTITY_CACHE[href]
    headers = moysklad_headers()
    if not headers:
        logger.error("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")
        with ENTITY_CACHE_LOCK:
            ENTITY_CACHE[href] = {}
        return {}
    logger.info("Fetching entity: %s", href)
    try:
        response = requests.get(href, headers=headers, timeout=10)
        if response.status_code in {401, 403}:
            response.raise_for_status()
        response.raise_for_status()
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as exc:
        logger.warning("Timeout fetching entity %s: %s", href, exc)
        with ENTITY_CACHE_LOCK:
            ENTITY_CACHE[href] = {}
        return {}
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error fetching entity %s: %s", href, exc)
        with ENTITY_CACHE_LOCK:
            ENTITY_CACHE[href] = {}
        return {}
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to fetch entity %s: %s", href, exc)
        with ENTITY_CACHE_LOCK:
            ENTITY_CACHE[href] = {}
        return {}
    data = response.json()
    with ENTITY_CACHE_LOCK:
        ENTITY_CACHE[href] = data
    return data


def fetch_order_positions(href: str) -> List[Dict[str, Any]]:
    headers = moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")
    logger.info("Fetching order positions: %s", href)
    try:
        response = requests.get(href, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("rows", [])
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as exc:
        logger.warning("Timeout fetching positions %s: %s", href, exc)
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error fetching positions %s: %s", href, exc)
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to fetch positions %s: %s", href, exc)
    return []


def fetch_customer_orders(limit: int = 100, max_days: int = 7) -> List[Dict[str, Any]]:
    headers = moysklad_headers()
    if not headers:
        raise RuntimeError("Missing MS_TOKEN or MS_BASIC_TOKEN for MoySklad API access")

    since = msk_now().subtract(days=max_days)
    filter_since = f"moment>={since.format('YYYY-MM-DD HH:mm:ss')}"
    store_href = store_href_from_env()
    logger.info(
        "Fetching customer orders: days=%s filter=%s store=%s",
        max_days,
        filter_since,
        store_href or "none",
    )

    orders: List[Dict[str, Any]] = []
    offset = 0
    while True:
        try:
            response = requests.get(
                "https://api.moysklad.ru/api/remap/1.2/entity/customerorder",
                headers=headers,
                params={
                    "limit": limit,
                    "offset": offset,
                    "expand": "state,store",
                    "filter": filter_since,
                },
                timeout=10,
            )
            response.raise_for_status()
            rows = response.json().get("rows", [])
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as exc:
            logger.warning("Timeout fetching orders (offset=%s): %s", offset, exc)
            break
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error fetching orders (offset=%s): %s", offset, exc)
            break
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to fetch orders (offset=%s): %s", offset, exc)
            break
        logger.info("Fetched %s orders (offset=%s)", len(rows), offset)
        if store_href:
            rows = [order for order in rows if order_matches_store(order, store_href)]
        orders.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
    logger.info("Total orders collected: %s", len(orders))
    return orders


def normalize_text(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def attribute_value(order: Dict[str, Any], attribute_name: str) -> Optional[Any]:
    attributes = order.get("attributes", [])
    if not isinstance(attributes, list):
        return None
    name_normalized = attribute_name.casefold()
    for attribute in attributes:
        if attribute.get("name", "").casefold() == name_normalized:
            return attribute.get("value")
    return None


def as_dict(value: Optional[Any]) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def attribute_first(order: Dict[str, Any], *attribute_names: str) -> Optional[Any]:
    for name in attribute_names:
        value = attribute_value(order, name)
        if value:
            return value
    return None


def extract_city(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    candidate = address.split(",")[0].strip()
    return candidate or None


def compose_shipment_address(shipment_full: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(shipment_full, dict):
        return None
    address = normalize_text(shipment_full.get("address"))
    if address:
        return address
    parts: List[str] = []
    for key in ("postalCode", "country", "region", "city"):
        value = normalize_text(shipment_full.get(key))
        if value:
            parts.append(value)
    street = normalize_text(shipment_full.get("street"))
    house = normalize_text(shipment_full.get("house"))
    apartment = normalize_text(shipment_full.get("apartment"))
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
        value = normalize_text(shipment_full.get(key))
        if value:
            parts.append(value)
    if not parts:
        return None
    return ", ".join(parts)


def extract_delivery_method(value: Optional[Any]) -> Optional[str]:
    if isinstance(value, dict):
        return normalize_text(value.get("name"))
    return normalize_text(value)


def first_non_empty(*values: Optional[Any]) -> Optional[str]:
    for value in values:
        normalized = normalize_text(value)
        if normalized:
            return normalized
    return None


def get_state_name(order: Dict[str, Any]) -> str:
    state_info = as_dict(order.get("state"))
    state = state_info.get("name")
    if not state:
        state_href = state_info.get("meta", {}).get("href")
        if state_href:
            state_data = fetch_entity(state_href)
            if state_data:
                state = state_data.get("name")
    return state or EMPTY_VALUE


def get_agent_details(order: Dict[str, Any]) -> Optional[Dict[str, Optional[str]]]:
    agent_info = as_dict(order.get("agent"))
    agent = agent_info.get("name")
    agent_phone = agent_info.get("phone")
    agent_email = agent_info.get("email")
    agent_href = agent_info.get("meta", {}).get("href")
    if agent_href and (not agent or not agent_phone or not agent_email):
        agent_details = fetch_entity(agent_href)
        if agent_details:
            agent = agent or agent_details.get("name")
            agent_phone = agent_phone or agent_details.get("phone")
            agent_email = agent_email or agent_details.get("email")
    if not agent and not agent_phone and not agent_email:
        return None
    return {
        "agent": agent,
        "agent_phone": agent_phone,
        "agent_email": agent_email,
    }


def order_link(order: Dict[str, Any]) -> str:
    order_id = order.get("id")
    if order_id:
        return f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"
    return order.get("meta", {}).get("href") or EMPTY_VALUE


def build_order_dto(order: Dict[str, Any]) -> OrderDTO:
    agent_details = get_agent_details(order)
    shipment_full_data = as_dict(order.get("shipmentAddressFull"))
    delivery_address_attribute = normalize_text(attribute_value(order, "адрес доставки"))

    if agent_details is None:
        recipient = "Неизвестно"
        phone = None
        email = None
    else:
        recipient = first_non_empty(
            shipment_full_data.get("recipient"),
            attribute_value(order, "получатель"),
            order.get("recipient"),
            agent_details.get("agent"),
        ) or EMPTY_VALUE

        phone = first_non_empty(
            shipment_full_data.get("phone"),
            attribute_value(order, "телефон"),
            order.get("phone"),
            agent_details.get("agent_phone"),
        )

        email = first_non_empty(
            shipment_full_data.get("email"),
            attribute_value(order, "email"),
            order.get("email"),
            agent_details.get("agent_email"),
        )

    address = first_non_empty(
        compose_shipment_address(shipment_full_data),
        delivery_address_attribute,
        attribute_first(
            order,
            "адрес",
            "адрес доставки",
            "адрес получателя",
            "адрес доставки получателя",
            "address",
        ),
        order.get("shipmentAddress"),
        order.get("address"),
    ) or EMPTY_VALUE

    city = first_non_empty(
        shipment_full_data.get("city"),
        shipment_full_data.get("settlement"),
        shipment_full_data.get("region"),
        extract_city(normalize_text(shipment_full_data.get("address"))),
        extract_city(delivery_address_attribute),
        attribute_first(
            order,
            "город",
            "город доставки",
            "населенный пункт",
            "city",
        ),
        extract_city(normalize_text(order.get("shipmentAddress"))),
    ) or EMPTY_VALUE

    delivery_method = first_non_empty(
        extract_delivery_method(shipment_full_data.get("deliveryService")),
        extract_delivery_method(shipment_full_data.get("shipmentMethod")),
        attribute_value(order, "способ доставки"),
        order.get("deliveryMethod"),
        order.get("shipmentMethod"),
    ) or EMPTY_VALUE

    comment = first_non_empty(
        shipment_full_data.get("comment"),
        shipment_full_data.get("addInfo"),
        attribute_value(order, "комментарий"),
        order.get("description"),
    ) or EMPTY_VALUE

    moment_raw = order.get("moment")
    moment_dt = parse_msk(moment_raw)
    day_key = moment_dt.format("YYYY-MM-DD") if moment_dt else None
    day_label = moment_dt.format("DD.MM") if moment_dt else EMPTY_VALUE

    sum_value = order.get("sum")

    return OrderDTO(
        id=str(order.get("id") or ""),
        name=order.get("name") or EMPTY_VALUE,
        state=get_state_name(order),
        moment=format_msk(moment_dt),
        moment_ms=int(moment_dt.int_timestamp * 1000) if moment_dt else 0,
        sum=int(sum_value) if isinstance(sum_value, (int, float)) else 0,
        sum_display=f"{format_money(sum_value)} руб.",
        recipient=recipient,
        phone=phone,
        email=email,
        delivery_method=delivery_method,
        city=city,
        address=address,
        comment=comment,
        link=order_link(order),
        day_key=day_key,
        day_label=day_label,
    )


def is_cdek_state(state: str) -> bool:
    return "сдек" in state.casefold()


def is_new_order(state: str) -> bool:
    if not state or state == EMPTY_VALUE:
        return True
    state_value = state.casefold()
    return any(word in state_value for word in ["нов", "принят", "оплачен", "обработ"]) and "сдек" not in state_value


def format_positions(positions: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for position in positions:
        assortment = position.get("assortment", {})
        name = assortment.get("name")
        if not name:
            assortment_href = assortment.get("meta", {}).get("href")
            if assortment_href:
                assortment_data = fetch_entity(assortment_href)
                if assortment_data:
                    name = assortment_data.get("name")
        name = name or "Товар"
        quantity = position.get("quantity") or 0
        if isinstance(quantity, float) and quantity.is_integer():
            quantity = int(quantity)
        price = format_money(position.get("price"))
        lines.append(f"{name} - {quantity} шт. - {price} руб.")
    if not lines:
        return "нет позиций"
    return "\n".join(lines)


def build_message(order: Dict[str, Any]) -> str:
    dto = build_order_dto(order)
    phone_display = dto.phone or EMPTY_VALUE
    email_display = dto.email or EMPTY_VALUE
    delivery_link = attribute_value(order, "ссылка на доставку") or EMPTY_VALUE
    track_number = attribute_value(order, "трек-номер") or EMPTY_VALUE

    positions_meta = order.get("positions", {}).get("meta", {}).get("href")
    positions = order.get("positions", {}).get("rows") or []
    if positions_meta and not positions:
        positions = fetch_order_positions(positions_meta)
    positions_text = format_positions(positions)

    return (
        f"📦 {dto.state}\n"
        f"ID заказа: {dto.name}\n\n"
        f"👤 Получатель: {dto.recipient}\n"
        f"📞 Номер телефона: {phone_display}\n"
        f"📧 Email: {email_display}\n"
        f"Способ доставки: {dto.delivery_method}\n\n"
        f"🏠 Адрес доставки: {dto.address}\n"
        f"Ссылка на доставку: {delivery_link}\n"
        f"Трек-номер: {track_number}\n\n"
        "Состав заказа:\n"
        f"{positions_text}\n\n"
        f"Сумма заказа: {dto.sum_display}\n\n"
        f"Комментарий: {dto.comment}\n"
        f"Создан: {dto.moment}\n"
        f"Ссылка: {dto.link}"
    )


def build_cdek_message(order: Dict[str, Any]) -> str:
    dto = build_order_dto(order)
    phone_display = dto.phone or EMPTY_VALUE
    delivery_link = attribute_value(order, "ссылка на доставку") or EMPTY_VALUE
    track_number = attribute_value(order, "трек-номер") or EMPTY_VALUE

    return (
        f"🚚 {dto.state}\n"
        f"ID заказа: {dto.name}\n\n"
        f"👤 Получатель: {dto.recipient}\n"
        f"📞 Номер телефона: {phone_display}\n"
        f"🏠 Адрес доставки: {dto.address}\n"
        f"Ссылка на доставку: {delivery_link}\n"
        f"Трек-номер: {track_number}\n"
        f"Ссылка: {dto.link}"
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


def serialize_order(dto: OrderDTO) -> Dict[str, Any]:
    return dto.model_dump()


def order_moment_ms(order: Dict[str, Any]) -> int:
    raw = order.get("moment_ms")
    if isinstance(raw, (int, float)):
        return int(raw)
    moment = parse_msk(order.get("moment"))
    if moment:
        return int(moment.int_timestamp * 1000)
    day_key = order.get("day_key")
    if day_key:
        parsed = parse_msk(f"{day_key} 00:00:00")
        if parsed:
            return int(parsed.int_timestamp * 1000)
    return 0


def weekly_sales_stats(orders: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    week_start = msk_day_start().subtract(days=6)
    week_start_ms = int(week_start.int_timestamp * 1000)
    stats = {
        "new_orders": {"count": 0, "sum": 0},
        "cdek_orders": {"count": 0, "sum": 0},
    }
    for order in orders:
        moment_ms = order_moment_ms(order)
        if moment_ms and moment_ms < week_start_ms:
            continue
        state = str(order.get("state") or "")
        sum_value = order.get("sum")
        sum_amount = int(sum_value) if isinstance(sum_value, (int, float)) else 0
        if is_cdek_state(state):
            stats["cdek_orders"]["count"] += 1
            stats["cdek_orders"]["sum"] += sum_amount
        elif is_new_order(state):
            stats["new_orders"]["count"] += 1
            stats["new_orders"]["sum"] += sum_amount
    return stats


def stats_from_orders(orders: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    stats = {"new_orders": 0, "cdek_orders": 0, "total_orders": 0}
    for order in orders:
        stats["total_orders"] += 1
        state = str(order.get("state") or "")
        if is_cdek_state(state):
            stats["cdek_orders"] += 1
        elif is_new_order(state):
            stats["new_orders"] += 1
    stats["weekly_sales"] = weekly_sales_stats(orders)
    return stats


def cache_payload(orders: List[Dict[str, Any]], updated_at: Optional[str] = None) -> Dict[str, Any]:
    updated = updated_at or format_msk(msk_now())
    return {
        "updated_at": updated,
        "stats": stats_from_orders(orders),
        "orders": orders,
    }


def cache_is_valid(cache: Optional[Dict[str, Any]]) -> bool:
    return isinstance(cache, dict) and "orders" in cache


def ensure_cache_dir() -> None:
    cache_dir = os.path.dirname(CACHE_PATH)
    if not cache_dir:
        return
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError as exc:
        logger.warning("Failed to ensure cache directory %s: %s", cache_dir, exc)


def load_cache_unlocked() -> Optional[Dict[str, Any]]:
    try:
        with open(CACHE_PATH, "rb") as handle:
            return orjson.loads(handle.read())
    except FileNotFoundError:
        return None
    except orjson.JSONDecodeError as exc:
        logger.warning("Failed to decode cache: %s", exc)
        return None


def write_cache_unlocked(cache: Dict[str, Any]) -> None:
    ensure_cache_dir()
    cache_dir = os.path.dirname(CACHE_PATH)
    with NamedTemporaryFile("wb", delete=False, dir=cache_dir) as handle:
        handle.write(orjson.dumps(cache))
        handle.flush()
        os.fsync(handle.fileno())
        temp_name = handle.name
    os.replace(temp_name, CACHE_PATH)


def read_cache() -> Optional[Dict[str, Any]]:
    with CACHE_LOCK:
        cache = load_cache_unlocked()
    if cache_is_valid(cache):
        return cache
    return None


def write_cache(cache: Dict[str, Any]) -> None:
    with CACHE_LOCK:
        write_cache_unlocked(cache)


def dedupe_orders(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    for order in orders:
        order_id = str(order.get("id") or "")
        fallback_key = f"{order.get('name') or ''}-{order.get('moment') or ''}"
        key = order_id or fallback_key
        existing = seen.get(key)
        if not existing or (order.get("moment_ms", 0) > existing.get("moment_ms", 0)):
            seen[key] = order
    return list(seen.values())


def update_cache_with_order(order_payload: Dict[str, Any]) -> Dict[str, Any]:
    with CACHE_LOCK:
        cache = load_cache_unlocked() or cache_payload([])
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
        updated_orders = dedupe_orders(updated_orders)
        cache = cache_payload(updated_orders)
        write_cache_unlocked(cache)
    return cache


def build_cache_from_orders(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    serialized_orders: List[Dict[str, Any]] = []
    for order in orders:
        try:
            serialized_orders.append(serialize_order(build_order_dto(order)))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to serialize order %s: %s", order.get("id"), exc)
    logger.info("[CACHE] serialized_orders=%s total_orders=%s", len(serialized_orders), len(orders))
    if orders and not serialized_orders:
        logger.error("ALL orders failed serialization")
    serialized_orders = dedupe_orders(serialized_orders)
    return cache_payload(serialized_orders)


def cache_is_stale(cache: Dict[str, Any]) -> bool:
    updated_at = cache.get("updated_at")
    if not updated_at:
        return True
    parsed = None
    try:
        parsed = pendulum.from_format(str(updated_at), "YYYY-MM-DD HH:mm", tz=MSK_TZ)
    except (ValueError, pendulum.parsing.exceptions.ParserError):
        parsed = parse_msk(str(updated_at))
    if not parsed:
        return True
    return (msk_now() - parsed).total_seconds() > CACHE_TTL_SECONDS


def log_stats(cache: Dict[str, Any]) -> None:
    stats = cache.get("stats", {})
    logger.info(
        "[CACHE] orders=%s new=%s cdek=%s",
        len(cache.get("orders", [])),
        stats.get("new_orders"),
        stats.get("cdek_orders"),
    )
    logger.info(
        "[STATS] total=%s new=%s cdek=%s updated_at=%s",
        stats.get("total_orders"),
        stats.get("new_orders"),
        stats.get("cdek_orders"),
        cache.get("updated_at"),
    )


def event_payload_dict(cache: Dict[str, Any]) -> Dict[str, Any]:
    now = msk_now()
    return {
        "updated_at": cache.get("updated_at"),
        "server_msk_now_ms": int(now.int_timestamp * 1000),
        "server_msk_today_start_ms": int(msk_day_start(now).int_timestamp * 1000),
        "ttl_seconds": CACHE_TTL_SECONDS,
        "stats": cache.get("stats", {}),
        "orders": cache.get("orders", []),
        "stale": cache_is_stale(cache),
        "days": msk_day_labels(),
    }


def event_payload(cache: Dict[str, Any]) -> str:
    return orjson.dumps(event_payload_dict(cache)).decode("utf-8")


def safe_json_for_html(payload: Dict[str, Any]) -> str:
    return orjson.dumps(payload).decode("utf-8").replace("<", "\\u003c")


LANDING_TEMPLATE = """
<!doctype html>
<html lang="ru">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>CASHER OPS DASHBOARD</title>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link
            href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
            rel="stylesheet"
        />
        <style>
            :root {
                color-scheme: dark;
                font-family: "Inter", "Segoe UI", "Helvetica Neue", Arial, sans-serif;
                --matte-green: #00f5a0;
                --matte-black: #050706;
                --matte-surface: rgba(8, 14, 11, 0.92);
                --matte-surface-strong: rgba(12, 18, 15, 0.95);
                --matte-white: #f7f7f5;
                --matte-muted: rgba(247, 247, 245, 0.6);
                --matte-border: rgba(247, 247, 245, 0.08);
                --neon-green: #4cffb2;
                --silver: rgba(247, 247, 245, 0.75);
            }
            * {
                box-sizing: border-box;
            }
            body {
                margin: 0;
                background:
                    radial-gradient(circle at top left, rgba(0, 245, 160, 0.18) 0%, transparent 45%),
                    radial-gradient(circle at 80% 10%, rgba(0, 245, 160, 0.14) 0%, transparent 50%),
                    linear-gradient(140deg, #050806 0%, #0b130f 50%, #020403 100%);
                color: var(--matte-white);
                min-height: 100vh;
            }
            .container {
                max-width: 1320px;
                margin: 0 auto;
                padding: 32px 20px 80px;
            }
            .hero {
                display: grid;
                grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr);
                gap: 24px;
            }
            .hero-panel {
                background: var(--matte-surface);
                border: 1px solid var(--matte-border);
                border-radius: 20px;
                padding: 24px 28px;
                box-shadow: 0 20px 40px rgba(0, 0, 0, 0.45);
            }
            .hero-eyebrow {
                font-size: 11px;
                letter-spacing: 0.42em;
                text-transform: uppercase;
                color: var(--matte-muted);
            }
            h1 {
                font-size: clamp(24px, 3.5vw, 36px);
                margin: 10px 0 12px;
                letter-spacing: 0.14em;
                text-transform: uppercase;
            }
            .subtitle {
                color: var(--matte-muted);
                max-width: 640px;
            }
            .meta-row {
                display: flex;
                flex-wrap: wrap;
                gap: 14px 24px;
                align-items: center;
                font-size: 13px;
                color: var(--matte-muted);
            }
            .meta-row strong {
                color: var(--matte-white);
            }
            .status-pill {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 6px 12px;
                border-radius: 999px;
                border: 1px solid rgba(76, 255, 178, 0.35);
                background: rgba(7, 15, 19, 0.6);
            }
            .refresh-button {
                border: 1px solid rgba(76, 255, 178, 0.65);
                background: linear-gradient(120deg, rgba(76, 255, 178, 0.4), rgba(31, 68, 49, 0.7));
                color: #04110a;
                font-weight: 600;
                padding: 10px 18px;
                border-radius: 999px;
                cursor: pointer;
                transition: box-shadow 0.2s ease, transform 0.2s ease;
            }
            .refresh-button:disabled {
                opacity: 0.6;
                cursor: progress;
            }
            .refresh-button:not(:disabled):hover {
                box-shadow: 0 12px 24px rgba(76, 255, 178, 0.2);
                transform: translateY(-1px);
            }
            .store-button {
                border: 1px solid rgba(76, 255, 178, 0.3);
                color: var(--matte-white);
                padding: 10px 18px;
                border-radius: 999px;
                text-decoration: none;
                font-weight: 600;
                font-size: 13px;
                background: rgba(7, 15, 12, 0.6);
                transition: box-shadow 0.2s ease, transform 0.2s ease;
            }
            .store-button:hover {
                box-shadow: 0 12px 24px rgba(76, 255, 178, 0.2);
                transform: translateY(-1px);
            }
            .kpi-row {
                margin-top: 24px;
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                gap: 18px;
            }
            .weekly-row {
                margin-top: 18px;
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                gap: 18px;
            }
            .weekly-card {
                background: var(--matte-surface);
                border-radius: 18px;
                padding: 20px 24px;
                border: 1px solid var(--matte-border);
                display: grid;
                gap: 12px;
            }
            .weekly-title {
                font-size: 12px;
                letter-spacing: 0.2em;
                text-transform: uppercase;
                color: var(--matte-muted);
            }
            .weekly-metrics {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                gap: 12px;
            }
            .weekly-metric {
                background: rgba(7, 12, 9, 0.7);
                border-radius: 14px;
                padding: 12px 14px;
                border: 1px solid rgba(247, 247, 245, 0.08);
            }
            .weekly-label {
                font-size: 11px;
                color: var(--matte-muted);
                text-transform: uppercase;
                letter-spacing: 0.12em;
            }
            .weekly-value {
                margin-top: 6px;
                font-size: 20px;
                font-weight: 600;
                color: var(--matte-white);
            }
            .kpi-card {
                background: var(--matte-surface);
                border-radius: 18px;
                padding: 22px 26px;
                border: 1px solid var(--matte-border);
                display: flex;
                flex-direction: column;
                gap: 12px;
                transition: box-shadow 0.2s ease;
            }
            .kpi-card.neon {
                box-shadow: 0 0 24px rgba(76, 255, 178, 0.35);
            }
            .kpi-value {
                font-size: clamp(32px, 5vw, 44px);
                font-weight: 700;
                letter-spacing: 0.04em;
            }
            .kpi-label {
                font-size: 12px;
                letter-spacing: 0.24em;
                text-transform: uppercase;
                color: var(--matte-muted);
            }
            .filters {
                margin-top: 28px;
                display: flex;
                flex-wrap: wrap;
                gap: 16px;
                align-items: center;
            }
            .filter-group {
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                align-items: center;
                background: var(--matte-surface);
                border: 1px solid var(--matte-border);
                border-radius: 999px;
                padding: 8px 12px;
            }
            .filter-label {
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 0.16em;
                color: var(--matte-muted);
            }
            .filter-button {
                border: 1px solid var(--matte-border);
                background: var(--matte-surface-strong);
                color: var(--matte-white);
                padding: 8px 14px;
                border-radius: 999px;
                cursor: pointer;
                font-size: 13px;
            }
            .filter-button.active {
                border-color: rgba(76, 255, 178, 0.6);
            }
            .reset-button {
                border: 1px solid rgba(1, 50, 32, 0.4);
                color: var(--matte-muted);
                padding: 8px 16px;
                border-radius: 999px;
                cursor: pointer;
                font-size: 13px;
                background: transparent;
            }
            .list-wrapper {
                margin-top: 26px;
                background: var(--matte-surface);
                border-radius: 18px;
                border: 1px solid var(--matte-border);
                overflow: hidden;
            }
            .list-header,
            .order-row {
                display: grid;
                grid-template-columns: 140px 160px 140px 180px 150px 180px 160px 140px 1.4fr 140px 1fr 120px;
                align-items: center;
                column-gap: 18px;
                min-width: 0;
            }
            .list-header {
                padding: 16px 20px;
                font-size: 11px;
                letter-spacing: 0.2em;
                text-transform: uppercase;
                color: var(--matte-muted);
                background: rgba(8, 14, 11, 0.98);
                border-bottom: 1px solid var(--matte-border);
                position: sticky;
                top: 0;
                z-index: 2;
            }
            .orders {
                display: flex;
                flex-direction: column;
            }
            .order-card {
                border-bottom: 1px solid rgba(247, 247, 245, 0.06);
                background: rgba(7, 12, 9, 0.88);
            }
            .order-card:last-child {
                border-bottom: none;
            }
            .order-row {
                padding: 12px 20px;
                min-height: 72px;
                transition: background 0.2s ease, box-shadow 0.2s ease;
                cursor: pointer;
            }
            .order-row:hover {
                background: rgba(10, 18, 14, 0.92);
                box-shadow: inset 0 0 0 1px rgba(76, 255, 178, 0.12);
            }
            .order-row.new {
                box-shadow: inset 0 0 0 1px rgba(76, 255, 178, 0.2);
            }
            .order-cell {
                font-size: 13px;
                color: var(--silver);
                line-height: 1.4;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
                min-width: 0;
            }
            .order-cell.primary {
                color: var(--matte-white);
                font-weight: 600;
            }
            .order-cell.wrap {
                white-space: normal;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                word-break: break-word;
            }
            .order-cell.link a {
                color: var(--matte-green);
                text-decoration: none;
                font-weight: 600;
            }
            .order-detail {
                padding: 0 20px 16px;
                display: none;
            }
            .order-card.expanded .order-detail {
                display: block;
            }
            .order-detail-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 12px;
                background: rgba(7, 12, 9, 0.7);
                border-radius: 16px;
                border: 1px solid rgba(247, 247, 245, 0.08);
                padding: 16px;
            }
            .order-detail-item {
                display: grid;
                gap: 6px;
            }
            .order-detail-label {
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 0.12em;
                color: var(--matte-muted);
            }
            .order-detail-value {
                font-size: 13px;
                color: var(--matte-white);
                line-height: 1.4;
                word-break: break-word;
            }
            .status-badge {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 4px 10px;
                border-radius: 999px;
                border: 1px solid rgba(76, 255, 178, 0.3);
                color: var(--matte-white);
                font-size: 11px;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                max-width: 120px;
            }
            .status-cdek {
                border-color: rgba(247, 247, 245, 0.35);
            }
            .status-new {
                border-color: rgba(76, 255, 178, 0.55);
            }
            .chart-section {
                margin-top: 24px;
                background: var(--matte-surface);
                border-radius: 18px;
                border: 1px solid var(--matte-border);
                padding: 18px 22px 26px;
                position: relative;
            }
            .chart-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 12px;
            }
            .chart-title {
                font-size: 12px;
                letter-spacing: 0.2em;
                text-transform: uppercase;
                color: var(--matte-muted);
            }
            .chart-meta {
                font-size: 12px;
                color: var(--matte-muted);
            }
            .chart-canvas {
                height: 220px;
            }
            .chart-empty {
                position: absolute;
                inset: 56px 22px 26px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 13px;
                color: var(--matte-muted);
                background: rgba(7, 12, 9, 0.4);
                border-radius: 14px;
                border: 1px dashed rgba(247, 247, 245, 0.12);
                pointer-events: none;
                opacity: 0;
                transition: opacity 0.2s ease;
            }
            .chart-empty.visible {
                opacity: 1;
            }
            .warning {
                margin-top: 20px;
                padding: 14px 18px;
                border-radius: 14px;
                border: 1px solid rgba(243, 227, 176, 0.45);
                background: rgba(15, 21, 18, 0.7);
                color: #f1e3b1;
                font-size: 13px;
            }
            .tooltip {
                position: absolute;
                z-index: 10;
                padding: 10px 12px;
                border-radius: 10px;
                background: rgba(8, 12, 10, 0.95);
                color: var(--matte-white);
                font-size: 12px;
                letter-spacing: 0.04em;
                border: 1px solid rgba(76, 255, 178, 0.2);
                pointer-events: none;
                opacity: 0;
                transform: translateY(6px);
                transition: opacity 0.15s ease, transform 0.15s ease;
            }
            .tooltip.visible {
                opacity: 1;
                transform: translateY(0);
            }
            .empty-state {
                padding: 24px 20px;
                text-align: center;
                color: var(--matte-muted);
            }
            @media (max-width: 1100px) {
                .hero {
                    grid-template-columns: 1fr;
                }
                .list-header,
                .order-row {
                    grid-template-columns: 140px 150px 120px 160px 150px 160px 140px 120px 260px 120px 200px 120px;
                }
                .list-wrapper {
                    overflow-x: auto;
                }
                .orders {
                    min-width: 1280px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <section class="hero">
                <div class="hero-panel">
                    <div class="hero-eyebrow">Live Ops</div>
                    <h1>CASHER OPS Dashboard</h1>
                    <div class="subtitle">
                        Премиальный операционный контроль CASHER: поток заказов, статусная лента,
                        KPI и продажи без задержек.
                    </div>
                </div>
                <div class="hero-panel">
                    <div class="meta-row">
                        <span>Обновлено: <strong id="updated-at">__UPDATED_AT__</strong></span>
                        <span class="status-pill">Статус: <strong id="status-text">__STATUS_TEXT__</strong></span>
                        <button class="refresh-button" id="refresh-button" type="button">Обновить</button>
                        <a class="store-button" href="https://online.moysklad.ru/app/" target="_blank" rel="noreferrer">
                            Перейти в МойСклад
                        </a>
                    </div>
                </div>
            </section>

            <div class="kpi-row">
                <div class="kpi-card" id="kpi-new-orders">
                    <div class="kpi-value" id="new-orders-count">__NEW_ORDERS__</div>
                    <div class="kpi-label">Новые заказы</div>
                </div>
                <div class="kpi-card" id="kpi-cdek-orders">
                    <div class="kpi-value" id="cdek-orders-count">__CDEK_ORDERS__</div>
                    <div class="kpi-label">Отправлено СДЭК</div>
                </div>
            </div>

            <div class="weekly-row">
                <div class="weekly-card">
                    <div class="weekly-title">Новые заказы • 7 дней</div>
                    <div class="weekly-metrics">
                        <div class="weekly-metric">
                            <div class="weekly-label">Кол-во продаж</div>
                            <div class="weekly-value" id="weekly-new-count">0</div>
                        </div>
                        <div class="weekly-metric">
                            <div class="weekly-label">Сумма продаж</div>
                            <div class="weekly-value" id="weekly-new-sum">0</div>
                        </div>
                    </div>
                </div>
                <div class="weekly-card">
                    <div class="weekly-title">Собран СДЭК • 7 дней</div>
                    <div class="weekly-metrics">
                        <div class="weekly-metric">
                            <div class="weekly-label">Кол-во продаж</div>
                            <div class="weekly-value" id="weekly-cdek-count">0</div>
                        </div>
                        <div class="weekly-metric">
                            <div class="weekly-label">Сумма продаж</div>
                            <div class="weekly-value" id="weekly-cdek-sum">0</div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="chart-section">
                <div class="chart-header">
                    <div class="chart-title">Продажи за 7 дней</div>
                    <div class="chart-meta" id="chart-meta">Последние 7 дней</div>
                </div>
                <div class="chart-canvas">
                    <canvas id="sales-chart"></canvas>
                </div>
                <div class="chart-empty" id="chart-empty">Нет данных для диаграммы</div>
            </div>

            <div class="filters">
                <div class="filter-group" data-filter-group="period">
                    <span class="filter-label">Период</span>
                    <button class="filter-button" type="button" data-period="today">Сегодня</button>
                    <button class="filter-button" type="button" data-period="three_days">3 дня</button>
                    <button class="filter-button active" type="button" data-period="week">7 дней</button>
                </div>
                <div class="filter-group" data-filter-group="status">
                    <span class="filter-label">Статус</span>
                    <button class="filter-button active" type="button" data-status="all">Все</button>
                    <button class="filter-button" type="button" data-status="new">Новые</button>
                    <button class="filter-button" type="button" data-status="cdek">СДЭК</button>
                </div>
                <button class="reset-button" id="reset-filters" type="button">Сбросить</button>
            </div>

            <div class="list-wrapper">
                <div class="list-header">
                    <div>Номер</div>
                    <div>Дата</div>
                    <div>Статус</div>
                    <div>Получатель</div>
                    <div>Телефон</div>
                    <div>Email</div>
                    <div>Доставка</div>
                    <div>Город</div>
                    <div>Адрес</div>
                    <div>Сумма</div>
                    <div>Комментарий</div>
                    <div>Ссылка</div>
                </div>
                <div class="orders" id="orders-list">
                    <div class="empty-state">Загрузка данных...</div>
                </div>
            </div>
            __WARNING_BLOCK__
            __EMPTY_BLOCK__
        </div>

        <div class="tooltip" id="tooltip"></div>

        <script src="https://cdn.jsdelivr.net/npm/gsap@3.12.5/dist/gsap.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/@studio-freight/lenis@1.0.42/bundled/lenis.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
        <script src="https://unpkg.com/@floating-ui/dom@1.6.7/dist/floating-ui.dom.umd.min.js"></script>
        <script>
            const initialPayload = __INITIAL_PAYLOAD__;
            let currentPayload = initialPayload;
            let knownOrderIds = new Set((initialPayload.orders || []).map((order) => order.id));
            let activeFilters = { period: 'week', status: 'all' };
            let filteredOrders = [];
            let renderIndex = 0;
            const PAGE_SIZE = 30;

            const refreshButton = document.getElementById('refresh-button');
            const statusText = document.getElementById('status-text');
            const updatedAt = document.getElementById('updated-at');
            const newOrdersCount = document.getElementById('new-orders-count');
            const cdekOrdersCount = document.getElementById('cdek-orders-count');
            const ordersList = document.getElementById('orders-list');
            const resetFilters = document.getElementById('reset-filters');
            const periodButtons = document.querySelectorAll('[data-period]');
            const statusButtons = document.querySelectorAll('[data-status]');
            const kpiNewOrders = document.getElementById('kpi-new-orders');
            const kpiCdekOrders = document.getElementById('kpi-cdek-orders');
            const tooltip = document.getElementById('tooltip');
            const chartMeta = document.getElementById('chart-meta');
            const chartEmpty = document.getElementById('chart-empty');
            const weeklyNewCount = document.getElementById('weekly-new-count');
            const weeklyNewSum = document.getElementById('weekly-new-sum');
            const weeklyCdekCount = document.getElementById('weekly-cdek-count');
            const weeklyCdekSum = document.getElementById('weekly-cdek-sum');

            let lenis = null;
            if (window.Lenis) {
                lenis = new Lenis({
                    lerp: 0.12,
                    smoothWheel: true,
                });
                function raf(time) {
                    lenis.raf(time);
                    requestAnimationFrame(raf);
                }
                requestAnimationFrame(raf);
            }

            const animateIntro = () => {
                gsap.from('.hero-panel', { opacity: 0, y: 20, duration: 0.6, stagger: 0.12 });
                gsap.from('.kpi-card', { opacity: 0, y: 16, duration: 0.5, stagger: 0.1, delay: 0.2 });
                gsap.from('.chart-section', { opacity: 0, y: 16, duration: 0.5, delay: 0.3 });
            };

            console.info('[Dashboard] Initial payload loaded', initialPayload);

            const getStatusClass = (state) => {
                const value = (state || '').toLowerCase();
                if (value.includes('сдек')) return 'status-cdek';
                if (value.includes('нов') || value.includes('принят') || value.includes('оплачен') || value.includes('обработ')) {
                    return 'status-new';
                }
                return '';
            };

            const isNewOrder = (state) => {
                const value = (state || '').toLowerCase();
                return (
                    value.includes('нов') ||
                    value.includes('принят') ||
                    value.includes('оплачен') ||
                    value.includes('обработ')
                ) && !value.includes('сдек');
            };

            const getMskTodayStartMs = () => {
                return currentPayload?.server_msk_today_start_ms || 0;
            };

            const getRecentDayKeys = () => {
                const days = currentPayload?.days || [];
                if (!days.length) return new Set();
                if (activeFilters.period === 'today') {
                    return new Set(days.slice(-1).map((day) => day.key));
                }
                if (activeFilters.period === 'three_days') {
                    return new Set(days.slice(-3).map((day) => day.key));
                }
                return new Set(days.map((day) => day.key));
            };

            const filterByPeriod = (orders) => {
                if (activeFilters.period === 'week') return orders;
                const nowMs = currentPayload?.server_msk_now_ms || 0;
                const todayStart = getMskTodayStartMs();
                const recentDayKeys = getRecentDayKeys();
                return orders.filter((order) => {
                    const orderTime = order.moment_ms || 0;
                    if (!orderTime) {
                        const dayKey = order.day_key;
                        if (!dayKey) {
                            return true;
                        }
                        return recentDayKeys.has(dayKey);
                    }
                    if (activeFilters.period === 'today') {
                        return orderTime >= todayStart;
                    }
                    const diffDays = (nowMs - orderTime) / (1000 * 60 * 60 * 24);
                    return diffDays <= 3;
                });
            };

            const filterByStatus = (orders) => {
                if (activeFilters.status === 'all') return orders;
                if (activeFilters.status === 'cdek') {
                    return orders.filter((order) => (order.state || '').toLowerCase().includes('сдек'));
                }
                if (activeFilters.status === 'new') {
                    return orders.filter((order) => isNewOrder(order.state));
                }
                return orders;
            };

            const applyFilters = () => {
                const orders = currentPayload.orders || [];
                const filtered = filterByStatus(filterByPeriod(orders));
                filteredOrders = filtered.sort((a, b) => (b.moment_ms || 0) - (a.moment_ms || 0));
                renderIndex = 0;
                if (!filteredOrders.length) {
                    ordersList.innerHTML = '<div class="empty-state">Нет заказов</div>';
                    return;
                }
                ordersList.innerHTML = '';
                ordersList.appendChild(loadMoreSentinel);
                renderNextChunk();
                console.info('[Dashboard] Filters applied', { filters: activeFilters, count: filteredOrders.length });
            };

            const escapeHtml = (value) => {
                if (value === null || value === undefined) return '';
                return String(value)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/\"/g, '&quot;')
                    .replace(/'/g, '&#39;');
            };

            const formatMoney = (value) => {
                const number = Number(value) || 0;
                const formatted = new Intl.NumberFormat('ru-RU', {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                }).format(number / 100);
                return `${formatted} руб.`;
            };

            const renderOrderRow = (order, highlightedIds) => {
                const card = document.createElement('div');
                card.className = 'order-card';
                const row = document.createElement('div');
                row.className = `order-row ${isNewOrder(order.state) ? 'new' : ''}`;
                row.dataset.orderId = order.id || '';
                if (highlightedIds.has(order.id)) {
                    gsap.fromTo(
                        row,
                        { boxShadow: '0 0 0 rgba(76, 255, 178, 0)' },
                        { boxShadow: '0 0 20px rgba(76, 255, 178, 0.4)', duration: 0.6, yoyo: true, repeat: 1 }
                    );
                }
                const statusClass = getStatusClass(order.state);
                const address = escapeHtml(order.address || '');
                const comment = escapeHtml(order.comment || '');
                const link = escapeHtml(order.link || '#');
                row.innerHTML = `
                    <div class="order-cell primary">${escapeHtml(order.name || '')}</div>
                    <div class="order-cell">${escapeHtml(order.moment || '')}</div>
                    <div class="order-cell"><span class="status-badge ${statusClass}">${escapeHtml(order.state || '')}</span></div>
                    <div class="order-cell">${escapeHtml(order.recipient || '')}</div>
                    <div class="order-cell">${escapeHtml(order.phone || '')}</div>
                    <div class="order-cell">${escapeHtml(order.email || '')}</div>
                    <div class="order-cell">${escapeHtml(order.delivery_method || '')}</div>
                    <div class="order-cell">${escapeHtml(order.city || '')}</div>
                    <div class="order-cell wrap" data-tooltip="${address}">${address}</div>
                    <div class="order-cell">${escapeHtml(order.sum_display || '')}</div>
                    <div class="order-cell wrap" data-tooltip="${comment}">${comment}</div>
                    <div class="order-cell link"><a href="${link}" target="_blank" rel="noreferrer">Открыть</a></div>
                `;
                const detail = document.createElement('div');
                detail.className = 'order-detail';
                detail.innerHTML = `
                    <div class="order-detail-grid">
                        <div class="order-detail-item">
                            <div class="order-detail-label">Кол-во продаж</div>
                            <div class="order-detail-value">1</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Сумма продаж</div>
                            <div class="order-detail-value">${escapeHtml(order.sum_display || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Статус</div>
                            <div class="order-detail-value">${escapeHtml(order.state || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Создан</div>
                            <div class="order-detail-value">${escapeHtml(order.moment || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Получатель</div>
                            <div class="order-detail-value">${escapeHtml(order.recipient || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Телефон</div>
                            <div class="order-detail-value">${escapeHtml(order.phone || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Email</div>
                            <div class="order-detail-value">${escapeHtml(order.email || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Доставка</div>
                            <div class="order-detail-value">${escapeHtml(order.delivery_method || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Город</div>
                            <div class="order-detail-value">${escapeHtml(order.city || '')}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Адрес</div>
                            <div class="order-detail-value">${address || '—'}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Комментарий</div>
                            <div class="order-detail-value">${comment || '—'}</div>
                        </div>
                        <div class="order-detail-item">
                            <div class="order-detail-label">Ссылка</div>
                            <div class="order-detail-value"><a href="${link}" target="_blank" rel="noreferrer">Открыть заказ</a></div>
                        </div>
                    </div>
                `;
                card.appendChild(row);
                card.appendChild(detail);
                return card;
            };

            const renderNextChunk = () => {
                if (!filteredOrders.length) {
                    ordersList.innerHTML = '<div class="empty-state">Нет заказов по выбранным фильтрам</div>';
                    return;
                }
                const slice = filteredOrders.slice(renderIndex, renderIndex + PAGE_SIZE);
                const fragment = document.createDocumentFragment();
                const highlightedIds = new Set(currentPayload.highlighted_ids || []);
                slice.forEach((order) => fragment.appendChild(renderOrderRow(order, highlightedIds)));
                ordersList.appendChild(fragment);
                renderIndex += slice.length;
                if (renderIndex >= filteredOrders.length) {
                    observer.disconnect();
                } else {
                    observer.observe(loadMoreSentinel);
                }
            };

            const loadMoreSentinel = document.createElement('div');
            loadMoreSentinel.style.height = '1px';
            ordersList.appendChild(loadMoreSentinel);

            const observer = new IntersectionObserver((entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        observer.unobserve(entry.target);
                        renderNextChunk();
                    }
                });
            });

            const showTooltip = (target, text) => {
                if (!text) return;
                if (!window.FloatingUIDOM) {
                    console.warn('[Dashboard] FloatingUI not loaded');
                    return;
                }
                tooltip.textContent = text;
                tooltip.classList.add('visible');
                window.FloatingUIDOM.computePosition(target, tooltip, {
                    placement: 'top',
                    middleware: [window.FloatingUIDOM.offset(8), window.FloatingUIDOM.shift({ padding: 8 })],
                }).then(({ x, y }) => {
                    Object.assign(tooltip.style, { left: `${x}px`, top: `${y}px` });
                });
            };

            const showTooltipAt = (x, y, text) => {
                if (!text) return;
                if (!window.FloatingUIDOM) {
                    console.warn('[Dashboard] FloatingUI not loaded');
                    return;
                }
                tooltip.textContent = text;
                tooltip.classList.add('visible');
                const virtualEl = {
                    getBoundingClientRect() {
                        return { x, y, top: y, left: x, right: x, bottom: y, width: 0, height: 0 };
                    },
                };
                window.FloatingUIDOM.computePosition(virtualEl, tooltip, {
                    placement: 'top',
                    middleware: [window.FloatingUIDOM.offset(8), window.FloatingUIDOM.shift({ padding: 8 })],
                }).then(({ x: nextX, y: nextY }) => {
                    Object.assign(tooltip.style, { left: `${nextX}px`, top: `${nextY}px` });
                });
            };

            const hideTooltip = () => {
                tooltip.classList.remove('visible');
            };

            document.addEventListener('mouseover', (event) => {
                const target = event.target.closest('[data-tooltip]');
                if (!target) return;
                const text = target.getAttribute('data-tooltip');
                if (text && text.trim().length > 0) {
                    showTooltip(target, text);
                }
            });

            document.addEventListener('mouseout', (event) => {
                if (event.target.closest('[data-tooltip]')) {
                    hideTooltip();
                }
            });

            ordersList.addEventListener('click', (event) => {
                const row = event.target.closest('.order-row');
                if (!row) return;
                if (event.target.closest('a')) return;
                const card = row.closest('.order-card');
                if (!card) return;
                card.classList.toggle('expanded');
            });

            let salesChart = null;

            const buildSeriesFromOrders = (orders, days) => {
                const seriesMap = new Map(days.map((day) => [day.key, { ...day, sum: 0, count: 0 }]));
                orders.forEach((order) => {
                    const key = order.day_key;
                    if (!key) return;
                    if (!seriesMap.has(key)) return;
                    const entry = seriesMap.get(key);
                    entry.sum += Number(order.sum) || 0;
                    entry.count += 1;
                });
                return Array.from(seriesMap.values());
            };

            const updateChart = (payload) => {
                const orders = payload.orders || [];
                const days = payload.days || [];
                const series = buildSeriesFromOrders(orders, days);
                const labels = series.map((item) => item.label);
                const counts = series.map((item) => item.count);
                const sums = series.map((item) => item.sum);
                const totalCount = counts.reduce((acc, value) => acc + value, 0);
                const totalSum = sums.reduce((acc, value) => acc + value, 0);
                const maxValue = Math.max(...counts, 0);
                if (chartMeta) {
                    chartMeta.textContent =
                        totalCount > 0
                            ? `Всего ${totalCount} заказов • ${formatMoney(totalSum)}`
                            : 'Нет данных';
                }
                if (chartEmpty) {
                    chartEmpty.classList.toggle('visible', totalCount === 0);
                }
                if (!salesChart) {
                    const ctx = document.getElementById('sales-chart').getContext('2d');
                    salesChart = new Chart(ctx, {
                        type: 'bar',
                        data: {
                            labels,
                            datasets: [{
                                label: 'Заказы',
                                data: counts,
                                backgroundColor: 'rgba(76, 255, 178, 0.45)',
                                borderRadius: 8,
                                borderSkipped: false,
                                hoverBackgroundColor: 'rgba(76, 255, 178, 0.8)',
                                minBarLength: 6,
                            }],
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    suggestedMax: maxValue || 1,
                                    grid: { color: 'rgba(247, 247, 245, 0.08)' },
                                    ticks: { color: 'rgba(247, 247, 245, 0.5)', precision: 0 },
                                },
                                x: {
                                    grid: { display: false },
                                    ticks: { color: 'rgba(247, 247, 245, 0.6)' },
                                },
                            },
                            plugins: {
                                legend: { display: false },
                                tooltip: { enabled: false },
                            },
                            hover: {
                                mode: 'index',
                                intersect: true,
                            },
                            animation: false,
                        },
                    });
                    salesChart.canvas.addEventListener('mousemove', (event) => {
                        const points = salesChart.getElementsAtEventForMode(event, 'nearest', { intersect: true }, true);
                        if (!points.length) {
                            hideTooltip();
                            return;
                        }
                        const point = points[0];
                        const count = series[point.index]?.count || 0;
                        const sum = series[point.index]?.sum || 0;
                        showTooltipAt(
                            event.clientX,
                            event.clientY,
                            `${labels[point.index]} • ${count} заказов • ${formatMoney(sum)}`
                        );
                    });
                    salesChart.canvas.addEventListener('mouseleave', hideTooltip);
                } else {
                    salesChart.data.labels = labels;
                    salesChart.data.datasets[0].data = counts;
                    salesChart.options.scales.y.suggestedMax = maxValue || 1;
                    salesChart.update();
                }
            };

            const updateKpi = (payload) => {
                const newCount = payload.stats?.new_orders ?? 0;
                const cdekCount = payload.stats?.cdek_orders ?? 0;
                const weeklyNew = payload.stats?.weekly_sales?.new_orders || {};
                const weeklyCdek = payload.stats?.weekly_sales?.cdek_orders || {};
                const prevNew = Number(newOrdersCount.textContent || 0);
                const prevCdek = Number(cdekOrdersCount.textContent || 0);
                newOrdersCount.textContent = newCount;
                cdekOrdersCount.textContent = cdekCount;
                if (weeklyNewCount) {
                    weeklyNewCount.textContent = weeklyNew.count ?? 0;
                }
                if (weeklyNewSum) {
                    weeklyNewSum.textContent = formatMoney(weeklyNew.sum ?? 0);
                }
                if (weeklyCdekCount) {
                    weeklyCdekCount.textContent = weeklyCdek.count ?? 0;
                }
                if (weeklyCdekSum) {
                    weeklyCdekSum.textContent = formatMoney(weeklyCdek.sum ?? 0);
                }
                updatedAt.textContent = payload.updated_at || '';
                statusText.textContent = payload.stale ? 'Данные устарели' : 'Данные обновлены';
                if (newCount > prevNew) {
                    kpiNewOrders.classList.add('neon');
                    setTimeout(() => kpiNewOrders.classList.remove('neon'), 500);
                }
                if (cdekCount > prevCdek) {
                    kpiCdekOrders.classList.add('neon');
                    setTimeout(() => kpiCdekOrders.classList.remove('neon'), 500);
                }
            };

            const updateFromPayload = (payload) => {
                if (!payload || !payload.stats) return;
                const orders = payload.orders || [];
                const newIds = new Set(orders.map((order) => order.id));
                const highlightedIds = [];
                newIds.forEach((id) => {
                    if (id && !knownOrderIds.has(id)) {
                        highlightedIds.push(id);
                    }
                });
                payload.highlighted_ids = highlightedIds;
                knownOrderIds = newIds;
                currentPayload = payload;
                updateKpi(payload);
                updateChart(payload);
                applyFilters();
                console.info('[Dashboard] Payload updated', {
                    updated_at: payload.updated_at,
                    total: payload.stats?.total_orders,
                    new: payload.stats?.new_orders,
                    cdek: payload.stats?.cdek_orders,
                });
            };

            const setActiveButton = (buttons, activeValue, dataAttr) => {
                buttons.forEach((button) => {
                    const value = button.getAttribute(dataAttr);
                    button.classList.toggle('active', value === activeValue);
                });
            };

            periodButtons.forEach((button) => {
                button.addEventListener('click', () => {
                    activeFilters.period = button.getAttribute('data-period');
                    setActiveButton(periodButtons, activeFilters.period, 'data-period');
                    applyFilters();
                });
            });

            statusButtons.forEach((button) => {
                button.addEventListener('click', () => {
                    activeFilters.status = button.getAttribute('data-status');
                    setActiveButton(statusButtons, activeFilters.status, 'data-status');
                    applyFilters();
                });
            });

            resetFilters.addEventListener('click', () => {
                activeFilters = { period: 'week', status: 'all' };
                setActiveButton(periodButtons, activeFilters.period, 'data-period');
                setActiveButton(statusButtons, activeFilters.status, 'data-status');
                applyFilters();
            });

            kpiNewOrders.addEventListener('click', () => {
                activeFilters.status = 'new';
                setActiveButton(statusButtons, activeFilters.status, 'data-status');
                applyFilters();
            });

            kpiCdekOrders.addEventListener('click', () => {
                activeFilters.status = 'cdek';
                setActiveButton(statusButtons, activeFilters.status, 'data-status');
                applyFilters();
            });

            refreshButton.addEventListener('click', async () => {
                refreshButton.disabled = true;
                statusText.textContent = 'Обновляем...';
                console.info('[Dashboard] Manual refresh triggered');
                try {
                    const response = await fetch('/refresh', { method: 'POST' });
                    const result = await response.json();
                    if (result?.payload) {
                        updateFromPayload(result.payload);
                    } else if (result?.updated_at) {
                        updatedAt.textContent = result.updated_at;
                    }
                    statusText.textContent = result?.payload?.stale ? 'Данные устарели' : 'Данные обновлены';
                    console.info('[Dashboard] Manual refresh completed', result);
                } catch (error) {
                    statusText.textContent = 'Ошибка обновления';
                    console.error('[Dashboard] Manual refresh failed', error);
                } finally {
                    refreshButton.disabled = false;
                }
            });

            updateFromPayload(initialPayload);
            animateIntro();

            let eventSource = null;
            let fallbackTimer = null;

            const startFallbackRefresh = () => {
                if (fallbackTimer) return;
                fallbackTimer = setInterval(async () => {
                    try {
                        const response = await fetch('/refresh', { method: 'POST' });
                        const result = await response.json();
                        if (result?.payload) {
                            updateFromPayload(result.payload);
                        }
                    } catch (error) {
                        console.warn('[Dashboard] Fallback refresh failed', error);
                    }
                }, 30000);
            };

            const stopFallbackRefresh = () => {
                if (fallbackTimer) {
                    clearInterval(fallbackTimer);
                    fallbackTimer = null;
                }
            };

            const startEventSource = () => {
                if (eventSource) {
                    eventSource.close();
                }
                eventSource = new EventSource('/events');
                eventSource.onmessage = (event) => {
                    try {
                        const payload = JSON.parse(event.data);
                        updateFromPayload(payload);
                        stopFallbackRefresh();
                    } catch (error) {
                        console.warn('Failed to parse event', error);
                    }
                };
                eventSource.onerror = (event) => {
                    console.warn('[Dashboard] EventSource error', event);
                    startFallbackRefresh();
                };
            };

            startEventSource();
        </script>
    </body>
</html>
"""


def render_landing_page(cache: Optional[Dict[str, Any]]) -> str:
    has_cache = bool(cache)
    cache = cache or cache_payload([])
    stats = cache.get("stats", {})
    orders = sorted(cache.get("orders", []), key=lambda order: order.get("moment_ms", 0), reverse=True)
    updated_at = cache.get("updated_at") or EMPTY_VALUE
    stale = cache_is_stale(cache) if has_cache else False
    status_text = "Данные обновлены" if has_cache else "Данные загружаются"
    warning_block = ""
    if stale and has_cache:
        warning_block = (
            "<div class=\"warning\">Данные устарели. Нажмите «Обновить», чтобы подтянуть актуальные значения.</div>"
        )
    empty_block = ""
    if not has_cache:
        empty_block = "<div class=\"warning\">Данные загружаются. Попробуйте обновить позже.</div>"

    initial_payload = safe_json_for_html(
        {
            "updated_at": updated_at,
            "server_msk_now_ms": int(msk_now().int_timestamp * 1000),
            "server_msk_today_start_ms": int(msk_day_start().int_timestamp * 1000),
            "ttl_seconds": CACHE_TTL_SECONDS,
            "stats": {
                "new_orders": int(stats.get("new_orders", 0)),
                "cdek_orders": int(stats.get("cdek_orders", 0)),
                "total_orders": int(stats.get("total_orders", 0)),
                "weekly_sales": stats.get("weekly_sales", {}),
            },
            "orders": orders,
            "stale": stale,
            "days": msk_day_labels(),
        }
    )

    return (
        LANDING_TEMPLATE.replace("__UPDATED_AT__", escape(updated_at))
        .replace("__STATUS_TEXT__", escape(status_text))
        .replace("__NEW_ORDERS__", str(stats.get("new_orders", 0)))
        .replace("__CDEK_ORDERS__", str(stats.get("cdek_orders", 0)))
        .replace("__WARNING_BLOCK__", warning_block)
        .replace("__EMPTY_BLOCK__", empty_block)
        .replace("__INITIAL_PAYLOAD__", initial_payload)
    )


async def broadcast_event(cache: Dict[str, Any]) -> None:
    payload = event_payload(cache)
    async with SUBSCRIBERS_LOCK:
        for queue in list(SUBSCRIBERS):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning("Dropping SSE event for slow client")


async def refresh_cache(reason: str) -> Optional[Dict[str, Any]]:
    async with UPDATE_LOCK:
        existing_cache = await anyio.to_thread.run_sync(read_cache)
        try:
            logger.info("Refreshing cache: %s", reason)
            orders = await anyio.to_thread.run_sync(fetch_customer_orders)
            cache = await anyio.to_thread.run_sync(build_cache_from_orders, orders)
            if cache_is_valid(cache):
                await anyio.to_thread.run_sync(write_cache, cache)
                log_stats(cache)
                await broadcast_event(cache)
                logger.info("Cache refreshed: %s", reason)
                return cache
            if existing_cache and cache_is_valid(existing_cache):
                logger.warning("Refresh produced empty cache; keeping existing data")
                return existing_cache
            await anyio.to_thread.run_sync(write_cache, cache)
            return cache
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to refresh cache: %s", exc)
            if existing_cache and cache_is_valid(existing_cache):
                return existing_cache
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


async def process_webhook_event(href: str) -> None:
    logger.info("Processing webhook order: %s", href)
    try:
        order = await anyio.to_thread.run_sync(fetch_entity, href)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch order details: %s", exc)
        return
    if not order:
        logger.warning("Order data missing for webhook href: %s", href)
        return
    store_href = store_href_from_env()
    if store_href and not order_matches_store(order, store_href):
        logger.info("Skipping order %s: store mismatch", order.get("id"))
        return

    try:
        order_payload = await anyio.to_thread.run_sync(lambda: serialize_order(build_order_dto(order)))
        async with UPDATE_LOCK:
            cache = await anyio.to_thread.run_sync(update_cache_with_order, order_payload)
        log_stats(cache)
        await broadcast_event(cache)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to update cache for webhook: %s", exc)

    try:
        state_name = await anyio.to_thread.run_sync(get_state_name, order)
        if is_cdek_state(state_name):
            message = await anyio.to_thread.run_sync(build_cdek_message, order)
        else:
            message = await anyio.to_thread.run_sync(build_message, order)
        await anyio.to_thread.run_sync(send_telegram_message, message)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to send Telegram notification: %s", exc)


@app.on_event("startup")
async def startup_event() -> None:
    ensure_cache_dir()
    logger.info("Starting up with cache path: %s", CACHE_PATH)
    await refresh_cache("startup")
    asyncio.create_task(auto_refresh_loop())


@app.get("/health", response_class=ORJSONResponse)
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def landing() -> HTMLResponse:
    cache: Optional[Dict[str, Any]] = None
    try:
        cache = read_cache()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read cache: %s", exc)
    html = render_landing_page(cache)
    return HTMLResponse(content=html, status_code=200)


@app.post("/refresh", response_class=ORJSONResponse)
async def refresh() -> ORJSONResponse:
    cache = await refresh_cache("manual")
    if not cache:
        cache = await anyio.to_thread.run_sync(read_cache)
    payload = event_payload_dict(cache) if cache else None
    return ORJSONResponse(
        {
            "status": "ok",
            "updated_at": cache.get("updated_at") if cache else None,
            "payload": payload,
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
                yield f"data: {event_payload(cache)}\n\n"
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


class WebhookPayload(BaseModel):
    events: List[Dict[str, Any]] = Field(default_factory=list)


@app.post("/webhook/moysklad", response_class=ORJSONResponse)
async def moysklad_webhook(request: Request) -> ORJSONResponse:
    try:
        payload = WebhookPayload.model_validate(await request.json())
    except Exception as exc:  # noqa: BLE001
        logger.exception("Invalid webhook payload: %s", exc)
        return ORJSONResponse({"status": "ok"})

    if not payload.events:
        logger.info("Webhook received without events")
        return ORJSONResponse({"status": "ok"})

    for event in payload.events:
        meta = event.get("meta", {})
        if meta.get("type") != "customerorder":
            continue
        href = meta.get("href")
        if not href:
            continue
        asyncio.create_task(process_webhook_event(href))

    return ORJSONResponse({"status": "ok"})
