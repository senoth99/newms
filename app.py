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


def build_message(order: Dict[str, Any]) -> str:
    agent = order.get("agent", {}).get("name") or "не указан"
    state = order.get("state", {}).get("name") or "не указан"
    moment = _format_datetime(order.get("moment"))
    name = order.get("name") or "без номера"
    sum_value = _format_money(order.get("sum"))
    description = order.get("description") or "нет"
    href = order.get("meta", {}).get("href") or "нет"

    return (
        "СОЗДАН НОВЫЙ ЗАКАЗ\n\n"
        f"Номер: {name}\n"
        f"Дата: {moment}\n"
        f"Контрагент: {agent}\n"
        f"Сумма: {sum_value}\n"
        f"Статус: {state}\n"
        f"Комментарий: {description}\n"
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
