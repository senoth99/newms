# MoySklad → Telegram notifier

Минимальное Python-приложение для получения webhook-уведомлений о создании заказов в МойСклад и отправки сообщения в Telegram через бота.

## Возможности

- Принимает webhook-события от МойСклад.
- Достает подробности заказа по `meta.href` через API.
- Отправляет структурированное сообщение в Telegram.

## Переменные окружения

Все секреты передаются через переменные окружения:

- `MS_TOKEN` — токен МойСклад для авторизации (Bearer).
- `MS_BASIC_TOKEN` — base64 токен для Basic auth (если используете этот способ).
- `TG_BOT_TOKEN` — токен Telegram-бота.
- `TG_CHAT_ID` — chat ID, куда отправлять уведомления.

Пример шаблона: `.env.example`.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Webhook

Настройте webhook в МойСклад на эндпоинт:

```
POST /webhook/moysklad
```

Пример сообщения в Telegram:

```
СОЗДАН НОВЫЙ ЗАКАЗ

Номер: 000123
Дата: 2024-09-24 12:00:00
Контрагент: ООО Ромашка
Сумма: 1500.00
Статус: Новый
Комментарий: нет
Ссылка: https://api.moysklad.ru/api/remap/1.2/entity/customerorder/...
```
