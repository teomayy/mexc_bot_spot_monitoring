import asyncio
import json
import logging
import random
import websockets
import sys
import os
import requests
from datetime import datetime, timezone, timedelta
from google.protobuf.message import DecodeError

# Добавляем websocket-proto в PYTHONPATH
sys.path.append(os.path.abspath("websocket_proto"))
import PushDataV3ApiWrapper_pb2

# 🔧 Конфигурация
TELEGRAM_BOT_TOKEN = "TOKEN БОТА"
TELEGRAM_CHAT_ID = "ВАШ ЧАТ ID"
ORDER_THRESHOLD = 2000  # 💰 Порог сделки (настраиваемый)
VOLUME_THRESHOLD = 50  # 📊 Порог объема за 1 минуту
WS_URL = "wss://wbs-api.mexc.com/ws"
RECONNECT_DELAY = 5
TICKERS_PER_BATCH = 10  # Количество тикеров в одном соединении

# 🐜 Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Трекинг объема сделок
volume_tracker = {}

# ❌ Чёрный список бирж (Binance, Bybit, OKX)
EXCLUDED_EXCHANGES = {"binance", "bybit", "okx"}

async def get_all_tickers():
    """Получает список всех тикеров с MEXC API"""
    url = "https://api.mexc.com/api/v3/defaultSymbols"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Логируем ответ API
        logger.info(f"🔍 Ответ API MEXC: {data}")

        # Извлекаем тикеры
        if isinstance(data, dict) and "data" in data:
            tickers = data["data"]
            if isinstance(tickers, list):
                logger.info(f"✅ Загружено {len(tickers)} тикеров")
                return tickers
            else:
                logger.error(f"❌ Ошибка: 'data' не список! {type(tickers)}")
                return []
        else:
            logger.error(f"❌ Некорректный формат API: {data}")
            return []
    except requests.RequestException as e:
        logger.error(f"Ошибка получения тикеров: {e}")
        return []

def filter_tickers(tickers):
    """Фильтрует тикеры: оставляет только спот и исключает дубли с Binance, Bybit, OKX"""
    filtered_tickers = [
        ticker for ticker in tickers
        if not any(exchange in ticker.lower() for exchange in EXCLUDED_EXCHANGES)
    ]
    logger.info(f"✅ После фильтрации осталось {len(filtered_tickers)} тикеров")
    return filtered_tickers

def update_volume_tracker(ticker, volume):
    """Обновляет объем сделок за 1 минуту"""
    now = datetime.now(timezone.utc)
    
    if ticker not in volume_tracker:
        volume_tracker[ticker] = []

    volume_tracker[ticker].append((volume, now))

    # Удаляем старые сделки (старше 1 минуты)
    volume_tracker[ticker] = [(v, t) for v, t in volume_tracker[ticker] if now - t < timedelta(minutes=1)]
    
    total_volume = sum(v for v, _ in volume_tracker[ticker])
    print(f"📈 Обновлен объем для {ticker}: {total_volume} USDT")  # Добавили print

    return total_volume


async def subscribe_to_tickers(ws, tickers):
    print(f"📡 Подписка на тикеры: {tickers}")  # <-- Добавили print
    subscribe_msg = {
        "method": "SUBSCRIPTION",
        "params": [f"spot@public.aggre.deals.v3.api.pb@100ms@{ticker}" for ticker in tickers],
        "id": random.randint(1, 100000)
    }
    print(f"📡 Отправка WebSocket-сообщения: {json.dumps(subscribe_msg, indent=2)}")
    await ws.send(json.dumps(subscribe_msg))
    logger.info(f"✅ Подписка на {len(tickers)} тикеров")



async def send_ping(ws):
    while True:
        try:
            print("📡 Отправка PING")  # <-- Добавили print
            logger.info("📡 Отправлен PING")
            await ws.send(json.dumps({"method": "PING"}))
        except websockets.exceptions.ConnectionClosed:
            print("⚠️ WebSocket закрыт, прекращаем PING")
            logger.warning("⚠️ WebSocket закрыт, прекращаем PING")
            break
        await asyncio.sleep(30)


def on_open(ws):
    print("✅ WebSocket подключен")  # <-- Добавили print
    tickers = get_tickers()  # Предполагаем, что у вас есть функция получения тикеров
    subscribe_to_tickers(ws, tickers)

def on_close(ws, close_status_code, close_msg):
    print(f"⚠️ WebSocket закрыт! Код: {close_status_code}, Сообщение: {close_msg}")  # <-- Добавили print

def on_error(ws, error):
    print(f"❌ Ошибка WebSocket: {error}")  # <-- Добавили print


async def handle_messages(ws):
    """Обрабатывает входящие сообщения WebSocket"""
    async for message in ws:
        print(f"📩 Получено сообщение от WebSocket: {message[:500]}")  # Логируем первые 500 символов
        try:
            if isinstance(message, str):  # JSON-ответ
                data_json = json.loads(message)

                if "deals" in data_json:
                    symbol = data_json.get("symbol", "UNKNOWN")
                    print(f"📊 Получена сделка по {symbol}: {data_json['deals']}")  # Добавляем print

                    for deal in data_json["deals"]:
                        price = float(deal.get("price", 0))
                        volume = float(deal.get("quantity", 0))
                        total = price * volume
                        side = "BUY" if deal.get("tradeType") == 1 else "SELL"

                        total_volume = update_volume_tracker(symbol, volume)

                        print(f"💰 Сделка {side} {symbol}: {total:.2f} USDT (Порог {ORDER_THRESHOLD}, Объем {total_volume})")

                        if total >= ORDER_THRESHOLD and total_volume >= VOLUME_THRESHOLD:
                            await send_telegram_message(total, symbol, side)
                continue

            # Если данные бинарные, парсим Protobuf
            data = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
            data.ParseFromString(message)

            if hasattr(data, "publicAggreDeals"):
                symbol = data.symbol
                print(f"📊 Protobuf-сделка по {symbol}: {data}")  # Логируем Protobuf-сообщение

                for deal in data.publicAggreDeals.deals:
                    price = float(deal.price)
                    volume = float(deal.quantity)
                    total = price * volume
                    side = "BUY" if deal.tradeType == 1 else "SELL"

                    total_volume = update_volume_tracker(symbol, volume)

                    print(f"💰 Protobuf {side} {symbol}: {total:.2f} USDT (Порог {ORDER_THRESHOLD}, Объем {total_volume})")

                    if total >= ORDER_THRESHOLD and total_volume >= VOLUME_THRESHOLD:
                        await send_telegram_message(total, symbol, side)
            else:
                logger.warning(f"⚠️ Нет 'publicAggreDeals' в Protobuf: {data}")

        except json.JSONDecodeError:
            logger.error("❌ Ошибка обработки JSON данных")
        except DecodeError:
            logger.error("❌ Ошибка обработки Protobuf данных")


async def send_telegram_message(order_size, ticker, side):
    """Отправляет уведомление в Telegram"""

    if text:
        message = text
    else: 
        message = f"{'🟢' if side == 'BUY' else '🔴'} *{ticker.replace('USDT', '')}*\n\n💰 {order_size:.2f} $"

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    print(f"📨 Отправка в Telegram: {message}")  # Добавили print
    print(f"🔗 Запрос: {telegram_url}, Данные: {payload}")  # Логируем URL и payload

    try:
        response = requests.post(telegram_url, json=payload)
        response_data = response.json()
        print(f"📩 Ответ Telegram API: {response_data}")  # Логируем ответ API
        response.raise_for_status()

        if not response_data.get("ok"):
            print(f"⚠️ Ошибка отправки сообщения: {response_data.get('description')}")
    except requests.RequestException as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")




async def connect_to_mexc(tickers):
    while True:
        try:
            print(f"🔌 Подключаемся к WebSocket для {len(tickers)} тикеров")  # <-- Добавили print
            async with websockets.connect(WS_URL) as ws:
                logger.info("✅ Подключение к WebSocket MEXC")
                await subscribe_to_tickers(ws, tickers)
                await asyncio.gather(handle_messages(ws), send_ping(ws))
        except websockets.exceptions.ConnectionClosedError:
            print(f"🔌 Соединение закрыто, переподключение через {RECONNECT_DELAY} секунд...")
            logger.warning(f"🔌 Соединение закрыто, переподключение через {RECONNECT_DELAY} секунд...")
            await asyncio.sleep(RECONNECT_DELAY)


MAX_ACTIVE_CONNECTIONS = 5  # Ограничим одновременные соединения

async def send_telegram_message(order_size=None, ticker=None, side=None, text=None):
    """Отправляет уведомление в Telegram"""
    if text:
        message = text  # Если передан текст, просто отправляем его
    else:
        message = f"{'🟢' if side == 'BUY' else '🔴'} *{ticker.replace('USDT', '')}*\n\n💰 {order_size:.2f} $"

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    print(f"📨 Отправка в Telegram: {message}")  # Логирование
    print(f"🔗 Запрос: {telegram_url}, Данные: {payload}")  # Логирование URL и payload

    try:
        response = requests.post(telegram_url, json=payload)
        response_data = response.json()
        print(f"📩 Ответ Telegram API: {response_data}")  # Логирование ответа API
        response.raise_for_status()

        if not response_data.get("ok"):
            print(f"⚠️ Ошибка отправки сообщения: {response_data.get('description')}")
    except requests.RequestException as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")

async def main():
    tickers = await get_all_tickers()
    tickers = filter_tickers(tickers)

    if not tickers:
        logger.error("❌ Нет доступных тикеров!")
        return

    # Получаем имя пользователя из Telegram API
    user_info_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat?chat_id={TELEGRAM_CHAT_ID}"
    try:
        response = requests.get(user_info_url)
        user_data = response.json()
        username = user_data.get("result", {}).get("first_name", "друг")  # Дефолтное значение "друг"
    except requests.RequestException:
        username = "друг"

    # Отправляем приветственное сообщение
    await send_telegram_message(text=f"👋 Привет, {username}!\nЯ запущен и готов мониторить сделки на MEXC.")

    tasks = []
    for i in range(0, len(tickers), TICKERS_PER_BATCH):
        if len(tasks) >= MAX_ACTIVE_CONNECTIONS:  # Ограничение количества активных соединений
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        batch = tickers[i:i + TICKERS_PER_BATCH]
        tasks.append(asyncio.create_task(connect_to_mexc(batch)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())