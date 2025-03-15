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
TELEGRAM_BOT_TOKEN = "7704411203:AAEKrNkZK1KaVXYqI7QiVszNVDqe6PNopbs"
TELEGRAM_CHAT_ID = "7800907892"
ORDER_THRESHOLD = 2000  # 💰 Порог сделки (настраиваемый)
VOLUME_THRESHOLD = 50  # 📊 Порог объема за 1 минуту
WS_URL = "wss://wbs-api.mexc.com/ws"
RECONNECT_DELAY = 5
TICKERS_PER_BATCH = 10  # Количество тикеров в одном соединении
MAX_ACTIVE_CONNECTIONS = 20  # Лимит на WebSocket-соединения
PROGRESS_STEP = 100  # Шаг для логирования прогресса

# 🐜 Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Вывод в консоль
        logging.FileHandler("mexc_bot.log", encoding="utf-8")  # Лог в файл
    ],
)
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

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            tickers = [t for t in data["data"] if t.endswith("USDT")]
            logger.info(f"✅ Загружено {len(tickers)} тикеров (только USDT)")
            return tickers
        else:
            logger.error(f"❌ Некорректный формат API MEXC: {data}")
            return []
    except requests.RequestException as e:
        logger.error(f"Ошибка получения тикеров: {e}")
        return []

def get_binance_tickers():
    """Загружает тикеры с Binance (только USDT)"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "symbols" in data and isinstance(data["symbols"], list):
            tickers = {item["symbol"] for item in data["symbols"] if item["symbol"].endswith("USDT")}
            logger.info(f"✅ Загружено {len(tickers)} тикеров с Binance")
            return tickers
        else:
            logger.error(f"❌ Ошибка формата данных Binance: {data}")
            return set()
    except requests.RequestException as e:
        logger.error(f"❌ Ошибка загрузки тикеров с Binance: {e}")
        return set()

def get_bybit_tickers():
    url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {item["symbol"] for item in data.get("result", {}).get("list", [])}
    except requests.RequestException as e:
        logger.error(f"❌ Ошибка загрузки тикеров с Bybit: {e}")
        return set()

def get_okx_tickers():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {item["instId"].replace("-", "") for item in data.get("data", [])}
    except requests.RequestException as e:
        logger.error(f"❌ Ошибка загрузки тикеров с OKX: {e}")
        return set()

def load_blacklist_tickers():
    """Загружает тикеры с Binance, Bybit и OKX для исключения"""
    binance_tickers = get_binance_tickers()
    bybit_tickers = get_bybit_tickers()
    okx_tickers = get_okx_tickers()

    blacklist = binance_tickers | bybit_tickers | okx_tickers
    logger.info(f"📛 Итоговый список исключенных тикеров: {len(blacklist)}")
    return blacklist



def filter_tickers(tickers, blacklist):
    """Фильтрует тикеры, исключая тикеры с Binance, Bybit и OKX"""
    filtered_tickers = [t for t in tickers if t not in blacklist]
    logger.info(f"✅ Окончательное количество тикеров после фильтрации: {len(filtered_tickers)}")
    return filtered_tickers

def update_volume_tracker(ticker, volume, ticker_index, total_tickers):
    """Обновляет объем сделок за 1 минуту и логирует прогресс отслеживания тикеров"""
    now = datetime.now(timezone.utc)
    
    if ticker not in volume_tracker:
        volume_tracker[ticker] = []

    volume_tracker[ticker].append((volume, now))

    # Удаляем старые сделки (старше 1 минуты)
    volume_tracker[ticker] = [(v, t) for v, t in volume_tracker[ticker] if now - t < timedelta(minutes=1)]
    
    total_volume = sum(v for v, _ in volume_tracker[ticker])

    # Логируем обновление объема + отслеживание тикера
    logger.info(f"📈 Обновлен объем для {ticker}: {total_volume} USDT")
    logger.info(f"🔍 Отслеживание тикера {ticker} [{ticker_index} / {total_tickers}]")

    return total_volume


async def subscribe_to_tickers(ws, tickers, start_index, total_tickers):
    """Подписка на сделки через WebSocket"""
    subscribe_msg = {
        "method": "SUBSCRIPTION",
        "params": [f"spot@public.aggre.deals.v3.api.pb@100ms@{ticker}" for ticker in tickers],
        "id": random.randint(1, 100000)
    }
    await ws.send(json.dumps(subscribe_msg))
    
    for i, ticker in enumerate(tickers, start=start_index):
        logger.info(f"📡 Отслеживание тикера {ticker} [{i} / {total_tickers}]")


async def send_ping(ws):
    """Периодически отправляет PING, чтобы поддерживать соединение"""
    while True:
        try:
            await ws.send(json.dumps({"method": "PING"}))
            logger.info("📡 Отправлен PING")
        except websockets.exceptions.ConnectionClosed:
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


async def handle_messages(ws, ticker_count, total_tickers):
    """Обрабатывает входящие сообщения WebSocket"""
    async for message in ws:
        try:
            if isinstance(message, str):  # JSON-ответ
                data_json = json.loads(message)

                if "deals" in data_json:
                    symbol = data_json.get("symbol", "UNKNOWN")
                    deals = data_json["deals"]
                    logger.info(f"🔍 JSON-сделка по {symbol}: {deals}")  # Логируем сделки

                    for deal in deals:
                        price = float(deal.get("price", 0))
                        volume = float(deal.get("quantity", 0))
                        total = price * volume
                        side = "BUY" if deal.get("tradeType") == 1 else "SELL"

                        total_volume = update_volume_tracker(symbol, volume, ticker_count, total_tickers)

                        logger.info(f"💰 {side} {symbol}: {total:.2f} USDT (Порог {ORDER_THRESHOLD}, Объем {total_volume})")

                        if total >= ORDER_THRESHOLD and total_volume >= VOLUME_THRESHOLD:
                            await send_telegram_message(total, symbol, side)
                continue

            # Если данные бинарные, парсим Protobuf
            if PushDataV3ApiWrapper_pb2:
                data = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                data.ParseFromString(message)

                if hasattr(data, "publicAggreDeals"):
                    symbol = data.symbol
                    deals = data.publicAggreDeals.deals
                    logger.info(f"🔍 Protobuf-сделка по {symbol}: {deals}")  # Логируем сделки

                    for deal in deals:
                        price = float(deal.price)
                        volume = float(deal.quantity)
                        total = price * volume
                        side = "BUY" if deal.tradeType == 1 else "SELL"

                        total_volume = update_volume_tracker(symbol, volume, ticker_count, total_tickers)

                        logger.info(f"💰 Protobuf {side} {symbol}: {total:.2f} USDT (Порог {ORDER_THRESHOLD}, Объем {total_volume})")

                        if total >= ORDER_THRESHOLD and total_volume >= VOLUME_THRESHOLD:
                            await send_telegram_message(total, symbol, side)
                else:
                    logger.warning(f"⚠️ Нет 'publicAggreDeals' в Protobuf: {data}")

        except json.JSONDecodeError:
            logger.error("❌ Ошибка обработки JSON данных")
        except DecodeError:
            logger.error("❌ Ошибка обработки Protobuf данных")

async def connect_to_mexc(tickers, ticker_count, total_tickers):
    """Подключение к WebSocket и подписка на тикеры"""
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                logger.info(f"✅ Подключение к WebSocket для {ticker_count}/{total_tickers} тикеров")
                await subscribe_to_tickers(ws, tickers, ticker_count, total_tickers)
                await asyncio.gather(handle_messages(ws, ticker_count, total_tickers), send_ping(ws))
        except websockets.exceptions.ConnectionClosedError:
            logger.warning(f"🔌 Переподключение через {RECONNECT_DELAY} секунд...")
            await asyncio.sleep(RECONNECT_DELAY)

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
    blacklist = load_blacklist_tickers()
    tickers = await get_all_tickers()
    tickers = filter_tickers(tickers, blacklist)

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

    total_tickers = len(tickers)
    tasks = []
    progress = 0

    for i in range(0, total_tickers, TICKERS_PER_BATCH):
        if len(tasks) >= MAX_ACTIVE_CONNECTIONS:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            tasks = list(tasks)

        batch = tickers[i:i + TICKERS_PER_BATCH]
        ticker_count = i + len(batch)
        tasks.append(asyncio.create_task(connect_to_mexc(batch, ticker_count, total_tickers)))

        # Логируем прогресс каждые PROGRESS_STEP тикеров
        if ticker_count >= progress + PROGRESS_STEP:
            logger.info(f"📈 Прогресс: {ticker_count} / {total_tickers} тикеров отслеживается")
            progress = ticker_count

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    logger.info("🚀 Бот запущен!")
    asyncio.run(main())