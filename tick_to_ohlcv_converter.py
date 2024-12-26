# -*- coding: utf-8 -*-
import json
import os
import asyncio
import threading
import time
import logging
import pandas as pd
from datetime import datetime
from algolab import API
from ws import AlgoLabSocket
from config import *

logging.basicConfig(
    filename="converter_logs.txt",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

# Constants
RESAMPLE_INTERVAL = '1min'
MAX_BUFFER_SIZE = BUFFER_SIZE if 'BUFFER_SIZE' in globals() else 100
last_trade_time = None
stop_session_thread = False

# Initialize API and websocket
algo = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True, keep_alive=False)
soket = None

# In-memory data stores
trade_data_buffer = {}  # Key: (symbol, market) -> List of (Date, Price, Qty)
ohlcv_cache = {}         # Key: (symbol, market) -> OHLCV DataFrame


def threaded_session_refresh(interval=60):
    """Thread-based function to send session refresh requests."""
    global stop_session_thread
    while not stop_session_thread:
        try:
            # logging.info("Running session refresh in a separate thread via threaded_session_refresh function.")
            session_refresh()
            time.sleep(interval)
        except Exception as e:
            logging.error(f"Error in session refresh thread: {e}")


# Start the session refresh thread
def start_session_refresh_thread(interval=60):
    """Start the session refresh thread."""
    logging.info("start_session_refresh_thread function calling.")
    thread = threading.Thread(target=threaded_session_refresh, args=(interval,))
    thread.daemon = True  # Daemon thread will exit when the main program exits
    thread.start()
    return thread


def load_or_create_ohlcv(symbol, market):
    """Load OHLCV data if it exists, otherwise create an empty DataFrame."""
    key = (symbol, market)
    if key in ohlcv_cache:
        return ohlcv_cache[key]

    candles_folder_path = os.path.join("db", "candles", market)
    os.makedirs(candles_folder_path, exist_ok=True)
    candles_path = os.path.join(candles_folder_path, f"{symbol}.json")

    if os.path.exists(candles_path):
        try:
            df = pd.read_json(candles_path)
        except ValueError:
            logging.warning(f"Invalid JSON in {candles_path}, creating an empty DataFrame.")
            df = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    else:
        df = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

    # Ensure correct dtypes
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df.set_index("Date", inplace=True)

    ohlcv_cache[key] = df
    return df


def session_refresh():
    """Send session refresh and log its status."""
    try:
        if not soket.connected:
            logging.warning("Socket is not connected. Attempting to reconnect.")
            if not reconnect_socket():
                logging.error("Socket reconnection failed during session refresh.")
                return

        algo.SessionRefresh()  # API üzerinden oturum yenileme işlemi.
        data_request = {"Type": "H"}
        soket.send(data_request)  # Socket üzerinden "H" türü talep gönderiliyor.
        # logging.info("Session refresh request sent via socket.")
    except Exception as e:
        logging.error(f"Session refresh error: {e}")

def process_incoming_trade(content):
    """Process a single incoming trade and append it to the in-memory buffer."""
    global last_trade_time

    try:
        symbol = content["Symbol"]
        market = content["Market"]
        price = content["Price"]
        qty = content["TradeQuantity"]

        # Parse as UTC
        timestamp = pd.to_datetime(content["Date"], utc=True)
        # Convert to Europe/Istanbul time
        timestamp = timestamp.tz_convert('Europe/Istanbul')
        # Convert to tz-naive
        timestamp = timestamp.tz_localize(None)

        # Update last trade time
        last_trade_time = timestamp

        key = (symbol, market)
        if key not in trade_data_buffer:
            trade_data_buffer[key] = []
        trade_data_buffer[key].append((timestamp, price, qty))
    except Exception as e:
        logging.error(f"Error processing trade: {e}, content: {content}")


def resample_and_merge_ohlcv():
    """Convert buffered trades into OHLCV format and merge into OHLCV DataFrames."""
    global trade_data_buffer
    for (symbol, market), trades in trade_data_buffer.items():
        if not trades:
            continue

        try:
            df_temp = pd.DataFrame(trades, columns=["Date", "Price", "TradeQuantity"])
            df_temp.set_index("Date", inplace=True)
            df_temp["Volume"] = df_temp["Price"] * df_temp["TradeQuantity"]

            # Resample to OHLCV
            ohlcv_resampled = df_temp.resample(RESAMPLE_INTERVAL).agg({
                "Price": ["first", "max", "min", "last"],
                "Volume": "sum"
            })
            ohlcv_resampled.columns = ["Open", "High", "Low", "Close", "Volume"]

            # Merge with existing OHLCV
            ohlcv_df = load_or_create_ohlcv(symbol, market)
            for idx, row in ohlcv_resampled.iterrows():
                if idx in ohlcv_df.index:
                    # Update row
                    ohlcv_df.at[idx, 'High'] = max(ohlcv_df.at[idx, 'High'], row['High'])
                    ohlcv_df.at[idx, 'Low'] = min(ohlcv_df.at[idx, 'Low'], row['Low'])
                    ohlcv_df.at[idx, 'Close'] = row['Close']
                    ohlcv_df.at[idx, 'Volume'] += row['Volume']
                else:
                    # Add new row
                    ohlcv_df.loc[idx] = row

            ohlcv_df.sort_index(inplace=True)
        except Exception as e:
            logging.error(f"Error processing data for {symbol}-{market}: {e}")

    trade_data_buffer.clear()


def write_ohlcv_to_disk():
    """Write OHLCV DataFrames to disk and log last trade time."""
    global last_trade_time

    try:
        now = datetime.now()

        for (symbol, market), df in ohlcv_cache.items():
            if df.empty:
                logging.warning(f"OHLCV DataFrame for {symbol}-{market} is empty. Skipping write.")
                continue

            # Kontrol: Tüm sütunların eksiksiz olduğundan emin olun
            required_columns = ["Open", "High", "Low", "Close", "Volume"]
            for col in required_columns:
                if col not in df.columns:
                    logging.warning(f"Missing column '{col}' in OHLCV for {symbol}-{market}. Filling with 0.")
                    df[col] = 0  # Eksik sütunları sıfırlarla doldur

            # Kontrol: NaN değerleri tespit edip doldur
            if df.isnull().values.any():
                logging.warning(f"NaN values detected in OHLCV for {symbol}-{market}. Applying corrections.")
                df.fillna(method="ffill", inplace=True)  # Forward fill
                df.fillna(0, inplace=True)  # Hala eksik olanları sıfırla

            # Dosya yollarını oluştur
            candles_folder_path = os.path.join("db", "candles", market)
            os.makedirs(candles_folder_path, exist_ok=True)
            candles_path = os.path.join(candles_folder_path, f"{symbol}.json")

            # JSON'a yazmadan önce tarih formatını kontrol et
            df_to_write = df.copy()
            df_to_write.reset_index(inplace=True)

            try:
                df_to_write["Date"] = df_to_write["Date"].apply(
                    lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + f'.{int(x.microsecond / 1000):03d}'
                )
            except Exception as e:
                logging.error(f"Error formatting Date column for {symbol}-{market}: {e}. Skipping file.")
                continue

            # JSON'a yaz
            with open(candles_path, 'w', encoding='utf-8') as f:
                json.dump(df_to_write.to_dict(orient='records'), f, indent=4, ensure_ascii=False)

        # Son işlem zamanı kaydını logla
        if last_trade_time is not None:
            time_diff = now - last_trade_time
            logging.info(
                f"Last trade time: {last_trade_time}, system time: {now}, "
                f"difference: {time_diff.total_seconds():.3f} seconds."
            )
        else:
            logging.info("No trades processed yet.")
    except Exception as e:
        logging.error(f"Error writing data to disk: {e}")


def reconnect_socket(max_retries=3):
    """Attempt to reconnect if disconnected."""
    for attempt in range(1, max_retries+1):
        logging.info(f"Attempting socket reconnect... Attempt: {attempt}")
        if soket.connect():
            logging.info("Socket reconnected successfully.")
            data_request = {"Type": "T", "Symbols": ["ALL"]}
            soket.send(data_request)
            return True
        else:
            logging.warning(f"Socket reconnection failed. Attempt {attempt}/{max_retries}")
    return False


async def process_messages():
    global soket
    data_request = {"Type": "T", "Symbols": ["ALL"]}
    soket.send(data_request)
    trade_count = 0

    while True:
        try:
            if not soket.connected:
                logging.error("Socket connection lost.")
                connected = reconnect_socket()
                if not connected:
                    logging.error("Failed to reconnect after 3 attempts, exiting.")
                    exit(1)
                continue

            msg = soket.recv()
            if not msg:
                await asyncio.sleep(0.01)
                continue

            try:
                message = json.loads(msg)
                content = message.get("Content", {})
                mtype = message.get("Type", "")

                if mtype != "O" and "Market" in content and "Symbol" in content:
                    if (not TRACKED_MARKETS and not TRACKED_SYMBOLS) or \
                            (content["Market"] in TRACKED_MARKETS) or \
                            (content["Symbol"] in TRACKED_SYMBOLS):
                        process_incoming_trade(content)
                        trade_count += 1

                        if trade_count >= MAX_BUFFER_SIZE:
                            resample_and_merge_ohlcv()
                            write_ohlcv_to_disk()
                            trade_count = 0
            except Exception as e:
                logging.error(f"Error processing message: {e}, Raw: {msg}")
                soket.close()
                continue

        except Exception as e:
            if trade_count > 0:
                resample_and_merge_ohlcv()
                write_ohlcv_to_disk()
            logging.error(f"Error in main loop: {e}")
            soket.close()
            connected = reconnect_socket()
            if not connected:
                logging.error("Failed to reconnect after 3 attempts, exiting.")
                exit(1)


async def main():
    await process_messages()


if __name__ == "__main__":
    try:
        # Initialize the websocket
        soket = AlgoLabSocket(algo.api_key, algo.hash, "H")

        if not soket.connect():
            logging.error("Initial socket connection failed. Exiting.")
            exit(1)

        # Start the session refresh thread
        session_thread = start_session_refresh_thread(interval=60)

        # Run the asyncio event loop
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Unexpected error in the program: {e}")
    finally:
        # Stop the session refresh thread
        stop_session_thread = True
        if session_thread.is_alive():
            session_thread.join()

        # Save any remaining data
        if any(len(v) > 0 for v in trade_data_buffer.values()):
            resample_and_merge_ohlcv()
            write_ohlcv_to_disk()