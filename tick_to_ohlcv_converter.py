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
import sys

logging.basicConfig(
    filename="converter_logs.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

# Constants
RESAMPLE_INTERVAL = '1min'  # OHLCV verilerinin yeniden örnekleme aralığı
MAX_BUFFER_SIZE = BUFFER_SIZE if 'BUFFER_SIZE' in globals() else 100  # Maksimum tampon boyutu
last_trade_time = None  # Son işlem zamanını takip etmek için
stop_session_thread = False  # Oturum yenileme thread'ini kontrol etmek için
first_trade_processed = False  # İlk işlem işlenip işlenmediğini takip etmek için
first_log_shown = False  # İlk log gösterildi mi kontrolü için

# Initialize API and websocket
algo = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True, keep_alive=False)
soket = None

# In-memory data stores
trade_data_buffer = {}  # İşlem verilerini geçici olarak saklar. Format: (sembol, piyasa) -> [(tarih, fiyat, miktar),...]
ohlcv_cache = {}  # OHLCV verilerini önbellekte tutar. Format: (sembol, piyasa) -> DataFrame


def session_refresh():
    """API oturumunu ve soket bağlantısını yeniler."""
    global soket
    try:
        logging.info("Starting session refresh...")
        
        if not soket.connected:
            logging.warning("Socket is not connected during session refresh.")
            return False

        success = algo.SessionRefresh()
        if not success:
            logging.error("API session refresh failed")
            return False

        data_request = {"Type": "H"}
        if not soket.send(data_request):
            logging.error("Failed to send heartbeat")
            return False

        logging.info("Session refresh completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Session refresh error: {e}")
        return False


def load_or_create_ohlcv(symbol, market):
    """
    Belirtilen sembol ve piyasa için OHLCV verilerini yükler veya yeni oluşturur.
    """
    key = (symbol, market)
    if key in ohlcv_cache:
        return ohlcv_cache[key]

    candles_folder_path = os.path.join("db", "candles", market)
    os.makedirs(candles_folder_path, exist_ok=True)
    candles_path = os.path.join(candles_folder_path, f"{symbol}.json")

    if os.path.exists(candles_path):
        try:
            # Dosya boyutunu kontrol et
            if os.path.getsize(candles_path) == 0:
                logging.warning(f"Empty file found: {candles_path}, creating new DataFrame")
                raise ValueError("Empty file")

            with open(candles_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not data:  # Boş liste kontrolü
                raise ValueError("Empty JSON data")
                
            df = pd.DataFrame(data)
            
            # Gerekli sütunların varlığını kontrol et
            required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            if not all(col in df.columns for col in required_columns):
                raise ValueError("Missing required columns")

        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"Invalid JSON in {candles_path}, creating an empty DataFrame. Error: {e}")
            # Bozuk dosyayı yedekle
            if os.path.exists(candles_path):
                backup_path = candles_path + '.bak'
                try:
                    os.rename(candles_path, backup_path)
                    logging.info(f"Backed up corrupted file to {backup_path}")
                except Exception as e:
                    logging.error(f"Failed to backup corrupted file: {e}")
            
            df = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    else:
        df = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

    try:
        # Ensure correct dtypes
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        df.set_index("Date", inplace=True)
        
        # NaN değerleri temizle
        df = df.dropna(how='all')  # Tüm değerleri NaN olan satırları sil
        df.fillna(0, inplace=True)  # Kalan NaN değerleri 0'a çevir
        
    except Exception as e:
        logging.error(f"Error processing DataFrame for {symbol}-{market}: {e}")
        df = pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])
        df.set_index("Date", inplace=True)

    ohlcv_cache[key] = df
    return df


def process_incoming_trade(content):
    """
    Gelen işlem verilerini işler ve tampona ekler.
    İlk çalıştığında bir sonraki periyodun başlangıcını bekler.
    
    Args:
        content (dict): İşlem verilerini içeren sözlük
    """
    global last_trade_time, first_log_shown

    try:
        symbol = content["Symbol"]
        market = content["Market"]
        price = float(content["Price"] or 0)
        qty = float(content["TradeQuantity"] or 0)

        # Parse as UTC
        timestamp = pd.to_datetime(content["Date"], utc=True)
        # Convert to Europe/Istanbul time
        timestamp = timestamp.tz_convert('Europe/Istanbul')
        # Convert to tz-naive
        timestamp = timestamp.tz_localize(None)

        # İlk veri için bir sonraki periyodun başlangıç zamanını hesapla
        if last_trade_time is None:
            # Her zaman bir sonraki dakikaya yuvarla
            next_period = timestamp.ceil('1min')
            if not first_log_shown:
                logging.info(f"Waiting for next period: {next_period}")
                first_log_shown = True
            
            # Eğer şu anki veri bu periyoddan önceyse, kaydetme
            if timestamp < next_period:
                return
            
            last_trade_time = timestamp

        # Update last trade time
        last_trade_time = timestamp

        key = (symbol, market)
        if key not in trade_data_buffer:
            trade_data_buffer[key] = []
        trade_data_buffer[key].append((timestamp, price, qty))
        
    except Exception as e:
        logging.error(f"Error processing trade: {e}, content: {content}")

def resample_and_merge_ohlcv():
    """
    Tamponda biriken işlem verilerini OHLCV formatına dönüştürür.
    
    İşlemler:
    1. Her sembol-piyasa çifti için:
        - İşlem verilerini DataFrame'e dönüştürür
        - Belirtilen aralıkta yeniden örnekler (OHLCV)
        - Mevcut verilerle birleştirir
    2. İşlem tamponunu temizler
    """
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
                "Volume": "sum",
                "TradeQuantity": "sum"
            })
            ohlcv_resampled.columns = ["Open", "High", "Low", "Close", "Volume", "Amount"]

            # Merge with existing OHLCV
            ohlcv_df = load_or_create_ohlcv(symbol, market)
            
            # Amount sütununu kontrol et ve gerekirse oluştur
            if 'Amount' not in ohlcv_df.columns:
                ohlcv_df['Amount'] = 0.0

            for idx, row in ohlcv_resampled.iterrows():
                if idx in ohlcv_df.index:
                    # Update row
                    ohlcv_df.at[idx, 'High'] = max(ohlcv_df.at[idx, 'High'], row['High'])
                    ohlcv_df.at[idx, 'Low'] = min(ohlcv_df.at[idx, 'Low'], row['Low'])
                    ohlcv_df.at[idx, 'Close'] = row['Close']
                    ohlcv_df.at[idx, 'Volume'] = round(ohlcv_df.at[idx, 'Volume'] + row['Volume'], 2)
                    ohlcv_df.at[idx, 'Amount'] = round(ohlcv_df.at[idx, 'Amount'] + row['Amount'], 2)
                else:
                    # Add new row
                    ohlcv_df.loc[idx] = row

            ohlcv_df.sort_index(inplace=True)
        except Exception as e:
            logging.error(f"Error processing data for {symbol}-{market}: {e}")

    trade_data_buffer.clear()


def write_ohlcv_to_disk():
    """
    OHLCV verilerini diske JSON formatında kaydeder.
    
    İşlemler:
    1. Her sembol-piyasa çifti için:
        - Eksik sütunları kontrol eder ve doldurur
        - NaN değerleri temizler
        - Tarih formatını düzenler
        - JSON dosyasına kaydeder
    2. Son işlem zamanını loglar
    """
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
                    logging.warning(f"Missing column '{col}' in OHLCV for {symbol}-{market}. Filling with 0. Content: {df}")
                    df[col] = 0  # Eksik sütunları sıfırlarla doldur

            # Kontrol: NaN değerleri tespit edip doldur
            if df.isnull().values.any():
                logging.warning(f"NaN values detected in OHLCV for {symbol}-{market}. Applying corrections. Content: {df}")
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


async def reconnect_socket(max_retries=3):
    """
    Soket bağlantısını yeniden kurmaya çalışır.
    
    Args:
        max_retries (int): Maksimum deneme sayısı
    
    Returns:
        bool: Bağlantı başarılı ise True, değilse False
    """
    global soket, algo
    for attempt in range(1, max_retries+1):
        try:
            logging.info(f"Attempting socket reconnect... Attempt: {attempt}")
            algo.SessionRefresh()
            soket = AlgoLabSocket(algo.api_key, algo.hash, "H")
            if soket.connect():
                logging.info("Socket reconnected successfully.")
                data_request = {"Type": "T", "Symbols": ["ALL"]}
                soket.send(data_request)
                return True
            await asyncio.sleep(2)
        except Exception as e:
            logging.error(f"Reconnection attempt {attempt} failed: {e}")
            await asyncio.sleep(5)
            
    logging.error("All reconnection attempts failed")
    return False


async def process_messages():
    """Ana mesaj işleme döngüsü."""
    global soket
    data_request = {"Type": "T", "Symbols": ["ALL"]}
    soket.send(data_request)
    trade_count = 0
    last_write_time = time.time()
    last_heartbeat_time = time.time()
    heartbeat_interval = 30  # 30 saniyede bir kontrol et

    while True:
        try:
            current_time = time.time()
            
            # Soket bağlantısını düzenli kontrol et
            if current_time - last_heartbeat_time > heartbeat_interval:
                if not soket.connected:
                    logging.error(f"Socket connection lost at {datetime.now()}")
                    if not await reconnect_socket():
                        logging.error("Failed to reconnect after socket loss")
                        if trade_count > 0:
                            resample_and_merge_ohlcv()
                            write_ohlcv_to_disk()
                        return False
                else:
                    logging.info("Socket connection is alive")  # Bağlantı durumu log
                last_heartbeat_time = current_time

            # Her 5 dakikada bir verileri diske yaz
            if current_time - last_write_time > 300:
                if trade_count > 0:
                    resample_and_merge_ohlcv()
                    write_ohlcv_to_disk()
                    trade_count = 0
                last_write_time = current_time

            msg = soket.recv()
            if not msg:
                await asyncio.sleep(0.001)
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
            logging.error(f"Error in main loop at {datetime.now()}: {e}")
            await asyncio.sleep(1)


async def main():
    """Ana program döngüsü."""
    # Session refresh thread'ini başlat
    session_thread = threading.Thread(target=threaded_session_refresh)
    session_thread.daemon = True
    session_thread.start()
    
    try:
        # Ana mesaj işleme döngüsünü çalıştır
        await process_messages()
    finally:
        # Program sonlandığında thread'i durdur
        global stop_session_thread
        stop_session_thread = True
        session_thread.join(timeout=2)  # Thread'in durmasını bekle


def threaded_session_refresh():
    """Thread olarak çalışan session refresh fonksiyonu"""
    global stop_session_thread
    while not stop_session_thread:
        try:
            session_refresh()
            time.sleep(120)  # 60 saniye bekle
        except Exception as e:
            logging.error(f"Error in session refresh thread: {e}")
            time.sleep(5)  # Hata durumunda 5 saniye bekle


if __name__ == "__main__":
    """
    Program başlangıç noktası.
    
    İşlemler:
    1. Soket bağlantısını kurar
    2. Bağlantı başarısız olursa 3 kez dener
    3. Ana döngüyü başlatır
    4. Hata durumunda kalan verileri kaydeder
    """
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            # Initialize the websocket
            soket = AlgoLabSocket(algo.api_key, algo.hash, "H")

            if not soket.connect():
                logging.error("Initial socket connection failed.")
                retry_count += 1
                time.sleep(5)
                continue

            # Run the asyncio event loop
            asyncio.run(main())
            break

        except Exception as e:
            logging.error(f"Unexpected error in the program: {e}")
            retry_count += 1
            time.sleep(5)
            
        finally:
            # Save any remaining data
            if any(len(v) > 0 for v in trade_data_buffer.values()):
                resample_and_merge_ohlcv()
                write_ohlcv_to_disk()

    if retry_count >= max_retries:
        logging.error("Maximum retry attempts reached. Exiting program.")
        sys.exit(1)