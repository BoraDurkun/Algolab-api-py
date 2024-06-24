# -*- coding: utf-8 -*-
import json,os,asyncio,logging,schedule,pandas as pd
from algolab import API
from ws import *
from config import *

logging.basicConfig(filename="converter_logs.txt"
                    ,level=logging.INFO)

columns = ['Date', 'Price', 'Volume', 'Symbol', 'Market']
buffered_data = []
algo = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True, keep_alive=False)
def session_refresh():
    try:
        data_request = {"Type": "H"}
        soket.send(data_request)
        algo.SessionRefresh()
    except Exception as e:
        logging.error(f"Heartbeat Hatası: {e}")

async def write_to_file(data, symbol_received, market_received):
    append_to_existing_data(data, symbol_received, market_received)
    
async def process_buffered_data():
    for data_item in buffered_data:
        await write_to_file(*data_item)
    buffered_data.clear()

def append_to_existing_data(ohlcv, symbol_received, market_received):
    try:
        candles_folder_path = os.path.join("db", "candles", market_received)
        os.makedirs(candles_folder_path, exist_ok=True)
        candles_path = os.path.join(candles_folder_path, f"{symbol_received}.json")
        if os.path.exists(candles_path):
            with open(candles_path, 'r') as f:
                existing_data = pd.read_json(f)
                
            # Ensure data types match
            ohlcv = ohlcv.astype(existing_data.dtypes.to_dict())
            # Check if there is already data for the given date
            idx = existing_data[existing_data['Date'] == ohlcv['Date'].iloc[0]].index
            
            if not idx.empty:
                for column in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    if column == 'High':
                        existing_data.at[idx[0], column] = max(existing_data.at[idx[0], column], ohlcv[column].iloc[0])
                    elif column == 'Low':
                        existing_data.at[idx[0], column] = min(existing_data.at[idx[0], column], ohlcv[column].iloc[0])
                    elif column == 'Close':
                        existing_data.at[idx[0], column] = ohlcv[column].iloc[0]
                    elif column == 'Volume':
                        existing_data.at[idx[0], column] += ohlcv[column].iloc[0]
                complete_data = existing_data
            else:
                complete_data = pd.concat([existing_data, ohlcv], ignore_index=True)
        else:
            complete_data = ohlcv
        with open(candles_path, 'w') as f:
            json.dump(complete_data.to_dict(orient='records'), f, indent=4, default=str)
            f.write('\n')
    except Exception as e:
        print("Error processing:", e)
        print("Existing data:")
        print(existing_data.head())
        print("\nNew OHLCV data:")
        print(ohlcv.head())

def process_data(content):
    try:
        df_temp = pd.DataFrame(columns=columns)
        volume = content["Price"] * content["TradeQuantity"]
        parsed_date = pd.to_datetime(content["Date"])
        if parsed_date.tzinfo is None or parsed_date.tzinfo.utcoffset(parsed_date) is None:
            date = parsed_date.tz_localize('UTC').tz_convert('Europe/Istanbul')
        else:
            date = parsed_date.tz_convert('Europe/Istanbul')
        price = content["Price"]
        symbol_received = content["Symbol"]
        market_received = content["Market"]

        df_temp.loc[0] = [date, price, volume, symbol_received, market_received]
        df_temp.set_index('Date', inplace=True)
        
        ohlcv_resampled = df_temp.resample('1min').agg({
            'Price': ['first', 'max', 'min', 'last'],
            'Volume': 'sum'
        })
        ohlcv_resampled.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        ohlcv_resampled['Date'] = ohlcv_resampled.index.strftime('%Y-%m-%dT%H:%M:%S.000')
        return ohlcv_resampled, symbol_received, market_received
    except Exception as e:
        print("Error processing:", e)

if __name__ == "__main__":
    algo.start()
    soket = AlgoLabSocket(algo.api_key, algo.hash, "H")
    if not soket.connect():
        logging.error("Failed to establish socket connection after multiple retries.")
        exit()
    schedule.every(10).minutes.do(session_refresh)
    data_request = {"Type": "T", "Symbols": ["ALL"]}
    soket.send(data_request)
    loop = asyncio.get_event_loop()
    while soket.connected:
        try:
            schedule.run_pending()
            data = soket.recv()       
            if data:
                try:
                    msg = json.loads(data)
                    content = msg["Content"]
                    type = msg["Type"]
                    if type != "O" and "Market" in content and "Symbol" in content:
                        if (not TRACKED_MARKETS and not TRACKED_SYMBOLS) or \
                        (content["Market"] in TRACKED_MARKETS) or \
                        (content["Symbol"] in TRACKED_SYMBOLS):
                            ohlcv_resampled, symbol_received, market_received = process_data(content)
                            buffered_data.append((ohlcv_resampled, symbol_received, market_received))
                            if len(buffered_data) >= BUFFER_SIZE:
                                loop.run_until_complete(process_buffered_data())
                except Exception as e:
                    print(f"Error processing message with data: {data}", e)  # Include raw data in error message
                    soket.close()
                    print("Socket closed")
                    break
        except Exception as e:
            logging.error(f"Ana döngüde hata: {e}")
    if buffered_data:
        loop.run_until_complete(process_buffered_data())