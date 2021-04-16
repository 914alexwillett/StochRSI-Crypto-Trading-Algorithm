import websocket, json, pprint, ta
import pandas as pd
import datetime as dt
import sqlite3
import pprint

import os
import psutil

from BinanceKeys import keys
from binance.client import Client

from trade_functions import update_class, trade_update, trade_logic

# fethcing current pid and entering 
# into the pid_log, to perform the run_check  
pid = str(os.getpid())
p_info = psutil.Process(int(pid))
with open('pid_log.txt', 'a') as pid_file:
    pid_file.write(f"{p_info}\n{pid}\n")


# Binance websocket path
# streams Bitcoin and Ether price data ticks per second,
# but marks a new candlestick every 15min
SOCKET = "wss://stream.binance.com:9443/stream?streams=ethusdt@kline_15m/btcusdt@kline_15m"

#trade database path for ease of use
path = r'filepath\for\trade_database.db'

# open binance client for historical candlestick data
client = Client(keys['api_key'], keys['api_secret'], tld="us")

# Update Ethereum prices to the Database missed when the algorithm wasn't running
update_class.__init__(update_class, path)
eth_update, ETH_df = update_class.get_up_to_date(update_class, path, 2)

trade_update.__init__(trade_update, path, 2)
for index, row in eth_update.iterrows():
    trade_update.trade_algo(trade_update, row, 2)
    
# Update Bitcoin prices to the Database missed when the algorithm wasn't running
update_class.__init__(update_class, path)
btc_update, BTC_df = update_class.get_up_to_date(update_class, path, 1)

trade_update.__init__(trade_update, path, 1)
for index, row in btc_update.iterrows():
    trade_update.trade_algo(trade_update, row, 1)

print('DataFrame of Bitcoin prices newly entered', btc_update)
print('DataFrame of Ethereum prices newly entered', eth_update)
print(BTC_df)
print(ETH_df)

# Setup Connection to the sqlite3 Database before opening websocket stream
connection = sqlite3.connect(path)
cursor = connection.cursor()


# Loose functions
def convert_to_datetime(_timestamp):
    timestamp = dt.datetime.fromtimestamp(int(_timestamp)/1000)
    _date = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return _date

def recalculate_indicators(df):
    # drop old stochRSI's
    df = df.drop(['stochRSI', 'stochRSIk', 'stochRSId', 'bb_width', '200_SMA'], 1)

    # StochasticRSIOscillator
    indicator_stochRSI = ta.momentum.StochRSIIndicator(close=df.close)

    # Add StochRSI columns
    df['stochRSI'] = indicator_stochRSI.stochrsi()
    df['stochRSIk'] = indicator_stochRSI.stochrsi_k()
    df['stochRSId'] = indicator_stochRSI.stochrsi_d()

    # Bollinger Bands indicator
    indicator_bb = ta.volatility.BollingerBands(close=df.close)

    # Add Bollinger Width
    df['bb_width'] = indicator_bb.bollinger_wband()

    # 200 period Simple Moving Average indicator
    indicator_sma = ta.trend.SMAIndicator(df.close, 200)

    # Add 200SMA indicator
    df['200_SMA'] = indicator_sma.sma_indicator()

    return df

def calc_portfolio_value(_path):
    connection = sqlite3.connect(_path)
    cursor = connection.cursor()
    
    cursor.execute("""SELECT close FROM asset_prices WHERE asset_id = 1
                        ORDER BY datetime DESC LIMIT 1""")
    btc_price = cursor.fetchall()
    btc_price = btc_price[0][0]
    print('latest bitcoin price:', btc_price)
    
    cursor.execute("SELECT quantity FROM portfolio WHERE asset_id = 1")
    btc_portfolio = cursor.fetchall()
    
    btc_trading = round((btc_portfolio[0][0] * btc_price), 2) 
    btc_holding = round((btc_portfolio[1][0] * btc_price), 2)
    btc_cash = round((btc_portfolio[2][0]), 2)
    btc_total_value = round(sum([btc_trading, btc_holding, btc_cash]), 2)
        
        
    cursor.execute("""SELECT close FROM asset_prices WHERE asset_id = 2
                        ORDER BY datetime DESC LIMIT 1""")
    eth_price = cursor.fetchall()
    eth_price = eth_price[0][0]
    print('latest ethereum price:', eth_price)
    
    cursor.execute("SELECT quantity FROM portfolio WHERE asset_id = 2")
    eth_portfolio = cursor.fetchall()
    
    eth_trading = round((eth_portfolio[0][0] * eth_price), 2)
    eth_holding = round((eth_portfolio[1][0] * eth_price), 2)
    eth_cash = round((eth_portfolio[2][0]), 2)
    eth_total_value = round(sum([eth_trading, eth_holding, eth_cash]), 2)
    
    connection.commit()
    cursor.close()
    
    portfolio_value = {
        'BTC Trading':btc_trading,
        'BTC Holding':btc_holding,
        'BTC Cash':btc_cash,
        'BTC total value':btc_total_value,
        'ETH Trading':eth_trading,
        'ETH Holding':eth_holding,
        'ETH Cash':eth_cash,
        'ETH total value':eth_total_value
    }
    
    return portfolio_value

""" Stream BTC and ETH Prices and Trade Accordingly """

def on_open(ws):
    print('connection opened')
    
def on_close(ws):
    print('connection closed')
    
def on_message(ws, message):
    global ETH_df
    global BTC_df
    # convert JSON message into a python dictionary
    json_message = dict(json.loads(message))
    
    # single out the stream_id to handle the different incoming coin data
    coin_type = json_message['stream']
    
    # single out the data from the string_id
    data = json_message['data']
    
    # single out the raw datetime data and convert to string format
    date_time = convert_to_datetime(data['E'])
    
    # single out the candlestick data
    candle = data['k']

    # condition to handle when next candlestick is formed
    is_candle_closed = candle['x']
    
    if is_candle_closed: 
        print('candle closed')
        # single out the candlestick closing_price
        close = float(candle['c'])
        
        # handle ETH data into the database
        if coin_type == 'ethusdt@kline_15m':
            eth_close = close
            eth_datetime = date_time
            
            # add price data into the database
            cursor.execute("""INSERT OR IGNORE INTO asset_prices (asset_id, datetime, close) VALUES (?, ?, ?)
            """, (2, eth_datetime, eth_close))
            connection.commit()
            print('ETH price inserted')
            ETH_df.loc[eth_datetime] = [eth_close, 0, 0, 0, 0, 0]
            ETH_df = recalculate_indicators(ETH_df)
            print(ETH_df[-5:])
            new_row_eth = ETH_df[-1:].copy()
            print('row in question:', new_row_eth)
            
            # algorithm logic to make trade decisions for buying and selling
            trade_logic.__init__(trade_logic, path, 2)
            
            for index, row in new_row_eth.iterrows():
                trade_logic.trade_algo(trade_logic, row, 2)
            
        elif coin_type == 'btcusdt@kline_15m':
            btc_close = close
            btc_datetime = date_time
            
            # add price data into the database
            cursor.execute("""INSERT OR IGNORE INTO asset_prices (asset_id, datetime, close) VALUES (?, ?, ?)
            """, (1, btc_datetime, btc_close))
            connection.commit()
            print('BTC price inserted')
            BTC_df.loc[btc_datetime] = [btc_close, 0, 0, 0, 0, 0]
            BTC_df = recalculate_indicators(BTC_df)
            print(BTC_df[-5:])
            new_row_btc = BTC_df[-1:].copy()
            print('row in question:', new_row_btc)
            
            # algorithm logic to make trade decisions for buying and selling
            trade_logic.__init__(trade_logic, path, 1)
                
            for index, row in new_row_btc.iterrows():
                trade_logic.trade_algo(trade_logic, row, 1)

                
ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
ws.run_forever()

cursor.close()
print('Database closed')

# after closing the websocket display 
# current portfolio values using the most recent asset prices
portfolio_values = calc_portfolio_value(path)
print(portfolio_values)

# newly calculated portfolio values added to a seperate
# txt file stamped with the time the webscoket was closed
with open('order_message_log.txt', 'a') as  close_entry:
    close_entry.write(f"""{dt.datetime.now()}\n{portfolio_values}\n==============================================\n""")
    