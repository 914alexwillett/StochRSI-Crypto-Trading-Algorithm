import pandas as pd
import datetime as dt
import sqlite3
import ta
import numpy as np
import pprint
import random

from BinanceKeys import keys
from binance.client import Client

# class of functions run prior to connecting to the Binance price data websocket
class update_class:
    def __init__(self, _DB_path):
        # open binance client for historical klines
        self.client = Client(keys['api_key'], keys['api_secret'])
        self.path = _DB_path
    
    def setup_indicators(self, df):
        # StochasticRSIOscillator
        indicator_stochRSI = ta.momentum.StochRSIIndicator(close=df.close)

        # Add StochRSI columns
        df['stochRSI'] = indicator_stochRSI.stochrsi()
        df['stochRSIk'] = indicator_stochRSI.stochrsi_k()
        df['stochRSId'] = indicator_stochRSI.stochrsi_d()

        # Bollinger Bands indicator
        indicator_bb = ta.volatility.BollingerBands(close = df.close)

        # Add Bollinger Width
        df['bb_width'] = indicator_bb.bollinger_wband()
        
        # 200 period Simple Moving Average indicator
        indicator_sma = ta.trend.SMAIndicator(df.close, 200)
        
        # Add 200SMA indicator
        df['200_SMA'] = indicator_sma.sma_indicator()

        return df

    def add_indicators(self, df):
        # drop old stochRSI's
        df = df.drop(['stochRSI', 'stochRSIk', 'stochRSId', 'bb_width', '200_SMA'], 1)

        # StochasticRSIOscillator
        indicator_stochRSI = ta.momentum.StochRSIIndicator(close=df.close)

        # Add StochRSI columns
        df['stochRSI'] = indicator_stochRSI.stochrsi()
        df['stochRSIk'] = indicator_stochRSI.stochrsi_k()
        df['stochRSId'] = indicator_stochRSI.stochrsi_d()

        # Bollinger Bands indicator
        indicator_bb = ta.volatility.BollingerBands(close = df.close)

        # Add Bollinger Width
        df['bb_width'] = indicator_bb.bollinger_wband()
                
        # 200 period Simple Moving Average indicator
        indicator_sma = ta.trend.SMAIndicator(df.close, 200)
        
        # Add 200SMA indicator
        df['200_SMA'] = indicator_sma.sma_indicator()

        return df
    
    def convert_to_datetime(_timestamp):
        timestamp = dt.datetime.fromtimestamp(int(_timestamp)/1000)
        _date = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return _date

    def get_now(self):
        now_raw = dt.datetime.now()
        now = int(now_raw.timestamp() * 1000)
        return now

    def reset_database(self, _DB_path, _asset_id):
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()

        cursor.execute("DELETE FROM buy_log")
        cursor.execute("DELETE FROM sell_log")
        cursor.execute("DELETE FROM transaction_history")
        cursor.execute("DELETE FROM transaction_history_missed")
        cursor.execute(f"UPDATE trade_status SET buy_position = 0 WHERE asset_id = {_asset_id}")
        cursor.execute(f"UPDATE trade_status SET sell_position = 0 WHERE asset_id = {_asset_id}")

        # setting portfolio values to arbitrary starting values
        if _asset_id == 1:
            cursor.execute("UPDATE portfolio SET quantity = 0.01 WHERE id = 1")
            cursor.execute("UPDATE portfolio SET quantity = 0.001 WHERE id = 2")
            cursor.execute("UPDATE portfolio SET quantity = 300 WHERE id = 5")

        if _asset_id == 2:
            cursor.execute("UPDATE portfolio SET quantity = 0.01 WHERE id = 3")
            cursor.execute("UPDATE portfolio SET quantity = 0.001 WHERE id = 4")
            cursor.execute("UPDATE portfolio SET quantity = 300 WHERE id = 6")

        connection.commit()
        cursor.close()
        print('database is reset')

    def get_up_to_date(self, _DB_path, _asset_id):
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        
        # fetches specifically the last 250 values to calculate 200 day moving average 
        # along with the other indicators
        
        cursor.execute(f"SELECT datetime, close FROM asset_prices WHERE asset_id = {_asset_id} ORDER BY datetime DESC LIMIT 250")
        last_250 = cursor.fetchall()
        last_250_df = pd.DataFrame(last_250)

        last_250_df = last_250_df.reindex(index=last_250_df.index[::-1]).reset_index(drop=True)
        last_250_df.columns = ['datetime', 'close']
        last_entry = last_250_df.iloc[-1,:]
        cutoff = (last_entry.name)

        now = update_class.get_now(update_class)
        last_datetime = int(dt.datetime.strptime(last_entry[0], '%Y-%m-%d %H:%M:%S').timestamp()) * 1000

        if _asset_id == 1:
            coin = 'BTCUSDT'

        elif _asset_id == 2:
            coin = 'ETHUSDT'

        update_df = pd.DataFrame(self.client.get_historical_klines(coin, '15m', last_datetime, now))
        print(update_df)

        updt_df = update_df[[0,4]].copy()
        updt_df.columns = ['datetime', 'close']

        for index, row in updt_df.iterrows():
            updt_df.loc[index, 'datetime'] = convert_to_datetime(row.datetime)

        updt_df['close'] = round(updt_df.close.astype(float),2)
        # dropping first row as to not have it repeated, when appended to last_250_df 
        updt_df = updt_df.drop([0], axis=0)

        for index, row in updt_df.iterrows():
            date = row['datetime']
            close = row['close']
            cursor.execute("""INSERT OR IGNORE INTO asset_prices (asset_id, datetime, close) VALUES (?, ?, ?)
            """, (_asset_id, date, close))

        connection.commit()
        cursor.close()

        final_df = last_250_df.append(updt_df, ignore_index=True)
        final_df = update_class.setup_indicators(update_class, final_df)
        
        df = final_df.loc[cutoff:].copy()
        df = df.set_index('datetime')        
        final_df = final_df.set_index('datetime')
        
        return df, final_df

###### RUN TRADE ALGORITHM WITH THE UPDATED DATA #######
##### LOOSE FUNCTIONS ######

def retrieve_trade_status(_path, _asset_id):
    connection = sqlite3.connect(_path)
    cursor = connection.cursor()
    cursor.execute(f"""SELECT buy_position, sell_position FROM trade_status WHERE asset_id = {_asset_id}""")
    # returns (buy_position, sell_position) in that order
    trade_status = cursor.fetchall()
    connection.commit()
    cursor.close()
    trade_status = trade_status[0]
    return trade_status

def retrieve_portfolio(_path, _asset_id):
    connection = sqlite3.connect(_path)
    cursor = connection.cursor()
    cursor.execute(f"""SELECT asset_name, quantity FROM portfolio WHERE asset_id = {_asset_id}""")
    portfolio = cursor.fetchall()
    connection.commit()
    cursor.close()
    return portfolio

def retrieve_buy_log(_path, _asset_id):
    connection = sqlite3.connect(_path)
    cursor = connection.cursor()
    cursor.execute(f"""SELECT datetime, cost, quantity, price FROM buy_log WHERE asset_id = {_asset_id}""")
    buy_log = cursor.fetchall()
    connection.commit()
    cursor.close()
    return buy_log

def convert_to_datetime(_timestamp):
    timestamp = dt.datetime.fromtimestamp(int(_timestamp)/1000)
    _date = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return _date

# function to get the average cost of recent purchases
def get_avg_cost(_buy_log):
    try:
        cost_sum = sum([item[1] for item in _buy_log])
        quant_sum = sum([item[2] for item in _buy_log])
        avg_cost = cost_sum / quant_sum
        
    except:
        avg_cost = 0
    
    return avg_cost

# fetches relevant portfolio values from binance account
# as opposed to the SQLite database to ensure 
# the values are up to date and accurate
def get_binance_balances():
    client = Client(keys['api_key'], keys['api_secret'], tld="us")
    eth_balance = client.get_asset_balance('ETH')
    btc_balance = client.get_asset_balance('BTC')
    usd_balance = client.get_asset_balance('USD')
    
    balances = {'ETH': float(eth_balance['free']),
               'BTC': float(btc_balance['free']),
               'USD': float(usd_balance['free'])}
    
    return balances

# syncs local SQLite Database with 
def sync_with_binance_account(_path):
    connection = sqlite3.connect(_path)
    cursor = connection.cursor()
    
    balances = get_binance_balances()
    cursor.execute(f"SELECT quantity FROM portfolio WHERE asset_id = 1")
    btc_portfolio = cursor.fetchall()
    btc_trading_to_hold_ratio = btc_portfolio[0][0] / (btc_portfolio[0][0] + btc_portfolio[1][0])    
    new_btc_trading_quantity = (btc_trading_to_hold_ratio * balances['BTC'])
    new_btc_hold_quantity = (1 - btc_trading_to_hold_ratio) * balances['BTC']
    # check to ensure the ratios were calculated properly
    assert (balances['BTC'] - (new_btc_trading_quantity + new_btc_hold_quantity)) < 0.00001
    
    cursor.execute(f"SELECT quantity FROM portfolio WHERE asset_id = 2")
    eth_portfolio = cursor.fetchall()
    eth_trading_to_hold_ratio = eth_portfolio[0][0] / (eth_portfolio[0][0] + eth_portfolio[1][0])    
    new_eth_trading_quantity = eth_trading_to_hold_ratio * balances['ETH']
    new_eth_hold_quantity = (1 - eth_trading_to_hold_ratio) * balances['ETH']
    # check to ensure the ratios were calculated properly
    assert (balances['ETH'] - (new_eth_trading_quantity + new_eth_hold_quantity)) < 0.00001
    
    btc_to_eth_cash_ratio =  btc_portfolio[2][0] / (btc_portfolio[2][0] + eth_portfolio[2][0])
    btc_cash_portion = btc_to_eth_cash_ratio * balances['USD']
    eth_cash_portion = (1 - btc_to_eth_cash_ratio) * balances['USD']
    portions_total = eth_cash_portion + btc_cash_portion
    # check to ensure the ratios were calculated properly
    assert (balances['USD'] - portions_total) < 0.00001
    
    cursor.execute(f"UPDATE portfolio SET quantity = {new_btc_trading_quantity} WHERE id = 1")
    cursor.execute(f"UPDATE portfolio SET quantity = {new_btc_hold_quantity} WHERE id = 2")
    cursor.execute(f"UPDATE portfolio SET quantity = {new_eth_trading_quantity} WHERE id = 3")
    cursor.execute(f"UPDATE portfolio SET quantity = {new_eth_hold_quantity} WHERE id = 4")
    cursor.execute(f"UPDATE portfolio SET quantity = {btc_cash_portion} WHERE id = 5")
    cursor.execute(f"UPDATE portfolio SET quantity = {eth_cash_portion} WHERE id = 6")
    
    print('database updated')
    
    connection.commit()
    cursor.close()

class trade_update:
    def __init__(self, _DB_path, _asset_id):
        self.DB_path = _DB_path
        self.buy_log = retrieve_buy_log(self.DB_path, _asset_id)
        sync_with_binance_account(self.DB_path)
        self.portfolio = retrieve_portfolio(self.DB_path, _asset_id)
        self.trading_account = self.portfolio[0][1]
        self.cash = self.portfolio[2][1]
        self.hold = self.portfolio[1][1]
        self.avg_cost = get_avg_cost(self.buy_log)
        self.trade_status = retrieve_trade_status(self.DB_path, _asset_id)
        self.buy_position = self.trade_status[0]
        self.sell_position = self.trade_status[1]
        
        if _asset_id == 1:
            self.symbol = 'BTCUSD'
        elif _asset_id == 2:
            self.symbol = 'ETHUSD'
        
        pprint.pprint(self.buy_log)
        pprint.pprint(self.portfolio)
        print(self.trading_account)
        print(self.cash)
        print(self.hold)
        print(self.symbol)
        print(self.avg_cost)
        print(self.buy_position)
        print(self.sell_position)
        print(self.DB_path)
        
    def test_buy_order(self, _amount, _price, _symbol, _index):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            buy_order = client.create_test_order(
                symbol= _symbol,
                side = Client.SIDE_BUY,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount)

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} BUY {_symbol} \n\n ERROR OCCURED {e} \n\n')
        
        date_int = int(dt.datetime.strptime(_index, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        qty = str(_amount)
        price = str(_price)
        cost = str(_price * _amount)
        fee = str(_amount * .001) # commission in for sales
        orderId_ = 111100000 + random.randint(0, 100000)
        
        
        # generate fake json messsage dict that would be returned normally

        message = {'symbol': _symbol,
                    'orderId': orderId_,
                    'orderListId': -1,
                    'clientOrderId': 'yn6zqWnIWasfdfstq8yLQjtWK',
                    'transactTime': date_int,
                    'price': price,
                    'origQty': qty,
                    'executedQty': qty,
                    'cummulativeQuoteQty': cost,
                    'status': 'MISSED',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'BUY',
                    'fills': [{'price': price,
                                'qty': qty,
                                'commission': fee,
                                'commissionAsset': _symbol[:3],
                                'tradeId': 2424209}]}
        
        return message
    
    def test_sell_order(self, _amount, _price, _symbol, _index):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            sell_order = client.create_test_order(
                symbol= _symbol,
                side = Client.SIDE_SELL,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount
            )

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} SELL {_symbol} \n\n ERROR OCCURED {e} \n\n')
        
        date_int = int(_index.timestamp()*1000)
        qty = str(_amount)
        price = str(_price)
        sell_return = str(_price * _amount)
        fee = str((_price * _amount) * .001) # commission in USD for sales
        orderId_ = 111100000 + random.randint(0, 100000)
        
        # generate fake json messsage dict that get's returned normally

        message = {'symbol': _symbol,
                    'orderId': orderId_,
                    'orderListId': -1,
                    'clientOrderId': 'yn6zqWnIW8nqtq8yLQjtWK',
                    'transactTime': date_int,
                    'price': price,
                    'origQty': qty,
                    'executedQty': qty,
                    'cummulativeQuoteQty': sell_return,
                    'status': 'MISSED',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'SELL',
                    'fills': [{'price': price,
                                'qty': qty,
                                'commission': fee,
                                'commissionAsset': _symbol[3:],
                                'tradeId': 2424209}]}
        
        return message
    
    def process_order_message(self, _message, _asset_id):
        date_ = convert_to_datetime(_message['transactTime'])
        price_  = float(_message['fills'][0]['price'])
        quantity_ = _message['fills'][0]['qty']
        fee_ = _message['fills'][0]['commission']
        buy_sell_ = _message['side']
        print(date_, price_, quantity_, fee_, buy_sell_, _asset_id)
        
        # update missed trade opportunities into the Database for further inquiry
        trade_update.update_transaction_history_missed(trade_update, self.DB_path, date_, price_, quantity_, fee_, buy_sell_, _asset_id)
        print('transaction history updated')
        
        # enters missed trade opportunities into order message log, labeled and timestamped
        # using the fake JSON lke order message, generated by the test buy/sell orders
        with open('order_message_log.txt', 'a') as message_entry:
            message_entry.write(f'MISSED TRANSACTION:\n{date_} {buy_sell_}\n\n {_message}\n\n')

        
    # function to get the dollar cost average of recent purchases
    def get_avg_cost(self, _buy_log):
        try:
            cost_sum = sum([item[1] for item in _buy_log])
            quant_sum = sum([item[2] for item in _buy_log])
            avg_cost = cost_sum / quant_sum
        except:
            avg_cost = 0
            
        return avg_cost
        
    def update_transaction_history_missed(self, _DB_path, _date, _price, _quantity, _fee, _buy_sell, _asset_id):
        print('UPDATING TRANSACTION HISTORY MISSED LOG', _date, _price, _quantity, _fee, _buy_sell, _asset_id)
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        date = _date
        price = _price
        quantity = _quantity
        fee = _fee
        buy_sell = _buy_sell
        
        if _asset_id == 1:
            asset_name = 'Bitcoin'
            
        elif _asset_id == 2:
            asset_name = 'Ethereum'
        
        # transaction (datetime, price, quantity, buy_sell (buy or sell), asset_id, asset_name))
        cursor.execute("""INSERT OR IGNORE INTO transaction_history_missed (datetime, price, quantity, fee, buy_sell, asset_id, asset_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",(date, price, quantity, fee, buy_sell, _asset_id, asset_name))
        
        connection.commit()
        cursor.close()
        
    def update_database(self, _DB_path, _var_change, _asset_id):
        # _var_change = (date, cash, portfolio, buy_position, sell_position, buy_log(list))
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        
        if _var_change[4] == True:
            buy_position = 1
        elif _var_change[4] == False:
            buy_position = 0
            
        if _var_change[5] == True:
            sell_position = 1
        elif _var_change[5] == False:
            sell_position = 0
            
        cursor.execute(f"UPDATE trade_status SET buy_position = {buy_position} WHERE id = {_asset_id}")
        cursor.execute(f"UPDATE trade_status SET sell_position = {sell_position} WHERE id = {_asset_id}")
        
        connection.commit()
        cursor.close()
        
        
##### TRADE ALGO TO TEST MISSED OPPORTUNITIES AND UPDATE BUY AND SELL POSITIONS #####
    def trade_algo(self, _row, _asset_id):
        index = _row.name
        self.buy_log = retrieve_buy_log(self.DB_path, _asset_id)
        buy_log_len = len(self.buy_log)
        self.portfolio = retrieve_portfolio(self.DB_path, _asset_id)
        avg_cost = self.get_avg_cost(trade_update, self.buy_log)
        price = _row['close']
        
        if self.buy_position and _row['stochRSI'] > .50:
            self.buy_position = False
        
        # SELL ORDER
        if (self.trading_account > 0) and (buy_log_len > 0):
            if self.sell_position and _row['stochRSId'] < .50:
                self.sell_position = False
                
            if not self.sell_position and (_row['stochRSIk'] > _row['stochRSId']) and  (_row['stochRSId'] > .80):
                self.sell_position = True
                
            if self.sell_position and (_row['stochRSIk'] < _row['stochRSId']) and (_row['stochRSId'] > .80) and (price > _row['200_SMA']):
                profit_ratio = 1 - (avg_cost / price)
                
                if profit_ratio > 0.05:
                    sell_quantity = round(.5 * (self.trading_account), 5)
                    returns = (sell_quantity * price)
                    profit = returns - (avg_cost * sell_quantity)
                    
                    if returns > 10:
                        print('%%% ###### SELL SELL SELL ##### %%%', index)
                        print(' ** PROFIT RATIO: ', profit_ratio)
                        _sell_order = trade_update.test_sell_order(trade_update, sell_quantity, price, self.symbol, index)
                        trade_update.process_order_message(trade_update, _sell_order, _asset_id)
#                         trade_update.update_sell_log(trade_update, self.DB_path, index, price, sell_quantity, profit, profit_ratio, avg_cost, _asset_id)
                        self.sell_position = False

                        # clear and repopulate buy_log with remaining quantity in the trading account and dollar avarage cost
#                         trade_update.clear_buy_log(trade_update, self.DB_path, _asset_id)

                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
                
    
        # BUY ORDERS
        if not self.buy_position and (_row['stochRSI'] < .20) and (_row['stochRSIk'] < _row['stochRSId']):
            print('READY TO BUY', index)
            self.buy_position = True
        
        if buy_log_len > 6:
            self.buy_position = False
        
        if self.buy_position and (_row['stochRSId'] < .20) and (_row['stochRSIk'] > _row['stochRSId'])\
        and (buy_log_len < 6) and (price < _row['200_SMA']) and (_row['bb_width'] > 2):
            
            if buy_log_len < 3:
                
                buy_cost = .5 * (self.cash)
                bid_quantity = round((buy_cost / price), 5)
                
                if buy_cost > 10:
                    print('++++++++ BUY BUY BUY ++++++++', index)
                    print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                    _buy_order = trade_update.test_buy_order(trade_update, bid_quantity, price, self.symbol, index)
                    trade_update.process_order_message(trade_update, _buy_order, _asset_id)
                    self.buy_position = False
                    print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
            
            else:
                if (.95 * avg_cost >  price) == True:
                
                    buy_cost = .5 * (self.cash)
                    bid_quantity = round((buy_cost / price), 5)
                    
                    if buy_cost > 10:
                        print('++++++++ BUY BUY BUY ++++++++', index)
                        print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                        _buy_order = trade_update.test_buy_order(trade_update, bid_quantity, price, self.symbol, index)
                        trade_update.process_order_message(trade_update, _buy_order, _asset_id)
                        self.buy_position = False
                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
            
        
        var_change = (index, self.cash, self.trading_account, self.hold, self.buy_position, self.sell_position, self.buy_log)
        trade_update.update_database(trade_update, self.DB_path, var_change, _asset_id)
        
        return var_change


    
######################################################################################################
######################################################################################################
###################### END OF PRE-OPEN STREAM FUNCTIONS

class trade_logic:
    def __init__(self, _DB_path, _asset_id):
        self.DB_path = _DB_path
        self.buy_log = retrieve_buy_log(self.DB_path, _asset_id)
        sync_with_binance_account(self.DB_path)
        self.portfolio = retrieve_portfolio(self.DB_path, _asset_id)
        self.trading_account = self.portfolio[0][1]
        self.cash = self.portfolio[2][1] # _trade_portfolio[2][1]
        self.returns = 0
        self.price = 1
        self.hold = self.portfolio[1][1] # _trade_portfolio[1][1]
        self.avg_cost = get_avg_cost(self.buy_log)
        self.trade_status = retrieve_trade_status(self.DB_path, _asset_id)
        self.buy_position = self.trade_status[0]
        self.sell_position = self.trade_status[1]
        
        if _asset_id == 1:
            self.symbol = 'BTCUSD'
        elif _asset_id == 2:
            self.symbol = 'ETHUSD'
        
        # print relevant database values for debugging
        pprint.pprint(self.buy_log)
        pprint.pprint(self.portfolio)
        print(self.trading_account)
        print(self.cash)
        print(self.hold)
        print(self.symbol)
        print(self.avg_cost)
        print(self.buy_position)
        print(self.sell_position)
    
    # fake sell order for debugging and testing connection to binance client
    def test_sell_order(self, _amount, _price, _symbol, _index):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            sell_order = client.create_test_order(
                symbol= _symbol,
                side = Client.SIDE_SELL,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount
            )

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} SELL {_symbol} \n\n ERROR OCCURED {e} \n\n')
        
        date_int = int(dt.datetime.strptime(_index, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        qty = str(_amount)
        price = str(_price)
        sell_return = str(_price * _amount)
        fee = str((_price * _amount) * .001) # commission in USD for sales
        orderId_ = 111100000 + random.randint(0, 100000)
        
        # generate fake json messsage dict that get's returned normally

        message = {'symbol': _symbol,
                    'orderId': orderId_,
                    'orderListId': -1,
                    'clientOrderId': 'yn6zqWnIW8nqtq8yLQjtWK',
                    'transactTime': date_int,
                    'price': price,
                    'origQty': qty,
                    'executedQty': qty,
                    'cummulativeQuoteQty': sell_return,
                    'status': 'TEST',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'SELL',
                    'fills': [{'price': price,
                                'qty': qty,
                                'commission': fee,
                                'commissionAsset': _symbol[3:],
                                'tradeId': 2424209}]}
        
        return message
                                      
    # actual sell order
    def market_sell_order(self, _amount, _symbol):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            sell_order = client.create_order(
                symbol=_symbol,
                side = Client.SIDE_SELL,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount
            )

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} SELL {_symbol} \n\n ERROR OCCURED {e} \n\n')

        return sell_order

    # fake buy order for debugging and testing connection to binance client
    def test_buy_order(self, _amount, _price, _symbol, _index):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            buy_order = client.create_test_order(
                symbol= _symbol,
                side = Client.SIDE_BUY,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount
            )

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} BUY {_symbol} \n\n ERROR OCCURED {e} \n\n')
        
        date_int = int(dt.datetime.strptime(_index, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        qty = str(_amount)
        price = str(_price)
        cost = str(_price * _amount)
        fee = str(_amount * .001) # commission in for sales
        orderId_ = 111100000 + random.randint(0, 100000)
        
        
        # generate fake json messsage dict that would be returned normally

        message = {'symbol': _symbol,
                    'orderId': orderId_,
                    'orderListId': -1,
                    'clientOrderId': 'yn6zqWnIW8nqtq8yLQjtWK',
                    'transactTime': date_int,
                    'price': price,
                    'origQty': qty,
                    'executedQty': qty,
                    'cummulativeQuoteQty': cost,
                    'status': 'TEST',
                    'timeInForce': 'GTC',
                    'type': 'MARKET',
                    'side': 'BUY',
                    'fills': [{'price': price,
                                'qty': qty,
                                'commission': fee,
                                'commissionAsset': _symbol[:3],
                                'tradeId': 2424209}]}
        
        return message
                                      
    # actual buy order
    def market_buy_order(self, _amount, _symbol):
        client = Client(keys['api_key'], keys['api_secret'], tld="us")
        try:
            buy_order = client.create_order(
                symbol= _symbol,
                side = Client.SIDE_BUY,
                type = Client.ORDER_TYPE_MARKET,
                quantity = _amount
            )

        except Exception as e:
            print(e)
            with open('order_message_log.txt', 'a') as message_entry:
                message_entry.write(f'{_index} SELL {_symbol} \n\n ERROR OCCURED {e} \n\n')

        return buy_order
    
    # recieves the JSON order message returned order executed
    # uses the info to update the database and logs the order in the order txt file
    def process_order_message(self, _message, _asset_id):
        date_ = convert_to_datetime(_message['transactTime'])
        price_  = float(_message['fills'][0]['price'])
        quantity_ = _message['fills'][0]['qty']
        fee_ = _message['fills'][0]['commission']
        buy_sell_ = _message['side']
        print(date_, price_, quantity_, fee_, buy_sell_, _asset_id)

        trade_logic.update_transaction_history(trade_logic, self.DB_path, date_, price_, quantity_, fee_, buy_sell_, _asset_id)
        print('transaction history updated')

        with open('order_message_log.txt', 'a') as message_entry:
            message_entry.write(f'{date_} {buy_sell_}\n\n {_message}\n\n')

        
    # function to get the dollar cost average of recent purchases
    def get_avg_cost(self, _buy_log):
        try:
            cost_sum = sum([item[1] for item in _buy_log])
            quant_sum = sum([item[2] for item in _buy_log])
            avg_cost = cost_sum / quant_sum
            
        except:
            avg_cost = 0
            
        return avg_cost
        
    def update_transaction_history(self, _DB_path, _date, _price, _quantity, _fee, _buy_sell, _asset_id):
        print('---$$$$$$$$ INSERTING TRANSACTION $$$$$$$$', _date, _price, _quantity, _fee, _buy_sell, _asset_id)
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        date = _date
        price = _price
        quantity = _quantity
        fee = _fee
        buy_sell = _buy_sell
        
        if _asset_id == 1:
            asset_name = 'Bitcoin'
            
        elif _asset_id == 2:
            asset_name = 'Ethereum'
        
        # transaction (datetime, price, quantity, buy_sell (buy or sell), asset_id, asset_name))
        cursor.execute("""INSERT OR IGNORE INTO transaction_history (datetime, price, quantity, fee, buy_sell, asset_id, asset_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",(date, price, quantity, fee, buy_sell, _asset_id, asset_name))
        
        connection.commit()
        cursor.close()
        
        
    def update_database(self,_DB_path, _var_change, _asset_id):
        # var_change = (date, cash, portfolio, buy_position, sell_position, buy_log(list))
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        date = str(_var_change[0])
        cash = _var_change[1]
        trade_account = _var_change[2]
        hold_account = _var_change[3]
        
        if _var_change[4] == True:
            buy_position = 1
        elif _var_change[4] == False:
            buy_position = 0
            
        if _var_change[5] == True:
            sell_position = 1
        elif _var_change[5] == False:
            sell_position = 0
            
        buy_log = _var_change[6]
        
        if _asset_id == 1:
            cursor.execute(f"UPDATE portfolio SET quantity = {trade_account} WHERE id = 1")
            cursor.execute(f"UPDATE portfolio SET quantity = {hold_account} WHERE id = 2")
            cursor.execute(f"UPDATE portfolio SET quantity = {cash} WHERE id = 5")
            
        if _asset_id == 2:
            cursor.execute(f"UPDATE portfolio SET quantity = {trade_account} WHERE id = 3")
            cursor.execute(f"UPDATE portfolio SET quantity = {hold_account} WHERE id = 4")
            cursor.execute(f"UPDATE portfolio SET quantity = {cash} WHERE id = 6")
        
        cursor.execute(f"UPDATE trade_status SET buy_position = {buy_position} WHERE id = {_asset_id}")
        cursor.execute(f"UPDATE trade_status SET sell_position = {sell_position} WHERE id = {_asset_id}")
            
        for item in buy_log:
            cursor.execute("""INSERT OR IGNORE INTO buy_log (datetime, cost, quantity, price, asset_id) VALUES (?, ?, ?, ?, ?)
             """, (item[0], item[1], item[2], item[3], _asset_id))
        
        connection.commit()
        cursor.close()
        
    def update_sell_log(self, _DB_path, _datetime, _price, _quantity, _profit, _profit_ratio, _avg_cost, _asset_id):
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        
        cursor.execute("""INSERT OR IGNORE INTO sell_log (datetime, price, quantity, profit, profit_ratio, avg_cost, asset_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""", (_datetime, _price, _quantity, _profit, _profit_ratio, _avg_cost, _asset_id))
        
        connection.commit()
        cursor.close()

    def update_sell_values(self, _message, _avg_cost):
        price = float(_message['fills'][0]['price'])
        sell_quantity = float(_message['fills'][0]['qty'])
        cash_return = float(_message['cummulativeQuoteQty'])
        fee = float(_message['fills'][0]['commission'])
        cash_delta = cash_return - fee
        profit = cash_delta - (_avg_cost * sell_quantity)
        
        print('sell quant:', sell_quantity, 'cash return:', cash_return, 'fee:', fee, 'profit:', profit, 'avg_cost', _avg_cost,
              'hold amount:', ((0.33 * profit) / price))

        # under users descretion the profits are split,
        # 2/3rds to be reallocated into trading, thus compounding gains
        # 1/3rd to be held as to accumulate asset position
        self.cash += cash_delta - (0.33 * profit)
        self.trading_account -= sell_quantity
        self.hold += (0.33 * profit) / price

        print(self.cash, self.trading_account, self.hold)
        
        
    def update_buy_values(self, _message):
        index = dt.datetime.fromtimestamp(int(_message['transactTime'])/1000).strftime("%Y-%m-%d %H:%M:%S")
        price = float(_message['fills'][0]['price'])
        buy_quantity = float(_message['fills'][0]['qty'])
        buy_cost = float(_message['cummulativeQuoteQty'])
        fee = float(_message['fills'][0]['commission'])
        
        print(index, 'price', price, 'buy quantity', buy_quantity, 'cost', buy_cost, 'fee', fee)    
        
        self.buy_log.append([index, buy_cost, buy_quantity, price])
        
        self.cash -= buy_cost
        self.trading_account += buy_quantity - fee
        
        print(self.cash, self.trading_account, self.buy_log)

    def clear_buy_log(self, _DB_path, _asset_id):
        connection = sqlite3.connect(_DB_path)
        cursor = connection.cursor()
        
        cursor.execute(f"DELETE FROM buy_log WHERE asset_id = {_asset_id}")
        
        connection.commit()
        cursor.close()
        
    def trade_algo(self, _row, _asset_id):
        index = _row.name
        self.buy_log = retrieve_buy_log(self.DB_path, _asset_id)
        buy_log_len = len(self.buy_log)
        self.portfolio = retrieve_portfolio(self.DB_path, _asset_id)
        avg_cost = self.get_avg_cost(trade_logic, self.buy_log)
        price = _row['close']
        
        if self.buy_position and _row['stochRSI'] > .50:
            self.buy_position = False
        
        # SELL ORDER
        if (self.trading_account > 0) and (buy_log_len > 0):
            if self.sell_position and _row['stochRSId'] < .50:
                self.sell_position = False
            
            # conditions for algo to ready selling
            if not self.sell_position and (_row['stochRSIk'] > _row['stochRSId']) and (_row['stochRSId'] > .80):
                self.sell_position = True
            
            # conditions to actually sell, uses a market order with binance client
            if self.sell_position and (_row['stochRSIk'] < _row['stochRSId']) and (_row['stochRSId'] > .80) and (price > _row['200_SMA']):
                profit_ratio = 1 - (avg_cost / price)

                # condition to ensure a sufficiently profitable trade
                if profit_ratio > 0.05:
                    # sells 80% of position per trade, users descretion
                    sell_quantity = round(.8 * (self.trading_account), 5)
                    returns = (sell_quantity * price)
                    profit = returns - (avg_cost * sell_quantity)
                    
                    # trade total must be at least $10
                    if returns > 10:
                        print('%%% ###### SELL SELL SELL ##### %%%', index)
                        print(' ** PROFIT RATIO: ', profit_ratio)
                         # market_sell_order(self, _amount, _symbol)
                        _sell_order = trade_logic.market_sell_order(trade_logic, sell_quantity,self.symbol)
                        trade_logic.process_order_message(trade_logic, _sell_order, _asset_id)
                        trade_logic.update_sell_values(trade_logic, _sell_order, avg_cost)

                        # update sell_log datetime, price, quantity, profit, avg_cost, asset_id
                        trade_logic.update_sell_log(trade_logic, self.DB_path, index, price, sell_quantity, profit, profit_ratio, avg_cost, _asset_id)

                        self.sell_position = False

                        # clear and repopulate buy_log with remaining quantity in the trading account and dollar avarage cost
                        trade_logic.clear_buy_log(trade_logic, self.DB_path, _asset_id)
                        self.buy_log = []
                        self.buy_log.append([index, (avg_cost * self.trading_account), self.trading_account, avg_cost])
                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
                
    
        # BUY ORDERS
        
        # alternative conditions to ready buying
#         if not self.buy_position and (_row['bb_li']) and (_row['bb_width'] > 3):
#             self.buy_position = True
        
        # conditions to for algo to ready buying
        if not self.buy_position and (_row['stochRSI'] < .20) and (_row['stochRSIk'] < _row['stochRSId']):
            self.buy_position = True
        
        # the algo can only buy 5 times in a row, it must sell
        # before it can buy again, limits over buying
        if buy_log_len > 6:
            self.buy_position = False
        
        # condditions to actually buy, uses a market order with binance client
        if self.buy_position and (_row['stochRSI'] < .20) and (_row['stochRSIk'] > _row['stochRSId'])\
        and (buy_log_len < 6) and (price < _row['200_SMA']) and (_row['bb_width'] > 2):
        
            if buy_log_len < 2:
                
                buy_cost = .5 * (self.cash)
                bid_quantity = round((buy_cost / price), 5)
                
                if buy_cost > 10:
                    print('++++++++ BUY BUY BUY ++++++++', index)
                    print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                    _buy_order = trade_logic.market_buy_order(trade_logic, bid_quantity, self.symbol)
                    trade_logic.process_order_message(trade_logic, _buy_order, _asset_id)
                    trade_logic.update_buy_values(trade_logic, _buy_order)
                    self.buy_position = False
                    print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
            
            # buy condition where subsequent purchases of the given asset
            # only occurs if price is lower than the existing average purchasing cost
            # to glean more favorable trades
            elif (2 <= buy_log_len < 6):
                if (.95 * avg_cost >  price) == True:
                    # useful for debugging, but not always necessary
#                     print('avg_cost Truth condition', (.95 * avg_cost) > price)
                
                    buy_cost = .5 * (self.cash)
                    bid_quantity = round((buy_cost / price), 5)
                    
                    if buy_cost > 10:
                        print('++++++++ BUY BUY BUY ++++++++', index)
                        print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                        _buy_order = trade_logic.market_buy_order(trade_logic, bid_quantity, self.symbol)
                        trade_logic.process_order_message(trade_logic, _buy_order, _asset_id)
                        trade_logic.update_buy_values(trade_logic, _buy_order)
                        self.buy_position = False
                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')            
        
        # var_change = (date, cash, portfolio, hold, buy_position, sell_position, buy_log(list))        
        var_change = (index, self.cash, self.trading_account, self.hold, self.buy_position, self.sell_position, self.buy_log)
        trade_logic.update_database(trade_logic, self.DB_path, var_change, _asset_id)
        
        return var_change
    
##### TRADE UPDATE TEST FAST #######
# an alternate trading algorithm with much less strict buying and selling conditions,
# as it may take several days between trades with the original criteria
# useful for testing, setup, and debugging
# this uses test buy/sell orders, which will interacts with the binance client
# without actually sending a market order
# will generate a fake order message to be written in the order message log txt file

    def trade_algo_test(self, _row, _asset_id):
        index = _row.name
        self.buy_log = retrieve_buy_log(self.DB_path, _asset_id)
        buy_log_len = len(self.buy_log)
        self.portfolio = retrieve_portfolio(self.DB_path, _asset_id)
        avg_cost = self.get_avg_cost(trade_logic, self.buy_log)
        price = _row['close']

        if self.buy_position and _row['stochRSI'] > .50:
            self.buy_position = False

        if self.trading_account > 0 and buy_log_len > 0:
            if self.sell_position and _row['stochRSId'] < .50:
                self.sell_position = False

            if (_row['bb_hi']):
#             if not self.sell_position and _row['bb_hi']: # and (_row['stochRSId'] > .80) and (_row['stochRSIk'] > _row['stochRSId'])
#                 print('READY TO SELL', index)
                self.sell_position = True

            if self.sell_position and (_row['stochRSI'] > .80):
#             if self.sell_position (_row['stochRSI'] > .80): # and (price > _row['200_SMA']) and (_row['stochRSIk'] < _row['stochRSId']):
                profit_ratio = 1 - (avg_cost / price)


                if profit_ratio > 0.05:
                    sell_quantity = .8 * (self.trading_account)
                    returns = (sell_quantity * price)
                    profit = returns - (avg_cost * sell_quantity)
                    if returns > 10:
                        print('%%% ###### SELL SELL SELL ##### %%%', index)
                        print(' ** PROFIT RATIO: ', profit_ratio)
                        _sell_order = trade_logic.test_sell_order(trade_logic, sell_quantity, price, self.symbol, index)
                        trade_logic.process_order_message(trade_logic, _sell_order, _asset_id)
                        trade_logic.update_sell_values(trade_logic, _sell_order, avg_cost)

                        # update sell_log datetime, price, quantity, profit, avg_cost, asset_id
                        trade_logic.update_sell_log(trade_logic, self.DB_path, index, price, sell_quantity, profit, profit_ratio, avg_cost, _asset_id)

                        self.sell_position = False

                        # clear and repopulate buy_log with remaining quantity in the trading account and dollar avarage cost
                        trade_logic.clear_buy_log(trade_logic, self.DB_path, _asset_id)

                        self.buy_log = []
                        self.buy_log.append([index, (avg_cost * self.trading_account), self.trading_account, avg_cost])
                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')
    #                     trade_logic.update_transaction_history(trade_logic, self.DB_path, index, price, sell_quantity, 'sell', _asset_id)
                
    
        # BUY ORDERS
        if not self.buy_position and _row['bb_li']: #(_row['stochRSI'] < .20) and (_row['stochRSIk'] < _row['stochRSId']):
            self.buy_position = True

        if buy_log_len > 6:
            self.buy_position = False

        if self.buy_position and (_row['stochRSI'] < .20) and (buy_log_len < 6):
#         if self.buy_position and (_row['stochRSI'] < .20) and (_row['stochRSIk'] > _row['stochRSId'])\
#             and (buy_log_len < 6) and (_row['bb_width'] > 3) and (price < _row['200_SMA']):

            if buy_log_len < 3:
                buy_cost = .5 * (self.cash)
                bid_quantity = round((buy_cost / price), 5)
                
                if buy_cost > 10:
                    print('++++++++ BUY BUY BUY ++++++++', index)
                    print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                    _buy_order = trade_logic.test_buy_order(trade_logic, bid_quantity, price, self.symbol, index)
                    trade_logic.process_order_message(trade_logic, _buy_order, _asset_id)
                    trade_logic.update_buy_values(trade_logic, _buy_order)
                    self.buy_position = False
                    print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')

            else:
                if (.95 * avg_cost >  price) == True:

                    buy_cost = .5 * (self.cash)
                    bid_quantity = round((buy_cost / price), 5)

                    if buy_cost > 10:
                        print('++++++++ BUY BUY BUY ++++++++', index)
                        print('Bought ', bid_quantity, 'units, for: $', buy_cost, 'at: $', price, 'per unit, on:', index)
                        _buy_order = trade_logic.test_buy_order(trade_logic, bid_quantity, price, self.symbol, index)
                        trade_logic.process_order_message(trade_logic, _buy_order, _asset_id)
                        trade_logic.update_buy_values(trade_logic, _buy_order)

                        self.buy_position = False

                        print('\n~~~~~ #### TRANSACTION #### ~~~~~~\n\n')            
        
        # var_change = (date, cash, portfolio, hold, buy_position, sell_position, buy_log(list))        
        var_change = (index, self.cash, self.trading_account, self.hold, self.buy_position, self.sell_position, self.buy_log)
        trade_logic.update_database(trade_logic, self.DB_path, var_change, _asset_id)
        
        return var_change