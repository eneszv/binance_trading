import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from binance.client import Client
from config import paper_traing_config


class PaperTrader():
    
    def __init__(self, config):
        self.config = config
        
    def log_in(self):
        self.client = Client(api_key = os.environ.get('BINANCE_TESTNET_API'),
                             api_secret = os.environ.get('BINANCE_TESTNET_SECRET'),
                             tld = 'com',
                             testnet = True)
    
    def load_res_data(self):
        """Loads results.csv file"""
        try:
            df_res = pd.read_csv(os.path.join(self.config['res_file_dir'], 'results.csv'))
        except:
            print('Result file not found. Returning empty data frame.')
            df_res = pd.DataFrame()
            
        return df_res
    
    def save_data(self, df):
        """Saves results.csv file"""
        if not os.path.exists(self.config['res_file_dir']):
            os.makedirs(self.config['res_file_dir'])
        df.to_csv(os.path.join(self.config['res_file_dir'], 'results.csv'), index=False)
    
    def get_df_from_bars(self, bars):
        """Loads and prepares dataframe from bars"""
        df = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df.iloc[:,0], unit = "ms")
        df.columns = ["open time", "open", "high", "low", "close", "volume",
                      "close time", "quote asset volume", "number of trades",
                      "taker buy base asset volume", "taker buy quote asset volume", "ignore", "date"]
        df = df[["date", "open", "high", "low", "close", "volume"]].copy()
        for column in df.columns:
            if column != 'date':
                df[column] = pd.to_numeric(df[column], errors = "coerce")
        df["log_return"] = np.log(df['close'] / df['close'].shift(1))
        df['ma_{}'.format(self.config['ma_high'])] = df['close'].rolling(self.config['ma_high']).mean()
        df['ma_{}'.format(self.config['ma_low'])] = df['close'].rolling(self.config['ma_low']).mean()
        df['position'] = df[['ma_{}'.format(self.config['ma_low']),
                             'ma_{}'.format(self.config['ma_high'])]].apply(
            lambda x: 1 if x[0] >= x[1] else 0, axis=1)
        
        return df
    
    def trade_logic(self, df, df_res):
        """Logic of the strategy"""
        if len(df_res):
            last_position = df_res['position'].to_list()[-1]
        else:
            last_position = 0
            
        temp_dict = {'date': [], 'position':[], 'price': []}

        temp_pos = df['position'].to_list()[-1]
        temp_dict['date'].append(df['date'].to_list()[-1])
        temp_dict['position'].append(temp_pos)
        
        if temp_pos:
            # buy
            if not last_position:
                order = self.client.create_order(symbol = self.config['symbol'],
                                                 side = "BUY",
                                                 type = "MARKET",
                                                 quantity = self.config['quantity'])
                base_units = float(order["executedQty"])
                quote_units = float(order["cummulativeQuoteQty"])
                price = round(quote_units / base_units, 5)
                temp_dict['price'].append(price)
            else:
                temp_dict['price'].append(df['close'].to_list()[-1])
        else:
            # sell
            if last_position:
                order = self.client.create_order(symbol = self.config['symbol'],
                                                 side = "SELL",
                                                 type = "MARKET",
                                                 quantity = self.config['quantity'])
                base_units = float(order["executedQty"])
                quote_units = float(order["cummulativeQuoteQty"])
                price = round(quote_units / base_units, 5)
                temp_dict['price'].append(price)
            else:
                temp_dict['price'].append(df['close'].to_list()[-1])
        
        df_res = df_res.append(pd.DataFrame(temp_dict))
        self.save_data(df_res)
    
    def execute_trade(self):
        """Main method of the class"""
        self.log_in()
        
        now = datetime.utcnow()
        past = str(now - timedelta(hours = self.config['ma_high']))
        bars = self.client.get_historical_klines(symbol = self.config['symbol'],
                                            interval = self.config['interval'],
                                            start_str = past,
                                            end_str = None,
                                            limit = 1000)
        
        df = self.get_df_from_bars(bars)
        df_res = self.load_res_data()
        
        self.trade_logic(df, df_res)

if __name__ == '__main__':

    pt = PaperTrader(paper_traing_config)
    pt.execute_trade()