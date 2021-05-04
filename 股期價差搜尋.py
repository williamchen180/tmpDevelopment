import csv
import datetime
import json
import os

target_date = '2021-05-03'


class Product:
    def __init__(self, stock_symbol, future_symbol):
        self.stock_symbol = stock_symbol
        self.future_symbol = future_symbol

        self.StockAskPrice = 0
        self.StockBidPrice = 0

        self.StockAskVolume = 0
        self.StockBidVolume = 0

        self.FutureAskPrice = 0
        self.FutureBidPrice = 0

        self.FutureAskVolume = 0
        self.FutureBidVolume = 0

        self.delta_min = 0
        self.delta_max = 0

        self.dict_delta = {}
        self.last_delta = -999

        with open(f'{target_date}/delta-{self.stock_symbol}-{self.future_symbol}.csv', 'w') as f:
            f.write('timestamp,delta\n')

    # 股票報價：
    # MKT/idcdmzpcr01/TSE/3016
    #   {"AmountSum": [65393000.0], "Close": [85.3], "Date": "2021/04/22", "TickType": [2],
    #   "Time": "09:01:42.240603", "VolSum": [763], "Volume": [2]}
    # QUT/idcdmzpcr01/TSE/3016
    #   {"AskPrice": [60.8, 60.9, 61.0, 61.1, 61.2], "AskVolume": [1, 1, 15, 6, 14],
    #   "BidPrice": [60.6, 60.5, 60.4, 60.3, 60.2], "BidVolume": [20, 61, 28, 19, 8], "Date": "2021/04/22", "Time": "09:01:42.265745"}

    # 期貨報價：
    # L/TFE/CGFE1
    #   {"Amount": [0.0], "AmountSum": [0.0], "AvgPrice": [0.0], "Close": [0.0], "Code": "CGFE1", "Date": "2021/04/22",
    #   "DiffPrice": [0.0], "DiffRate": [0.0], "DiffType": [0], "High": [0.0], "Low": [0.0], "Open": 0.0, "Simtrade": 1,
    #   "TargetKindPrice": 27.8, "TickType": [0], "Time": "08:44:15.000000", "TradeAskVolSum": 0, "TradeBidVolSum": 0,
    #   "VolSum": [0], "Volume": [0]}
    # Q/TFE/CGFE1
    #   {"AskPrice": [27.85, 27.9, 27.95, 28.0, 28.05], "AskVolSum": 21, "AskVolume": [5, 5, 5, 1, 5],
    #   "BidPrice": [27.8, 27.65, 27.4, 27.3, 27.1], "BidVolSum": 16, "BidVolume": [10, 1, 3, 1, 1],
    #   "Code": "CGFE1", "Date": "2021/04/22", "DiffAskVol": [0, 0, 0, 0, 0], "DiffAskVolSum": 0, "DiffBidVol": [0, 0, 0, 0, 0],
    #   "DiffBidVolSum": 0, "FirstDerivedAskPrice": 0.0, "FirstDerivedAskVolume": 0, "FirstDerivedBidPrice": 0.0,
    #   "FirstDerivedBidVolume": 0, "Simtrade": 1, "TargetKindPrice": 27.8, "Time": "08:44:15.091000"}

    def feed_tick_tuple(self, timestamp, topic: str, quote: dict):

        if 'Time' not in quote.keys() or 'Date' not in quote.keys():
            return



        # 股票報價
        if topic.startswith('QUT'):
            self.StockAskPrice = quote['AskPrice'][0]
            self.StockBidPrice = quote['BidPrice'][0]

            self.StockAskVolume = quote['AskVolume'][0]
            self.StockBidVolume = quote['BidVolume'][0]

        if topic.startswith('Q/TFE'):
            self.FutureAskPrice = quote['AskPrice'][0]
            self.FutureBidPrice = quote['BidPrice'][0]

            self.FutureAskVolume = quote['AskVolume'][0]
            self.FutureBidVolume = quote['BidVolume'][0]

        if quote['Time'] < '09:00' or quote['Time'] > '13:25':
            return

        if self.StockAskPrice == 0 or self.StockBidPrice == 0 or self.FutureAskPrice == 0 or self.FutureBidPrice == 0:
            return

        delta = self.StockBidPrice - self.FutureAskPrice


        if delta != self.last_delta:
            self.last_delta = delta
            self.dict_delta[timestamp] = delta
            with open(f'{target_date}/delta-{self.stock_symbol}-{self.future_symbol}.csv', 'a+') as f:
                f.write(f'{timestamp},{delta}\n')



        self.delta_max = delta if delta > self.delta_max else self.delta_max
        self.delta_min = delta if delta < self.delta_min else self.delta_min




    def feed_tick(self, line):
        timestamp, topic, quote = line.split('\t')

        quote = json.loads(quote)
        self.feed_tick_tuple(timestamp, topic, quote)



    @staticmethod
    def parse_date_time(self, date_string, time_string):
        if time_string is not None and '.' in time_string:
            if '/' in date_string:
                timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y/%m/%d %H:%M:%S.%f')
            else:
                timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y-%m-%d %H:%M:%S.%f')
        else:
            if '/' in date_string:
                if time_string is None:
                    timestamp = datetime.datetime.strptime(f'{date_string}', '%Y/%m/%d')
                else:
                    timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y/%m/%d %H:%M:%S')
            else:
                if time_string is None:
                    timestamp = datetime.datetime.strptime(f'{date_string}', '%Y-%m-%d')
                else:
                    timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y-%m-%d %H:%M:%S')
        return timestamp



files = os.listdir(target_date)


for file in files:
    if not file.startswith('combined'):
        continue
    # print(file)
    stock_symbol, future_symbol = file[9:-4].split('_')

    if stock_symbol != '2606':
        continue

    print(f'{stock_symbol} {future_symbol}')

    product = Product(stock_symbol, future_symbol)

    with open(f'{target_date}/{file}') as f:
        for line in f.readlines():
            product.feed_tick(line=line)


    if product.StockAskPrice == 0:
        continue

    max_percentage = round(product.delta_max / product.StockAskPrice, 3)
    min_percentage = round(product.delta_min / product.StockAskPrice, 3)

    print(f'Max: {max_percentage} Min: {min_percentage}')
