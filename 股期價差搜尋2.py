import csv
import pandas as pd
import datetime
import json
import os
import plotly.express as px
import plotly.graph_objs as go

target_date = '2021-09-22'

bool_target_symbols = False
target_symbols = ['2603']

class Product:

    array_dummy = {0.007: [], 0.01: [], 0.012: []}

    array_before_0930 = {0.007: [], 0.01: [], 0.012: []}

    array_between_0930_to_1030 = {0.007: [], 0.01: [], 0.012: []}

    array_between_1030_to_1230 = {0.007: [], 0.01: [], 0.012: []}

    array_after_1230 = {0.007: [], 0.01: [], 0.012: []}


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

        self.max_percentage = 0
        self.min_percentage = 0

        self.dict_delta = {}
        self.last_delta = -999


        # 這部分是因為有時候排中會因為價格波動進入「試搓」的狀態，如果進入試搓的時候，就必須要等是搓結束之後，重新開始所有價格報價。
        # 不能沿用是搓前的價格，因為早已經失真了...
        self.stock_disabled = False
        self.future_disabled = False

        self.file_csv = f'tmp/{target_date}/delta-{target_date}-{self.stock_symbol}-{self.future_symbol}.csv'

        if not os.path.isdir(f'tmp/html-{target_date}/'):
            os.mkdir(f'tmp/html-{target_date}')

        self.file_ticks_html = f'tmp/html-{target_date}/ticks-{target_date}-{self.stock_symbol}-{self.future_symbol}.html'
        self.file_rate_html = f'tmp/html-{target_date}/rate-{target_date}-{self.stock_symbol}-{self.future_symbol}.html'

        with open(self.file_csv, 'w') as f:
            f.write('timestamp,delta,cover,delta rate,cover rate,delta volume,cover volume,future bid price, stock ask price\n')


    def to_html(self):

        df = pd.read_csv(self.file_csv)

        if len(df) < 100:
            return

        fig = px.line(df, x='timestamp', y=['delta', 'cover'])
        print(f'write to {self.file_ticks_html}')
        fig.write_html(self.file_ticks_html)

        fig = px.line(df, x='timestamp', y=['delta rate', 'cover rate'])
        print(f'write to {self.file_rate_html}')
        fig.write_html(self.file_rate_html)



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

            if 'Simtrade' in quote.keys():
                self.stock_disabled = True
                return
            else:
                self.stock_disabled = False

            self.StockAskPrice = quote['AskPrice'][0] if quote['AskPrice'][0] != 0 else self.StockAskPrice
            self.StockBidPrice = quote['BidPrice'][0] if quote['BidPrice'][0] != 0 else self.StockBidPrice

            self.StockAskVolume = quote['AskVolume'][0]
            self.StockBidVolume = quote['BidVolume'][0]

        if topic.startswith('Q/TFE'):

            if 'Simtrade' in quote.keys():
                self.future_disabled = True
                return
            else:
                self.future_disabled = False

            self.FutureAskPrice = quote['AskPrice'][0] if quote['AskPrice'][0] != 0 else self.FutureAskPrice
            self.FutureBidPrice = quote['BidPrice'][0] if quote['BidPrice'][0] != 0 else self.FutureBidPrice

            self.FutureAskVolume = quote['AskVolume'][0]
            self.FutureBidVolume = quote['BidVolume'][0]

        if self.future_disabled or self.stock_disabled:
            return

        if quote['Time'] < '09:00' or quote['Time'] > '13:25':
            return

        if self.StockAskPrice == 0 or self.StockBidPrice == 0 or self.FutureAskPrice == 0 or self.FutureBidPrice == 0:
            return

        # 逆價差：
        # 股票內盤 - 期貨外盤
        delta = self.StockBidPrice - self.FutureAskPrice

        delta_volume = min(self.StockBidVolume / 2, self.FutureAskVolume)

        # 正價差：
        # 期貨內盤 - 股票外盤
        delta_reverse = self.FutureBidPrice - self.StockAskPrice

        delta_reverse_volume = min(self.FutureBidVolume, self.StockAskVolume / 2)

        delta_rate = delta / self.StockBidPrice
        delta_reverse_rate = delta_reverse / self.FutureBidPrice

        if delta != self.last_delta:
            self.last_delta = delta
            self.dict_delta[timestamp] = delta
            with open(self.file_csv, 'a+') as f:
                f.write(f'{timestamp},{delta},{delta_reverse},{delta_rate},{delta_reverse_rate},{delta_volume},{delta_reverse_volume},{self.FutureBidPrice},{self.StockAskPrice}\n')


            cls = self.__class__

            array_of_this = cls.array_dummy

            if quote['Time'] <= '09:30':
                array_of_this = cls.array_before_0930

            if '09:30' < quote['Time'] <= '10:30':
                array_of_this = cls.array_between_0930_to_1030

            if '10:30' < quote['Time'] <= '12:30':
                array_of_this = cls.array_between_1030_to_1230

            if quote['Time'] > '12:30':
                array_of_this = cls.array_after_1230

            if delta_rate >= 0.007:
                if self.stock_symbol not in array_of_this[0.007]:
                    array_of_this[0.007].append(self.stock_symbol)

            if delta_rate >= 0.01:
                if self.stock_symbol not in array_of_this[0.01]:
                    array_of_this[0.01].append(self.stock_symbol)

            if delta_rate >= 0.012:
                if self.stock_symbol not in array_of_this[0.012]:
                    array_of_this[0.012].append(self.stock_symbol)

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


array_product = []

files = os.listdir(f'tmp/{target_date}')
for file in files:
    if not file.startswith('combined'):
        continue
    # print(file)
    stock_symbol, future_symbol = file[9:-4].split('_')

    if bool_target_symbols:
        if stock_symbol not in target_symbols:
            continue

    print(f'{stock_symbol} {future_symbol}')

    product = Product(stock_symbol, future_symbol)

    array_product.append(product)

    with open(f'tmp/{target_date}/{file}') as f:
        for line in f.readlines():
            product.feed_tick(line=line)

    # if product.StockAskPrice == 0:
    #     continue

    if product.StockAskPrice != 0:
        product.max_percentage = round(product.delta_max / product.StockAskPrice, 3)
        product.min_percentage = round(product.delta_min / product.StockAskPrice, 3)

    print(f'Max: {product.max_percentage} Min: {product.min_percentage}')

    product.to_html()


array_max_threshold = [0.007, 0.01, 0.012]

array_min_threshold = [-0.007, -0.01, -0.012]

for threshold in array_max_threshold:
    print(f'最大正價差大於 {threshold}')
    for p in array_product:
        if p.max_percentage > threshold:
            print(p.stock_symbol, p.future_symbol)


print('\n\n')






