import datetime
import json

start_time = datetime.datetime.now()



def parse_date_time(date_string, time_string):
    if '.' in time_string:
        if '/' in date_string:
            timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y/%m/%d %H:%M:%S.%f')
        else:
            timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y-%m-%d %H:%M:%S.%f')
    else:
        if '/' in date_string:
            timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y/%m/%d %H:%M:%S')
        else:
            timestamp = datetime.datetime.strptime(f'{date_string} {time_string}', '%Y-%m-%d %H:%M:%S')
    return timestamp


print('Timestamp,symbol,AskPrice,BidPrice,AskVolume,BidVolume,ODD AskPrice, ODD BidPrice,AskShares,BidShares')

stocks = {}

with open('all.txt') as f:
    for l in f.readlines():
        topic: str
        # timestamp, topic, quote = l.split('\t')
        topic, quote = l.split('\t')
        quote = json.loads(quote)
        timestamp = parse_date_time(quote['Date'], quote['Time'])

        topics: list = topic.split('/')

        if len(topics) == 4:
            symbol = topics[3]
        else:
            symbol = topics[4]

        if symbol not in stocks.keys():
            stocks[symbol] = {}

        if topic[0:3] == 'QUT':
            stocks[symbol]['QUT'] = quote

        if topic[0:3] == 'QUO':



            if quote['Simtrade'] == 0 and 'QUO' in stocks[symbol].keys() and 'QUT' in stocks[symbol].keys():
                print(f'{timestamp},'
                      f'{symbol},'
                      f'{stocks[symbol]["QUT"]["AskPrice"][0]},'
                      f'{stocks[symbol]["QUT"]["BidPrice"][0]},'
                      f'{stocks[symbol]["QUT"]["AskVolume"][0]},'
                      f'{stocks[symbol]["QUT"]["BidVolume"][0]},'
                      f'{stocks[symbol]["QUO"]["AskPrice"][0]},'
                      f'{stocks[symbol]["QUO"]["BidPrice"][0]},'
                      f'{stocks[symbol]["QUO"]["AskShares"][0]},'
                      f'{stocks[symbol]["QUO"]["BidShares"][0]}')


            stocks[symbol]['QUO'] = quote


end_time = datetime.datetime.now()
print(f'thread finished. {(end_time - start_time).seconds} seconds')
