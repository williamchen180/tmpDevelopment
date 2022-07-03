import csv
from collections import defaultdict


dict_day = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

with open('all.txt') as fp:
    for line in fp.readlines():
        symbol, date, time, price, move = line.split(',')
        move = move.rstrip()
        print(date, symbol, move, time[:5])
        dict_day[date][move][time[:5]].append({'symbol': symbol, 'price': price})
