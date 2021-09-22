import os
import datetime
import json


source_dir = '../stocks/stocks-2021-05-12'

files = os.listdir(source_dir)

ticks = 0

start_time = datetime.datetime.now()

for file in files:
    path = f'{source_dir}/{file}'

    with open(file, 'a+') as fout:
        with open(path) as f:
            for l in f.readlines():
                ticks += 1

                timestamp, topic, quote = l.split('\t')

                json.loads(quote)

                #fout.write(l)


end_time = datetime.datetime.now()
print(f'{ticks} ticks')
seconds = (end_time - start_time).seconds
print(f'thread finished. {seconds} seconds')
print(f'average: {ticks/seconds}')

