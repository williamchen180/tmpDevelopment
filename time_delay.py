import pandas as pd
import plotly.express as px
import os
import datetime
import json
import plotly.express as px

file_name = '../stocks/stocks-2021-05-13/2021-05-13_SF_list1.txt'
file_name = '/Users/chenwei-ting/Downloads/TickProcess_admin1.txt'
file_name = '/Users/chenwei-ting/Downloads/TickProcess_home.txt'


def parse_date_time(date_string, time_string) -> datetime.datetime:
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


with open(file_name) as f:

    with open('delta_time.csv', 'w+') as fout:

        fout.write('timestamp,delta\n')

        for l in f.readlines():
            timestamp, topic, quote = l.split('\t')

            quote = json.loads(quote)

            date, time = timestamp.split(' ')

            our_timestamp = parse_date_time(date, time)

            src_timestamp = parse_date_time(quote['Date'], quote['Time'])

            fout.write(f'{timestamp},{(our_timestamp - src_timestamp).microseconds}\n')


df = pd.read_csv('delta_time.csv', index_col=0, parse_dates=[0])

df = df.resample('10s').mean()

df = df.reset_index()

fig = px.line(df, x='timestamp', y=['delta'])

fig.show()

fig.write_html('delta_home_tick_process.html')