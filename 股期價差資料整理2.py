import collections
import os
import json
import numpy as np
import multiprocessing
import datetime



def login(id='H121933940',
          password='1qaz2wsx3edc',
          ca_path="SinoPac.pfx",
          ca_password='H121933940',
          person_id='H121933940'):
    api = sj.Shioaji()
    api.login(id, password)
    if os.uname()[0] == 'Darwin':
        pass
    else:
        api.activate_ca(
            ca_path=ca_path,
            ca_passwd=ca_password,
            person_id=person_id,
            store=1000
        )
    return api



class Product:
    def __init__(self, target_date, future_symbol, stock_symbol):
        self.target_date = target_date
        self.future_symbol = future_symbol
        self.stock_symbol = stock_symbol

        self.tick_dict = {}

        self.file_path = f'{target_date}/combined_{self.stock_symbol}_{self.future_symbol}.txt'

        if os.path.isfile(self.file_path):
            os.unlink(self.file_path)

    def add_tick(self, timestamp, topic, quote):

        if timestamp in self.tick_dict.keys():
            print(f'{self.stock_symbol} {self.future_symbol} {timestamp} duplicated')
            print(f'\torg:{timestamp} {self.tick_dict[timestamp][1]} {self.tick_dict[timestamp][2]}')
            print(f'\tnew:{timestamp} {topic} {quote}')


            org_topic = self.tick_dict[timestamp][1]
            org_quote = self.tick_dict[timestamp][2]
            org_quote_json = json.loads(org_quote)
            org_timestamp = f'{org_quote_json["Date"].replace("/", "-")} {org_quote_json["Time"]}'

            new_quote_json = json.loads(quote)

            new_timestamp = f'{new_quote_json["Date"].replace("/", "-")} {new_quote_json["Time"]}'

            del self.tick_dict[timestamp]

            if org_timestamp == new_timestamp:
                print(f'\033[1;33mStill duplicated: {org_timestamp}\033[0m')

            self.tick_dict[org_timestamp] = (org_timestamp, org_topic, org_quote)

            self.tick_dict[new_timestamp] = (new_timestamp, topic, quote)

        else:
            self.tick_dict[timestamp] = (timestamp, topic, quote)


    def dump_ticks(self):

        ordered_tick_dict = collections.OrderedDict(sorted(self.tick_dict.items()))

        with open(self.file_path, 'w') as f:
            for key in ordered_tick_dict:
                timestamp = ordered_tick_dict[key][0]
                topic = ordered_tick_dict[key][1]
                quote = ordered_tick_dict[key][2]
                f.write(f'{timestamp}\t{topic}\t{quote}')


def comb_stock_and_stock_future(index,
                                target_date,
                                stock_index_mapping,
                                targets,
                                return_dict):
    return_products = []




    for target in targets:
        future_symbol = target[0]
        stock_symbol = target[1]

        if stock_symbol not in stock_index_mapping:
            continue
        product = Product(target_date, future_symbol, stock_symbol)

        return_products.append(product)

        source_files = [
                        f'stocks-{target_date}/{target_date}_SF_list{stock_index_mapping[product.stock_symbol]}.txt'
                        ]

        for source_file in source_files:
            with open(source_file) as f:
                for line in f.readlines():
                    timestamp, topic, quote = line.split('\t')
                    if product.stock_symbol in topic or product.future_symbol in topic:
                        product.add_tick(timestamp, topic, quote)

    return_dict[index] = return_products

    return

if __name__ == "__main__":

    start_time = datetime.datetime.now()



    target_date = '2021-05-03'

    all_targets = []
    with open('stockFutureList/list_all.txt') as f:
        for l in f.readlines():
            code, symbol, name = l.split(' ')
            all_targets.append((symbol, code))

    stock_index_mapping = {}
    for file in ['list1.txt', 'list2.txt', 'list3.txt', 'list4.txt', 'list5.txt']:
        index = int(file[4:].split('.')[0])
        with open(f'stockFutureList/{file}') as f:

            for line in f.readlines():
                code, symbol, name = line.split(' ')
                stock_index_mapping[code] = index

    cpu_count = multiprocessing.cpu_count()
    print(f'Number CPUs of this platform: {cpu_count}')

    splitted_targets = np.array_split(all_targets, cpu_count)
    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    if False:

        jobs = []
        for i in range(cpu_count):
            p = multiprocessing.Process(target=comb_stock_and_stock_future,
                                        args=(i, target_date, stock_index_mapping, splitted_targets[i], return_dict))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()
    else:
        for i in range(cpu_count):
            comb_stock_and_stock_future(i, target_date, stock_index_mapping, splitted_targets[i], return_dict)

    for index in return_dict:
        products = return_dict[index]
        product: Product
        for product in products:
            print(f'Writing {product.stock_symbol} {product.future_symbol}')
            product.dump_ticks()

    end_time = datetime.datetime.now()
    print(f'thread finished. {(end_time - start_time).seconds} seconds')



