import os
import multiprocessing
from mypylib import *
from mypylib.mvp import *
import csv
from decimal import Decimal

bool_single_CPU = False


def load_all_files(source_dir='../shioaji_ticks'):
    all_files = []

    for d in os.listdir(source_dir):
        if not os.path.isdir(f'{source_dir}/{d}'):
            continue
        if d[0] == '.':
            continue
        if not d[0] in '1234567890':
            continue
        for f in os.listdir(f'{source_dir}/{d}'):
            if not f.startswith('20'):
                continue
            full_path = f'{d} {source_dir}/{d}/{f}'
            all_files.append(full_path)
    return all_files




def visit_a_stock(queue_in: multiprocessing.Queue,
                  queue_out: multiprocessing.Queue,
                  parameter_dict: dict,
                  process_id=0):
    with open(f'record_{process_id}.txt', 'w+') as fout:
        while True:
            try:
                data = queue_in.get(block=True, timeout=1)

                symbol, file = data.split(' ')

                # print(file)
                Highest = 0
                Lowest = 99999999
                Close = 0
                with open(file) as fp:
                    rows = csv.DictReader(fp)
                    # tick_type
                    # ask_volume
                    # bid_volume
                    # bid_price
                    # close
                    # volume
                    # ask_price
                    # ts
                    for row in rows:
                        Close = Decimal(row['close'])

                        if Close > Highest:
                            Highest = Close
                            bool_highest = True
                        else:
                            bool_highest = False

                        if Close < Lowest:
                            Lowest = Close
                            bool_lowest = True
                        else:
                            bool_lowest = False

                        Date, Time = row['ts'].split(' ')
                        if Time < '09:03':
                            continue

                        if bool_lowest or bool_highest:
                            fout.write(f'{symbol} {row["ts"]} {"higher" if bool_highest else "lower" if bool_lowest else ""}\n')


            except queue.Empty:
                break

    queue_out.put(f'{process_id}')


if __name__ == '__main__':

    parameter_dict = {}

    with timeIt('visit all files'):

        queue_in = multiprocessing.Queue()
        queue_out = multiprocessing.Queue()

        all_files = load_all_files(source_dir='../shioaji_ticks')
        print(f'There are {len(all_files)} files.')
        for file in all_files:
            queue_in.put(file)
        print('All file queued')

        if bool_single_CPU:
            visit_a_stock(queue_in, queue_out, parameter_dict, 0)
        else:
            tasks = []

            cpu_count = multiprocessing.cpu_count()
            for idx in range(cpu_count):
                p = multiprocessing.Process(target=visit_a_stock, args=(queue_in, queue_out, parameter_dict, idx))
                tasks.append(p)
                p.start()

            for task in tasks:
                task.join()
                print(f'{task} process end.')


        while True:
            try:
                out = queue_out.get(block=True, timeout=1)

                print(out)
            except queue.Empty:
                break


