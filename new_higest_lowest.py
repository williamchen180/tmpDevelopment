import os
import traceback
import sys
import multiprocessing
import queue
import time
from mypylib import *
from mypylib.ti import *
from mypylib.mvp import *
from mypylib.shioaji_history_ticks import shioaji_history_ticks
import pickle
import copy
from termcolor import cprint
import glob
import csv

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
            full_path = f'{source_dir}/{d}/{f}'
            all_files.append(full_path)
    return all_files


def visit_a_stock(queue_in: multiprocessing.Queue,
                  queue_out: multiprocessing.Queue,
                  parameter_dict: dict,
                  process_id=0):
    while True:
        try:
            file = queue_in.get(block=True, timeout=1)

            # print(file)
            with open(file) as f:
                # TODO Do something here
                pass
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


