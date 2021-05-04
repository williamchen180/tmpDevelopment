

import multiprocessing


def worker(procnum, return_dict):
    """worker function"""
    print(str(procnum) + " represent!")

    ret_dict = {}
    return_dict['idx'] = procnum
    return_dict['test'] = 'test'

    return_dict[procnum] = return_dict



if __name__ == "__main__":
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker, args=(i, return_dict))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()
    print(return_dict.values())