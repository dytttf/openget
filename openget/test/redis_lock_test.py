from openget.util import RedisLock


from concurrent.futures import ThreadPoolExecutor


def work(name):
    while 1:
        with RedisLock(f"test:{1}", timeout=60, wait_timeout=10) as r:
            if r.locked:
                print(name)
    return


pool = ThreadPoolExecutor(max_workers=30)
pool.map(work, range(10))
