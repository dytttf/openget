# coding:utf8
"""
isort:skip_file
"""
# may be memory leak
from gevent import monkey  # isort:skip # noqa

# tips if patch subprocess, os.popen will slow
monkey.patch_all(os=False, subprocess=False, signal=False)  # isort:skip  # noqa

import warnings  # isort:skip  # noqa

warnings.filterwarnings("ignore")  # isort:skip  # noqa

#
import os
import sys
import threading
import time
from os import path
from queue import Empty, Queue
from typing import Callable, Optional

from graper import util
from graper.network import downloader
from graper.spiders import Request, Response
from graper.utils import log
from graper.db import DB

logger = log.get_logger(__file__)


class Spider(object):
    def __init__(self, **kwargs):
        """

        Args:
            **kwargs:
                pool_size:
                use_sqlite:
                downloader:

        """

        super().__init__()
        try:
            self.name = self.__class__.__name__
        except:
            self.name = ""
        #
        self.downloader = kwargs.get("downloader", downloader.Downloader())
        #
        self.db = None
        # sqlite mode
        # only support One Process
        self.use_sqlite = kwargs.get("use_sqlite") or False
        # create sqlite db
        if self.use_sqlite is True:
            sqlite_db_path = path.join(
                path.dirname(path.join(os.getcwd(), sys.argv[0])), "sqlite.db"
            )
            self.db = DB.create("sqlite://{}".format(sqlite_db_path))
            logger.info("use sqlite db in path {}".format(sqlite_db_path))

        #
        self.pool_size = kwargs.get("pool_size", 100)
        self.event_exit = threading.Event()
        self.request_queue = Queue()

        #
        self._thread_status = {}
        #
        self.max_request_retry = 9999

        # 内存使用上限 比例 默认0.9 超过0.8则主动 被kill
        self.memory_utilization_limit = 0.8
        self._killed = False
        self._last_check_memory_utilization_ts = 0

        # break_spider
        self.break_spider_check_interval = 5
        self._last_check_break_spider_ts = 0
        #

        # 注意回调函数仅在 run方法里运行
        # 在start前执行的一系列函数
        self._before_start_callbacks = [self.before_start]
        # 在close前执行的一系列函数
        self._before_stop_callbacks = [self.before_stop]

        self._closed = False
        #
        self._close_reason = ""

        # Is in docker runing
        self._in_docker = (
            True if os.getenv("GRAPER_IN_DOCKER", "false").lower() != "false" else False
        )

        self._stop = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if not self._closed:
                self._close()
                self._closed = True
        except:
            pass

    def _close(self, **kwargs):
        try:
            self.close()
        except Exception as e:
            logger.exception(e)
        # close some connection
        for con in [self.db, self.downloader]:
            if con:
                try:
                    con.close()
                except Exception as e:
                    logger.exception(e)
        return

    def before_start(self, **kwargs):
        """
        Called before start
        Args:
            **kwargs:

        Returns:

        """
        pass

    def before_stop(self, **kwargs):
        """
        Called before stop
        Args:
            **kwargs:

        Returns:

        """
        pass

    def break_spider(self, **kwargs) -> Optional[bool]:
        """
            Called in function start_requests, user can custom spider stop conditions
        Returns:
            1: stop iterated start_requests
            other: pass
        """
        return self._stop

    def _break_spider(self) -> Optional[bool]:
        """
            decrease the numbers of break_spider calls
        Returns:

        """
        if self.break_spider_check_interval <= 0:
            return self.break_spider()
        if (
            time.time() - self._last_check_break_spider_ts
        ) > self.break_spider_check_interval:
            self._last_check_break_spider_ts = time.time()
            return self.break_spider()
        return False

    def close(self, **kwargs):
        pass

    @property
    def container_memory_utilization(self):
        """
            Get memory usage of container
        Returns:

        """
        if not self._in_docker:
            return 0
        try:
            memory_info = util.ContainerInfo.memory_info()
        except Exception as e:
            if "memory" in str(e):
                logger.debug(f"Insufficient system memory: {str(e)}")
                return 1
            raise e
        if not memory_info:
            return 0
        return memory_info["container"]["memory_utilization"]

    def download(
        self, request: Request, downloader: downloader.Downloader = None, **kwargs
    ):
        """

        Args:
            request
            downloader:
            **kwargs:

        Returns:

        """
        if not downloader:
            downloader = self.downloader
        return downloader.download(request.request, **kwargs)

    def handle_request(self, thread_num: int):
        while 1:
            try:
                request_obj: Request = self.request_queue.get(True, 1)
                if not request_obj:
                    continue
                #
                if request_obj.retry > self.max_request_retry:
                    put_retry = getattr(self.request_queue, "put_retry", None)
                    if put_retry:
                        #
                        request_obj.retry = 0
                        put_retry(request_obj)
                    #
                    logger.warn(
                        "retry times over limit {}, task delete: {}".format(
                            self.max_request_retry, request_obj
                        )
                    )
                    self.request_queue.task_done()
                    continue
            except Exception as e:
                if not isinstance(e, Empty):
                    logger.exception(e)
                if self.event_exit.is_set():
                    break
                self._thread_status[thread_num] = 0
                continue
            self._thread_status[thread_num] = 1
            if not isinstance(request_obj, Request):
                continue

            _request = request_obj.request
            if _request:
                try:
                    _response = self.download(
                        request_obj,
                        downloader=request_obj.downloader,
                    )
                except Exception as e:
                    logger.exception(e)
                    _response = None
            else:
                _response = None
            # 记录下载次数
            request_obj.retry += 1

            # response
            if isinstance(_response, Response):
                response = _response
            else:
                response = Response(_response, request_obj)

            try:
                _callback = request_obj.callback
                if not _callback:
                    _callback = self.parse
                if isinstance(_callback, (str, bytes)):
                    _callback = getattr(self, _callback)
                result = _callback(response)
                if result is not None:
                    # 迭代
                    for item in result:
                        if isinstance(item, Request):
                            self.request_queue.put(item)
            except Exception as e:
                logger.exception(e)
            self.request_queue.task_done()
        return

    def make_request(self, *args, **kwargs) -> Optional[Request]:
        """

        Args:
            *args:
            **kwargs:

        Returns:

        """
        pass

    def register_before_start(self, function: Callable):
        """
            Add function to self._before_start_callbacks
        Args:
            function:

        Returns:

        """
        if not callable(function):
            raise TypeError("must be callable: {}".format(function))
        self._before_start_callbacks.append(function)
        logger.debug("register_before_start successfully: {}".format(function))
        return True

    def register_before_stop(self, function: Callable):
        """
            Add function to self._before_stop_callbacks
        Args:
            function:

        Returns:

        """
        if not callable(function):
            raise TypeError("must be callable: {}".format(function))
        self._before_stop_callbacks.append(function)
        logger.debug("register_before_stop successfully: {}".format(function))
        return True

    def run(self, **kwargs):
        #
        thread_list = []
        for i in range(self.pool_size):
            t = threading.Thread(target=self.handle_request, args=(i,))
            t.start()
            thread_list.append(t)

        max_queue_size = min(100, self.pool_size)

        logger.debug("Spider start")
        # exec before_start_callbacks
        for _callback in self._before_start_callbacks:
            try:
                _callback_name = getattr(_callback, "__name__", _callback)
                logger.debug("call function -> {}".format(_callback_name))
                _callback()
                logger.debug("call function -> {} ok".format(_callback_name))
            except Exception as e:
                logger.exception(e)

        #
        raise_exception = None
        #
        spider_break = 0
        # 此处若不捕获异常 可能卡死爬虫
        try:
            # 减少日志量
            _last_show_qsize_ts = 0
            if self._break_spider() != 1 and not self._killed:
                for item in self.start_requests():
                    if isinstance(item, Request):
                        self.request_queue.put(item)
                    else:
                        if item is None:
                            logger.warning("Got a None Request")
                            continue
                        # todo
                        logger.error("Not a Request Object: {}".format(item))
                    # 检查是否需要中断
                    if self._break_spider() == 1 or self._killed:
                        spider_break = 1
                        if not self._close_reason:
                            self._close_reason = "Break Spider"
                        break
                    # check queue size
                    for i in range(1, 1000):
                        qsize = self.request_queue.qsize()
                        if qsize < max_queue_size:
                            break
                        _max_wait_ts = 3
                        # adjust wait time
                        _wait_ts = min(0.1 * i, _max_wait_ts)
                        time.sleep(_wait_ts)
                        if _wait_ts == _max_wait_ts:
                            # check whether break_spider
                            if self.break_spider() == 1 or self._killed:
                                spider_break = 1
                                break
                        #
                        _t = time.time()
                        if _t - _last_show_qsize_ts > 10:
                            logger.debug("waiting Request count: {} ...".format(qsize))
                            _last_show_qsize_ts = _t
                    if self.should_oom_killed() == 1:
                        logger.debug("OOM is coming soon")
                        spider_break = 1
                        self.suicide()
                        break
            else:
                logger.debug("break spider")
        except Exception as e:
            raise_exception = e
        while 1:
            if self.request_queue.qsize() <= 0:
                # when all thread is stopped, stop self
                if sum(self._thread_status.values()) == 0:
                    self.event_exit.set()
                    break
            if spider_break:
                logger.debug(
                    "main thread was stopped, "
                    "waiting for sub thread stoping... "
                    "remain tasks: {} "
                    "active threads: {} "
                    "stop reason: {}".format(
                        self.request_queue.qsize(),
                        sum(self._thread_status.values()),
                        self._close_reason,
                    )
                )
            time.sleep(5)

        # 2018/06/08 上边这几行代码 可以代替join  应该是这样的  出了问题再说啊
        for t in thread_list:
            t.join()
        logger.debug("all thread closed")

        # execute before_stop_callbacks
        for _callback in self._before_stop_callbacks:
            try:
                _callback_name = getattr(_callback, "__name__", _callback)
                logger.debug("call function -> {}".format(_callback_name))
                _callback()
                logger.debug("call function -> {} ok".format(_callback_name))
            except Exception as e:
                logger.exception(e)

        #
        try:
            self._close()
            self._closed = True
            logger.debug("self.close execute successfully")
        except Exception as e:
            logger.exception(e)

        #
        if raise_exception:
            raise raise_exception
        if self._killed:
            logger.debug("Spider done: Killed")
            util.oom_killed_exit()
        logger.debug("Spider done: {}".format(self._close_reason))
        return

    def should_oom_killed(self):
        """
            whether kill self when OOM is coming soon.
        Returns:

        """
        if time.time() > self._last_check_memory_utilization_ts:
            if self.container_memory_utilization > self.memory_utilization_limit:
                return 1
            else:
                self._last_check_memory_utilization_ts = time.time() + 60
        return 0

    def suicide(self):
        """
        Returns:

        """
        logger.info("please kill me ......")
        self._killed = True
        self._close_reason = "Killed(suicide)"
        return

    def stop(self, message="stopped by user"):
        logger.info(message)
        self._stop = True
        self._close_reason = "Stopped(By User)"
        return

    def start_requests(self):
        """
        Returns:

        Examples:
            def start_requests(self):
                while 1:
                    yield Request("https://www.baidu.com")

        """
        raise NotImplementedError

    def parse(self, response: Response):
        """

        Args:
            response:

        Returns:

        """
        raise NotImplementedError


if __name__ == "__main__":
    spider = Spider()
    spider.run()
