# coding:utf8
"""
"""
import hashlib
import logging
import os
import time
import json
import random
import socket
import datetime
from urllib import parse
from collections import deque
from typing import Union, List, Dict, Optional
from os import path

import redis
import httpx

from graper.utils import log

#
cache_dir = path.join(path.dirname(__file__), "proxy_file")
os.makedirs(cache_dir, exist_ok=True)


def get_proxy_from_uri(uri_list: Union[str, List[str]], **kwargs):
    """
        Get proxies from multi uri
    Args:
        uri_list:
        **kwargs:
            key: if uri is redis://..., must special a key

    Returns:

    """
    if not isinstance(uri_list, list):
        uri_list = [uri_list]
    uri_list = [x for x in set(uri_list) if x]
    if not uri_list:
        raise ValueError(f"Not specify proxy source: {uri_list}")
    kwargs = kwargs.copy()
    r = []
    for uri in uri_list:
        if uri.startswith("http"):
            r.extend(get_proxy_from_http(uri, **kwargs))
        elif uri.startswith("redis"):
            key = kwargs.pop("key")
            r.extend(get_proxy_from_redis(key, uri, **kwargs))
        elif uri.startswith("file://"):
            r.extend(get_proxy_from_file(uri.replace("file://", ""), **kwargs))

    if r:
        random.shuffle(r)

    return r


def get_proxy_from_http(
    url: str, cache_file: str = "", cache_expire: int = 60, **kwargs
):
    """
        Get proxy from http server
    Args:
        url:
        cache_file:
        cache_expire:
        **kwargs:

    Returns:

    """

    cache_file = cache_file or f"{hashlib.md5(url.encode()).hexdigest()}.txt"
    cache_filepath = path.join(cache_dir, cache_file)
    if (
        (cache_expire <= 0)
        or (not path.exists(cache_filepath))
        or (time.time() - os.stat(cache_filepath).st_mtime > cache_expire)
    ):
        response = httpx.get(url, timeout=10)
        with open(cache_filepath, "w") as f:
            f.write(response.text)
    return get_proxy_from_file(cache_filepath, **kwargs)


def _get_proxies(line: str):
    line = line.strip()
    if not line:
        return None
    #
    auth = ""
    if "@" in line:
        auth, line = line.split("@")
    #
    items = line.split(":")
    if len(items) < 2:
        return None

    ip, port, *protocol = items
    if not all([port, ip]):
        return None
    if auth:
        ip = f"{auth}@{ip}"
    if not protocol:
        proxies = {
            "https": f"http://{ip}:{port}",
            "http": f"http://{ip}:{port}",
        }
    else:
        proxies = {protocol[0]: "%s://%s:%s" % (protocol[0], ip, port)}
    return proxies


def get_proxy_from_file(filepath, **kwargs) -> List[Dict]:
    """
        Get proxy from local file.
        each line in file will be parsed with a proxy format as follows:
            ip:port
            ip:port:protocol
            user:password@ip:port
    Args:
        filepath:
        **kwargs:

    Returns:

    """
    proxies_list = []
    with open(filepath, "r") as f:
        lines = f.readlines()

    for line in lines:
        proxies = _get_proxies(line)
        if proxies:
            proxies_list.append(proxies)
    return proxies_list


def get_proxy_from_redis(
    key, redis_uri="", redis_client: redis.Redis = None, **kwargs
) -> List[Dict]:
    """
        Get proxy from redis zset

        Proxy format in redis zset must be as follow:
            ip:port
            ip:port:protocol
            user:password@ip:port
        And the score of value can be a timestamp

    Args:
        key:
        redis_uri:
        redis_client:
        **kwargs:

    Returns:

    """

    if redis_uri:
        from graper.db.db_redis import create_redis_client

        redis_client = create_redis_client(redis_uri)
    # TODO Support expire proxy by timestamp score
    proxies = redis_client.zrange(key, 0, -1)
    proxies_list = []
    for proxy in proxies:
        proxies = _get_proxies(proxy.decode())
        if proxies:
            proxies_list.append(proxies)
    return proxies_list


def check_proxy(
    ip="",
    port="",
    proxies: Dict = None,
    type: int = 0,
    timeout: int = 5,
    logger: logging.Logger = None,
    show_error_log=False,
    **kwargs,
) -> int:
    """
        Check whether the proxy is valid
    Args:
        ip:
        port:
        proxies:
        type:
            0: check socket connect, fast but not guarantee 100% successful.
            1: use requests.get to check
        timeout:
        logger:
        show_error_log:
        **kwargs:

    Returns:
            0   failed
            1   succeed

    """
    if not logger:
        logger = log.get_logger(__file__)
    ok = 0
    if type == 0 and ip and port:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
            sk.settimeout(timeout)
            try:
                #
                sk.connect((ip, int(port)))
                ok = 1
            except Exception as e:
                if show_error_log:
                    logger.debug("check proxy failed: {} {}:{}".format(e, ip, port))
            sk.close()
    else:
        target_url = random.choice(
            [
                "http://www.baidu.com",
                "http://www.taobao.com",
            ]
        )
        try:
            httpx.stream(
                "GET",
                target_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },
                proxies=proxies,
                timeout=timeout,
            )
            ok = 1
        except Exception as e:
            if show_error_log:
                logger.debug("check proxy failed: {} {}".format(e, proxies))
    return ok


class ProxyTag:
    deprecated = -1
    normal = 0
    frozen = 1

    choices = (
        deprecated,
        normal,
        frozen,
    )


class ProxyItem(object):
    def __init__(
        self,
        proxies=None,
        check_valid_timeout=20,
        check_interval=180,
        max_proxy_use_num=10000,
        delay=30,
        use_interval=None,
        logger=None,
        **kwargs,
    ):
        """
        Args:
            proxies:
            check_valid_timeout: Timeout for check proxy valid
            check_interval: Interval for check proxy valid
            max_proxy_use_num:
            delay: Wait seconds when the proxy was frozen
            use_interval: Seconds for next use
            logger:
            **kwargs:
        """
        # {"http": ..., "https": ...}
        self.proxies = proxies
        #
        self.check_valid_timeout = check_valid_timeout
        #
        self.check_interval = check_interval

        #
        self._tag = ProxyTag.normal
        # last tag time
        self._tag_ts = 0
        #
        self.last_check_valid_ts = 0
        #
        self.max_proxy_use_num = max_proxy_use_num
        self.use_num = 0
        #
        self.delay = delay
        #
        self.use_interval = use_interval
        # last use time
        self._use_ts = 0

        #
        self.info = self.parse_proxies(self.proxies)
        self.ip = self.info["ip"]
        self.port = self.info["port"]
        self.id = self.info["id"]
        self.ip_port = self.info["ip_port"]

        #
        self.logger = logger or log.get_logger(__file__)

    @property
    def use_ts(self):
        return self._use_ts

    @use_ts.setter
    def use_ts(self, value):
        self._use_ts = value

    @property
    def tag(self):
        return self._tag

    @tag.setter
    def tag(self, value):
        self._tag = value

    @property
    def tag_ts(self):
        return self._tag_ts

    @tag_ts.setter
    def tag_ts(self, value):
        self._tag_ts = value

    def get_proxies(self):
        self.use_num += 1
        return self.proxies

    def is_delay(self):
        return self.tag == 1

    def is_valid(self, force=0, type=0):
        """
            check and return self valid status
                1 valid
                2 frozen
                0 not valid
        Args:
            force: Force to check valid
            type:

        Returns:

        """
        if self.use_num > self.max_proxy_use_num > 0:
            self.logger.debug(f"Maximum times ({self.use_num}) reached: {self.id}")
            return 0
        if self.tag == ProxyTag.deprecated:
            self.logger.debug(f"Proxy {self.id} was marker as deprecated")
            return 0
        now = time.time()
        if self.delay > 0 and self.tag == ProxyTag.frozen:
            _delay = int(now - self.tag_ts)
            if _delay < self.delay:
                self.logger.debug(f"Proxy {self.id} was frozen, {_delay} seconds left")
                return 2
            else:
                self.tag = ProxyTag.normal
                self.logger.debug("Unfreeze {}".format(self.id))
        if self.use_interval:
            if now - self.use_ts < self.use_interval:
                return 2
        if not force:
            if now - self.last_check_valid_ts < self.check_interval:
                return 1
        if self.check_valid_timeout > 0:
            ok = check_proxy(
                proxies=self.proxies,
                type=type,
                timeout=self.check_valid_timeout,
                logger=self.logger,
            )
        else:
            ok = 1
        self.last_check_valid_ts = time.time()
        return ok

    @staticmethod
    def parse_proxies(proxies):
        """
            Proxies Split
        Examples:
            >>> ProxyItem.parse_proxies({"http": "http://root:root@127.0.0.1:8888"})
            {'protocol': ['http'], 'ip': '127.0.0.1', 'port': '8888', 'user': 'root', 'password': 'root', 'ip_port': '127.0.0.1:8888'}

        Args:
            proxies:

        Returns:
        """
        if not proxies:
            return {}
        if isinstance(proxies, (str, bytes)):
            proxies = json.loads(proxies)
        protocol = list(proxies.keys())
        if not protocol:
            return {}
        _url = proxies.get(protocol[0])
        if not _url.startswith("http"):
            _url = "http://" + _url
        _url_parse = parse.urlparse(_url)
        netloc = _url_parse.netloc
        if "@" in netloc:
            netloc_auth, netloc_host = netloc.split("@")
        else:
            netloc_auth, netloc_host = "", netloc
        ip, *port = netloc_host.split(":")
        port = port[0] if port else "80"
        user, *password = netloc_auth.split(":")
        password = password[0] if password else ""
        return {
            "protocol": protocol,
            "ip": ip,
            "port": port,
            "user": user,
            "password": password,
            "ip_port": f"{ip}:{port}",
            "id": f"{user}:{password}@{ip}:{port}" if user else f"{ip}:{port}",
        }


class RedisProxyItem(ProxyItem):
    """
    Same as ProxyItem, But some attr was saved to redis.
    Mainly used for share proxy status with different process.

    """

    def __init__(
        self, redis_client: redis.Redis, namespace: str = "default", *args, **kwargs
    ):
        """
            Isolate the different application
        Args:
            redis_client:
            namespace:
            *args:
            **kwargs:
        """

        super().__init__(*args, **kwargs)
        #
        #
        self.redis_client = redis_client
        self.namespace = namespace
        #
        self.use_time_key = f"{self.namespace}:proxy_use_time"
        self.tag_key = f"{self.namespace}:proxy_tag"
        self.tag_ts_key = f"{self.namespace}:proxy_tag_ts"

    @property
    def use_ts(self):
        r = self.redis_client.hget(self.use_time_key, self.id)
        r = float(r) if r else 0
        return r

    @use_ts.setter
    def use_ts(self, value):
        value = float(value)
        self.redis_client.hset(self.use_time_key, self.id, value)

    @property
    def tag(self):
        r = self.redis_client.hget(self.tag_key, self.id)
        r = int(r) if r else 0
        return r

    @tag.setter
    def tag(self, value):
        value = int(value)
        self.redis_client.hset(self.tag_key, self.id, value)

    @property
    def tag_ts(self):
        r = self.redis_client.hget(self.tag_ts_key, self.id)
        r = float(r) if r else 0
        return r

    @tag_ts.setter
    def tag_ts(self, value):
        value = float(value)
        self.redis_client.hset(self.tag_ts_key, self.id, value)


class ProxyPool:
    def __init__(
        self,
        proxy_source_uri: List = None,
        size=-1,
        min_reset_interval=5,
        max_reset_interval=180,
        check_valid=True,
        logger: logging.Logger = None,
        **kwargs,
    ):
        """
        Args:
            size: pool size
            min_reset_interval: Min seconds to reset pool
            max_reset_interval:
                Max seconds to reset pool
                Mainly used for prevent the proxy pool was updated for a long times and new proxy can not be used.
            check_valid: Whether to check valid when get proxy was called.
            logger:
            **kwargs:
                cache_expire: local cache proxy expire seconds, See get_proxy_from_http
                proxy_source_uri: the source uri to get proxy

        """
        super().__init__(**kwargs)
        #
        self.kwargs = kwargs
        # check proxy source
        proxy_source_uri = proxy_source_uri
        if not isinstance(proxy_source_uri, list):
            proxy_source_uri = [proxy_source_uri]
        proxy_source_uri = list(set([x for x in proxy_source_uri if x]))
        self.kwargs["proxy_source_uri"] = proxy_source_uri

        #
        self.max_queue_size = size
        #
        self.real_max_proxy_count = 1000

        self.logger = logger or log.get_logger(__file__)
        self.kwargs["logger"] = self.logger

        #
        self.min_reset_interval = min_reset_interval
        self.max_reset_interval = max_reset_interval
        self.check_valid = check_valid

        #
        self.proxy_queue = None
        # {id: ProxyItem, ...}
        self.proxy_dict = {}
        #
        self.invalid_proxy_dict = {}
        #
        # A temporary list for proxies, self.get try to get proxy from it firstly.
        # it items was added by user.
        self.vip_proxy_list = deque()

        #
        self.reset_lock = None
        #
        self.last_reset_time = 0
        #
        self.reset_fast_count = 0
        #
        self.no_valid_proxy_times = 0
        #
        self.last_get_ts = time.time()

        # Cache the proxy update time to prevent repeat check valid in a short time.
        self.proxy_item_last_check_valid_ts_dict = {}
        #
        # Whether to use RedisProxyItem
        self.redis_client: redis.Redis = self.kwargs.get("redis_client", "")
        self.namespace = self.kwargs.get("namespace", "")
        if self.redis_client:
            self.proxy_item_class = RedisProxyItem
        else:
            self.proxy_item_class = ProxyItem

    @property
    def queue_size(self):
        return self.proxy_queue.qsize() if self.proxy_queue is not None else 0

    def clear(self):
        """
            Clear everything
        Returns:

        """
        self.proxy_queue = None
        self.proxy_dict = {}
        # clear invalid_proxy record where expire more than 10 minutes.
        _limit = datetime.datetime.now() - datetime.timedelta(minutes=10)
        self.invalid_proxy_dict = {
            k: v for k, v in self.invalid_proxy_dict.items() if v > _limit
        }
        # clear proxy_item_update_ts record where expire more than 10 minutes.
        _limit = time.time() - 600
        self.proxy_item_last_check_valid_ts_dict = {
            k: v
            for k, v in self.proxy_item_last_check_valid_ts_dict.items()
            if v > _limit
        }

        if self.namespace:
            keys = self.redis_client.keys(f"{self.namespace}:proxy*")
            if keys:
                self.redis_client.delete(*keys)
        return

    def get(self, retry: int = 0) -> Optional[Dict]:
        """
            Get proxies from pool
        Args:
            retry:

        Returns:

        """
        retry += 1
        if retry > 3:
            self.no_valid_proxy_times += 1
            return None
        #
        try:
            return self.vip_proxy_list.pop()
        except:
            pass
        #
        if time.time() - self.last_get_ts > 3 * 60:
            #
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        #
        self.last_get_ts = time.time()
        #
        proxy_item = self.get_random_proxy()
        if proxy_item:
            #
            if not self.check_valid:
                # put back
                proxies = proxy_item.get_proxies()
                self.put_proxy_item(proxy_item)
                return proxies
            else:
                is_valid = proxy_item.is_valid()
                if is_valid:
                    #
                    self.proxy_item_last_check_valid_ts_dict[
                        proxy_item.id
                    ] = proxy_item.last_check_valid_ts
                    #
                    proxies = proxy_item.get_proxies()
                    self.put_proxy_item(proxy_item)
                    if is_valid == 1:
                        if proxy_item.use_interval:
                            proxy_item.use_ts = time.time()
                        return proxies
                else:
                    #
                    self.proxy_dict.pop(proxy_item.id, "")
                    self.invalid_proxy_dict[proxy_item.id] = datetime.datetime.now()
        else:
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        if self.no_valid_proxy_times >= 5:
            # fix:bug
            # When there was only one spider task left, Since only one thread check proxy,
            # If many proxy was invalid, the task may be waiting for a very very long time.
            try:
                self.reset_proxy_pool()
            except Exception as e:
                self.logger.exception(e)
        return self.get(retry)

    get_proxy = get

    def get_random_proxy(self) -> Optional[ProxyItem]:
        """

        Returns:

        """
        if self.proxy_queue is not None:
            if random.random() < 0.5:
                # This is a high frequency operation, use random to slow it.
                if time.time() - self.last_reset_time > self.max_reset_interval:
                    self.reset_proxy_pool(force=True)
                else:
                    min_q_size = (
                        min(self.max_queue_size / 2, self.real_max_proxy_count / 2)
                        if self.max_queue_size > 0
                        else self.real_max_proxy_count / 2
                    )
                    if self.proxy_queue.qsize() < min_q_size:
                        self.reset_proxy_pool()
            try:
                return self.proxy_queue.get_nowait()
            except Exception:
                pass
        return None

    def append_proxies(self, proxies_list: List) -> int:
        """

        Args:
            proxies_list:

        Returns:

        """
        count = 0
        if not isinstance(proxies_list, list):
            proxies_list = [proxies_list]
        for proxies in proxies_list:
            if proxies:
                proxy_item = self.proxy_item_class(proxies=proxies, **self.kwargs)
                if proxy_item.id in self.invalid_proxy_dict:
                    continue
                if proxy_item.id not in self.proxy_dict:
                    if not proxy_item.last_check_valid_ts:
                        proxy_item.last_check_valid_ts = (
                            self.proxy_item_last_check_valid_ts_dict.get(
                                proxy_item.id, 0
                            )
                        )
                    self.put_proxy_item(proxy_item)
                    self.proxy_dict[proxy_item.id] = proxy_item
                    count += 1
        return count

    def put_proxy_item(self, proxy_item: ProxyItem):
        return self.proxy_queue.put_nowait(proxy_item)

    def reset_proxy_pool(self, force: bool = False):
        """
            Reset proxy pool
        Args:
            force: if True, per call will execute reset, otherwise, will check some condition.

        Returns:

        """
        if not self.reset_lock:
            # Must be import when using, otherwise, gevent.monkey_patch may be invalid.
            import threading

            self.reset_lock = threading.RLock()
        with self.reset_lock:
            if (
                force
                or self.proxy_queue is None
                or (
                    self.max_queue_size > 0
                    and self.proxy_queue.qsize() < self.max_queue_size / 2
                )
                or (
                    self.max_queue_size < 0
                    and self.proxy_queue.qsize() < self.real_max_proxy_count / 2
                )
                or self.no_valid_proxy_times >= 5
            ):
                if time.time() - self.last_reset_time < self.min_reset_interval:
                    self.reset_fast_count += 1
                    if self.reset_fast_count % 10 == 0:
                        self.logger.debug(
                            f"Reset pool too fast :) {self.reset_fast_count}"
                        )
                        time.sleep(1)
                else:
                    self.clear()
                    if self.proxy_queue is None:
                        import queue

                        self.proxy_queue = queue.Queue()
                    proxies_list = get_proxy_from_uri(
                        self.kwargs["proxy_source_uri"], **self.kwargs
                    )
                    self.real_max_proxy_count = len(proxies_list)
                    if 0 < self.max_queue_size < self.real_max_proxy_count:
                        proxies_list = random.sample(proxies_list, self.max_queue_size)
                    _valid_count = self.append_proxies(proxies_list)
                    self.last_reset_time = time.time()
                    self.no_valid_proxy_times = 0
                    self.logger.debug(
                        "Reset pool succeed: get {}, valid {}, invalid {}, current total {},".format(
                            len(proxies_list),
                            _valid_count,
                            len(self.invalid_proxy_dict),
                            len(self.proxy_dict),
                        )
                    )
        return

    def tag_proxy(self, proxies_list: Union[List, Dict], tag: int, *, delay=30) -> bool:
        """

        Args:
            proxies_list:
            tag: One of ProxyTag
            delay: set frozen seconds for ProxyTag.frozen

        Returns:

        """
        if int(tag) not in ProxyTag.choices or not proxies_list:
            return False
        if not isinstance(proxies_list, list):
            proxies_list = [proxies_list]
        for proxies in proxies_list:
            if not proxies:
                continue
            proxy_id = self.proxy_item_class(
                self.kwargs["redis_client"],
                self.kwargs["namespace"],
                proxies,
                **self.kwargs,
            ).id
            if proxy_id not in self.proxy_dict:
                continue
            self.proxy_dict[proxy_id].tag = tag
            self.proxy_dict[proxy_id].tag_ts = time.time()
            self.proxy_dict[proxy_id].delay = delay
            if tag == -1:
                # 处理失效代理
                self.proxy_dict.pop(proxy_id, "")
                self.invalid_proxy_dict[proxy_id] = datetime.datetime.now()

        return True

    def get_proxy_item(self, proxy_id="", proxies=None) -> Optional[ProxyItem]:
        """
            Get ProxyItem object by proxy_id
        Args:
            proxy_id:
            proxies:

        Returns:

        """
        if proxy_id:
            return self.proxy_dict.get(proxy_id)
        if proxies:
            proxy_id = self.proxy_item_class(proxies, **self.kwargs).id
            return self.proxy_dict.get(proxy_id)
        return

    def copy(self):
        return ProxyPool(**self.kwargs)

    def all(self) -> list:
        """
        Get all proxy from source
        """
        return get_proxy_from_uri(self.proxy_source_uri, **self.kwargs)

    def add_vip_proxy(self, proxies=None):
        """
        Add VIP proxy

        Args:
            proxies:

        Returns:

        """
        if not proxies:
            return
        if not isinstance(proxies, list):
            proxies = [proxies]
        self.vip_proxy_list.extend(proxies)
        return True


#
default_proxy_pool = ProxyPool(proxy_source_uri=[os.getenv("GRAPER_PROXY_SERVICE_URL")])


if __name__ == "__main__":
    #
    fake_proxy_file = path.join(os.getcwd(), "fake_proxy.txt")
    with open(fake_proxy_file, "w") as f:
        f.write("1.1.1.1:8888")

    # Basic Usage
    proxy_pool = ProxyPool(
        proxy_source_uri=[
            # "http://1.1.1.1/proxy/all.txt",
            # "http://1.1.1.1/proxy/good.txt",
            "file://{}/fake_proxy.txt".format(os.getcwd())
        ]
    )

    while 1:
        proxies = proxy_pool.get()
        print(proxies)
        time.sleep(1)
