# coding:utf8
import datetime
import html
import json
import os
import base64
import hashlib
import re
import sys
import time
import zlib
import platform
import tempfile
import oss2
from typing import Callable, List, Tuple, Optional, Dict, AnyStr, Generator
from urllib import parse
from oss2 import SizedFileAdapter, determine_part_size, Bucket
from oss2.models import PartInfo, SimplifiedObjectInfo

import redis
import urllib3
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)


from graper.utils import log
from graper import setting


logger = log.get_logger(__file__)

cur_path = os.path.dirname(os.path.abspath(__file__))

temp_dir = tempfile.mkdtemp()

#
global_redis_lock_connection_pool_cache = {}


def delta_month(date: datetime.date, months: int):
    """
        Calc month change
    Args:
        date: datetime.date or datetime.datetime
        months: the number of month to increase

    Returns:

    Usage:
    >>> import datetime
    >>> date = datetime.date(2021, 1, 1)
    >>> delta_month(date, -1)
    datetime.date(2020, 12, 1)

    """
    if months < 0:
        months = abs(months)
        delta = date.month - months
        if delta < 1:
            date = date.replace(
                year=date.year - int(abs(delta) // 12 + 1),
                month=12 - abs(delta) % 12,
            )
        else:
            date = date.replace(month=date.month - months)
    elif months > 0:
        delta = date.month + months
        if delta > 12:
            date = date.replace(
                year=date.year + int((delta - 1) // 12),
                month=(delta - 1) % 12 + 1,
            )
        else:
            date = date.replace(month=date.month + months)
    return date


def get_proxies_by_id(proxy_id, schema="http") -> Dict:

    proxies = {
        "http": f"{schema}://{proxy_id}",
        "https": f"{schema}://{proxy_id}",
    }
    return proxies


def key2index(key: str, index_range: list = None) -> int:
    """
        Map the key to a fixed range
    Args:
        key:
        index_range:

    Returns:

    """
    if not index_range:
        raise ValueError("index_range is empty")
    if not isinstance(key, bytes):
        key = key.encode()
    hex_key = hashlib.md5(key).hexdigest()
    _sum = sum([ord(x) for x in hex_key])
    return index_range[_sum % len(index_range)]


def remove_control_characters(text: str):
    """
        Remove Ctrl Character from text but ignore:
            9 \t
            10 \n
            13 \r
        https://zh.wikipedia.org/wiki/%E6%8E%A7%E5%88%B6%E5%AD%97%E7%AC%A6
    Args:
        text:

    Returns:

    """
    text = re.sub("[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", text)
    return text


# compress text
def compress_text(text: bytes) -> bytes:
    text = zlib.compress(text)
    text = base64.b64encode(text)
    return text


def decompress_text(text: bytes) -> bytes:
    """"""
    text = base64.b64decode(text)
    text = zlib.decompress(text)
    return text


class RedisLock(object):
    def __init__(
        self,
        key,
        timeout=300,
        wait_timeout=8 * 3600,
        break_wait=None,
        redis_uri=None,
        connection_pool=None,
        auto_release=True,
        logger=None,
    ):
        """
        Redis Lock

        Args:
            key: Unique string to identifier different project
            timeout:
            wait_timeout:
                Timeout for waiting acquire lock.
                default are 8 hours, to prevent waiting forever in multi threading
                if set 0, will return immediately when lock failed
            break_wait:
                A hook for wait_timeout, the waiting will be interrupted when this function return value is True

            redis_uri:
            connection_pool:
            auto_release:
                whether release lock when exit with statement.
            logger:

        Usage:
            with RedisLock(key="test", timeout=10, wait_timeout=100, redis_uri="") as _lock:
                if _lock.locked:
                    # do somethings
                    pass
        """
        self.redis_index = -1
        if not key:
            raise Exception("lock key is empty")
        if connection_pool:
            self.redis_client = redis.StrictRedis(connection_pool=connection_pool)
        else:
            self.redis_client = self.get_redis_client(
                redis_uri or setting.default_redis_uri
            )

        self.logger = logger or log.get_logger(__file__)

        self.lock_key = "redis_lock:{}".format(key)
        #
        self.timeout = timeout
        #
        self.wait_timeout = wait_timeout
        #
        self.break_wait = break_wait
        if self.break_wait is None:
            self.break_wait = lambda: False
        if not callable(self.break_wait):
            raise TypeError(
                "break_wait must be function or None, but got {}".format(
                    type(self.break_wait)
                )
            )

        self.locked = False
        self.auto_release = auto_release

    def __enter__(self):
        if not self.locked:
            self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.auto_release:
            self.release()

    def __repr__(self):
        return "<RedisLock: {} index: {}>".format(self.lock_key, self.redis_index)

    @staticmethod
    def get_redis_client(redis_uri):
        if redis_uri not in global_redis_lock_connection_pool_cache:
            connection_pool = redis.BlockingConnectionPool.from_url(
                redis_uri, max_connections=100, timeout=60
            )
            global_redis_lock_connection_pool_cache[redis_uri] = connection_pool
        return redis.Redis(
            connection_pool=global_redis_lock_connection_pool_cache[redis_uri]
        )

    def acquire(self):
        start = time.time()
        self.logger.debug("Prepare for acquire lock {} ...".format(self))
        while 1:
            #
            if self.redis_client.setnx(self.lock_key, time.time()):
                self.redis_client.expire(self.lock_key, self.timeout)
                self.locked = True
                self.logger.debug("acquire lock successfully {}".format(self))
                break
            else:
                _ttl = self.redis_client.ttl(self.lock_key)
                if _ttl < 0:
                    self.redis_client.delete(self.lock_key)
                elif _ttl > self.timeout:
                    self.redis_client.expire(self.lock_key, self.timeout)

            if self.wait_timeout > 0:
                if time.time() - start > self.wait_timeout:
                    break
            else:
                break
            if self.break_wait():
                self.logger.debug("break_wait take effect")
                break
            self.logger.debug(
                "Waiting for acquire lock {} was been {} seconds".format(
                    self, time.time() - start
                )
            )
            if self.wait_timeout > 10:
                time.sleep(5)
            else:
                time.sleep(1)
        if not self.locked:
            self.logger.debug("Acquire lock fail {}".format(self))
        return

    def release(self, force=False):
        """
            Release Lock
        Args:
            force: If True, lock will be released in any case.

        Returns:

        """
        if self.locked or force:
            self.redis_client.delete(self.lock_key)
            self.locked = False
        return

    def prolong_life(self, life_time: int) -> int:
        """
            Prolong the life of the lock
        Args:
            life_time:

        Returns:

        """
        expire = self.redis_client.ttl(self.lock_key)
        if expire < 0:
            return expire
        expire += life_time
        self.redis_client.expire(self.lock_key, expire)
        return self.redis_client.ttl(self.lock_key)

    @property
    def ttl(self):
        """
            Get the ttl for self lock.
        Returns:

        """
        expire = self.redis_client.ttl(self.lock_key)
        return expire


class HeaderFormater(object):
    @staticmethod
    def format_content_disposition(value: str):
        """
            Format Content-Disposition
                https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Headers/Content-Disposition

            Examples:
                'attachment; filename="InterimStaffReportNYMEX_WTICrudeOil.pdf"',
                'attachment; filename=InterimStaffReportNYMEX_WTICrudeOil.pdf'
                'attachment;filename=%E5%8D%8E%E5%A4%8F%E7%BA%A2%E5%88%A9%E6%B7%B7%E5%90%88%E5%9E%8B%E8%AF%81%E5%88%B8%E6%8A%95%E8%B5%84%E5%9F%BA%E9%87%912019%E5%B9%B4%E7%AC%AC3%E5%AD%A3%E5%BA%A6%E6%8A%A5%E5%91%8A.pdf'
        Args:
            value: the content of Content-Disposition

        Returns:

        """
        r = {"type": "", "name": "", "filename": "", "filename*": ""}
        value = value.strip()
        if not value:
            return r
        value = [x.strip() for x in value.split(";")]
        r["type"] = value[0]

        for v in value[1:]:
            k_park, v_part = v.split("=", 1)
            k_park = k_park.lower()
            # sometimes, the value was contains quote
            r[k_park] = v_part.strip("'\"")
        return r


def retry_decorator(exception: Exception, retry=2, delay=1, delay_ratio=1):
    """
        Retry functions
    Args:
        exception: exception of this types will be ignored and re-execute function
        retry: retry times
        delay: retry interval, default is: delay * (delay_ratio ** retry)
        delay_ratio:

    Returns:

    """

    def deco_retry(f):
        def f_retry(self, *args, **kwargs):
            real_delay = delay
            lastException = None
            for mretry in range(retry):
                try:
                    return f(self, *args, **kwargs)
                except exception as e:
                    # logger.error("function {} retring {} ...".format(f, mretry))
                    time.sleep(real_delay)
                    real_delay = delay * (delay_ratio ** mretry)
                    lastException = e
            if lastException is not None:
                # logger.exception(lastException)
                raise lastException

        return f_retry

    return deco_retry


def format_headers(headers: Dict, url: str = "") -> dict:
    """
        Format HTTP headers to Title case.
    Args:
        headers:
        url:

    Returns:

    """
    # headers to Title case
    _headers = {}
    for k, v in headers.items():
        k = "-".join([x.capitalize() for x in k.split("-")])
        _headers[k] = v
    headers = _headers
    if not headers.get("Referer") and url:
        headers["Referer"] = url
    return headers


def html_unescape(text: str) -> str:
    """
        convert HTML content to readable
        1. unescape html
        2. remove some empty code
    Args:
        text:

    Returns:

    """
    text = html.unescape(text).strip()
    text = text.replace("\xa0", "")
    return text


def get_json(text, flag="{}", _eval=False, _globals: dict = None, _locals: dict = None):
    """
        Parse standard or nonstandard text to JSON, such as jsonp.
    Args:
        text:
        flag: leading and trailing characters for json format.
        _eval: whether to use eval.
        _globals: eval args
            example: convert true to True, you can set _globals={"true": True}
        _locals: same as _globals

    Returns:

    """
    l, r = text.find(flag[0]), text.rfind(flag[1])
    if _eval:
        if not _globals:
            _globals = globals()
        if not _locals:
            _locals = locals()
        return eval(text[l : r + 1], _globals, _locals)
    return json.loads(text[l : r + 1])


def find_pairs(text: str, flag: str = "()", error: str = "strict") -> List[Tuple]:
    """
    >>> find_pairs("(1)(2)")
    [(0, 2), (3, 5)]
    >>> find_pairs("((1)(2))")
    [(0, 7), (1, 3), (4, 6)]
    >>> find_pairs("'1'2'3'", flag="''")
    [(0, 2), (4, 6)]

    Args:
        text:
        flag:
        error:
            strict: raise an error if meet an error pair
            other: stop iterate and return current result when meet an error pair

    Returns:

    """
    brackets = []
    _brackets = []
    # TODO  support """1'2'34"5"6'7'7"""
    if flag[0] == flag[1]:
        for _idx, s in enumerate(text):
            if s == flag[0] and not _brackets:
                _brackets.append(_idx)
            elif s == flag[0]:
                try:
                    brackets.append((_brackets.pop(-1), _idx))
                except Exception as e:
                    if error == "strict":
                        raise e
                    break
    else:
        for _idx, s in enumerate(text):
            if s == flag[0]:
                _brackets.append(_idx)
            elif s == flag[1]:
                try:
                    brackets.append((_brackets.pop(-1), _idx))
                except Exception as e:
                    if error == "strict":
                        raise e
                    break
    brackets.sort(key=lambda x: x[0])
    return brackets


def local_datetime(data: AnyStr) -> Optional[datetime.datetime]:
    """
        Convert string to datetime
        tips: result is NOT UTC
    Args:
        data:

    Returns:

    """
    dt = datetime.datetime.now()
    # html实体字符转义
    data = html.unescape(data)
    data = data.strip()
    try:
        if isinstance(data, bytes):
            data = data.decode()
    except Exception as e:
        logger.error("local_datetime() error: data is not utf8 or unicode : %s" % data)

    # 归一化
    data = (
        data.replace("年", "-")
        .replace("月", "-")
        .replace("日", " ")
        .replace("/", "-")
        .strip()
    )
    data = re.sub("\s+", " ", data)

    year = dt.year

    regex_format_list = [
        # 2013年8月15日 22:46:21
        ("(\w+ \w+ \d+ \d+:\d+:\d+ \+\d+ \d+)", "%a %b %d %H:%M:%S +0800 %Y", ""),
        # Wed Sep  5 12:37:25 2018
        ("(\w+ \w+ \d+ \d+:\d+:\d+ \d+)", "%a %b %d %H:%M:%S %Y", ""),
        # 2013年8月15日 22:46:21
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", ""),
        # "2013年8月15日 22:46"
        ("(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", ""),
        # "2014年5月11日"
        ("(\d{4}-\d{1,2}-\d{1,2})", "%Y-%m-%d", ""),
        # "2014年5月"
        ("(\d{4}-\d{1,2})", "%Y-%m", ""),
        # "13年8月15日 22:46:21",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%y-%m-%d %H:%M:%S", ""),
        # "13年8月15日 22:46",
        ("(\d{2}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%y-%m-%d %H:%M", ""),
        # "8月15日 22:46:21",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "+year"),
        # "8月15日 22:46",
        ("(\d{1,2}-\d{1,2} \d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "+year"),
        # "8月15日",
        ("(\d{1,2}-\d{1,2})", "%Y-%m-%d", "+year"),
        # "3 秒前",
        ("(\d+)\s*秒前", "", "-seconds"),
        # "3 秒前",
        ("(\d+)\s*分钟前", "", "-minutes"),
        # "3 小时前",
        ("(\d+)\s*小时前", "", "-hours"),
        # "3 秒前",
        ("(\d+)\s*天前", "", "-days"),
        # 今天 15:42:21
        ("今天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-0"),
        # 昨天 15:42:21
        ("昨天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-1"),
        # 前天 15:42:21
        ("前天\s*(\d{1,2}:\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M:%S", "date-2"),
        # 今天 15:42
        ("今天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-0"),
        # 昨天 15:42
        ("昨天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-1"),
        # 前天 15:42
        ("前天\s*(\d{1,2}:\d{1,2})", "%Y-%m-%d %H:%M", "date-2"),
    ]

    for regex, dt_format, flag in regex_format_list:
        m = re.search(regex, data)
        if m:
            if not flag:
                dt = datetime.datetime.strptime(m.group(1), dt_format)
            elif flag == "+year":
                # need add year
                dt = datetime.datetime.strptime("%s-%s" % (year, m.group(1)), dt_format)
            elif flag in ("-seconds", "-minutes", "-hours", "-days"):
                # sub seconds
                flag = flag.strip("-")
                delta = eval("datetime.timedelta(%s=int(m.group(1)))" % flag)
                dt = dt - delta
            elif flag.startswith("date"):
                del_days = int(flag.split("-")[1])
                _date = dt.date() - datetime.timedelta(days=del_days)
                _date = _date.strftime("%Y-%m-%d")
                dt = datetime.datetime.strptime(
                    "%s %s" % (_date, m.group(1)), dt_format
                )
            return dt
    else:
        logger.error("Unknown datetime format: %s" % data)
        dt = None
    return dt


def oom_killed_exit():
    """
        Exit by OOM code
    Returns:

    """
    import sys

    sys.exit(137)


#
class ContainerInfo(object):
    """
    Get container info

        Tips:
            This operation will be failed when the system remaining memory are very less.
            Because os.popen need to create a new process

    """

    @classmethod
    def _get_cgroup_mem_info(cls, name):
        with os.popen("cat /sys/fs/cgroup/memory/{}".format(name)) as f:
            value = f.read()
        return value

    @classmethod
    def _node_memory_info(cls):
        info = {}
        with os.popen("cat /proc/meminfo") as f:
            meminfo = dict([x.strip().split(":") for x in f.readlines() if x.strip()])
        info["total_bytes"] = int(meminfo["MemTotal"].replace("kB", "").strip()) * 1024
        info["available_bytes"] = (
            int(meminfo["MemAvailable"].replace("kB", "").strip()) * 1024
        )
        info["memory_utilization"] = 1.0 - (
            info["available_bytes"] * 1.0 / info["total_bytes"]
        )
        return info

    @classmethod
    def _container_memory_info(cls):
        info = {}
        info["limit_in_bytes"] = int(cls._get_cgroup_mem_info("memory.limit_in_bytes"))
        info["usage_in_bytes"] = int(cls._get_cgroup_mem_info("memory.usage_in_bytes"))
        info["memory_utilization"] = (
            info["usage_in_bytes"] * 1.0 / info["limit_in_bytes"]
        )
        return info

    @classmethod
    def memory_info(cls):
        if platform.system() in ["Linux"]:
            info = {
                "node": cls._node_memory_info(),  # Node
                "container": cls._container_memory_info(),  # Container
            }
        else:
            info = {}
        return info


def send_message(*args, **kwargs):
    # raise NotImplementedError
    logger.error("send_message need Implement")


class RequestArgsPool(object):
    def __init__(self, **kwargs):
        pass

    def get(self):
        raise NotImplementedError

    def close(self):
        pass

    def add(self, *args, **kwargs):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        raise NotImplementedError


class UrlHandler(object):
    def __init__(self, **kwargs):
        self.url = kwargs.get("url", "")

    @classmethod
    def parse_qs(
        cls,
        qs,
        keep_blank_values=False,
        strict_parsing=False,
        encoding="utf-8",
        errors="replace",
    ):
        parsed_result = {}
        pairs = cls.parse_qsl(
            qs, keep_blank_values, strict_parsing, encoding=encoding, errors=errors
        )
        for name, value in pairs:
            if name in parsed_result:
                parsed_result[name].append(value)
            else:
                parsed_result[name] = [value]
        return parsed_result

    @classmethod
    def parse_qsl(
        cls,
        qs,
        keep_blank_values=False,
        strict_parsing=False,
        encoding="utf-8",
        errors="replace",
    ):
        qs, _coerce_result = parse._coerce_args(qs)
        pairs = [s2 for s1 in qs.split("&") for s2 in s1.split(";")]
        r = []
        for name_value in pairs:
            if not name_value and not strict_parsing:
                continue
            nv = name_value.split("=", 1)
            if len(nv) != 2:
                if strict_parsing:
                    raise ValueError("bad query field: %r" % (name_value,))
                # Handle case of a control-name with no equal sign
                if keep_blank_values:
                    nv.append("")
                else:
                    continue
            if len(nv[1]) or keep_blank_values:
                name = nv[0].replace("+", " ")
                # 区别在这里
                # name = unquote(name, encoding=encoding, errors=errors)
                name = _coerce_result(name)
                value = nv[1].replace("+", " ")
                # value = unquote(value, encoding=encoding, errors=errors)
                value = _coerce_result(value)
                r.append((name, value))
        return r

    @classmethod
    def remove_query(cls, url="", is_delete=lambda x: x, keep_fragment=False, **kwargs):
        """
            Remove special parameter from url
        Args:
            url:
            is_delete:
                hook for charge whether the parameter should reserved.
            keep_fragment: whether reserved fragment (#)
            **kwargs:
                encoding: url encoding, default to utf8.
        Returns:

        """
        url = url or cls.url
        # 获取参数
        p = parse.urlparse(url)
        qs = cls.parse_qs(p.query)
        #
        query = []
        for key, value in qs.items():
            if is_delete(key):
                continue
            for v in value:
                query.append("{}={}".format(key, v))
        query = "&".join(query)
        # scheme, netloc, url, params, query, fragment
        fragment = p.fragment
        if not keep_fragment:
            fragment = ""
        url = parse.urlunparse((p.scheme, p.netloc, p.path, p.params, query, fragment))
        return url


class OSSHandler(object):
    """https://help.aliyun.com/document_detail/32027.html"""

    def __init__(
        self,
        AccessKeyId="",
        AccessKeySecret="",
        endpoint="",
        bucket_name="",
        logger=None,
    ):
        self.AccessKeyId = AccessKeyId
        self.AccessKeySecret = AccessKeySecret
        self.bucket_name = bucket_name
        if not self.bucket_name:
            raise ValueError("bucket name must not empty")
        self.endpoint = endpoint
        if not endpoint:
            raise ValueError("endpoint must not empty")

        self.logger = logger or log.get_logger(__file__)

        self.bucket: Bucket = None
        self.init()

        # list obj 迭代过程中游标记录
        self.list_next_marker = ""

    def init(self):
        try:
            auth = oss2.Auth(self.AccessKeyId, self.AccessKeySecret)
            self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        except Exception as e:
            self.logger.exception(e)
        return

    def download(
        self,
        filename: str = "",
        local_file_name: str = "",
        local_dir: str = "",
        delimiter: str = "/",
        show_progress: bool = False,
        **kwargs,
    ):
        """
        文件下载
        Args:
            filename: oss文件全名 包含路径
            local_file_name: 下载到本地之后的文件名  若不指定则从filename获取 按路径切分到最后一级
            local_dir: 本地目录
            delimiter: 路径切割符
            show_progress: 是否显示下载进度
            **kwargs:

        Returns:

        """
        if not local_file_name:
            local_file_name = filename.split(delimiter)[-1]
        local_file_name = os.path.join(local_dir, local_file_name)

        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r下载 {} -> {:.2f}%".format(local_file_name, rate), end="")
                sys.stdout.flush()

        res = self.bucket.get_object_to_file(
            filename,
            local_file_name,
            progress_callback=progress_callback if show_progress else None,
        )
        if show_progress:
            print("\r")
        self.logger.debug("oss文件下载成功: {} > {}".format(filename, local_file_name))
        return res

    def download_object(self, filename, show_progress: bool = False, **kwargs):
        """
            文件下载
        Args:
            filename: oss文件全名 包含路径
            show_progress: 是否显示下载进度
            **kwargs:

        Returns:
            file-like object

        """

        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r下载 {} -> {:.2f}%".format(filename, rate), end="")
                sys.stdout.flush()

        res = self.bucket.get_object(
            filename, progress_callback=progress_callback if show_progress else None
        )
        return res

    def download_prefix(
        self, prefix: str = "", local_dir: str = "", delimiter: str = "/", **kwargs
    ):
        """
            批量下载
        Args:
            prefix:
            local_dir:
            delimiter:
            **kwargs:

        Returns:

        """
        for obj in self.list(prefix=prefix):
            filename = obj.key
            if filename.endswith(delimiter):
                continue
            self.download(filename=filename, local_dir=local_dir, delimiter=delimiter)
        return

    def exists(self, file_name=None, remote_dir=None, root_dir=None, key=None):
        """
            check file exists
        Args:
            file_name:
            remote_dir:
            root_dir:
            key:

        Returns:

        """
        if not key:
            if not remote_dir or not root_dir:
                raise ValueError("args remote_dir or root_dir empty")
            file_name = os.path.basename(file_name)
            key = "{}/{}/{}".format(
                root_dir.strip("/"), remote_dir.strip("/"), file_name
            )
        return self.bucket.object_exists(key)

    def get_dir_info(self, prefix: str = "", cache: bool = False, **kwargs) -> dict:
        """
            获取指定前缀的目录内文件（递归）信息 例如：文件数、总文件大小
        Args:
            prefix:
            cache:
            **kwargs:

        Returns:
            {
                "count": 1,
                "size": 10000, # Byte
                "files": [
                        {
                            "name": "",
                            "size": "",
                        },
                    ], # 文件列表
            }

        """
        cache_filename = "cache_{}.json".format(
            hashlib.md5(prefix.encode()).hexdigest()
        )
        cache_filename = os.path.join(temp_dir, cache_filename)
        if cache:
            # 尝试从本地获取
            if os.path.exists(cache_filename):
                with open(cache_filename, "r") as f:
                    info = json.load(f)
                return info

        file_count = 0
        file_size = 0
        files = []
        for obj in self.list(prefix=prefix):
            file_count += 1
            file_size += obj.size
            files.append({"name": obj.key, "size": obj.size})
        #
        files.sort(key=lambda x: x["name"])
        #
        info = {"count": file_count, "size": file_size, "files": files}
        with open(cache_filename, "w") as f:
            f.write(json.dumps(info))
        return info

    def list_dir(self, prefix: str = "", level: int = 1, **kwargs):
        """
            遍历指定目录下的子目录
        Args:
            prefix:
            level:
            **kwargs:

        Returns:

        """
        if level < 1:
            return
        prefix = "{}/".format(prefix.strip("/"))
        for obj in self.list(prefix=prefix, delimiter="/", **kwargs):
            if obj.is_prefix():
                if level == 1:
                    yield obj.key
                yield from self.list_dir(prefix=obj.key, level=level - 1)
        return

    def list(
        self, prefix: str = "", delimiter: str = "", marker="", **kwargs
    ) -> Generator[SimplifiedObjectInfo, None, None]:
        """
        遍历指定前缀的文件
            tips:
                1. oss 本无目录
                2. 若想递归遍历某个 prefix 下的全部文件 则仅设置prefix即可
        Args:
            prefix:
            delimiter:
                目录分隔符 默认空
                若设置此参数则
                    迭代过程中仅返回prefix下的文件和子文件夹名 而不对子文件夹进行递归
            marker:
            **kwargs:

        Returns:

        """
        obj_iter = oss2.ObjectIterator(
            self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=1000,
            max_retries=10,
            marker=marker,
        )
        for idx, obj in enumerate(obj_iter):
            yield obj
            if idx % 100 == 0:
                self.list_next_marker = obj_iter.next_marker

    def quit(self):
        return

    def upload(
        self,
        file_path,
        remote_dir="",
        root_dir="",
        remote_filename="",
        delete=False,
        overwrite=True,
        show_progress=False,
        **kwargs,
    ):
        """
        上传文件
            Examples:
                >>> OSSHandler().upload(__file__, "default", "/spider")
        Args:
            file_path: 本地文件路径
            remote_dir: 远程服务器文件夹
            root_dir: 远程服务器根文件夹
            delete: 上传完成是否删除本地文件 默认为 False
            overwrite: 是否覆盖现有文件 默认True
            show_progress:
            **kwargs:

        Returns:

        """
        if not remote_dir or not root_dir:
            raise ValueError("remote_dir or root_dir empty")
        file_name = os.path.basename(file_path)
        key = "{}/{}/{}".format(
            root_dir.strip("/"), remote_dir.strip("/"), remote_filename or file_name
        )
        if not overwrite:
            if self.bucket.object_exists(key):
                # 返回  meta
                self.logger.debug("文件已存在 不上传: {}".format(key))
                return self.bucket.get_object_meta(key)

        #
        def progress_callback(consumed_bytes, total_bytes):
            if total_bytes:
                rate = 100 * (float(consumed_bytes) / float(total_bytes))
                print("\r上传 {} -> {:.2f}%".format(file_name, rate), end="")
                sys.stdout.flush()

        resp = None
        for i in range(3):
            try:
                file_size = os.path.getsize(file_path)
                if file_size < 10 * 1024 * 1024:
                    resp = self.bucket.put_object_from_file(key, file_path)
                else:
                    # 分片大小
                    part_size = determine_part_size(
                        file_size, preferred_size=1000 * 1024
                    )
                    # 初始化分片
                    upload_id = self.bucket.init_multipart_upload(key).upload_id
                    parts = []
                    # 逐个上传分片
                    with open(file_path, "rb") as fileobj:
                        part_number = 1
                        offset = 0
                        while offset < file_size:
                            num_to_upload = min(part_size, file_size - offset)
                            result = self.bucket.upload_part(
                                key,
                                upload_id,
                                part_number,
                                SizedFileAdapter(fileobj, num_to_upload),
                            )
                            parts.append(PartInfo(part_number, result.etag))
                            offset += num_to_upload
                            part_number += 1
                            if show_progress:
                                progress_callback(part_size * part_number, file_size)
                    if show_progress:
                        # 这个是为了在连续上传时不把进度条覆盖掉
                        print("")
                    # 完成分片上传
                    resp = self.bucket.complete_multipart_upload(key, upload_id, parts)
                break
            except Exception as e:
                self.logger.exception(e)
                self.init()
        if delete:
            try:
                os.remove(file_path)
            except Exception as e:
                pass
        return resp

    def upload_object(
        self, file_obj, file_name, remote_dir="", root_dir="", overwrite=True, **kwargs
    ):
        """
        上传文件对象 或者 文件内容
        Args:
            file_obj:
            file_name:
            remote_dir:
            root_dir:
            overwrite:
            **kwargs:

        Returns:

        """
        if not remote_dir or not root_dir:
            raise ValueError("args remote_dir or root_dir empty")
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        if not overwrite:
            if self.bucket.object_exists(key):
                # 返回  meta
                self.logger.debug("文件已存在 不上传: {}".format(key))
                return self.bucket.get_object_meta(key)
        resp = None
        for i in range(3):
            try:
                resp = self.bucket.put_object(key, file_obj)
                break
            except Exception as e:
                self.logger.exception(e)
                self.init()
        return resp

    def upload_object_part(
        self, data, file_name, upload_id, part_id, remote_dir="", root_dir=""
    ):
        """
        上传分片内容

        Args:
            data:
            file_name:
            upload_id:
            part_id:
            remote_dir:
            root_dir:

        Returns:

        """
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        result = self.bucket.upload_part(key, upload_id, part_id, data)
        return result

    def complete_multipart_upload(
        self, file_name, upload_id, parts, remote_dir="", root_dir=""
    ):
        """
        完成分片上传任务
        Args:
            file_name:
            upload_id:
            parts: 分片信息 [(part_id, etag), ...]
            remote_dir:
            root_dir:

        Returns:

        """
        key = "{}/{}/{}".format(root_dir.strip("/"), remote_dir.strip("/"), file_name)
        parts = [PartInfo(x[0], x[1]) for x in parts]
        return self.bucket.complete_multipart_upload(key, upload_id, parts)

    def restore_obj(self, key):
        r = self.bucket.restore_object(key)
        return r


# Weibo url and mid
ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if num == 0:
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return "".join(arr)


def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = strlen - (idx + 1)
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num


def mid_to_url(midint):
    """"""
    midint = str(midint)[::-1]
    size = len(midint) // 7 if len(midint) % 7 == 0 else len(midint) // 7 + 1
    result = []
    for i in range(size):
        s = midint[i * 7 : (i + 1) * 7][::-1]
        s = base62_encode(int(s))
        s_len = len(s)
        if i < size - 1 and len(s) < 4:
            s = "0" * (4 - s_len) + s
        result.append(s)
    result.reverse()
    return "".join(result)


def url_to_mid(url):
    """"""
    url = str(url)[::-1]
    size = len(url) // 4 if len(url) % 4 == 0 else len(url) // 4 + 1
    result = []
    for i in range(size):
        s = url[i * 4 : (i + 1) * 4][::-1]
        s = str(base62_decode(str(s)))
        s_len = len(s)
        if i < size - 1 and s_len < 7:
            s = (7 - s_len) * "0" + s
        result.append(s)
    result.reverse()
    return int("".join(result))


def cron_exec(
    func: Callable,
    cron_expr: str,
    max_times=-1,
    default_utc=False,
    countdown=True,
    countdown_desc="next execute after",
    ignore_exception=False,
    func_name="",
):
    """

    Args:
        func: function to be executed
        cron_expr:
        max_times: Max execute times
        default_utc: whether to use UTC
        countdown:
        countdown_desc:
        ignore_exception: if set True, exception will not interrupt the next execute

    Returns:

    """
    from crontab import CronTab

    #
    exec_count = 0
    cron = CronTab(cron_expr)
    #
    func_name = func_name or func.__name__
    exc = None
    while 1:
        if 0 < max_times < exec_count:
            break
        _next = cron.next(default_utc=default_utc)
        for i in range(int(_next)):
            time.sleep(1)
            _next -= 1
            if countdown:
                print(f"\r{countdown_desc}: {int(_next)} s", end="")
        print()
        print(f" {func_name} starting ...")
        try:
            func()
        except Exception as e:
            exc = e
            if not ignore_exception:
                raise e
            else:
                logger.exception(e)

        print(f"{func_name} finished ...")
        time.sleep(1)
        exec_count += 1
    return exc


if __name__ == "__main__":
    pass
