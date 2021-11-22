# coding:utf8
"""
oss database
tips:
    有很多redis写入操作在碰到OOM异常时被忽略 主要是为了当redis内存用完时此脚本仍然可以运行以清理内存
"""
import os
import re
import sys
import copy
import json
import time
import uuid
import datetime
import tempfile
from typing import Dict

import redis

from graper import util
from graper.utils import log

#
redis_pool_cache = {}


class RedisCacheList(object):
    def __init__(
        self,
        key="",
        cache_timeout=300,
        cache_limit=50000,
        cache_size_limit=32 * 1024 * 1024,
        record_redis_uri="",
        data_cache_redis: Dict = None,
        data_cache_key_count_limit=1000,
        redis_lock_class: util.RedisLock.__class__ = None,
        compress_data=False,
        **kwargs
    ):
        """
        Args:
            key:
            cache_timeout:
            cache_limit:
            cache_size_limit:
            record_redis_uri: record data count in every day
            data_cache_redis:
                {0: "redis://....", 1:"redis://...."}
            data_cache_key_count_limit:
            redis_lock_class: util.RedisLock
            compress_data:
            **kwargs:
        """
        self._base_key = key
        self._data_key = "{}:data".format(self._base_key)
        self._flush_time_key = "{}:flush_time".format(self._base_key)
        self._data_size_key = "{}:size".format(self._base_key)
        #
        self.redis_lock_class = redis_lock_class
        assert self.redis_lock_class, "must special self.redis_lock_class"
        #

        self.logger = kwargs.get("logger", log.get_logger(__file__))

        self.logger.info("init <RedisCacheList: {}>".format(self._base_key))
        #
        self.record_redis_client = redis.StrictRedis.from_url(record_redis_uri)
        #
        self.table_attrs_key = "h_table_attrs"
        #
        if not data_cache_redis:
            raise ValueError("data_cache_redis is {}".format(data_cache_redis))
        self.data_cache_redis = data_cache_redis
        self.data_cache_key_count_limit = data_cache_key_count_limit
        self.redis_client = self.get_redis_conn()
        if not self.redis_client:
            raise Exception(
                "get data cache redis client failed: {}".format(self._base_key)
            )

        # 缓存清理间隔
        self._cache_timeout = cache_timeout
        # 缓存清理大小
        self._cache_size_limit = cache_size_limit
        # 缓存数据数量
        self._cache_limit = cache_limit
        # 针对html类型做特殊处理  太大了
        if self._base_key.startswith("html:"):
            self._cache_limit = 1000 if self._cache_limit > 1000 else self._cache_limit

        # 是否在数据入redis前压缩
        self.compress_data = compress_data

    def __repr__(self):
        return "<RedisCacheList len: {}>".format(self.len())

    def _get_redis_conn(self):
        """
        可随时增加新的redis, 应对以后可能出现的一个redis做缓存不够的情况
        根据key名判断需要放到哪个redis中
        根据序号依次判断此redis是否满员
            满员跳过
            不满员注册

        """
        redis_index_list = list(self.data_cache_redis.keys())
        redis_index_list.sort()
        for redis_index in redis_index_list:
            data_cache_key_store_key = "spider_data_cache_key_count:{}".format(
                redis_index
            )
            # 先检查此key是否属于此redis
            if self.record_redis_client.zrank(data_cache_key_store_key, self._base_key):
                # 存在则直接返回
                break
            # 检查此序号redis的现有key数
            key_count = self.record_redis_client.zcard(data_cache_key_store_key)
            if key_count >= self.data_cache_key_count_limit:
                # 此redis已满员则跳过
                self.logger.info("data_cache_redis: {} 满员")
                continue
            # 将此key加入此序号redis 然后结束
            self.record_redis_client.zadd(
                data_cache_key_store_key, {self._base_key: time.time()}
            )
            break
        else:
            # 没找到合适的
            return None
        redis_uri = self.data_cache_redis[redis_index]
        if redis_uri not in redis_pool_cache:
            connection_pool = redis.BlockingConnectionPool.from_url(
                redis_uri, max_connections=100, timeout=60
            )
            redis_pool_cache[redis_uri] = connection_pool
        return redis.StrictRedis(connection_pool=redis_pool_cache[redis_uri])

    def get_redis_conn(self):
        with self.redis_lock_class(
            key="{}:data_cache_get_redis_conn".format(self._base_key),
            timeout=60,
            wait_timeout=100,
        ) as _lock:
            if _lock.locked:
                return self._get_redis_conn()

    def len(self):
        return self.redis_client.llen(self._data_key)

    def is_flush_len(self):
        return self.len() > self._cache_limit

    # 模拟列表操作
    def append(self, data):
        if not data:
            return 0
        if not isinstance(data, list):
            data = [data]
        # 转换为json
        data = [json.dumps(x, ensure_ascii=False) for x in data]
        if self.compress_data:
            data = [util.compress_text(x.encode("utf8")) for x in data]
        # 蛋疼的统计  不遍历子元素的话 特别不准
        _size = sum([sys.getsizeof(x) for x in data])
        pipe = self.redis_client.pipeline(transaction=False)
        # 统计size
        pipe.incrby(self._data_size_key, _size)
        # 保存数据
        pipe.lpush(self._data_key, *data)
        resp = pipe.execute()
        return None

    extend = append

    # 大小判定
    def size(self):
        """数据占用空间大小 size"""
        size = self.redis_client.get(self._data_size_key)
        size = int(size) if size else 0
        return size

    def is_flush_size(self):
        return self.size() >= self._cache_size_limit

    # 超时判定
    @property
    def last_flush_time(self):
        """获取上次缓存时间"""
        last_flush_time = self.redis_client.get(self._flush_time_key)
        if last_flush_time:
            last_flush_time = float(last_flush_time)
        else:
            try:
                self.redis_client.set(self._flush_time_key, time.time())
            except Exception as e:
                if "OOM" in str(e):
                    pass
                else:
                    raise e
            last_flush_time = time.time()
        return last_flush_time

    @last_flush_time.setter
    def last_flush_time(self, value):
        """设置上次缓存时间"""
        try:
            self.redis_client.set(self._flush_time_key, time.time())
        except Exception as e:
            if "OOM" in str(e):
                pass
            else:
                raise e
        return

    def is_flush_time(self):
        """检查是否到达"""
        return time.time() - self.last_flush_time > self._cache_timeout

    def is_flush(self):
        """检查是否应该清理缓存"""
        if self.is_flush_len():
            self.logger.info("oss缓存条数限制达到")
            return 1
        if self.is_flush_size():
            self.logger.info("oss缓存大小限制达到")
            return 1
        if self.is_flush_time():
            self.logger.info("oss缓存超时限制达到")
            return 1
        return 0

    def get_unique_id(self):
        """
        获取唯一id 增加时间戳方便查看
        :return:
        """
        return "{}_{}".format(str(time.time()), str(uuid.uuid1()))

    def split(self, key):
        """"切分"""
        data_length_limit = int(self._cache_limit * 1.2)
        # 获取一条数据
        _data = self.redis_client.lindex(key, 0)
        if _data is not None:
            _data_size = sys.getsizeof(_data)
            _data_length_limit = int(self._cache_size_limit // _data_size)
            # 根据数据大小算出来一个长度上限  使用最小的
            if _data_length_limit < data_length_limit:
                data_length_limit = _data_length_limit
            data_length_limit = max(data_length_limit, 1)
        else:
            # 没有数据 无须切分
            return
        while 1:
            le = self.redis_client.llen(key)
            if le <= data_length_limit:
                # 这里必须是 <= 否则永远在切分
                self.logger.info("数据无须切分: {} 长度{}".format(key, le))
                break
            #
            new_key = key.split(":")
            new_key[-1] = self.get_unique_id()
            new_key = ":".join(new_key)
            # 锁住当前 new_key 否则 可能别的进程会在切分过程中去处理 造成数据丢失
            with self.redis_lock_class(
                key=new_key, timeout=300, wait_timeout=0
            ) as _lock:
                if _lock.locked:
                    pipe = self.redis_client.pipeline()
                    for i in range(data_length_limit):
                        pipe.rpoplpush(key, new_key)
                        if i % 1000 == 0:
                            pipe.execute()
                    pipe.execute()
                    self.logger.info(
                        "切分成功: {} 长度: {}".format(
                            new_key, self.redis_client.llen(new_key)
                        )
                    )
        return

    def flush(self):
        """获取缓存数据并返回"""
        # 此处加锁保证同一时刻只有一个人 # 不等待锁
        with self.redis_lock_class(
            key=self._data_key, timeout=10, wait_timeout=0
        ) as _lock:
            if _lock.locked:
                try:
                    # 重命名
                    new_data_key = "{}:temp:{}".format(
                        self._data_key, self.get_unique_id()
                    )
                    pipe = self.redis_client.pipeline()
                    pipe.rename(self._data_key, new_data_key)
                    # 清空大小
                    pipe.delete(self._data_size_key)
                    pipe.execute()
                except Exception as e:
                    pass
                # 多加点注释 这里有点晕
                # 1、锁不等待 直接跳过 则同时进来的只有一个能执行操作
                # 2、操作完成后停2s
                #       此时已清空数据缓存
                #           跟我使用同样清理条件进来的人拿不到锁
                #           2s 之后的人就不满足条件了 进不来
                #       其实主要是防止这里的操作太快 导致比我慢一点点的人也能拿到锁
                #       晚点释放锁
                # 停留足够的时间 让别人基本不可能在未达到清理条件时进入
                time.sleep(2)
        # 如果你发现日志中特别的多获取到数据长度为0的日志 并且通过看代码找到了这里
        # 嗯 这个是正常的情况 因为这里会有很多线程先把key列表拿到 然后每个线程都去处理相同的key列表
        # 大家是一个竞争的关系 可能我处理的是别人已经处理过的key
        # 此时key已经不存在了 所以 重复处理没关系的
        for i in range(3):
            # 防止切分特别多的key 然后由于没有更新key列表 所以无人处理
            keys = self.redis_client.keys("{}:temp:*".format(self._data_key))
            if not keys:
                break
            for key in keys:
                # 判断key是否存在
                if not self.redis_client.llen(key):
                    continue
                key = key.decode()
                #
                # 不等待锁
                with self.redis_lock_class(
                    key=key, timeout=120, wait_timeout=0
                ) as _lock:
                    if _lock.locked:
                        # 大key切分
                        # 异常情况下 导致key超长 比如一直加锁失败 锁bug。。。。
                        self.split(key)
                        #
                        self.logger.info("开始获取缓存数据: {}".format(key))
                        ori_flush_data = self.redis_client.lrange(key, 0, -1)
                        self.logger.info(
                            "获取缓存数据成功: {} 长度: {}".format(key, len(ori_flush_data))
                        )
                        if not ori_flush_data:
                            continue
                        #
                        flush_data = []
                        for item in ori_flush_data:
                            try:
                                if not isinstance(item, bytes):
                                    item = item.encode(encoding="utf8")
                                item = util.decompress_text(item)
                            except Exception as e:
                                pass
                            item = json.loads(item)
                            flush_data.append(item)
                        yield key, flush_data
            time.sleep(2)
        self.last_flush_time = time.time()

    def clean(self, key, **kwargs):
        le = self.redis_client.llen(key)
        #
        resp = self.redis_client.delete(key)
        self.logger.info("清理缓存数据: {} {}".format(key, resp))
        # 统计长度
        if "table_name" in kwargs:
            count_key = "{}:{}".format(
                kwargs.get("table_name"), datetime.date.today().strftime("%Y-%m-%d")
            )
            try:
                self.record_redis_client.incrby(count_key, le)
            except Exception as e:
                if "OOM" in str(e):
                    pass
                else:
                    raise e
        return resp

    def set_attrs(self, **kwargs):
        """
        保存属性 主要是oss路径
        :param kwargs:
        :return:
        """
        old_attrs = self.record_redis_client.hget(self.table_attrs_key, self._base_key)
        if not old_attrs:
            old_attrs = {}
        else:
            old_attrs = json.loads(old_attrs)
        if "ts" not in kwargs:
            kwargs["ts"] = time.time()
        old_attrs.update(**kwargs)
        return self.record_redis_client.hset(
            self.table_attrs_key, self._base_key, json.dumps(old_attrs)
        )

    def get_attrs(self):
        """获取属性"""
        attrs = self.record_redis_client.hget(self.table_attrs_key, self._base_key)
        return json.loads(attrs) if attrs else {}


class DefaultDict(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

        self.maps = {}

        # 加锁
        self.getitem_lock = None

    def __repr__(self):
        return "{}".format(self.maps)

    def keys(self):
        return self.maps.keys()

    def __getitem__(self, item):
        if not self.getitem_lock:
            import threading

            self.getitem_lock = threading.RLock()
        with self.getitem_lock:
            # 此处加锁 由于初始化RedisCacheList较慢 所以可能会导致并发初始化
            # 会增加redis压力 并且拖慢代码速度
            if item not in self.maps:
                cache_list = RedisCacheList(key=item, **self.kwargs)
                self.maps[item] = cache_list
        return self.maps[item]


class OSSOpt(object):
    temp_dir = tempfile.gettempdir()

    def __init__(
        self,
        setting_dict: dict,
        html_cache_bucket: str = "",
        test_buckets: list = None,
        cache_timeout: int = 300,
        cache_limit: int = 50000,
        cache_size_limit: int = 32 * 1024 * 1024,
        html_cache_redis_uri: str = "",
        record_redis_uri: str = "",
        data_cache_redis: dict = None,
        data_cache_key_count_limit: int = 1000,
        keep_gtime: bool = False,
        redis_lock_class: util.RedisLock.__class__ = None,
        logger=None,
        compress_cache_data=False,
        **kwargs
    ):
        """
        Args:
            setting_dict:
            html_cache_bucket:
            test_buckets:
            cache_timeout: oss上传缓存间隔 默认300秒
            cache_limit: oss上传缓存条数 默认 50000条
            cache_size_limit:
            html_cache_redis_uri:
            record_redis_uri:
            data_cache_redis:
            data_cache_key_count_limit:
            keep_gtime: 是否覆盖gtime
            logger:
            compress_cache_data: 是否压缩缓存数据
            **kwargs:
        """
        self.kwargs = kwargs
        self.setting_dict = setting_dict
        self.test_buckets = test_buckets

        self.AccessKeyId = self.setting_dict["username"].strip()
        self.AccessKeySecret = self.setting_dict["password"].strip()
        self.endpoint = self.setting_dict["host"].strip()
        self.bucket_name = self.setting_dict["db"].strip()
        self.service_line = (
            self.setting_dict["params"].get("service_line", [""])[0].strip()
        )
        self.module = self.setting_dict["params"].get("module", [""])[0].strip()
        if not self.service_line or not self.module:
            raise ValueError("no service_line or module")

        self.file_handler = util.OSSHandler(
            AccessKeyId=self.AccessKeyId,
            AccessKeySecret=self.AccessKeySecret,
            endpoint=self.endpoint,
            bucket_name=self.bucket_name,
        )
        if html_cache_bucket:
            self.html_file_handler = util.OSSHandler(
                AccessKeyId=self.AccessKeyId,
                AccessKeySecret=self.AccessKeySecret,
                endpoint=self.endpoint,
                bucket_name=html_cache_bucket,
            )
        else:
            self.html_file_handler = None

        self.redis_lock_class = redis_lock_class
        self.table_name = ""
        #
        self.keep_gtime = keep_gtime

        self.logger = logger or log.get_logger(__file__)
        # 数据存储目录
        self.root_dir_data = kwargs.get("root_dir_data") or "data"
        # html存放目录
        self.root_dir_html = kwargs.get("root_dir_html") or "html"

        # cache
        self.cache_timeout = cache_timeout
        self.cache_limit = cache_limit
        self.cache_size_limit = cache_size_limit  # 64M 转换为字节bytes

        self._data_cache = DefaultDict(
            cache_timeout=self.cache_timeout,
            cache_limit=self.cache_limit,
            cache_size_limit=self.cache_size_limit,
            record_redis_uri=record_redis_uri,
            data_cache_redis=data_cache_redis,
            data_cache_key_count_limit=data_cache_key_count_limit,
            logger=self.logger,
            redis_lock_class=self.redis_lock_class,
            compress_data=compress_cache_data,
        )

        # 缓存检查时间间隔
        self.cache_check_interval = 20
        self.last_cache_check_time = 0

        # 获取html缓存记录redis
        self.redis_client_html_cache = redis.StrictRedis.from_url(html_cache_redis_uri)
        # html缓存信息
        self.html_cache_info_key = "html_cache_info"

    def close(self):
        self.logger.info("oss_db关闭 开始清理缓存数据")
        self.flush_cache(force=1, is_all=1)
        # 再执行一次 防止缓存中遗留数据
        time.sleep(5)
        self.flush_cache(force=1, is_all=1)
        self.logger.info("缓存清理完成: {}".format(self._data_cache))
        return

    def handle_values(self, data, data_type=""):
        """
        处理字典中的值 转换为json接受的格式
        :param data: 数据
        :param data_type: 数据类型  html、data
        :return: OrderedDict
        """
        for k in data.keys():
            # 缓存专用key 在入库时会被删除
            if k == "__cache":
                continue
            v = data[k]
            if isinstance(v, str):
                v = v.strip()
            elif isinstance(v, (int, float)):
                pass
            elif isinstance(v, (datetime.date, datetime.time)):
                v = str(v)
            data[k] = v
        if data_type == "html":
            # 强制必须存在html
            assert "html" in data
        # 处理html字段
        html = data.get("html", "")
        if html:
            html = html.encode("utf8").strip()
            # html压缩
            html = util.compress_html(html)
            data["html"] = html.decode()
        data.update({"uuid": str(uuid.uuid1())})
        # 追加字段
        if not self.keep_gtime or "gtime" not in data:
            data.update({"gtime": int(time.time() * 1000)})
        return data

    @classmethod
    def generate_file(cls, data):
        if isinstance(data, dict):
            data = [data]
        filename = "{}.json".format(str(uuid.uuid1()))
        filename = os.path.join(cls.temp_dir, filename)

        with open(filename, "w", encoding="utf8") as f:
            for item in data:
                item.pop("__cache", "")
                f.write("{}\n".format(json.dumps(item, ensure_ascii=False)))
        return filename

    @classmethod
    def get_remote_time_path(cls):
        """获取上传文件路径中的时间部分
        默认：
            datetime.datetime.now().strftime("%Y/%m/%d/%H/%M/%S")

        之所以拿出来是为了爬虫数据从mysql转oss时需要调整时间路径 可覆盖此方法
        """
        return datetime.datetime.now().strftime("%Y/%m/%d/%H/%M/%S")

    def upload(
        self,
        data,
        table_name: str = "",
        root_dir: str = "",
        data_type: str = "data",
        compress: bool = None,
        **kwargs
    ):
        """
        数据上传到oss
        :param data: 数据列表
        :param table_name: 表名
        :param root_dir: 根目录
        :param data_type: 数据类型 data、html
        :param compress: 是否压缩
        :return:
        """
        data = copy.deepcopy(data)
        filepath = self.generate_file(data)
        # 兼容html类型
        file_handler = self.file_handler
        if data_type == "html":
            table_name = table_name.replace("html:", "")
            file_handler = self.html_file_handler
            if not file_handler:
                raise Exception("没有指定 html_cache_bucket")

        if not root_dir:
            if data_type == "html":
                root_dir = self.root_dir_html
            elif data_type == "data":
                root_dir = self.root_dir_data

        remote_dir = "{service_line}/{module}/{tablename}/{time}/".format(
            **{
                "service_line": self.service_line,
                "module": self.module,
                "tablename": table_name.split(".")[-1],
                "time": self.get_remote_time_path(),
            }
        )
        # 压缩
        if compress is None:
            compress = True
            if file_handler.bucket_name in self.test_buckets:
                compress = False
        if compress:
            # 上传压缩文件 2018/10/24
            # 压缩并删除原文件
            snappy_filepath = "{}.snappy".format(filepath)
            util.snappy_hadoop_compress_file(filepath, snappy_filepath, delete_src=True)
            resp = file_handler.upload(
                snappy_filepath, remote_dir=remote_dir, root_dir=root_dir, delete=True
            )
        else:
            # 上传原文件
            resp = file_handler.upload(
                filepath, remote_dir=remote_dir, root_dir=root_dir, delete=True
            )
        return resp

    def add(self, data: dict, *, table_name: str = "", cache: bool = True, **kwargs):
        """
        保存数据
        :param data: 数据
        :param table_name: 表名
        :param kwargs:
        :return:
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        table_name = table_name.strip()
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        # 处理数据
        data = self.handle_values(data)
        if cache:
            self._data_cache[table_name].append(data)
            self.flush_cache(table_name)
            del data
            return 1
        return self.upload(data, table_name=table_name, data_type="data")

    def add_many(
        self,
        data,
        *,
        table_name: str = "",
        batch: int = 100,
        cache: bool = True,
        **kwargs
    ):
        """
        保存数据  无视错误
        :param data: 数据
        :param table_name: 表名
        :param kwargs:
        :return:
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        table_name = table_name.strip()
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        if not isinstance(data, (list, tuple)):
            data = [data]
        resp = 0
        while data:
            _data = data[:batch]
            _data = [self.handle_values(x) for x in _data]
            # 处理缓存
            if cache:
                self._data_cache[table_name].extend(_data)
                self.flush_cache(table_name)
            else:
                self.upload(_data, table_name=table_name, data_type="data")
            del _data
            data = data[batch:]
        return resp

    def add_html(self, data, *, table_name: str = "", cache: bool = True, **kwargs):
        """
        保存数据
        :param data: 数据
        :param table_name: 表名
        :param kwargs:
        :return:
        """
        # todo  合并到add
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        table_name = table_name.strip()
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))

        table_name = "html:{}".format(table_name)

        # 处理数据
        data = self.handle_values(data, data_type="html")
        if cache:
            self._data_cache[table_name].append(data)
            self.flush_cache(table_name)
            return 1
        return self.upload(data, table_name=table_name, data_type="html")

    def add_many_html(
        self,
        data,
        *,
        table_name: str = "",
        batch: int = 100,
        cache: bool = True,
        **kwargs
    ):
        """
        保存数据
        :param data: 数据
        :param table_name: 表名
        :param kwargs:
        :return:
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        table_name = table_name.strip()
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        if not isinstance(data, (list, tuple)):
            data = [data]

        table_name = "html:{}".format(table_name)

        resp = 0
        while data:
            _data = data[:batch]
            _data = [self.handle_values(x, data_type="html") for x in _data]
            # 处理缓存
            if cache:
                self._data_cache[table_name].extend(_data)
                self.flush_cache(table_name)
            else:
                self.upload(_data, table_name=table_name, data_type="html")
            data = data[batch:]
        return resp

    def flush_cache(self, table_name: str = "", force: int = 0, is_all: int = 0):
        """
        刷新缓存
        Args:
            table_name:
            force:
            is_all: 是否刷新所有表

        Returns:

        """
        # 优先级
        # table_name > is_all
        table_name_list = []
        if is_all:
            table_name_list = self._data_cache.keys()
        if table_name:
            table_name_list = [table_name]

        # 检查间隔设置为1分钟 减少redis操作
        if not force:
            if time.time() - self.last_cache_check_time < self.cache_check_interval:
                return
        self.last_cache_check_time = time.time()
        for table_name in table_name_list:
            cache_list = self._data_cache[table_name]
            # 强制刷新
            if force or cache_list.is_flush():
                #
                try:
                    # 保存表的oss属性
                    cache_list.set_attrs(
                        bucket_name=self.bucket_name,
                        service_line=self.service_line,
                        module=self.module,
                    )
                except Exception as e:
                    self.logger.exception(e)
                #
                for _temp_data_key, flush_data in cache_list.flush():
                    if flush_data:
                        if table_name.startswith("html:"):
                            data_type = "html"
                        else:
                            data_type = "data"
                        # 上传
                        resp = self.upload(
                            flush_data, table_name=table_name, data_type=data_type
                        )
                        self.logger.info(
                            "oss缓存数据上传: {} {}条 状态: {}".format(
                                table_name, len(flush_data), resp.status
                            )
                        )
                        del flush_data
                        if resp.status == 200:
                            cache_list.clean(_temp_data_key, table_name=table_name)
        return

    def get_html_cache_info(
        self,
        table_name: str = "",
        start: str = "",
        end: str = "",
        use_cache: bool = True,
        **kwargs
    ) -> dict:
        """
        获取html缓存信息
        tips:
            1.文件越多 此函数执行越慢
        :param table_name: 表名
        :param start: 开始时间 2019-01-01 00:00:00  必须是此格式
        :param end: 结束时间 2019-02-02 00:00:00 必须是此格式
        :param use_cache: 是否使用redis中的缓存信息
        :param kwargs:
        :return: {
                "count": 1,
                "size": 10000, # Byte   # size/1024 KB
                "files": [
                        "name": 2019/02/02/02/02/02/fadsf-fasdf-fasdfa.json.snappy,
                        "size": 1111
                    ], # 文件属性列表
                }
        """
        if not self.html_file_handler:
            raise Exception("没有指定 html_cache_bucket")
        if not table_name:
            raise ValueError("table_name is empty")
        if use_cache:
            # 尝试从redis中获取
            _info = self.redis_client_html_cache.hget(
                self.html_cache_info_key, table_name
            )
            if _info:
                return json.loads(_info.decode())
        if not all([start, end]):
            raise ValueError("时间范围不对: ({}, {})".format(start, end))
        if start >= end:
            raise ValueError("时间范围不对： 开始时间大于结束时间")
        #
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        base_prefix = "html/{service_line}/{module}/{table}".format(
            **{
                "service_line": self.service_line,
                "module": self.module,
                "table": table_name,
            }
        )

        def split_time(start: datetime.datetime, end: datetime.datetime):
            """
            切割时间
                切分为 小时序列 + 天序列 + 小时序列
            :param start:
            :param end:
            :return:
            """
            # 将时间归一化为小时
            start = start.replace(minute=0, second=0, microsecond=0)
            end = end.replace(minute=0, second=0, microsecond=0)
            #
            time_seq = []
            while start < end:
                if start.hour == 0:
                    # 起始为整点
                    if start + datetime.timedelta(days=1) <= end:
                        # 剩余间隔大于一天
                        time_seq.append((start.date(), "day"))
                        start = start + datetime.timedelta(days=1)
                    else:
                        # 剩余间隔小于一天
                        for hour in range(0, end.hour + 1):
                            time_seq.append((start.replace(hour=hour), "hour"))
                        break
                else:
                    # 起始非整点
                    for hour in range(start.hour, 24):
                        time_seq.append((start.replace(hour=hour), "hour"))
                    # 起始置为明天0点
                    start = (start + datetime.timedelta(days=1)).replace(hour=0)
            return time_seq

        #
        files = []
        # 优化查询速度 自动确定是按照小时还是天来切分
        time_seq = split_time(start, end)
        for _datetime, _type in time_seq:
            if _type == "hour":
                _time = _datetime.strftime("%Y/%m/%d/%H/")
            if _type == "day":
                _time = _datetime.strftime("%Y/%m/%d/")

            _prefix = "{}/{}".format(base_prefix, _time)
            # 获取全部文件列表
            _files_info = self.html_file_handler.get_dir_info(prefix=_prefix)
            files.extend(_files_info["files"])

        # 过滤掉时间范围外的文件
        filter_files = []
        for _info in files:
            filename = _info["name"]
            filename = filename.replace("{}/".format(base_prefix), "")
            _datetime = re.search("^(\d+/\d+/\d+/\d+/\d+/\d+)/", filename).group(1)
            _datetime = datetime.datetime.strptime(_datetime, "%Y/%m/%d/%H/%M/%S")
            if _datetime < start or _datetime > end:
                continue
            _info["name"] = filename
            filter_files.append(_info)
        # 按时间排序
        filter_files.sort(key=lambda x: x["name"])
        #
        file_list_info = {
            "count": len(filter_files),
            "size": sum([x["size"] for x in filter_files]),
            "files": filter_files[:],
            "table_name": table_name,
            "service_line": self.service_line,
            "module": self.module,
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end.strftime("%Y-%m-%d %H:%M:%S"),
            "ts": time.time(),
        }
        # 缓存到redis
        self.redis_client_html_cache.hset(
            self.html_cache_info_key, table_name, json.dumps(file_list_info)
        )
        return file_list_info

    def get_html_cache_file(
        self,
        file_name: str = "",
        table_name: str = "",
        local_dir: str = "",
        decompress_snappy: bool = False,
        decompress_html: bool = False,
        delete_snappy=False,
        **kwargs
    ) -> str:
        """
        html缓存文件下载
            注意由于 文件名中存在目录分隔符  所以在写入本地的时候会使用 file_name 去掉路径后的文件名
        :param file_name: 文件名 格式 2019/02/02/02/02/02/fdlaife-afdfs-fdaf-fda.json.snappy
        :param table_name: 表名
        :param local_dir: 本地缓存文件夹 若不指定则使用当前目录(但这个有时候会很诡异 建议最好指定一个目录)
        :param decompress_snappy: 是否解压 snappy
        :param decompress_html: 是否解压 html 字段  必须先解压 snappy  不建议使用此选项 因为会重复打开文件并且重复使用json解析每一行
        :param kwargs:
        :return: 下载后的文件路径
        """
        if not self.html_file_handler:
            raise Exception("没有指定 html_cache_bucket")
        # 拼接正确的oss文件名
        base_prefix = "html/{service_line}/{module}/{table}".format(
            **{
                "service_line": self.service_line,
                "module": self.module,
                "table": table_name,
            }
        )
        oss_file_name = "{}/{}".format(base_prefix, file_name.strip("/"))
        #
        local_base_file_name = oss_file_name.split("/")[-1]
        local_file_name = os.path.join(local_dir, local_base_file_name)
        if not os.path.exists(local_file_name):
            # 下载
            self.html_file_handler.download(
                oss_file_name, local_file_name=local_base_file_name, local_dir=local_dir
            )
        #
        # 解压
        if decompress_snappy:
            decompress_local_file_name = local_file_name.replace(".snappy", "")
            if not os.path.exists(decompress_local_file_name):
                util.snappy_hadoop_decompress_file(
                    local_file_name, decompress_local_file_name
                )
            if delete_snappy:
                os.remove(local_file_name)
            local_file_name = decompress_local_file_name
            if decompress_html:
                new_lines = []
                with open(local_file_name, "r", encoding="utf8") as f:
                    for line in f.readlines():
                        data = json.loads(line.strip())
                        html = data.get("html", "")
                        if html:
                            html = util.decompress_html(html.encode()).decode("utf8")
                        data["html"] = html
                        new_lines.append(json.dumps(data, ensure_ascii=False))
                #
                with open(local_file_name, "w", encoding="utf8") as f:
                    for line in new_lines:
                        f.write("{}\n".format(line))
        return local_file_name


if __name__ == "__main__":
    # 测试代码
    pass
