# coding:utf8
from graper.spiders import Request, Response, Spider  # isort:skip # noqa

import copy
import datetime
import json
import time
from typing import Union
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Full, Queue
from threading import Lock

import redis

from graper import setting, util
from graper.db import DB
from graper.utils import log

logger = log.get_logger(__file__)


class JsonTask(object):
    """
    JsonTask Object
        tips:
            Only can use update method to add new field, not support "."

    Examples:
        >>> task_obj = JsonTask(task={"name": "a", "age": 1})

        >>> task_obj = JsonTask(task='{"name": "a", "age": 1}')


        # get task field
        >>> task_obj.name
        'a'

        # change field value
        >>> task_obj.name = "b"

        # keep keyword: retry, record retry time of this task
        >>> task_obj.retry += 1

        #
        >>> task_obj.to_json()
        '{"name": "b", "age": 1, "retry": 1}'

        # generate next pages task
        >>> task_list = task_obj.generate_tasks(field="page", start=1, end=10)

        # if field not in task, return None
        >>> task_obj.no_exists is None
        True
        >>> task_obj.get("no_exists", "a") == "a"
        True

        >>> task_obj.copy(update={"page": 2})
        {"name": "b", "age": 1, "page": 2}
    """

    def __init__(self, task=None, **kwargs):
        """

        Args:
            task:
            **kwargs:
        """
        super().__init__(**kwargs)
        if isinstance(task, (str, bytes)):
            task = json.loads(task)
        if isinstance(task, JsonTask):
            task = json.loads(task.to_json())
        if not isinstance(task, dict):
            raise TypeError("task is not a dict: {} but a {}".format(task, type(task)))
        self._task = copy.deepcopy(task)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.to_json()

    def __getattr__(self, key):
        if key == "retry" and "retry" not in self._task:
            return 0
        return self._task.get(key)

    def __setattr__(self, key, value):
        if key != "_task" and key in self._task:
            self._task[key] = value
        elif key == "retry":
            self._task[key] = value
        else:
            super().__setattr__(key, value)

    def get(self, key, default=None):
        v = getattr(self, key)
        if v is None:
            v = default
        return v

    def copy(self, *, keep_retry=False, update: dict = None):
        """
            Copy self
        Args:
            keep_retry: whether keep retry value, default False
            update:
                Examples:
                    new_task_obj = task_obj.copy()
                    new_task_obj.update({"page": page+1})

                    can replace with:

                    new_task_obj = task_obj.copy(update={"page": page+1})
        Returns:

        """
        _task = self._task.copy()
        if not keep_retry:
            _task.pop("retry", 0)
        if update:
            _task.update(update)
        _obj = JsonTask(task=_task)
        return _obj

    def generate_tasks(
        self,
        field: str = "page",
        start: int = 1,
        end: int = 1,
        is_obj: bool = False,
        json_dumps: bool = False,
        keep_retry: bool = False,
    ) -> list:
        """
            Automatic generate next page task, page range is [start, end)
        Args:
            field: page number filed, defualt is "page"
            start: start page number
            end: end page number, not inclued
            is_obj: whether return a JsonTask
            json_dumps: whether use json.dumps convert task dict to string
            keep_retry:

        Returns:

        """
        task_list = []
        for i in range(start, end):
            _task = self._task.copy()
            _task[field] = i
            if not keep_retry:
                _task.pop("retry", 0)
            if json_dumps:
                _task = json.dumps(_task)
            if is_obj:
                _task = self.__class__(task=_task)
            task_list.append(_task)
        return task_list

    def to_json(self):
        return json.dumps(self._task, ensure_ascii=False)

    def to_dict(self):
        return self._task

    def update(self, *args, **kwargs):
        """
        Args:
            *args:
            **kwargs:

        Returns:

        """
        return self._task.update(*args, **kwargs)


class SingleBatchSpider(Spider):
    """
    Single Batch Spider.
        Use MySQL table to avoid loss task
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        default_redis_uri = kwargs.get("default_redis_uri", setting.default_redis_uri)
        if not self.use_sqlite:
            self.redis_client = redis.StrictRedis(
                connection_pool=redis.BlockingConnectionPool.from_url(
                    default_redis_uri, max_connections=10, timeout=60
                )
            )
        default_mysql_uri = kwargs.get("default_mysql_uri", setting.default_mysql_uri)
        if self.db is None:
            self.db = DB().create(default_mysql_uri)

        # redis集群地址
        self._simple_redis_cluster_conn: redis.Redis = None

        # 必填字段
        self.task_key = "task:temp"  # 需修改
        # 取任务表 必填
        self.task_base_table_name = ""  # 占位用  可不填
        self.task_table_name = ""
        # 数据表 占位用
        self.data_table_name = ""

        # 非必填 但一般应该是需要设置的
        # 任务字段列表 建的表中必须有id字段 用于修改状态
        self.task_field_list = []
        # 任务标识 用于日志 或 报警中的任务名字
        self.task_tag_name = ""
        # 消息通知人
        self.message_recipients = []

        # 非必填 有默认值的字段
        # 状态字段名 默认state
        self.state_field_name = "state"

        # 状态值 用于标记任务状态
        self.state_dict = {"error": -1, "finish": 1, "wait": 0, "run": 2}

        # 每次从mysql获取任务数
        self.task_mysql_limit = 10000
        self.task_mysql_limit_step = 100_000
        # 将多少条mysql任务组合为一个任务 适用于批量接口 默认1 # 大于1时 任务示例：task = {"group": [{"item_id": 1}, {"item_id": 2}]} # 固定属性 group
        self.task_group_limit = 1

        #
        self.lost_task_reset_limit = 100_000
        # 自动重置丢失任务间隔
        self.auto_check_lost_interval = -1
        self._last_check_lost_time = 0

        # 锁超时时间 针对大量任务爬虫时可能用到
        self.lock_timeout = 10 * 60
        # 等待锁时间
        self.wait_lock_timeout = 10 * 60

        # 注册爬虫启动前回调函数
        self.register_before_start(self._send_spider_start_signal)
        # 注册爬虫停止前回调函数
        self.register_before_stop(self._send_spider_stop_signal)

        # redis任务缓存
        # 是否将redis任务临时使用内存缓存
        self.cache_task = self.use_sqlite or False
        self._cache_redis_task_list = Queue(120)
        if self.use_sqlite:
            self._cache_redis_task_list = Queue(10000)
        # 是否使用线程池在获取任务时加速任务状态修改
        self.multi_update_cache_task = False

        # 是否开启急速模式 会将各种主动的sleep调低到极限
        # tips: 必须开启debug
        self.fast_mode = False

        # 以下不应被修改
        # 调试模式
        self.debug = False

    def add_task(self):
        pass

    def break_spider(self, **kwargs):
        """
        在覆盖我的时候请注意
            如果你明白覆盖我的后果 则随意
            否则 请学我调用
                if super().break_spider() == 1:
                    return 1
        Args:
            **kwargs:

        Returns:

        """
        if super().break_spider() == 1:
            return 1
        # todo 此处可能有bug  若某个爬虫覆盖了 则这套机制失效
        if self.other_spider_stoped:
            state_key = f"{self.task_key}:other_spider_stoped"
            idx = util.key2index(
                state_key, index_range=list(setting.redis_util_cluster)
            )
            self._close_reason = "redis: {},由于已有爬虫停止 故终止当前爬虫".format(idx)
            logger.debug(self._close_reason)
            return 1

    def break_wait_get_task_from_mysql(self):
        """
            从mysql中获取任务时 中断锁等待的条件
                返回True则中断等待
        Returns:

        """
        return False

    def check_lost_task(self):
        """
            当mysql取不到任务时，检查是否有任务丢失
            目前状态分布：
                0 待执行
                1 完成
                2 正在执行
                -1 任务格式错误
            判断任务全部完成标志：(执行这个函数时应该已经没有0的了)
                全为 1 或 -1  无 2
        Returns:

        """

        # 获取查看是否任务已经全部完成
        _db = self.db.copy()
        # 重置 2 为 0
        sql = f"""
            update {self.task_table_name} 
            set {self.state_field_name}={self.state_dict['wait']} 
            where {self.state_field_name}={self.state_dict['run']} 
        """
        if not self.use_sqlite:
            # https://stackoverflow.com/questions/29071169/update-query-with-limit-cause-sqlite
            sql += f" limit {self.lost_task_reset_limit}"
        _db.cursor.execute(sql)
        loss_count = _db.cursor.rowcount
        logger.debug("重置丢失任务数量: {}".format(loss_count))
        _db.close()
        return

    def delete_task(self, *, condition: dict = None):
        """
            Delete task
        Args:
            condition:

        Returns:
            affect rows
        """
        return self.db.delete(condition=condition, table_name=self.task_table_name)

    def get_task(
        self, obj: bool = False, group: bool = False, redis_delay: int = None, **kwargs
    ) -> Union[str, JsonTask]:
        """

        Args:
            obj:
            group:
            redis_delay: see self._get_task_from_redis
            **kwargs:

        Returns:
            string of json.dumps
                or
            JsonTask

        """
        if self.auto_check_lost_interval > 0:
            if self.auto_check_lost_interval < 3600:
                self.auto_check_lost_interval = 3600
            self._auto_check_lost_task()
        #
        kwargs["group"] = group
        kwargs["redis_delay"] = redis_delay
        #
        if obj:
            return self._get_task_obj(**kwargs)
        if self.task_key == "task:temp":
            if not self.use_sqlite:
                raise ValueError("no task_key: {}".format(self.task_key))
        task = self._get_task_from_redis(delay=redis_delay)
        if not task:
            self._get_task_from_mysql()
            task = self._get_task_from_redis(delay=redis_delay)
        if not task:
            self._check_lost_task()
            task = self._get_task_from_redis(delay=redis_delay)
        # 兼容 "{'a':1}"
        try:
            _task = eval(task)
            if isinstance(_task, dict):
                _task = json.dumps(_task)
                task = _task
        except:
            pass
        return task

    def get_task_from_mysql(self, redis_lock=None, **kwargs) -> int:
        """
            Get task from mysql and then ** save to redis **
        Args:
            redis_lock: 调用此函数前使用的redis锁
            **kwargs:

        Returns:
            从mysql获取的任务条数

        """
        if not self.task_field_list:
            raise NotImplemented
        _db = self.db.copy()
        task_mysql_limit = self.task_mysql_limit
        # 本次获取到的任务数记录
        current_get_task_count = 0
        while task_mysql_limit > 0:
            # 从mysql读取任务数据
            task_field_str = ", ".join(["`{}`".format(x) for x in self.task_field_list])
            step_limit = min(task_mysql_limit, self.task_mysql_limit_step)
            sql = f"""select `id`, {task_field_str} 
                      from {self.task_table_name} 
                      where {self.state_field_name}={self.state_dict["wait"]} limit {step_limit};
                    """
            # 某些情况 比如京东这里会很慢 所以做一个增加锁超时时间的操作 防止由于锁超时导致并发查询
            _query_start = time.time()
            sql_result = _db.query_all(sql)
            _query_use_time = time.time() - _query_start
            if redis_lock and _query_use_time > 10:
                redis_lock.prolong_life(int(_query_use_time))
            #

            logger.info(
                "Get {} tasks, take {:0.3} s".format(len(sql_result), _query_use_time)
            )
            if not sql_result:
                break

            # 20191101 先更新mysql状态 然后放入redis
            # 1、如果任务消耗特别快 比如京东  则大概率将已完成任务重置为2 导致任务重复执行且极大重复率
            # 2、先更新状态的情况下 除非挂了 否则没啥影响 就算挂了 也只是重置了 但任务不会重复执行
            condition_list = [str(x[0]) for x in sql_result]
            _cache_update_sql = []
            while condition_list:
                sql = "update {} set {}={} where id in ({}) and {}={}".format(
                    self.task_table_name,
                    self.state_field_name,
                    self.state_dict["run"],
                    ",".join(condition_list[:10000]),
                    self.state_field_name,
                    self.state_dict["wait"],
                )
                if self.multi_update_cache_task:
                    _cache_update_sql.append(sql)
                else:
                    _db.cursor.execute(sql)
                condition_list = condition_list[10000:]
            #
            if _cache_update_sql:
                # 批量更新
                _update_thread_pool = ThreadPoolExecutor(10)

                def _update_work(sql):
                    # 此处新建连接  多线程中不能共用  #todo 协程也不行
                    __db = _db.copy(protocol="mysql+pymysql")
                    __db.cursor.execute(sql)
                    r = __db.cursor.rowcount
                    __db.close()
                    return r

                for r in _update_thread_pool.map(_update_work, _cache_update_sql):
                    pass
                _update_thread_pool.shutdown()

            #
            task_mysql_limit -= self.task_mysql_limit_step

            _cache_group_task_list = []
            task_list = []
            for _, *field in sql_result:
                # 强转datetime
                _task = {
                    _k: "'{}'".format(_v) if isinstance(_v, datetime.date) else _v
                    for _k, _v in zip(self.task_field_list, field)
                }
                if self.task_group_limit > 1:
                    _cache_group_task_list.append(_task)
                    if len(_cache_group_task_list) >= self.task_group_limit:
                        task_list.append(json.dumps({"group": _cache_group_task_list}))
                        _cache_group_task_list = []
                else:
                    task_list.append(json.dumps(_task))
                _task_count = len(task_list) * self.task_group_limit
                if _task_count > 1000:
                    current_get_task_count += _task_count
                    if self.use_sqlite:
                        self.put_task(task_list, show_log=False)
                    else:
                        self.redis_client.lpush(self.task_key, *task_list)
                    task_list = []
            del sql_result
            if _cache_group_task_list:
                task_list.append(json.dumps({"group": _cache_group_task_list}))
                del _cache_group_task_list
            if task_list:
                current_get_task_count += len(task_list) * self.task_group_limit
                if self.use_sqlite:
                    self.put_task(task_list, show_log=False)
                else:
                    self.redis_client.lpush(self.task_key, *task_list)
                del task_list
        _db.close()
        logger.info(
            "A total of {} tasks are obtained from MySQL this time".format(
                current_get_task_count
            )
        )
        return current_get_task_count

    def make_request(self, task_obj: JsonTask, *args, **kwargs) -> Request:
        """

        Args:
            task_obj:
            *args:
            **kwargs:

        Returns:

        """
        pass

    def put_task(
        self, task, retry=True, show_log=True, task_key: str = None, message: str = ""
    ):
        """
            Append task to redis
        Args:
            task: string of json.dumps or JsonTask or a list
            retry:
            show_log:
            task_key:
            message:

        Returns:

        """
        r = 0
        #
        task_list = task if isinstance(task, list) else [task]
        task_list = [
            x if not isinstance(x, JsonTask) else x.to_json() for x in task_list
        ]

        task_list = [
            json.dumps(x, ensure_ascii=False) if not isinstance(x, str) else x
            for x in task_list
        ]
        task_size = len(task_list)
        example_task = task_list[0] if task_size == 1 else task_list
        if show_log:
            message = message or ("Task Retry" if retry else "Task New")
            logger.debug(f"{message}: {task_size} {example_task}")
        #
        if self.cache_task:
            while task_list:
                try:
                    self._cache_redis_task_list.put_nowait(task_list[-1])
                    task_list.pop()
                except Full:
                    break
            if not task_list:
                r = 1
        if task_list:
            task_key = task_key or self.task_key
            r = self.redis_client.lpush(task_key, *task_list)
        return r

    def set_task_state(
        self,
        state: int,
        condition: dict = None,
        where_sql: str = "",
        extra: dict = None,
    ) -> int:
        """
            Update task status
        Args:
            state: task state
            condition:
            where_sql:
            extra: other field to update

        Returns:

        """
        assert condition or where_sql, "must assign one of condition, where_sql"
        data = {self.state_field_name: state}
        assert isinstance(state, int), "state value must be integer"
        data.update(extra or {})
        r = self.db.update(
            data,
            condition=condition,
            table_name=self.task_table_name,
            where_sql=where_sql,
        )
        return r

    def send_message(self, message):
        message = "{}\n{}".format(self.task_tag_name, message)
        logger.debug(message)
        return

    def start_requests(self):
        while 1:
            task_obj = self.get_task(obj=True)
            if not task_obj:
                logger.debug("no task")
                break
            url = task_obj.url
            # 下载
            req = Request({"url": url}, meta={"task": task_obj}, callback=self.parse)
            yield req
        return

    def parse(self, response: Response):
        request: Request = response.request
        task_obj: JsonTask = request.meta["task"]
        _response = response.response
        try:
            batch_date = self.batch_date
            if _response:
                url = _response.url
                # todo 解析代码
                # 更新完成标志
                self.set_task_state(state=1, condition={"url": url})
            else:
                if _response is not None:
                    if _response.status_code == 404:
                        url = _response.url
                        # todo 解析代码
                        # 更新完成标志
                        self.set_task_state(state=-1, condition={"url": url})
                        return
                raise Exception
        except Exception as e:
            logger.exception(e)
            self.put_task(task_obj)
        return

    @property
    def other_spider_stoped(self):
        """
            获取其他爬虫的状态
            主要用来使多个爬虫同时停止
        Returns:

        """
        if self.use_sqlite:
            return 0
        state_key = f"{self.task_key}:other_spider_stoped"
        if not self._simple_redis_cluster_conn:
            self._init_simple_redis_cluster(state_key)
        state = self._simple_redis_cluster_conn.get(state_key)
        if state:
            ttl = self._simple_redis_cluster_conn.ttl(state_key)
            if ttl == -1:
                # 此处主要防止stop的时候设置失败 导致永远不start
                # todo  不知道是不是应该直接删除...
                self._simple_redis_cluster_conn.expire(state_key, 10 * 60)
        return int(state.decode()) if state else 0

    @other_spider_stoped.setter
    def other_spider_stoped(self, value):
        """
            获取其他爬虫的状态
            主要用来使多个爬虫同时停止
        Args:
            value:

        Returns:

        """
        if self.use_sqlite:
            return True
        state_key = f"{self.task_key}:other_spider_stoped"
        if not self._simple_redis_cluster_conn:
            self._init_simple_redis_cluster(state_key)
        if value == 0:
            self._simple_redis_cluster_conn.delete(state_key)
        else:
            # 抢占设置过期时间
            if self._simple_redis_cluster_conn.setnx(state_key, value):
                self._simple_redis_cluster_conn.expire(state_key, 10 * 60)
        return True

    def _auto_check_lost_task(self):
        """
            自动定时检测丢失任务
            需求：
                比如某些任务表会不断增加，一直能取到新任务，所以从不check_lost 导致丢失的任务永远不会被做。。。
        Returns:

        """
        if (
            not self.debug
            and self._last_check_lost_time < time.time() - self.auto_check_lost_interval
        ):
            self._last_check_lost_time = time.time()
            _lost_task_reset_limit = self.lost_task_reset_limit
            self.lost_task_reset_limit = 1000
            self._check_lost_task()
            self.lost_task_reset_limit = _lost_task_reset_limit

        return

    def _check_lost_task(self):
        key = "{}:check_lost_task".format(self.task_key)
        if self.use_sqlite:
            self.check_lost_task()
        else:
            with util.RedisLock(
                key, timeout=self.lock_timeout, wait_timeout=0
            ) as _lock:
                if _lock.locked:
                    self.check_lost_task()
                    # 重置间隔最少1分钟
                    _sleep_time = 60
                    if self.debug:
                        _sleep_time = 5
                        # if self.fast_mode:
                        #     _sleep_time = 1
                    time.sleep(_sleep_time)
        return self._get_task_from_mysql()

    def _get_task_from_mysql(self):
        """
            由于 get_task_from_mysql 会被重写 所以在这里加锁
        Returns:

        """
        check_lost_task_key = "{}:check_lost_task".format(self.task_key)

        if self.use_sqlite:
            self.get_task_from_mysql()
        else:
            # 在获取任务前检测一下是否正在重置丢失任务 若正在重置 则等待重置完毕
            # 因为正在重置的时候是获取不到任务的 并且会导致爬虫迅速结束 导致多进程变成单进程
            with util.RedisLock(
                key=check_lost_task_key,
                timeout=self.lock_timeout,
                wait_timeout=self.wait_lock_timeout,
            ) as _lock:
                # 由于仅仅做检测用 所以获取到锁后立刻释放
                # 没获取到则一直等 等到超时
                # 注意不能把获取任务放到此锁中间来 可能会造成释放两次锁的bug
                pass
            get_task_from_mysql_key = "{}:get_task_from_mysql".format(self.task_key)
            # 这个锁的超时时间一般也要改
            with util.RedisLock(
                key=get_task_from_mysql_key,
                timeout=self.lock_timeout,
                wait_timeout=self.wait_lock_timeout,
                break_wait=self.break_wait_get_task_from_mysql,
            ) as _lock:
                if _lock.locked:
                    # 检查内存
                    if self.container_memory_utilization > 0.6:
                        self.suicide()
                    else:
                        # 兼容无参数函数
                        try:
                            self.get_task_from_mysql(redis_lock=_lock)
                        except TypeError as e:
                            if "argument" in str(e):
                                self.get_task_from_mysql()
                            else:
                                raise e
        return

    def _get_task_from_redis(self, delay: int = None, task_key: str = None):
        """
        从redis中获取任务
            或者从缓存队列中取
        Args:
            delay:
            task_key: 可指定任务队列

        Returns:

        """
        if delay is None:
            delay = 30 if not self.debug else 5

        # 尝试从缓存中拿
        task = None
        try:
            if self.cache_task:
                task = self._cache_redis_task_list.get_nowait()
        except Empty:
            pass
        if not task:
            if self.use_sqlite:
                i = 0
                while not task and (i < delay // 1):
                    try:
                        task = self._cache_redis_task_list.get_nowait()
                    except Empty:
                        pass
                    time.sleep(1)
                    i += 1
            else:
                task_key = task_key or self.task_key
                i = 0
                task = self.redis_client.rpop(task_key)
                while not task and (i < delay // 1):
                    # 这里之所以重复n次获取 是为了应对一种特殊情况 比如
                    # 任务表是需要翻页采集的  然后其中某个任务翻页页码特别大  到最后只剩下这个任务在翻页了
                    # 由于redis中仅有一个任务 所以这里如果不等待的话 就会一直去mysql中获取  然后 在_get_task_from_mysql 会耽搁1分钟
                    # 于是每翻一页都需要1分钟。。。。。  xdf的翻页4000多页 我草  翻了好几天
                    # 暂停一会  然后从redis中获取翻页新发的任务  减少间隔
                    time.sleep(1)
                    task = self.redis_client.rpop(task_key)
                    i += 1
        return task

    def _get_task_obj(
        self, max_retry: int = 10, group: bool = False, **kwargs
    ) -> JsonTask:
        """
            获取任务执行 JsonTask 对象
        Args:
            max_retry: 任务重试次数上限 默认10次 若获取到的任务重试次数超过上限  则重新获取任务
            group: 是否为批量任务
            **kwargs:

        Returns:
            JsonTask
        """
        kwargs["group"] = group
        while 1:
            task = self.get_task(**kwargs)
            if task:
                task_obj = JsonTask(task=task)
                task_obj.retry += 1
                if task_obj.retry >= max_retry:
                    logger.warn("任务重试次数超限: {}".format(task_obj))
                    continue
                # 处理批量任务
                if group and isinstance(task_obj.group, list):
                    self.cache_task = True
                    # 切分任务并放入缓存
                    task_list = [JsonTask(x) for x in task_obj.group]
                    task_obj = task_list[0]
                    self.put_task(task_list[1:], retry=False, show_log=False)
            else:
                task_obj = task
            break
        return task_obj

    def _init_simple_redis_cluster(self, key):
        redis_index = util.key2index(
            key, index_range=list(setting.redis_util_cluster.keys())
        )
        redis_uri = setting.redis_util_cluster[redis_index]
        # connection_pool = redis.BlockingConnectionPool.from_url(
        #     redis_uri, max_connections=100, timeout=60
        # )
        # self.redis_client = redis.StrictRedis(connection_pool=connection_pool)
        self._simple_redis_cluster_conn = redis.StrictRedis.from_url(redis_uri)
        return

    def _send_spider_start_signal(self):
        """
        发送爬虫启动信号
            #目前主要作用是清理爬虫停止信号 此目的废弃
                由于以下情况：
                    当某个进程结束 并发出停止信号后  某个其他进程由于被kill而重启 导致 停止信号被清理
            # 目前主要作用是:
                调试的时候不受影响
                根据IS_SPIDER 自动判断是否为本地环境

            tips:
                清理停止信号的工作 由信号过期时间保证

        Returns:

        """
        if self.debug or setting.GRAPER_ENV == "DEV":
            self.other_spider_stoped = 0
        return

    def _send_spider_stop_signal(self):
        """
            发送爬虫停止信号
        Returns:

        """
        if not self.debug:
            # 被Kill时会重启爬虫 所以不发布停止信号
            if not self._killed:
                self.other_spider_stoped = 1
        return


class BatchSpider(SingleBatchSpider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 以下会保留与父类重复的属性定义 方便复制粘贴

        # 必填字段
        self.task_key = "task:temp"  # 需修改
        # 取任务表 必填
        self.task_base_table_name = ""  # 占位用  可不填
        self.task_table_name = ""
        # 任务批次记录表 必填
        self.task_batch_table_name = ""

        # 非必填 但一般应该是需要设置的
        # 任务字段列表 建的表中必须有id字段 用于修改状态
        self.task_field_list = []
        # 批次间隔
        self.batch_interval = 7
        # 任务标识 用于日志 或 报警中的任务名字
        self.task_tag_name = ""
        # 消息通知人
        self.message_recipients = []

        # 非必填 有默认值的字段
        # 状态字段名 默认state
        self.state_field_name = "state"
        # 额外重置字段
        self.reset_extra_fields = {}  # 但还有其他情况可能会重置page
        # 每次从mysql获取任务数
        self.task_mysql_limit = 10000
        # 批次间隔含义
        self.batch_interval_unit = "day"  # day, hour
        # 设置批次完成比例 默认 1.0（100%） 可设置为 0.9 则达到0.9且下一批次时间到来时会强制开始下一批次
        # 主要应对任务表持续不断增加的情况
        self.batch_complete_ratio = 1.0

        # 锁超时时间 针对大量任务爬虫时可能用到
        self.lock_timeout = 10 * 60
        # 等待锁时间
        self.wait_lock_timeout = 10 * 60

        #
        self.record_batch_count_interval = 2 * 60

        # 某些特殊情况参数
        # 可自定义批次日期的redis存储key名
        self._batch_date_key = ""

        # 调试模式
        self.debug = False

        # 以下不应被修改
        # 跨批次处理
        self.init_batch_date = ""
        # 默认批次时间字典
        self._default_batch_date_str_dict = {
            "day": "1970-01-01",
            "hour": "1970-01-01 00:00:00",
        }
        # 缓存 batch_date
        self._cache_batch_date = {}

        #
        self._get_task_error_count = 0

    @property
    def stop_task_key(self):
        if self.task_key == "task:temp":
            raise ValueError("self.task_key need override")
        return "stop:{}".format(self.task_key)

    @property
    def batch_date_key(self):
        if self.task_key == "task:temp":
            raise ValueError("self.task_key need override")
        if not self._batch_date_key:
            return "batch:{}".format(self.task_key)
        else:
            return self._batch_date_key

    @property
    def batch_date(self) -> str:
        """
            使用次数很少  但必须最新
            20180927 增加redis中batch_date缺失重置
        Returns:

        """
        # 缓存5秒
        cache_batch_date = self._cache_batch_date.get("batch_date")
        if cache_batch_date and (time.time() - self._cache_batch_date["ts"] < 5):
            return cache_batch_date
        #
        batch_date = self.redis_client.get(self.batch_date_key)
        if not batch_date:
            _db = self.db.copy()
            # 从mysql中获取最后批次
            sql = "select batch_date, done_count, total_count from {} order by id desc limit 1;".format(
                self.task_batch_table_name
            )
            last_batch_infos = _db.query_all(sql)
            _db.close()
            if last_batch_infos:
                batch_date_str, done_count, total_count = last_batch_infos[0]
                # 格式转换
                batch_date_str = (
                    batch_date_str.strftime(self.batch_date_format)
                    if not isinstance(batch_date_str, str)
                    else batch_date_str
                )
                self.redis_client.set(self.batch_date_key, batch_date_str)
            batch_date = self.redis_client.get(self.batch_date_key)
        if not batch_date:
            raise ValueError("获取batch_date失败")
        # 加入缓存
        self._cache_batch_date = {"batch_date": batch_date.decode(), "ts": time.time()}
        return self._cache_batch_date["batch_date"]

    @property
    def reset_fields(self):
        # 任务批次重置时重置的字段名及值 默认仅重置状态
        return {self.state_field_name: self.state_dict["wait"]}

    @property
    def batch_date_format(self):
        """
            批次时间格式
        Returns:

        """
        _format = {"day": "%Y-%m-%d", "hour": "%Y-%m-%d %H:00:00"}[
            self.batch_interval_unit
        ]
        return _format

    @property
    def default_batch_date_str(self):
        """
            默认批次时间 哨兵???
        Returns:

        """
        return self._default_batch_date_str_dict[self.batch_interval_unit]

    @default_batch_date_str.setter
    def default_batch_date_str(self, value: str):
        self._default_batch_date_str_dict[self.batch_interval_unit] = value
        return

    def _create_batch_table(self, table_name: str = ""):
        sql = """
        CREATE TABLE `{}` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `batch_date` varchar(50) DEFAULT NULL COMMENT '批次时间',
            `done_count` int(11) DEFAULT '0' COMMENT '完成数',
            `total_count` int(11) DEFAULT '0' COMMENT '任务总数',
            `fail_count` int (11) DEFAULT '0' COMMENT '任务异常数',
            `interval` int(11) DEFAULT '0' COMMENT '批次间隔',
            `interval_unit` varchar(20) DEFAULT 'day' COMMENT '批次间隔单位 day, hour',
            `update_time` datetime DEFAULT NULL COMMENT '本条记录更新时间',
            `is_done` int(11) DEFAULT '0' COMMENT '批次是否完成 0 未完成  1 完成',
            `create_time` datetime DEFAULT NULL COMMENT '本条记录插入时间',
            PRIMARY KEY (`id`),
            UNIQUE KEY `batch_date` (`batch_date`)
            ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
        """
        table_name = table_name or self.task_batch_table_name
        if table_name:
            sql = sql.format(table_name)
            self.db.cursor.execute(sql)
            logger.debug("创建批次表成功: {}".format(table_name))
        else:
            raise Exception("_create_batch_table: not special args table_name")
        return

    @property
    def is_reset_task(self):
        """
            获取重置任务状态
        Returns:

        """
        state_key = "{}:reset_task_state".format(self.task_key)
        state = self.redis_client.get(state_key)
        return int(state.decode()) if state else 0

    @is_reset_task.setter
    def is_reset_task(self, value: int):
        state_key = "{}:reset_task_state".format(self.task_key)
        return self.redis_client.set(state_key, value)

    def reset_task_table(self) -> int:
        """
        定义重置任务表的操作
        Returns:
            任务数量

        """
        # 重置 mysql state
        reset_fields = self.reset_fields.copy()
        reset_fields.update(self.reset_extra_fields)
        _resp = self.db.update(
            reset_fields,
            condition={self.state_field_name: self.state_dict["finish"]},
            table_name=self.task_table_name,
        )
        return _resp

    def _reset_task_table(self):
        self.is_reset_task = 1
        try:
            resp = self.reset_task_table()
        except Exception as e:
            raise e
        finally:
            self.is_reset_task = 0
        return resp

    def is_new_batch_start(self, **kwargs):
        """
        新批次是否要开始
            # 默认检测 now - batch_date >= batch_interval_timedelta
            # 特殊用例：
                携程机票 需要超前采集 连续采集推后15天每一天的信息
        Args:
            **kwargs:
                now: datetime 当前时间
                batch_date: datetime 上批次时间
                batch_interval_timedelta: timedelta 批次间隔

        Returns:

        """
        # 默认条件
        if kwargs["now"] - kwargs["batch_date"] >= kwargs["batch_interval_timedelta"]:
            return 1
        return 0

    def get_new_batch_date(self, **kwargs) -> str:
        """
        获取新批次时间
            默认使用当前时间格式化
        Args:
            **kwargs:
                now: datetime  当前时间
                batch_date: datetime 上批次时间
                batch_date_str: str 上批次时间
        Returns:
            若返回空 则下批次不会开始
        """
        return kwargs["now"].strftime(self.batch_date_format)

    def _get_batch_info(self, batch_date=""):
        """
            获取指定批次记录信息
        Args:
            batch_date:

        Returns:

        """
        _db = self.db.copy()
        batch_info = {}
        if not batch_date:
            batch_date = self.batch_date
        sql = "select done_count, total_count, fail_count from {} where batch_date='{}'".format(
            self.task_batch_table_name, batch_date
        )
        sql_result = _db.query_all(sql)
        _db.close()
        if sql_result:
            done_count, total_count, fail_count = sql_result[0]
            batch_info = {
                "done_count": done_count,
                "total_count": total_count,
                "fail_count": fail_count,
                "batch_date": batch_date,
            }
        return batch_info

    def _get_batch_info_list(self, last_n: int = None) -> list:
        """
        获取批次信息列表
        Args:
            last_n: 获取最后n个

        Returns:
            [
                {
                    "done_count": 100,
                    "total_count": 100,
                    "fail_count": 0,
                    "batch_date": "2020-01-01",
                },
                ...

            ]
        """
        batch_list = []
        if last_n:
            sql = "select done_count, total_count, fail_count, batch_date from {} order by id desc limit {}".format(
                self.task_batch_table_name, last_n
            )
            _db = self.db.copy()
            sql_result = _db.query_all(sql)
            _db.close()
            for done_count, total_count, fail_count, batch_date in sql_result:
                batch_list.append(
                    {
                        "done_count": done_count,
                        "total_count": total_count,
                        "fail_count": fail_count,
                        "batch_date": batch_date,
                    }
                )
        batch_list.sort(key=lambda x: x["batch_date"])
        return batch_list

    def batch_complete(self, total_count: int, done_count: int, **kwargs) -> int:
        """
        检查批次是否完成
            默认条件
                        total_count = done_count (done_count / total_count >= 1.0)
                    and
                        done_count != 0
            # 可自定义此条件判断 满足一些特殊情况
            例如：当任务是持续增加的时候 仅仅判断done_count == total_count 是不行的
                还要判断添加任务的爬虫是否完成
        Args:
            total_count:
            done_count:
            **kwargs:

        Returns:

        """
        if total_count == 0 or (
            done_count / total_count >= self.batch_complete_ratio and done_count != 0
        ):
            return 1
        return 0

    def append_new_batch_record(self, **kwargs):
        """
        定义新批次记录生成规则
            默认向批次表中插入一条记录

            # 特殊情况下可定制
            比如直聘：
                需要插入两张表

        Args:
            **kwargs:
                new_batch_date: 新批次时间
                now： 当前时间

        Returns:

        """
        now = kwargs["now"]
        new_batch_date = kwargs["new_batch_date"]
        # 获取任务总数
        sql = "select count(1) from {}".format(self.task_table_name)
        total_count = self.db.query_all(sql)[0][0]

        # 插入新批次记录
        sql = """INSERT INTO {} ( `batch_date`, `done_count`, `total_count`, `interval`, `interval_unit`, `update_time`, `create_time` )
                                    VALUES( '{}', {}, {}, {}, '{}', '{}', '{}' )
                            """
        sql = sql.format(
            self.task_batch_table_name,
            new_batch_date,
            0,
            total_count,
            self.batch_interval,
            self.batch_interval_unit,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            now.strftime("%Y-%m-%d %H:%M:%S"),
        )
        self.db.cursor.execute(sql)
        _resp = self.db.cursor.rowcount
        return _resp

    def check_batch(self):
        """
        批次检查
        Returns:

        """
        self._record_batch_count(debug=True)
        return self._check_batch()

    def _check_batch(self):
        """
        检查 开始新批次采集
        Returns:

        """
        # 定义一系列变量
        # 当前时间
        now = datetime.datetime.now()
        # 批次间隔
        batch_interval_timedelta = datetime.timedelta(
            **{
                {"day": "days", "hour": "hours"}[
                    self.batch_interval_unit
                ]: self.batch_interval
            }
        )
        ####
        # 检查上批次是否完成 完成后设置停止标志
        sql = "select batch_date, done_count, total_count from {} order by id desc limit 1;".format(
            self.task_batch_table_name
        )
        last_batch_infos = self.db.query_all(sql)
        if last_batch_infos:
            batch_date_str, done_count, total_count = last_batch_infos[0]
            # 格式转换
            batch_date_str = (
                batch_date_str.strftime(self.batch_date_format)
                if not isinstance(batch_date_str, str)
                else batch_date_str
            )
        else:
            batch_date_str, done_count, total_count = (
                self.default_batch_date_str,
                -1,
                -1,
            )
        # 上批次时间
        batch_date = datetime.datetime.strptime(batch_date_str, self.batch_date_format)
        if self.batch_complete(total_count, done_count):
            logger.debug(f"上批次 {batch_date_str} 已完成 {done_count}/{total_count}")
            # 清理残留任务
            self.redis_client.delete(self.task_key)
            # 检查本批次是否到达开始时间
            if self.is_new_batch_start(
                now=now,
                batch_date=batch_date,
                batch_interval_timedelta=batch_interval_timedelta,
            ):
                new_batch_date = self.get_new_batch_date(
                    now=now, batch_date=batch_date, batch_date_str=batch_date_str
                )
                if not new_batch_date:
                    logger.debug("上批次时间 {} 下批次时间未到".format(batch_date_str))
                    return
                # 类型检查  格式检查
                try:
                    datetime.datetime.strptime(new_batch_date, self.batch_date_format)
                except Exception as e:
                    logger.debug("新批次时间格式错误")
                    raise e
                #
                _message = "本批次开始时间到达 {}".format(new_batch_date)
                logger.info(_message)

                # 重置批次时间
                _resp = self.redis_client.set(self.batch_date_key, new_batch_date)
                logger.debug("redis批次时间重置为{} {}(成功)".format(new_batch_date, _resp))

                # 重置mysql任务表状态
                logger.info("开始重置mysql中任务状态")
                reset_count = self._reset_task_table()
                logger.debug(
                    "mysql重置 {} 成功 {}".format(self.state_field_name, reset_count)
                )

                # 假如之前的重置了一半失败了   那么下次会继续重新重置
                # 并且如果在重置失败期间 爬虫开始执行新的任务
                # 由于batch_date是从redis中获取的 所以不会影响
                # 统计批次时也是从redis中获取 也不会影响
                #
                r = self.append_new_batch_record(new_batch_date=new_batch_date, now=now)
                _message = "插入新批次 {} 记录成功 {} 任务重置 {}".format(
                    new_batch_date, r, reset_count
                )
                logger.info(_message)
            else:
                logger.debug("上批次时间 {} 下批次时间未到".format(batch_date_str))
        else:
            _message = "上次采集尚未完成 {} {}/{}".format(
                batch_date_str, done_count, total_count
            )
            # 计算剩余时间
            spend_ratio = (
                now - batch_date
            ).total_seconds() / batch_interval_timedelta.total_seconds()
            done_ratio = done_count / total_count if total_count > 0 else 1
            if spend_ratio > 0.5 and spend_ratio > done_ratio:
                # 花费时间比例 > 完成任务比例
                _message += "\n警告 任务可能超时 {:.2f} / {:.2f}".format(
                    spend_ratio, done_ratio
                )
                self.send_message(_message)
        return

    @property
    def last_record_batch_count_time(self):
        key = "{}:last_record_batch_count_time".format(self.task_key)
        ts = self.redis_client.get(key)
        return float(ts.decode()) if ts else 0

    @last_record_batch_count_time.setter
    def last_record_batch_count_time(self, value):
        key = "{}:last_record_batch_count_time".format(self.task_key)
        self.redis_client.set(key, value)
        return

    def _record_batch_count(self, debug: bool = False):
        if self.debug:
            debug = self.debug
        if not debug and (
            time.time() - self.last_record_batch_count_time
            < self.record_batch_count_interval
        ):
            return
        key = "{}:record_batch_count".format(self.task_key)
        # 加锁保证同一时间仅有一个进程在统计  因为多次重复统计没啥意义  还浪费资源
        with util.RedisLock(key, timeout=self.lock_timeout, wait_timeout=0) as _lock:
            if _lock.locked:
                logger.debug("开始统计批次进度: {}".format(self.task_key))
                total, done = self.record_batch_count()
                logger.debug(
                    "任务{} 批次 {} 进度: {}/{}".format(
                        self.task_key,
                        self._cache_batch_date.get("batch_date"),
                        done,
                        total,
                    )
                )
                # 保证1分钟最多统计一次
                _sleep_time = 60
                if self.debug:
                    _sleep_time = 5
                    if self.fast_mode:
                        _sleep_time = 1
                time.sleep(_sleep_time)
                self.last_record_batch_count_time = time.time()
        return

    def record_batch_count(self):
        """
            统计进度
        Returns:

        """
        total, done = -1, -1
        try:
            state_dict = self.state_dict
            #
            _db = self.db.copy()
            #
            sql = "select {field}, count({field}) from {table} group by {field}".format(
                field=self.state_field_name, table=self.task_table_name
            )
            state_count_info = _db.query_all(sql)
            # 字段类型校验
            for k in state_count_info:
                assert isinstance(k[0], int), "状态字段必须为整型"
            #
            total = sum([x[1] for x in state_count_info if x[0] in state_dict.values()])
            done = sum(
                [
                    x[1]
                    for x in state_count_info
                    if x[0] in (state_dict["error"], state_dict["finish"])
                ]
            )
            fail = sum([x[1] for x in state_count_info if x[0] == state_dict["error"]])

            # 更新状态
            update_data = {
                "done_count": done,
                "total_count": total,
                "fail_count": fail,
                "update_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "interval": self.batch_interval,
                "interval_unit": self.batch_interval_unit,
            }
            # 当有变化时更新记录 主要是保留最后变化时间
            where_sql = f"""`batch_date`='{self.batch_date}' and 
                            (`done_count` != {done} or `total_count` != {total} or `fail_count` != {fail} or `interval` != {self.batch_interval} or `interval_unit` != '{self.batch_interval_unit}')
                            """
            r = _db.update(
                update_data, where_sql=where_sql, table_name=self.task_batch_table_name
            )
            _db.close()
        except Exception as e:
            logger.exception(e)
        return total, done

    def _get_task_from_mysql(self):
        """
            每次从mysql中获取任务前 先统计一下
            然后再获取任务
                获取任务时保证顺序获取 可连续获取  不用保证不连续 （统计的时候是要保证不连续的）
        Returns:

        """
        self._record_batch_count()
        # 调用父类
        super()._get_task_from_mysql()
        return

    def get_task(
        self, obj: bool = False, block: bool = True, group: bool = False, **kwargs
    ):
        """
            获取任务
        Args:
            obj: 是否获取JsonTask对象
            block: 当批次未完成时是否一直尝试获取任务
            group: 是否为批量任务
            **kwargs:

        Returns:
            json序列化的字符串 or JsonTask对象
        """
        if not self.init_batch_date:
            self.init_batch_date = self.batch_date
        else:
            if self.batch_date != self.init_batch_date:
                logger.debug("当前批次与初始批次不一致 爬虫终止")
                return
        # 检查是否位于重置任务期间
        if self.is_reset_task:
            logger.debug("正在重置任务 暂停执行")
            return
        kwargs["obj"] = obj
        kwargs["block"] = block
        kwargs["group"] = group
        # 调用父类
        task = super().get_task(**kwargs)
        if not task and block:
            # 处理内存或其他导致进程自杀的情况
            if self._killed:
                pass
            else:
                # 检查批次是否完成
                _current_batch_info = self._get_batch_info()
                r = self.batch_complete(
                    total_count=_current_batch_info["total_count"],
                    done_count=_current_batch_info["done_count"],
                )
                if not r:
                    logger.debug("批次尚未完成 {} 继续获取任务...".format(_current_batch_info))
                    # 其实这属于不正常情况 所以需要警告一下
                    # 如果存在多个爬虫时  可能任务被别人取走了 导致获取不到 所以加一些延迟
                    self._get_task_error_count += 1
                    if self._get_task_error_count > 10:
                        message = "{} 任务获取异常".format(self.task_table_name)
                        logger.error(message)
                        # try:
                        # self.send_message(message)
                        # except Exception as e:
                        #     logger.exception(e)
                    return self.get_task(**kwargs)
        return task
