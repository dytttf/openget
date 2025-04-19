# -*- coding: utf-8 -*-

"""
"""
import json
import copy
import datetime
import threading
import time
from collections import OrderedDict, defaultdict
import psycopg2

from openget.utils import log

mutex = threading.RLock()


def escape_string(s: str):
    return s.replace("'", "''")


class Cursor(object):
    """重新封装cursor使得可以自动捕获mysql连接超时错误并重新连接"""

    def __init__(self, connection, options, logger=None, cursor_class=None, **kwargs):
        self.conn = connection
        self.cursor = None
        self.cursor_class = cursor_class
        self.reconnect(connection)

        self.kwargs = kwargs
        self.options = options

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

    def __getattr__(self, name):
        """不存在的属性调用原cursor"""
        return getattr(self.cursor, name)

    def reconnect(self, conn=None):
        conn = conn or PostgreSQLOpt.get_conn(self.options, **self.kwargs)
        self.cursor = conn.cursor()
        self.conn = conn
        return self.conn, self.cursor

    def execute(self, sql, args=None, retry=0):
        try:
            mutex.acquire()
            try:
                self.conn.ping(reconnect=True)
            except:
                pass
            self.cursor.execute(sql, args)
            result = 1
        except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
            # 捕获超时异常 关闭异常
            if retry < 20 and str(e.args[0]).lower() in (
                "server closed the connection unexpectedly",
                "connection has been closed unexpectedly",
                "server closed the connection without sending any data",
            ):
                self.logger.debug(f"pg连接错误:{e}  重连(retry={retry})中...")
                time.sleep(retry * 5)
                # 重连
                self.reconnect()
                return self.execute(sql, args=args, retry=retry + 1)
            raise e
        finally:
            mutex.release()
        return result


class PostgreSQLOpt(object):
    """和mysql方法保持一致，按说应该提取出基类，有点懒先这么写吧"""

    # 连接池 共用
    connection_list = {}

    def __init__(self, options, is_pool: bool = True, cursor_class: str = None, **kwargs):
        """
        Args:
            options: postgresql连接参数字典
            is_pool: 是否使用连接池模式
            cursor_class:
                cursor, dict_cursor, ss_cursor, ss_dict_cursor
            **kwargs:
                autocommit:
        """
        self.is_pool = is_pool
        self.cursor_class = cursor_class
        self.autocommit = kwargs.setdefault("autocommit", True)
        if self.is_pool:
            print("警告: 在多线程中用同一个postgresql连接同时进行插入和查询操作时，插入操作会影响到查询结果...")

        # 额外参数
        self.kwargs = kwargs

        self.options = options
        self.key = repr(options)

        self.logger = self.kwargs.pop("logger", None) or log.get_logger(__file__)

        self.table_name = ""

        # 私有变量
        self._conn = None
        self._cursor = None

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            pass

    @property
    def conn(self):
        """
        获取mysql连接
        Returns:

        """
        self._conn = self.cursor.conn
        return self._conn

    @property
    def cursor(self):
        """
        获取游标
        Returns:

        """
        if not self._cursor:
            if not self.is_pool or self.key not in self.connection_list.keys():
                self._cursor = self.get_cursor()
                if self.is_pool:
                    self.connection_list[self.key] = self._cursor
            else:
                self._cursor = self.connection_list[self.key]
        return self._cursor

    @staticmethod
    def get_conn(options, **kwargs):
        """
            建立pg连接
        Args:
            options:
            **kwargs:

        Returns:

        """

        autocommit = kwargs.pop("autocommit", True)

        db_user = options["username"]
        db_passwd = options["password"]
        db_host = options["host"]
        db_port = options["port"]
        if db_port is None:
            db_port = 5432
        db_default = options["db"]

        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_passwd,
            database=db_default,
            **kwargs,
        )

        conn.autocommit = autocommit
        return conn

    def get_cursor(self, connection=None):
        """
        mysql获取cursor  重新封装过的
        """
        connection = connection or PostgreSQLOpt.get_conn(self.options, **self.kwargs)
        cursor = Cursor(
            connection,
            self.options,
            logger=self.logger,
            cursor_class=self.cursor_class,
            **self.kwargs,
        )
        return cursor

    @staticmethod
    def handle_values(data, keys=None, strip=True) -> OrderedDict:
        """
        处理字典中的值 转换为pg接受的格式
        Args:
            data:
            keys: 使用给定的key顺序
            strip: 是否对字符串类型的值进行strip操作

        Returns:

        """
        if not keys:
            keys = list(data.keys())
        handle_k_list = []
        handle_v_list = []
        for k in keys:
            v = data[k]
            handle_k_list.append('"{}"'.format(k))
            if isinstance(v, str):
                if strip:
                    v = v.strip()
                handle_v_list.append("'{}'".format(escape_string(v)))
            elif isinstance(v, (int, float)):
                # np.nan is float
                handle_v_list.append("{}".format(v))
            elif isinstance(v, (datetime.date, datetime.time)):
                handle_v_list.append("'{}'".format(v))
            elif v is None:
                handle_v_list.append("null")
            else:
                v = json.dumps(v, ensure_ascii=False)
                handle_v_list.append("'{}'".format(escape_string(v)))
        data = OrderedDict(zip(handle_k_list, handle_v_list))
        return data

    @staticmethod
    def group_data_by_keys(data: list):
        """"""
        data = copy.deepcopy(data)
        group_data_dict = defaultdict(list)
        for item in data:
            keys = list(item.keys())
            keys.sort()
            keys_flag = " ".join(keys)
            group_data_dict[keys_flag].append(item)
        return list(group_data_dict.values())

    def add(
        self,
        data: dict,
        *,
        table_name: str = "",
        ignore_duplicate: bool = True,
        **kwargs,
    ):
        """
            save one data to database
        Args:
            data:
            table_name:
            ignore_duplicate: ignore duplicate errors
            **kwargs: 0写入成功  1重复

        Returns:
            0: success
            1: duplicate

        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            raise ValueError("data is {}".format(data))
        sql = "insert into {table_name} ({keys}) values({values});"
        # 拼接sql
        order_data = self.handle_values(data)
        sql = sql.format(
            **{
                "keys": ",".join(order_data.keys()),
                "values": ",".join(order_data.values()),
                "table_name": table_name,
            }
        )
        try:
            self.cursor.execute(sql)
            if self.autocommit:
                self.cursor.conn.commit()
            resp = 0
        except Exception as e:
            if ignore_duplicate and "Duplicate entry" in str(e):
                self.logger.error(e)
                resp = 1
            else:
                raise e
        return resp

    def add_many(
        self,
        data: list,
        *,
        table_name: str = "",
        batch=100,
        group_by_keys: bool = False,
        ignore_duplicate: bool = False,
        autocommit: bool = False,
        **kwargs,
    ):
        """
        批量保存数据
        Args:
            data: 数据
            table_name: 表名
            batch: 分批入库数量
            group_by_keys: 是否按照keys分组处理 默认False 则如果遇到数据中存在key不一致的情况 会抛出异常
            ignore_duplicate:
            autocommit:
            **kwargs:

        Returns:

        """
        for sql in self.get_add_many_sql(
            data,
            table_name=table_name,
            batch=batch,
            group_by_keys=group_by_keys,
            ignore_duplicate=ignore_duplicate,
        ):
            self.cursor.execute(sql)
            if autocommit:
                self.cursor.conn.commit()

    def get_add_many_sql(
        self,
        data: list,
        *,
        table_name: str = "",
        batch=100,
        group_by_keys: bool = False,
        ignore_duplicate: bool = False,
        **kwargs,
    ):
        """
        批量保存数据
        Args:
            data: 数据
            table_name: 表名
            batch: 分批入库数量
            group_by_keys: 是否按照keys分组处理 默认False 则如果遇到数据中存在key不一致的情况 会抛出异常
            ignore_duplicate:
            **kwargs:

        Returns:

        """
        data = copy.deepcopy(data)
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not data:
            return []
        if not isinstance(data, (list, tuple)):
            data = [data]

        # 数据分组 按照keys是否相同
        # 防止由于key个数不一致导致的出错
        data_group_list = self.group_data_by_keys(data)
        if not group_by_keys:
            assert len(data_group_list) == 1, "待入库数据结构不一致"

        sql_list = []

        for data in data_group_list:
            # 固定keys 取第一个数据
            # 如果不固定 则当给定数据的key顺序不固定时会出现入库错乱
            first_data_keys = list(data[0].keys())
            while data:
                _data = data[:batch]
                data = data[batch:]
                # 每次100
                if ignore_duplicate:
                    sql = "insert into {table_name} ({keys}) values{values} on conflict do nothing;"
                else:
                    sql = "insert into {table_name} ({keys}) values{values};"
                # 拼接sql
                values_list = []
                for item in _data:
                    order_data = self.handle_values(item, keys=first_data_keys)
                    keys = ",".join(order_data.keys())
                    values = ",".join(order_data.values())
                    values_list.append("({})".format(values))

                sql = sql.format(
                    **{
                        "keys": keys,
                        "values": ",".join(values_list),
                        "table_name": table_name,
                    }
                )
                sql_list.append(sql)
        return sql_list

    def update(
        self,
        data,
        *,
        condition: dict = None,
        table_name: str = "",
        where_sql: str = "",
        **kwargs,
    ):
        """
        Args:
            data: 待更新字段 dict
            condition: 条件字段 dict 默认无序直接and
            table_name: 表名
            where_sql: 若存在此字段则忽略 condition 适用于需要排序或者有除了and之外的where字句
            **kwargs:

        Returns:

        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not condition:
            condition = {}
        sql = "update {table_name} set {update_data} where {where_sql};"
        order_data = self.handle_values(data)
        # 组合待更新数据
        update_data = ["{}={}".format(k, v) for k, v in order_data.items()]
        update_data = ",".join(update_data)

        # 组合条件语句 默认使用and连接
        order_condition = self.handle_values(condition, strip=False)
        order_condition = [f"{k}={v}" for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # where_sql 优先
        where_sql = where_sql or order_condition

        sql = sql.format(
            **{
                "table_name": table_name,
                "update_data": update_data,
                "where_sql": where_sql,
            }
        )
        ret = self.cursor.execute(sql)
        if self.autocommit:
            self.cursor.conn.commit()
        return ret

    def delete(
        self,
        *,
        condition: dict = None,
        table_name: str = "",
        where_sql: str = "",
        **kwargs,
    ):
        """
        根据条件删除内容
        Args:
            condition: 条件字段 dict 默认无序直接and
            table_name:
            where_sql: 若存在此字段则忽略 condition 适用于需要排序或者有除了and之外的where字句
            **kwargs:

        Returns:
            受影响行数
        """

        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not condition:
            condition = {}
        sql = "delete from  {table_name}  where {where_sql};"

        # 组合条件语句 默认使用and连接
        order_condition = self.handle_values(condition)
        order_condition = ["{}={}".format(k, v) for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # where_sql 优先
        where_sql = where_sql or order_condition

        sql = sql.format(**{"table_name": table_name, "where_sql": where_sql})
        ret = self.cursor.execute(sql)
        if self.autocommit:
            self.cursor.conn.commit()
        return ret

    def get_upsert_sql(
        self,
        data: list,
        *,
        table_name: str = "",
        conflict_target: str = "",
        constraint_name: str = "",
        conflict_columns: list = None,
        **kwargs,
    ):
        """
        拼接upsert语法的sql
            https://www.postgresql.org/docs/devel/sql-insert.html
        Args:
            data: 数据
            conflict_target: 约束条件
                例如:
                    id
                    "id"
            constraint_name: 约束名称
                例如:
                    test_table_unique_index
            conflict_columns: 唯一索引中包含的全部列名
                例如:
                    ["name", "id"]
            table_name: 表名
            **kwargs:

        Returns:

        """
        data = copy.deepcopy(data)
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not any([conflict_target, conflict_columns, constraint_name]):
            raise ValueError("must be set one of conflict_columns or constraint_name")
        if not data:
            return []
        if not isinstance(data, (list, tuple)):
            data = [data]

        conflict_columns = conflict_columns or []

        sql_list = []

        if conflict_target:
            conflict_expr = f"conflict({conflict_target})"
        elif conflict_columns:
            conflict_expr = 'conflict("{}")'.format('","'.join(conflict_columns))
        elif constraint_name:
            conflict_expr = f'conflict on constraint "{constraint_name}"'
        else:
            raise ValueError("must be set one of conflict_columns or constraint_name")

        for item in data:
            # 固定keys 取第一个数据
            # 如果不固定 则当给定数据的key顺序不固定时会出现入库错乱
            sql = "insert into {table_name} ({keys}) values({values}) on {conflict_expr} do update set {update_data};"
            # 拼接sql
            order_data = self.handle_values(item)
            keys = ",".join(order_data.keys())
            values = ",".join(order_data.values())

            sql = sql.format(
                **{
                    "keys": keys,
                    "values": values,
                    "table_name": table_name,
                    "update_data": ",".join([f"{k} = {v}" for k, v in order_data.items()]),
                    "conflict_expr": conflict_expr,
                }
            )
            sql_list.append(sql)
        return sql_list

    def upsert(self, data: list, *, table_name: str = "", **kwargs):
        """
        upsert
        Args:
            data: 数据
            table_name: 表名
            **kwargs: 参考 get_upsert_sql 字段

        Returns:

        """
        r = 0
        for sql in self.get_upsert_sql(data, table_name=table_name, **kwargs):
            r += self.cursor.execute(sql)
            if self.autocommit:
                self.cursor.conn.commit()
        return r

    def get_table_field_list(self, table_name):
        """
        获取表字段

        :param table_name: 表名
        :return: 字段列表
        """
        _sql = f"SELECT * FROM {table_name} LIMIT 0"
        _cursor = self.get_cursor(connection=self.conn)
        _cursor.execute(_sql)
        # 获取表头字段
        column_names = [column[0] for column in _cursor.description]
        _cursor.close()
        # 打印表头字段
        return column_names

    def query_one(self, sql, *, args=None):
        """
            cursor.execute + fetchone
        Args:
            sql:
            args:

        Returns:

        """
        # 重新获取cursor是为了防止同一个cursor在多个线程下使用导致数据混乱
        _cursor = self.get_cursor(connection=self.conn)
        _cursor.execute(sql, args=args)
        result = _cursor.fetchone()
        _cursor.close()
        return result

    def query_all(self, sql, *, args=None, autocommit=False):
        """
            cursor.execute + fetchall
        Args:
            sql:
            args:
            autocommit:

        Returns:

        """

        self.cursor.execute(sql, args=args)
        result = self.cursor.fetchall()
        if autocommit:
            # 如果不commit的话，由于事务的隔离等级问题，会导致读到的数据是经过缓存无变化的
            self.cursor.conn.commit()
        return result

    def query_single_attr(self, sql, *, args=None):
        """
        查询单个字段使用
        Args:
            sql: 查询sql
            args: list [1,2,3,4,5]

        Returns:

        """
        # 假如用了 dict_cursor 会挂掉的
        return [item[0] for item in self.query_all(sql, args=args)]

    def close(self):
        """
        关闭数据库连接
        Returns:

        """
        try:
            if self._cursor:
                self._cursor.close()
        except:
            pass
        try:
            if self._conn:
                self._conn.close()
            self.logger.debug("pg连接关闭成功")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._conn = None
        PostgreSQLOpt.connection_list.pop(self.key, "")

    def copy(self, *, protocol="", **kwargs):
        """
        复制一个新连接 可以修改有限的一些参数

        Args:
            protocol: mysql连接协议  mysql+pymysql   mysql+mysqldb
            **kwargs:

        Returns:

        """
        _kwargs = copy.deepcopy(self.kwargs)
        _kwargs.update(kwargs)
        _kwargs.update({"is_pool": False})
        db_opt = PostgreSQLOpt(self.options, **_kwargs)
        return db_opt
