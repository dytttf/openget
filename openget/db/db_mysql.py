# coding:utf8
"""
MySQL utils

Known Issue:
    Sometimes, in gevent environment, pymysql will raise an error:
        RuntimeError: reentrant call inside <_io.BufferedReader name=8>
"""
import copy
import datetime
import json
import logging
import time
from collections import OrderedDict, defaultdict
from typing import List, Tuple, Dict, Union

import pymysql
import threading
from pymysql import cursors as pymysql_cursors

escape_str = pymysql.converters.escape_str

try:
    import_mysqldb_error = None
    import MySQLdb
    from MySQLdb import cursors as mysqlclient_cursors
except ImportError as e:
    MySQLdb = None
    import_mysqldb_error = e

from openget.utils import log

MysqlError = (pymysql.OperationalError, pymysql.ProgrammingError)

if MySQLdb:
    MysqlError = (
        pymysql.OperationalError,
        MySQLdb.OperationalError,
        pymysql.ProgrammingError,
        MySQLdb.ProgrammingError,
    )
else:
    catch_error = (pymysql.OperationalError, pymysql.ProgrammingError)

cursor_types = {
    "pymysql": {
        "cursor": pymysql_cursors.Cursor,
        "dict_cursor": pymysql_cursors.DictCursor,
        "ss_cursor": pymysql_cursors.SSCursor,
        "ss_dict_cursor": pymysql_cursors.SSDictCursor,
    },
    "mysqlclient": {
        "cursor": mysqlclient_cursors.Cursor,
        "dict_cursor": mysqlclient_cursors.DictCursor,
        "ss_cursor": mysqlclient_cursors.SSCursor,
        "ss_dict_cursor": mysqlclient_cursors.SSDictCursor,
    },
}

mutex = threading.RLock()


def get_connection(options: Dict, **kwargs):
    """
        建立mysql连接
    Args:
        options:
        **kwargs:

    Returns:

    """

    autocommit = kwargs.pop("autocommit", True)

    username = options["username"]
    password = options["password"]
    host = options["host"]
    port = options["port"]
    if port is None:
        port = 3306
    db_default = options["db"]
    params = options["params"]
    charset = params.get("charset", ["utf8mb4"])[0]

    if "pymysql" in options["type"]:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=username,
            passwd=password,
            db=db_default,
            charset=charset,
            **kwargs,
        )
        connection.set_charset(charset)
    elif "mysqldb" in options["type"]:
        if not MySQLdb:
            # 抛出异常
            raise import_mysqldb_error
        connection = MySQLdb.connect(
            host=host,
            port=port,
            user=username,
            passwd=password,
            db=db_default,
            charset=charset,
            **kwargs,
        )
        connection.set_character_set(charset)
    else:
        raise ValueError("unknown mysql type: {}".format(options["type"]))
    connection.autocommit(autocommit)
    return connection


class Cursor(object):
    """
    Cursor class, catch timeout errors and reconnect
    """

    def __init__(
        self,
        connection: pymysql.Connection,
        options: Dict,
        logger: logging.Logger = None,
        cursor_class=None,
        **kwargs,
    ):
        self.connection = connection
        self.cursor = None
        self.cursor_class = cursor_class
        self.reconnect(connection)

        self.kwargs = kwargs
        self.options = options

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

        self._init()

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def _init(self):
        """
            1、at the session level, set range_optimizer_max_mem_size=83886080 to optimize update performance, the default is 8388608(8M).

        Returns:

        """
        try:
            sql = "show variables like '%range_optimizer_max_mem_size%';"
            self.cursor.execute(sql)
            _r = self.cursor.fetchall()
            if _r:
                # PolorDB doesn't have this variable
                old_size = (
                    int(_r[0][1])
                    if not isinstance(_r[0], dict)
                    else int(_r[0]["Value"])
                )
                if old_size < 83_886_080:
                    self.cursor.execute(
                        "set session range_optimizer_max_mem_size=83886080;"
                    )
        except Exception:
            self.logger.exception("set range_optimizer_max_mem_size failed")
        return

    def reconnect(self, conn=None):
        mysql_conn = conn or get_connection(self.options, **self.kwargs)
        # 判断使用的哪个python库
        module_str = str(mysql_conn.cursorclass).lower()
        if "pymysql" in module_str:
            cursor_class = cursor_types["pymysql"].get(self.cursor_class or "cursor")
        elif "mysqldb" in module_str:
            cursor_class = cursor_types["mysqlclient"].get(
                self.cursor_class or "cursor"
            )
        self.cursor = mysql_conn.cursor(cursor_class)
        self.connection = mysql_conn
        return self.connection, self.cursor

    def execute(self, sql, args=None, retry=0):
        try:
            mutex.acquire()
            try:
                self.conn.ping(reconnect=True)
            except:
                pass
            result = self.cursor.execute(sql, args=args)
        except MysqlError as e:
            # catch: timeout error, closed error, ...
            if retry < 20 and str(e.args[0]).lower() in (
                "2006",
                "2013",
                "cursor closed",
                "server has gone away",
            ):
                self.logger.debug(f"mysql connect error:{e}  try reconnect({retry})...")
                time.sleep(retry * 5)
                # 重连
                self.reconnect()
                return self.execute(sql, args=args, retry=retry + 1)
            raise e
        finally:
            mutex.release()
        # except RuntimeError as e:
        #     # 处理 RuntimeError: reentrant call inside <_io.BufferedReader name=8>  gevent引发的错误
        #     # http://k.sina.com.cn/article_1708729084_65d922fc034002ecf.html
        #     # https://coveralls.io/builds/5985537/source?filename=src%2Fgevent%2Fsubprocess.py
        #     # 已知 pymysql==0.7.11 没问题
        #     # 而 pymysql==0.9.3 会在发现未知异常 比如 RuntimeError: reentrant call inside 强制关闭mysql连接
        #     # 所以不要升级
        #     # if "reentrant" in e.args[0] and retry < 3:
        #     #     self.logger.debug("mysql连接错误:{}".format(e))
        #     #     return self.execute(sql, args=args, retry=retry + 1)
        #     raise e
        return result


class MySQLOpt(object):
    # connection pool
    connection_list = {}

    def __init__(self, options, cursor_class: str = None, **kwargs):
        """
        Args:
            options: mysql connect args
            cursor_class:
                cursor, dict_cursor, ss_cursor, ss_dict_cursor
            **kwargs:
                autocommit:
                reuse_connection: reuse connection when copy self
        """
        self.reuse_connection = kwargs.pop("reuse_connection", True)
        self.cursor_class = cursor_class
        self.autocommit = kwargs.setdefault("autocommit", True)
        if self.reuse_connection:
            print("警告: 在多线程中用同一个mysql连接同时进行插入和查询操作时，插入操作会影响到查询结果...")

        # 额外参数
        self.kwargs = kwargs

        self.options = options
        self.key = repr(options)

        self.logger: logging.Logger = self.kwargs.pop("logger", None) or log.get_logger(
            __file__
        )

        self.table_name = ""

        #
        self._connection = None
        self._cursor = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    @property
    def connection(self):
        self._connection = self.cursor.connection
        return self._connection

    @property
    def cursor(self):
        if not self._cursor:
            if (
                not self.reuse_connection
                or self.key not in MySQLOpt.connection_list.keys()
            ):
                self._cursor = self.get_cursor()
                if self.reuse_connection:
                    MySQLOpt.connection_list[self.key] = self._cursor
            else:
                self._cursor = MySQLOpt.connection_list[self.key]
        return self._cursor

    def get_cursor(self, connection: pymysql.Connection = None):
        """
        Args:
            connection:

        Returns:

        """
        connection = connection or get_connection(self.options, **self.kwargs)
        cursor = Cursor(
            connection,
            self.options,
            logger=self.logger,
            cursor_class=self.cursor_class,
            **self.kwargs,
        )
        return cursor

    @staticmethod
    def escape_str(*args, **kwargs):
        return escape_str(*args, **kwargs)

    @staticmethod
    def escape_values(
        data: Dict, sort_keys: List = None, strip: bool = True
    ) -> OrderedDict:
        """
            Convert data to mysql support types.

            string/int/float/datetime -> string
            None -> null
            others -> json.dumps()

        Args:
            data:
            sort_keys:
            strip: if set True, the string value will be striped

        Returns:
            OrderedDict
        """
        if not sort_keys:
            sort_keys = list(data.keys())

        order_data = OrderedDict()
        for k in sort_keys:
            v = data[k]
            k = f"`{k}`"
            if isinstance(v, str):
                if strip:
                    v = v.strip()
                v = escape_str(v)
            elif isinstance(v, (int, float)):
                v = str(v)
            elif isinstance(v, (datetime.date, datetime.time)):
                v = "'{}'".format(v)
            elif v is None:
                v = "null"
            else:
                v = json.dumps(v, ensure_ascii=False)
                v = escape_str(v)
            order_data[k] = v
        return order_data

    @staticmethod
    def group_data_by_keys(data: List[Dict]) -> List[List]:
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
        data: Dict,
        *,
        table_name: str = "",
        ignore_duplicate: bool = True,
    ) -> int:
        """
            save one data to database
        Args:
            data:
            table_name:
            ignore_duplicate: ignore duplicate errors

        Returns:
            0: success
            1: duplicate
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name must be not empty")
        if not data:
            raise ValueError("data must be not empty")
        sql = "insert into {table_name} ({keys}) values({values});"
        # 拼接sql
        order_data = self.escape_values(data)
        sql = sql.format(
            **{
                "keys": ",".join(order_data.keys()),
                "values": ",".join(order_data.values()),
                "table_name": table_name,
            }
        )
        try:
            self.cursor.execute(sql)
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
        data: List[Dict],
        *,
        table_name: str = "",
        batch_size: int = 100,
        group_by_keys: bool = False,
        ignore_duplicate: bool = True,
        ignore_unknown_column: bool = False,
        show_log=True,
    ) -> Union[int, Tuple[int, List]]:
        """
            save data list to database
            !!! not atomicity
        Args:
            data:
            table_name:
            batch_size:
            group_by_keys:
                if set True, the data will be grouped by keys when is has different keys
                else, raise an error
            ignore_duplicate:
            ignore_unknown_column: 忽略表结构不一致错误 并且返回错误数据
                if set True, will ignore table struct error and return error data
                else, raise an error
            show_log

        Returns:

        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name must be not empty")
        if not data:
            raise ValueError("data must be not empty")
        if not isinstance(data, (list, tuple)):
            data = [data]

        data_group_list = self.group_data_by_keys(data)
        if not group_by_keys and len(data_group_list) != 1:
            raise ValueError("data has different keys")

        effect_count = 0
        error_data = []

        for data in data_group_list:
            # keys must has same order, otherwise, save data to database may be  in the confusion;
            first_data_keys = list(data[0].keys())
            while data:
                _data = data[:batch_size]
                data = data[batch_size:]
                #
                if ignore_duplicate:
                    sql = "insert ignore into {table_name} ({keys}) values{values};"
                else:
                    sql = "insert into {table_name} ({keys}) values{values};"
                # concat sql
                values_list = []
                for item in _data:
                    order_data = self.escape_values(item, sort_keys=first_data_keys)
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
                try:
                    rows = self.cursor.execute(sql)
                    effect_count += rows
                    if show_log:
                        self.logger.debug("insert rows {}".format(rows))
                except Exception as e:
                    if "Unknown column" in str(e):
                        self.logger.debug(f"error data: {_data[0]}")
                        if ignore_unknown_column:
                            error_data.extend(_data)
                            continue
                    raise e
        if ignore_unknown_column:
            return effect_count, error_data
        return effect_count

    def update(
        self,
        data,
        *,
        condition: Dict = None,
        table_name: str = "",
        where_sql: str = "",
    ):
        """
            update
        Args:
            data:
            condition:
                same as sql syntax: where
                but use dict and only support `and` with dict keys
            table_name:
            where_sql:
                same as sql syntax: where
                if set this field, the 'condition' field will be ignored.

        Returns:
            Number of affected rows
        """
        data = copy.deepcopy(data)
        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name must be not empty")
        if not condition:
            condition = {}
        sql = "update {table_name} set {update_data} where {where_sql};"
        order_data = self.escape_values(data)
        # set update data
        update_data = ["{}={}".format(k, v) for k, v in order_data.items()]
        update_data = ",".join(update_data)

        # set where condition
        order_condition = self.escape_values(condition, strip=False)
        order_condition = [f"{k}={v}" for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # first use where_sql
        where_sql = where_sql or order_condition

        sql = sql.format(
            **{
                "table_name": table_name,
                "update_data": update_data,
                "where_sql": where_sql,
            }
        )
        return self.cursor.execute(sql)

    def upsert(
        self,
        data: list,
        *,
        table_name: str = "",
        **kwargs,
    ):
        """
            使用upsert语法入库
        Args:
            data: 数据
            table_name: 表名
            **kwargs:

        Returns:

        """
        r = 0
        for sql in self.get_upsert_sql(data, table_name=table_name, **kwargs):
            r += self.cursor.execute(sql)
        return r

    def get_upsert_sql(
        self,
        data: list,
        *,
        table_name: str = "",
        **kwargs,
    ):
        """
            使用upsert语法入库
        Args:
            data: 数据
            table_name: 表名
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

        for item in data:
            # 如果不固定 则当给定数据的key顺序不固定时会出现入库错乱
            sql = "insert into {table_name} ({keys}) values({values}) on duplicate key update {update_data};"
            # 拼接sql
            order_data = self.escape_values(item)
            keys = ",".join(order_data.keys())
            values = ",".join(order_data.values())

            sql = sql.format(
                **{
                    "keys": keys,
                    "values": values,
                    "table_name": table_name,
                    "update_data": ",".join(
                        [f"{k} = {v}" for k, v in order_data.items()]
                    ),
                }
            )
            yield sql

    def delete(
        self,
        *,
        condition: Dict = None,
        table_name: str = "",
        where_sql: str = "",
    ):
        """
            delete
        Args:
            condition:
                same as sql syntax: where
                but use dict and only support `and` with dict keys
            table_name:
            where_sql:
                same as sql syntax: where
                if set this field, the 'condition' field will be ignored.
            **kwargs:

        Returns:
            Number of affected rows
        """

        table_name = table_name or self.table_name
        if not table_name:
            raise ValueError("table name {}".format(table_name))
        if not condition:
            condition = {}
        sql = "delete from {table_name} where {where_sql};"

        # set where condition
        order_condition = self.escape_values(condition)
        order_condition = ["{}={}".format(k, v) for k, v in order_condition.items()]
        order_condition = " and ".join(order_condition)

        # first use where_sql
        where_sql = where_sql or order_condition

        sql = sql.format(**{"table_name": table_name, "where_sql": where_sql})
        return self.cursor.execute(sql)

    def get_table_field_list(self, table_name):
        """
        Args:
            table_name:

        Returns:
            table's field name list
        """
        _sql = f"SELECT * FROM {table_name} LIMIT 0"
        _cursor = self.get_cursor(connection=self.connection)
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
        # create new cursor object to prevent multi query in same time
        _cursor = self.get_cursor(connection=self.connection)
        _cursor.execute(sql, args=args)
        result = _cursor.fetchone()
        _cursor.close()
        return result

    def query_all(self, sql, *, args=None) -> List[Tuple]:
        """
            cursor.execute + fetchall
        Args:
            sql:
            args:

        Returns:

        """
        # create new cursor object to prevent multi query in same time
        _cursor = self.get_cursor(connection=self.connection)
        _cursor.execute(sql, args=args)
        result = _cursor.fetchall()
        _cursor.close()
        return result

    def query_single_field(self, sql, *, args=None) -> List:
        """
            Collect first field in each row.

        Args:
            sql:
            args:

        Returns:

        """
        # if use dict_cursor, will failed
        return [item[0] for item in self.query_all(sql, args=args)]

    def close(self):
        try:
            if self._cursor:
                self._cursor.close()
        except:
            pass
        try:
            if self._connection:
                self._connection.close()
            self.logger.debug("Successfully closed mysql connection")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._connection = None
        MySQLOpt.connection_list.pop(self.key, None)

    def copy(self, *, protocol="", **kwargs):
        """
            Copy self and modify some properties
        Args:
            protocol:
                mysql+pymysql   mysql+mysqldb
            **kwargs:

        Returns:

        """
        _kwargs = copy.deepcopy(self.kwargs)
        _kwargs.update(kwargs)
        _kwargs.update({"reuse_connection": False})
        #
        options = self.options.copy()
        if protocol:
            assert protocol in ["mysql+pymysql", "mysql+mysqldb"]
            options["type"] = protocol
        #
        db_opt = MySQLOpt(options, **_kwargs)
        return db_opt

    def create(
        self,
        fields: Dict[str, str] = None,
        unique: List[str] = None,
        table_name: str = "",
        type="",
        exists_ok=True,
        sql="",
        encoding: str = "utf8mb4_0900_ai_ci",
        **kwargs,
    ):
        """
            create table
        Args:
            table_name:
            sql: DDL
            **kwargs:

        Returns:

        """

        assert type in ["", "task", "batch_record", "batch_value"]

        # default fields
        default_fields = [
            "id",
            "created_time",
            "state",
            "batch_date",
        ]

        # use translate
        sql_list = [
            "begin",
        ]

        if sql:
            sql_list.append(sql)
        else:
            if not fields:
                self.logger.warning("no fields")
                return 0
            fields_keys = [x.lower() for x in fields]

            if "id" not in fields_keys:
                fields["id"] = "bigint NOT NULL AUTO_INCREMENT"
            if "created_time" not in fields_keys:
                fields[
                    "created_time"
                ] = "datetime(0) NOT NULL  DEFAULT CURRENT_TIMESTAMP(0)"

            if type == "task":
                if "state" not in fields_keys:
                    fields["state"] = "int DEFAULT 0"
            elif type == "batch_value":
                if "batch_date" not in fields_keys:
                    fields["batch_date"] = "datetime(0) NULL"

            # fields sorted
            fields = fields.items()
            fields = (
                [x for x in fields if x[0].lower() == "id"]
                + [x for x in fields if x[0].lower() not in default_fields]
                + [
                    x
                    for x in fields
                    if x[0].lower() in default_fields and x[0].lower() != "id"
                ]
            )

            sql = """
CREATE TABLE {}(
    {},
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={};
            """.format(
                "IF NOT EXISTS {}".format(table_name) if exists_ok else table_name,
                ",\n    ".join("{} {}".format(k, v) for k, v in fields),
                encoding,
            )
            sql_list.append(sql)

            if unique:
                unique = [x.strip("`") for x in unique]
                sql = "ALTER TABLE {} ADD UNIQUE INDEX `{}_unique`(`{}`);".format(
                    table_name, table_name, "`,`".join(unique)
                )
                sql_list.append(sql)

        #
        cursor = self.get_cursor()
        for sql in sql_list:
            self.logger.info(sql)
            cursor.execute(sql)
        cursor.connection.commit()
        return 0
