# coding:utf8
"""
Oracle utils
"""
import os

os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"

import copy
import datetime
import json
import logging
import time
from collections import OrderedDict, defaultdict
from typing import List, Dict

import cx_Oracle

from graper.utils import log

OracleError = (cx_Oracle.DatabaseError,)


def escape_string(value):
    value = value.replace("'", "''")
    return value


def get_connection(options, **kwargs):
    """

    Args:
        options:
        **kwargs:

    Returns:

    """
    user = options["username"]
    password = options["password"]
    host = options["host"]
    port = options["port"]
    if port is None:
        port = 1521
    db_default = options["db"]
    params = options["params"]
    charset = params.get("charset", ["utf8"])[0]
    dsn = cx_Oracle.makedsn(host, port, db_default)
    connection = cx_Oracle.connect(
        user=user,
        password=password,
        dsn=dsn,
        encoding=charset,
        threaded=True,
        events=True,
        **kwargs,
    )
    connection.autocommit = 1
    return connection


class Cursor(object):
    """
    Cursor class, catch timeout errors and reconnect
    """

    def __init__(
        self,
        connection: cx_Oracle.Connection,
        options: Dict,
        logger: logging.Logger = None,
        **kwargs,
    ):

        self.connection = connection
        self.cursor = self.connection.cursor()

        self.kwargs = kwargs
        self.options = options

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, parameters=None, retry=0):
        try:
            parameters = parameters or {}
            result = self.cursor.execute(sql, **parameters)
        except OracleError as e:
            # 捕获超时异常 关闭异常
            error_string = str(e)
            if retry < 20 and (
                "超出最大空闲时间" in error_string
                or "重新连接" in error_string
                or "您将被注销" in error_string
                or "没有登录" in error_string
            ):
                # cx_Oracle.DatabaseError: ORA-02396: 超出最大空闲时间, 请重新连接
                self.logger.error(f"oracle connect error:{error_string}  reconnect...")
                time.sleep(retry * 5)
                # 重连
                connection = get_connection(self.options, **self.kwargs)
                self.connection = connection
                self.cursor = self.connection.cursor()
                return self.execute(sql, parameters=parameters, retry=retry + 1)
            raise e
        return result


class OracleOpt(object):
    # 连接池 共用
    connection_list = {}

    def __init__(self, options: Dict, logger: logging.Logger = None, **kwargs):
        self.reuse_connection = kwargs.pop("reuse_connection", True)
        # 额外参数
        self.kwargs = kwargs

        self.options = options
        self.key = repr(options)

        self.logger = self.kwargs.pop("logger", logger) or log.get_logger(__file__)

        self.table_name = ""

        # 私有变量
        self._connection = None
        self._cursor = None

    def __del__(self):
        try:
            self.close()
        except Exception as e:
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
                or self.key not in OracleOpt.connection_list.keys()
            ):
                self._cursor = self.get_cursor()
                if self.reuse_connection:
                    OracleOpt.connection_list[self.key] = self._cursor
            else:
                self._cursor = OracleOpt.connection_list[self.key]
        return self._cursor

    def get_cursor(self, connection=None):
        """

        Args:
            connection:

        Returns:

        """
        connection = connection or get_connection(self.options, **self.kwargs)
        cursor = Cursor(connection, self.options, logger=self.logger, **self.kwargs)
        return cursor

    @staticmethod
    def escape_values(data, sort_keys=None) -> OrderedDict:
        """
            Convert data to oracle support types.
        Args:
            data:
            sort_keys:

        Returns:

        """
        if not sort_keys:
            sort_keys = list(data.keys())
        data = OrderedDict()
        for k in sort_keys:
            v = data[k]
            if isinstance(v, str):
                v = v.strip()
                # 处理clob
                if len(v) > 4000:
                    v_list = []
                    _step = 2000
                    for i in range(0, len(v), _step):
                        v_list.append(v[i : i + _step])
                    v_list = ["to_clob('{}')".format(escape_string(x)) for x in v_list]
                    v = "||".join(v_list)
                else:
                    v = "'{}'".format(escape_string(v))
            elif isinstance(v, (int, float)):
                v = str(v)
            elif isinstance(v, (datetime.date, datetime.time)):
                v = "'{}'".format(v)
            elif v is None:
                v = "null"
            else:
                v = json.dumps(v, ensure_ascii=False)
                v = "'{}'".format(escape_string(v))
            data[str(k)] = v
        return data

    @staticmethod
    def group_data_by_keys(data: List[Dict]):
        """

        Args:
            data:

        Returns:

        """
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
            save data to database
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
        sql = 'INSERT INTO "{table_name}" ("{keys}") VALUES({values})'
        # 拼接sql
        order_data = self.escape_values(data)
        sql = sql.format(
            **{
                "keys": '","'.join(order_data.keys()),
                "values": ",".join(order_data.values()),
                "table_name": table_name,
            }
        )
        _cursor = self.get_cursor(self.connection)
        try:
            _cursor.execute(sql)
            resp = 0
        except Exception as e:
            if ignore_duplicate and (
                "unique constraint" in str(e) or "违反唯一约束条件" in str(e)
            ):
                self.logger.error(e)
                resp = 1
            else:
                raise e
        finally:
            _cursor.close()
        return resp

    def add_many(
        self,
        data: List,
        *,
        table_name: str = "",
        batch_size: int = 100,
        group_by_keys: bool = False,
        ignore_unique_key: str = None,
        show_log=True,
    ):
        """
            save data list to database
            !!! not atomicity
        Args:
            data:
            table_name: table name is case sensitive
            batch_size:
            group_by_keys:
                if set True, the data will be grouped by keys when is has different keys
                else, raise an error
            ignore_unique_key:
                the name of unique index, will be ignored.
                case sensitive
            show_log:

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

        error_data = []
        dup_index_hint = ""
        if ignore_unique_key:
            dup_index_hint = '/*+IGNORE_ROW_ON_DUPKEY_INDEX("{}","{}")*/'.format(
                table_name, ignore_unique_key
            )

        _cursor = self.get_cursor(self.connection)

        for data in data_group_list:
            # keys must has same order, otherwise, save data to database may be  in the confusion;
            first_data_keys = list(data[0].keys())
            resp = 0
            while data:
                _data = data[:batch_size]
                data = data[batch_size:]
                sql = 'INSERT {hint} INTO "{table_name}"("{keys}") {values}'
                # concat sql
                values_list = []
                for item in _data:
                    order_data = self.escape_values(item, sort_keys=first_data_keys)
                    keys = '","'.join(order_data.keys())
                    values = ",".join(order_data.values())
                    values_list.append(values)

                value_sql_list = ["select {} from dual".format(values_list[0])]
                if len(values_list) > 1:
                    value_sql_list.extend(
                        "union all select {} from dual".format(x)
                        for x in values_list[1:]
                    )
                sql = sql.format(
                    **{
                        "hint": dup_index_hint,
                        "table_name": table_name,
                        "keys": keys,
                        "values": " ".join(value_sql_list),
                    }
                )
                try:
                    _cursor.execute(sql)
                    if show_log:
                        self.logger.debug("insert rows {}".format(_cursor.rowcount))
                    resp = 0
                except Exception as e:
                    if "Unknown column" in str(e):
                        if show_log:
                            self.logger.debug("error data: {}".format(_data[0]))
                    raise e
        _cursor.close()
        return resp

    def query_all(self, sql, *, parameters: Dict = None):
        """

        Args:
            sql:
            parameters:

        Returns:

        """
        _cursor = self.get_cursor(connection=self.connection)
        r = _cursor.execute(sql, parameters=parameters)
        # when sql is not a select statement, r is None
        # https://cx-oracle.readthedocs.io/en/latest/cursor.html#Cursor.execute
        if r:
            result = _cursor.fetchall()
        else:
            result = []
        _cursor.close()
        return result

    def get_unique_indexs(self, table_name: str):
        """

        Args:
            table_name:

        Returns:

        """
        sql = "SELECT INDEX_NAME FROM USER_INDEXES WHERE UNIQUENESS='UNIQUE' AND TABLE_NAME='{}'".format(
            table_name
        )
        indexes = self.query_all(sql)
        return [x[0] for x in indexes]

    def close(self):
        """

        Returns:

        """
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.connection.close()
            self.logger.debug("Successfully closed oracle connection")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._connection = None
        OracleOpt.connection_list.pop(self.key, "")

    def copy(self, **kwargs):
        """
            Copy self
        Args:
            **kwargs:

        Returns:

        """
        _kwargs = copy.deepcopy(self.kwargs)
        _kwargs.update(kwargs)
        _kwargs.update({"reuse_connection": False})
        db_opt = OracleOpt(self.options, **_kwargs)
        return db_opt
