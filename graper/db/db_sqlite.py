# coding:utf8
"""
SQLite utils

    https://docs.python.org/zh-cn/3/library/sqlite3.html

"""
import copy
import datetime
import json
import logging
from collections import OrderedDict, defaultdict
from typing import List, Tuple, Dict

import sqlite3
import pymysql


escape_str = pymysql.converters.escape_str

from graper.utils import log


def get_connection(options, **kwargs):
    """
    Args:
        options:
        **kwargs:

    Returns:

    """
    kwargs.setdefault("check_same_thread", False)
    # autocommit
    kwargs.setdefault("isolation_level", None)
    connection = sqlite3.connect(options["database"], **kwargs)
    return connection


class SQLiteOpt(object):
    def __init__(self, options, **kwargs):
        """
        Args:
            options:
        """
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
    def connection(self) -> sqlite3.Connection:
        self._connection = self.cursor.connection
        return self._connection

    @property
    def cursor(self) -> sqlite3.Cursor:
        if not self._cursor:
            self._cursor = self.get_cursor()
        return self._cursor

    def get_cursor(self, connection: sqlite3.Connection = None) -> sqlite3.Cursor:
        """
        Args:
            connection:

        Returns:

        """
        connection = connection or get_connection(self.options, **self.kwargs)
        cursor = connection.cursor()
        return cursor

    def create(
        self,
        fields: Dict[str, str] = None,
        unique: List[str] = None,
        table_name: str = "",
        type="",
        exists_ok=True,
        sql="",
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
                fields["id"] = "integer primary key autoincrement"
            if "created_time" not in fields_keys:
                fields[
                    "created_time"
                ] = "timestamp default (datetime('now', 'localtime'))"

            if type == "task":
                if "state" not in fields_keys:
                    fields["state"] = "int key default 0"
            elif type == "batch_value":
                if "batch_date" not in fields_keys:
                    fields["batch_date"] = "date key"

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
            create table {}(
                {}
            )
            """.format(
                "if not exists {}".format(table_name) if exists_ok else table_name,
                ",".join("{} {}".format(k, v) for k, v in fields),
            )
            sql_list.append(sql)

            if unique:
                sql = (
                    "create unique index if not exists `{}_unique` on {} ({});".format(
                        table_name, table_name, ",".join(unique)
                    )
                )
                sql_list.append(sql)
        #
        cursor = self.get_cursor()
        for sql in sql_list:
            cursor.execute(sql)
        cursor.connection.commit()
        return 0

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
            if ignore_duplicate and "UNIQUE constraint" in str(e):
                self.logger.error(e)
                resp = 1
            else:
                raise e
        return resp

    def add_many(
        self,
        data: List,
        *,
        table_name: str = "",
        batch_size: int = 100,
        group_by_keys: bool = False,
        ignore_duplicate: bool = True,
        show_log=True,
    ) -> int:
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
            show_log:

        Returns:
            Number of affected rows

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
        for data in data_group_list:
            # keys must has same order, otherwise, save data to database may be  in the confusion;
            first_data_keys = list(data[0].keys())
            while data:
                _data = data[:batch_size]
                data = data[batch_size:]
                #
                if ignore_duplicate:
                    sql = "insert or ignore into {table_name} ({keys}) values{values};"
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
                    rows = self.cursor.execute(sql).rowcount
                    effect_count += rows
                    if show_log:
                        self.logger.debug("insert rows {}".format(rows))
                except Exception as e:
                    if "Unknown column" in str(e) and show_log:
                        self.logger.debug("error data: {}".format(_data[0]))
                    raise e
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
        return self.cursor.execute(sql).rowcount

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
        return self.cursor.execute(sql).rowcount

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
        _cursor.execute(sql, args or ())
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
            self.logger.debug("Successfully closed sqlite connection")
        except:
            pass
        # 删除链接池中的链接
        self._cursor = None
        self._connection = None

    def copy(self, **kwargs):
        return SQLiteOpt(self.options, **self.kwargs)
