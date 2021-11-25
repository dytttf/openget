# coding:utf8
"""
"""
try:
    import MySQLdb
except ImportError:
    MySQLdb = None

from graper.db.util import uri_to_dict


class DB(object):
    """
    Database Factory
        db = DB.create("mysql://root:******@localhost:3306/test")
        db = DB.create("mysql+pymysql://root:******@localhost:3306/test")
        db = DB.create("mysql+mysqldb://root:******@localhost:3306/test")

        oracle:
            oracle://username:password@ip:port/db?charset=utf8
            oracle://sys:chang_on_install@127.0.0.1:1521/orcl?charset=utf8

        sqllite:
            sqlite:///tmp/a.db
            sqlite://:memory:
            sqlite://{path}
    """

    @staticmethod
    def create(uri: str, *, reuse_connection=True, **kwargs):
        """
        create a db connection

        Args:
            uri: protocol://user:password@host:port/params
            reuse_connection:
                if set True, then the DB instance with same db_uri will share only one connection.
            **kwargs:

        Returns:

        """
        if uri is None:
            raise Exception("db uri is empty")

        # sqlite
        if uri.startswith("sqlite://"):
            db_options = {"database": uri.replace("sqlite://", "")}
            from graper.db import db_sqlite

            db_opt = db_sqlite.SQLiteOpt(db_options)
            return db_opt

        #
        kwargs["reuse_connection"] = reuse_connection

        db_options = uri_to_dict(uri)
        # TODO SQLite

        if db_options["type"].startswith("mysql"):
            # default use mysqldb
            if db_options["type"] == "mysql":
                if MySQLdb:
                    db_options["type"] = "mysql+mysqldb"
                else:
                    db_options["type"] = "mysql+pymysql"
            from graper.db import db_mysql

            db_opt = db_mysql.MySQLOpt(db_options, **kwargs)
        elif db_options["type"].startswith("oracle"):
            from graper.db import db_oracle

            db_opt = db_oracle.OracleOpt(db_options, **kwargs)
        elif db_options["type"].startswith("redis"):
            from graper.db.db_redis import create_redis_client

            db_opt = create_redis_client(uri)

        else:
            raise Exception("unknown protocol：%s" % db_options["type"])
        return db_opt
