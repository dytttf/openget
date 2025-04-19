# coding:utf8
"""
setting
"""
import os
from os import path, getenv

#
OPENGET_ENV = getenv("OPENGET_ENV", "DEV")

if 1:

    def get_mysql_uri(x, protocol="mysql"):
        return "{protocol}://{user}:{passwd}@{host}:{port}/{db}?charset={charset}".format(protocol=protocol, **x)

    # mysql config
    default_mysql_setting_dict = {
        "host": getenv("OPENGET_MYSQL_HOST", "localhost"),
        "port": int(getenv("OPENGET_MYSQL_PORT", "3306")),
        "user": getenv("OPENGET_MYSQL_USER"),
        "passwd": getenv("OPENGET_MYSQL_PASSWD"),
        "charset": getenv("OPENGET_MYSQL_CHARSET", "utf8mb4"),
        "db": getenv("OPENGET_MYSQL_DB", "openget"),
    }

    default_mysql_uri = get_mysql_uri(default_mysql_setting_dict)

if 1:
    # redis config
    def get_redis_uri(db, host, password=""):
        return "redis://{}{}/{}".format(f":{password}@" if password else "", host, db)

    #
    default_redis_uri = getenv("OPENGET_REDIS_URI", "redis://localhost:6379/0")

    # for redis lock
    redis_util_cluster = {
        0: default_redis_uri,
    }

if 1:
    # proxy config
    proxy_service_url = getenv("OPENGET_PROXY_SERVICE_URL")

# import setting from execute dir
try:
    if path.dirname(__file__) != os.getcwd():
        from setting import *
except ImportError:
    pass
