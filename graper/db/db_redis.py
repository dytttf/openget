"""


Examples:

    >>> create_redis_client("redis://:abcd@localhost:6379/0")
    Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>

    >>> for password in ["?$!@#$%2F^&*()！@#￥……[{:~12+s#word", "host"]:
    ...     r = parse_sentinel_uri(
    ...     "redis+sentinel://:{}@host1:12,host2:12/0?service_name=xxxx".format(password)
    ...     )
    ...     assert r["password"] == password
    ...     r = parse_redis_uri("redis://:{}@host1:12/0".format(password))
    ...     assert r["password"] == password
"""
from typing import Union, Tuple, Dict
import redis
from redis.sentinel import Sentinel
from urllib.parse import urlparse, parse_qs, unquote_plus

from graper.db.util import quote_uri_password


def parse_sentinel_uri(uri) -> Dict:
    """
        Parse sentinel uri info
            redis+sentinel://:password@host1:port1,host2:port2/db?service_name=xxxx
    Args:
        uri:

    Returns:

    """
    p = urlparse(quote_uri_password(uri))
    assert "sentinel" in p.scheme
    return {
        "hosts": p.netloc.split("@")[-1].split(","),
        "password": unquote_plus(p.password) if p.password else "",
        "db": int(p.path.strip("/").strip()),
        "service_name": parse_qs(p.query)["service_name"][0],
    }


def parse_redis_uri(uri) -> Dict:
    """
        Parse redis uri
    Args:
        uri:

    Returns:

    """
    p = urlparse(quote_uri_password(uri))
    return {
        "host": p.hostname,
        "port": p.port,
        "password": unquote_plus(p.password) if p.password else "",
        "db": int(p.path.strip("/").strip()),
    }


def create_redis_client(
    uri: str, **kwargs
) -> Union[redis.Redis, Tuple[redis.Redis, redis.Redis]]:
    """
        Create redis client
    Args:
        uri:
            redis://:password@host:port/db
            redis+sentinel://:password@host1:port1,host2:port2/db?service_name=asdf
    Returns:

    """
    kwargs["decode_responses"] = kwargs.get("decode_responses", True)

    options = {}
    if "redis://" in uri:
        options = parse_redis_uri(uri)
        return redis.Redis(
            host=options["host"],
            port=options["port"],
            password=options["password"],
            db=options["db"],
            **kwargs
        )
    elif "redis+sentinel" in uri:
        options = parse_sentinel_uri(uri)
        options.update({"type": "sentinel"})

    if isinstance(options, dict):
        if options.get("type", "") == "sentinel":
            # 支持哨兵模式
            hosts = options["hosts"]
            hosts = [x.split(":") for x in hosts]
            hosts = [(x[0], int(x[1])) for x in hosts]
            #
            sentinel = Sentinel(hosts, socket_timeout=0.5, **kwargs)
            master = sentinel.master_for(
                options["service_name"],
                password=options["password"],
                db=options["db"],
            )
            slave = sentinel.slave_for(
                options["service_name"],
                password=options["password"],
                db=options["db"],
            )
            return master, slave
    raise Exception("UnKnown redis connect info: {}".format(options))


if __name__ == "__main__":
    pass
