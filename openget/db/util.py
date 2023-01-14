# coding:utf8
"""
"""
import re
from urllib import parse


def quote_uri_password(uri: str):
    """
        Quote password in uri
    Args:
        uri:

    Returns:
    """
    pw_pos = re.compile("://[^:]*?:")
    if pw_pos.search(uri) and "@" in uri:
        left = pw_pos.search(uri).end(0)
        right = uri.rfind("@")
        password = uri[left:right]
        uri = uri.replace(password, parse.quote_plus(password), 1)
    return uri


def uri_to_dict(uri):
    p = parse.urlparse(quote_uri_password(uri))

    options = {
        "type": p.scheme.strip(),
        "host": p.hostname.strip(),
        "port": p.port,
        "username": p.username.strip(),
        "password": parse.unquote_plus(p.password.strip()),
        "db": p.path.strip("/").strip(),
        "params": parse.parse_qs(p.query),
    }
    return options
