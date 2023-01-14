# coding:utf8
"""
Get all environment variable

"""
import sys
import os
from os import path


# All env defines
env = {
    # PROD / DEV / ...
    "OPENGET_ENV": "DEV",
    # default mysql config
    "OPENGET_MYSQL_HOST": None,
    "OPENGET_MYSQL_PORT": "3306",
    "OPENGET_MYSQL_USER": None,
    "OPENGET_MYSQL_PASSWD": None,
    "OPENGET_MYSQL_CHARSET": "utf8mb4",
    "OPENGET_MYSQL_DB": "openget",
    # default redis config
    "OPENGET_REDIS_URI": "redis://localhost:6379/0",
    # default proxy service url
    "OPENGET_PROXY_SERVICE_URL": None,
    # docker mode
    "OPENGET_IN_DOCKER": "false",
    # logging
    # forbidden better_exceptions
    "OPENGET_FORBIDDED_BETTER_EXCEPTIONS": None,
}


def parse_env_from_file(filepath):
    _env = {}
    if path.isfile(filepath):
        with open(
            filepath,
            "r",
            encoding="utf8",
        ) as f:
            for line in f.readlines():
                line = line.strip()
                if not line:
                    continue
                # parse comment line
                if line.startswith(("#", "'", '"')):
                    continue
                # parse k,v line
                k, v = line.split("=", 1)
                if v.startswith(("'", '"')):
                    v = v[1:-1]
                _env[k] = v
    return _env


# from ~/.openget/.env
GLOBAL_ENV_FILE = path.join(path.expanduser("~"), ".openget/.env")
env.update(parse_env_from_file(GLOBAL_ENV_FILE))

# from $(pwd)/.env
if sys.argv[0]:
    LOCAL_ENV_FILE = path.join(
        path.dirname(path.join(os.getcwd(), sys.argv[0])), ".env"
    )
    env.update(parse_env_from_file(LOCAL_ENV_FILE))

for env_k, env_v in env.items():
    if env_v is not None:
        os.environ[env_k] = env_v
