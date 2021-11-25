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
    "GRAPER_ENV": "DEV",
    # default mysql config
    "GRAPER_MYSQL_HOST": None,
    "GRAPER_MYSQL_PORT": "3306",
    "GRAPER_MYSQL_USER": None,
    "GRAPER_MYSQL_PASSWD": None,
    "GRAPER_MYSQL_CHARSET": "utf8mb4",
    "GRAPER_MYSQL_DB": "graper",
    # default redis config
    "GRAPER_REDIS_URI": "redis://localhost:6379/0",
    # default proxy service url
    "GRAPER_PROXY_SERVICE_URL": None,
    # docker mode
    "GRAPER_IN_DOCKER": "false",
    # logging
    # forbidden better_exceptions
    "GRAPER_FORBIDDED_BETTER_EXCEPTIONS": None,
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


# from ~/.graper/.env
GLOBAL_ENV_FILE = path.join(path.expanduser("~"), ".graper/.env")
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
