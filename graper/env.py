# coding:utf8
"""
Get all environment variable

"""
import os
from os import path


# All env define
env = {
    # PROD / DEV / ...
    "GRAPER_ENV": "DEV",
    # default mysql config
    "GRAPER_MYSQL_HOST": None,
    "GRAPER_MYSQL_PORT": "3306",
    "GRAPER_MYSQL_USER": None,
    "GRAPER_MYSQL_PASSWD": None,
    "GRAPER_MYSQL_CHARSET": "utf8mb4",
    "GRAPER_MYSQL_DB": "GRAPER",
    # default redis config
    "GRAPER_REDIS_URI": "redis://localhost:6379/0",
    # default proxy service url
    "GRAPER_PROXY_SERVICE_URL": None,
    # docker mode
    "GRAPER_IN_DOCKER": "false",
}


def parse_env_from_file(filepath):
    _env = {}
    if path.isfile(filepath):
        with open(filepath, "r", encoding="utf8",) as f:
            for line in f.readlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith(("#", "'", '"')):
                    continue
                k, v = line.split("=", 1)
                if v.startswith(("'", '"')):
                    v = v[1:-1]
                _env[k] = v
    return _env


# Parse from ~/.graper/.env
GLOBAL_ENV_FILE = path.join(path.expanduser("~"), ".graper/.env")
env.update(parse_env_from_file(GLOBAL_ENV_FILE))

# Parse from $(pwd)/.env
LOCAL_ENV_FILE = path.join(os.getcwd(), ".env")
env.update(parse_env_from_file(LOCAL_ENV_FILE))

for k, v in env.items():
    os.environ[k] = v
