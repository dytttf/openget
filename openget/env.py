# coding:utf8
"""
Get all environment variable

"""
import sys
import os
from os import path
from dotenv import load_dotenv, dotenv_values


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
    "OPENGET_FORBIDDED_BETTER_EXCEPTIONS": "false",
}


# from ~/.openget/.env
GLOBAL_ENV_FILE = path.join(path.expanduser("~"), ".openget/.env")
global_env = {}
if GLOBAL_ENV_FILE:
    global_env = dotenv_values(GLOBAL_ENV_FILE)
    load_dotenv(GLOBAL_ENV_FILE)

# from $(pwd)/.env
LOCAL_ENV_FILE = ""
local_env = {}
if sys.argv[0]:
    LOCAL_ENV_FILE = path.join(path.dirname(path.join(os.getcwd(), sys.argv[0])), ".env")
    if LOCAL_ENV_FILE:
        local_env = dotenv_values(LOCAL_ENV_FILE)
        load_dotenv(LOCAL_ENV_FILE, override=True)
