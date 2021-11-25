# coding:utf8
"""
"""
# may be memory leak
from gevent import monkey  # isort:skip # noqa

# tips if patch subprocess, os.popen will slow
monkey.patch_all(os=False, subprocess=False, signal=False)  # isort:skip # noqa

from graper.network.request import Request
from graper.network.response import Response

from .base import Spider
from .batch_spider import BatchSpider, JsonTask, SingleBatchSpider

__all__ = [
    "BatchSpider",
    "Spider",
    "Request",
    "Response",
    "SingleBatchSpider",
    "JsonTask",
]
