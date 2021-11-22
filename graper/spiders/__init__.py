# coding:utf8
"""
"""
# 貌似这里有内存泄漏问题
from gevent import monkey  # isort:skip

# tips  注意 如果 patch了subprocess 会导致 os.popen特别慢...
monkey.patch_all(os=False, subprocess=False, signal=False)  # isort:skip

from graper.network.request import Request
from graper.network.response import Response

from .base import Spider
from .batch_spider import BatchSpider, JsonTask, SingleBatchSpider  # noqa

__all__ = [
    "BatchSpider",
    "Spider",
    "Request",
    "Response",
    "SingleBatchSpider",
    "JsonTask",
]
