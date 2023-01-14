# coding:utf8
from typing import Dict, Union, Callable


class Request(object):
    def __init__(
        self,
        request: Union[Dict, str],
        callback: Callable = None,
        meta: dict = None,
        downloader=None,
        **kwargs
    ):
        """
        Args:
            request: url or  {"url": "", "headers": {}},
            callback:
            meta:
            downloader:
            **kwargs:
        """
        self.request = request
        self.callback = callback
        self.meta = meta
        #
        self.retry = 0
        #
        self.downloader = downloader

        # convenient attr
        self.url = request
        self.data = None
        if isinstance(request, dict):
            self.url = request.get("url")
            self.data = request.get("data")
