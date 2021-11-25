# coding:utf8
from typing import Optional

from httpx import Response as HttpxResponse
from graper.network.request import Request


# Inhert HttpxResponse only for IDE inspect
class Response(HttpxResponse):
    def __init__(
        self,
        response: Optional[HttpxResponse],
        request: Request,
        exception: Optional[Exception] = None,
        **kwargs,
    ):
        """

        Args:
            response: httpx.Response
            request: graper.network.requests.Request
            exception: download exception
            **kwargs:
        """
        self.request: Request = request
        self.response = response
        self.exception = exception or (
            response.graper_exception if response is not None else None
        )
        self.kwargs = kwargs

    def __repr__(self):
        if self.response is not None:
            return f"<Response [{self.status_code} {self.reason_phrase}]>"
        else:
            return f"<Response None>"

    def __getattr__(self, item):
        """
            Find self first and then find response
        Args:
            item:

        Returns:

        """
        # TODO
        # if self.response is None, print(response) will raise Exception
        return getattr(self.response, item)

    def __del__(self):
        try:
            self.response.close()
        except Exception as e:
            pass

    @staticmethod
    def response_to_file(
        response: HttpxResponse,
        file_obj,
        chunk_size=128 * 1024 * 8,
        show_progress: bool = False,
        progress_config: dict = None,
    ):
        """

        Args:
            response:
            file_obj:
            chunk_size:
            show_progress:
            progress_config:

        Returns:

        Examples:
            //
            with httpx.stream("GET", "https://www.baidu.com") as r:
                with open("test.html", "wb") as f:
                    Response.response_to_file(r, f, show_progress=True, chunk_size=1024)

            //
            r = httpx.get("https://www.baidu.com")
            with open("test.html", "wb") as f:
                Response.response_to_file(r, f)

            //
            from graper.network.downloader import Downloader

            downloader = Downloader(proxy_enable=False)

            r = downloader.download("https://www.baidu.com", stream=True)
            with open("test.html", "wb") as f:
                Response.response_to_file(r, f, show_progress=True, chunk_size=1024)
            r.close()

        """
        if not show_progress:
            response.read()
            file_obj.write(response.content)
        else:
            import tqdm

            #
            _progress_config = {
                "desc": "file download:",
            }
            if progress_config:
                _progress_config.update(progress_config)
            #
            content_length = response.headers.get("Content-Length")
            t = tqdm.tqdm(
                desc=_progress_config["desc"],
                total=int(content_length) if content_length else None,
            )
            _iter = response.iter_bytes(chunk_size)
            while 1:
                try:
                    _content = next(_iter)
                except StopIteration:
                    break
                file_obj.write(_content)
                t.update(len(_content))
            t.close()
        return
