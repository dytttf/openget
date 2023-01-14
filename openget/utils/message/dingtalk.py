#
"""
钉钉机器人
"""
from typing import Dict

import httpx


class DingTalk:
    def __init__(self, api="", message_prefix=""):
        self.api = api
        self.message_prefix = message_prefix

    def send(
        self,
        message="",
        message_type="text",
        data: Dict = None,
        message_prefix="",
        headers: Dict = None,
        markdown: Dict = None,
        api="",
        **kwargs,
    ):
        """

        Args:
            message:
            message_type: text, markdown
            data:
            message_prefix:
            headers:
            markdown:
            api:
            **kwargs:

        Returns:

        """
        if not data:
            message_prefix = message_prefix or self.message_prefix
            if markdown:
                markdown["title"] = f"{message_prefix} {markdown['title']}"
            data = {
                "msgtype": message_type,
                "text": {"content": f"{message_prefix} {message}"},
                "markdown": markdown,
            }
        headers = headers or {"Content-Type": "application/json; charset=utf-8"}
        resp = httpx.post(
            api or self.api,
            json=data,
            headers=headers,
            verify=False,
        )
        return resp.json()


if __name__ == "__main__":

    api = "https://oapi.dingtalk.com/robot/send?access_token=xxxx"

    # DingTalk(api=api, message_prefix="【爬虫项目通知】",).send(
    #     message="test",
    # )
    #
    # DingTalk().send(
    #     message="test",
    #     api=api,
    #     message_prefix="【爬虫项目通知】",
    # )
    #
    # DingTalk().send(
    #     markdown={"title": "", "text": "# aaa"},
    #     api=api,
    #     message_prefix="【爬虫项目通知】",
    #     message_type="markdown",
    # )
