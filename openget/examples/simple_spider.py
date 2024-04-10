from openget.spiders import *
from openget.utils import log

logger = log.get_logger(__name__)


class SimpleSpider(Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.downloader.proxy_enable = False

    def start_requests(self):
        for i in range(10):
            yield Request("http://httpbin.org/ip")

    def parse(self, response: Response):
        logger.debug(response.json())


if __name__ == "__main__":
    with SimpleSpider(pool_size=100) as my_spider:
        my_spider.run()
