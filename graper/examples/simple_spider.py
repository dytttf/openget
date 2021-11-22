from graper.spiders import *


class SimpleSpider(Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # self.downloader.proxy_enable = False

    def start_requests(self):
        for i in range(100):
            yield Request("http://httpbin.org/ip")

    def parse(self, response: Response):
        print(response.json())


if __name__ == "__main__":
    with SimpleSpider(pool_size=100) as my_spider:
        my_spider.run()
