# coding:utf8
import time

from openget.spiders import *
import datetime

from openget.utils import log
from bs4 import BeautifulSoup

logger = log.get_logger(__file__)


class SampleSqliteSpider(SingleBatchSpider):
    def __init__(self, **kwargs):
        super(SampleSqliteSpider, self).__init__(**kwargs)

        # 必填字段
        self.task_key = "task:local:sqlite_test_list"  # 需修改
        # 取任务表 必填
        self.task_table_name = "test_sqlite_list_task"
        self.data_table_name = "test_sqlite_data"

        # 非必填 但一般应该是需要设置的
        # 任务字段列表 建的表中必须有id字段 用于修改状态
        self.task_field_list = ["page"]

        self.debug = True
        self.downloader.proxy_enable = False

    def init_db(self):
        self.db.create(
            fields={
                "page": "int",
            },
            unique=["page"],
            table_name=self.task_table_name,
            type="task",
        )
        #
        self.db.create(
            fields={
                "url": "str",
                "name": "str",
                "gtime": "datetime",
            },
            unique=["url"],
            table_name=self.data_table_name,
            type="batch_value",
        )
        return

    def add_task(self):
        task_list = []
        for page in range(1, 3):
            task_list.append({"page": page})
        self.db.add_many(task_list, table_name=self.task_table_name)
        return

    def start_requests(self):
        while 1:
            task_obj = self.get_task(obj=True)
            if not task_obj:
                logger.debug("没有任务")
                break
            page = task_obj.page
            # 下载
            req = Request(
                {
                    "url": "https://www.cnblogs.com/AggSite/AggSitePostList",
                    "method": "POST",
                    "json": {
                        "CategoryType": "SiteHome",
                        "ParentCategoryId": 0,
                        "CategoryId": 808,
                        "PageIndex": page,
                        "TotalPostCount": 4000,
                        "ItemListActionName": "AggSitePostList",
                    },
                },
                meta={"task": task_obj},
                callback=self.parse,
            )
            yield req
            time.sleep(1)
            # break
        return

    def parse(self, response: Response):
        request = response.request
        task_obj = request.meta["task"]
        page = task_obj.page
        _response = response.response
        try:
            condition = {"page": page}
            #
            data_list = []
            gtime = datetime.datetime.now()

            soup = BeautifulSoup(response.text)
            for item in soup.select("article.post-item"):
                url = item.select_one("a.post-item-title")["href"]
                name = item.select_one("a.post-item-title").text.strip()
                data_list.append(
                    {
                        "url": url,
                        "name": name,
                        "gtime": gtime,
                    }
                )

            self.db.add_many(data_list, table_name=self.data_table_name)
            self.set_task_state(state=1, condition=condition)
            logger.debug("采集完成: {}".format(task_obj))
        except Exception as e:
            logger.exception(e)
            self.put_task(task_obj)
        return


if __name__ == "__main__":
    with SampleSqliteSpider(pool_size=10, use_sqlite=True) as my_spider:
        my_spider.init_db()
        my_spider.add_task()
        my_spider.run()
