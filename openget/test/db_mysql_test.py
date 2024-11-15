from openget.db import DB
from openget.setting import *


def test():
    db = DB().create(default_mysql_uri, cursor_class="dict_cursor")

    tables = db.query_all("show tables;")
    print(tables)

    db.close()
    return


if __name__ == "__main__":
    test()
