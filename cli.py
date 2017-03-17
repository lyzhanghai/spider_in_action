# -*- coding:utf-8 -*-

from spider.thread_spider import ThreadSpider
from spider.tornado_spider import TornadoSpider


if __name__ == '__main__':
    import time
    t1 = time.time()
    target_url = 'http://www.jianshu.com/'
    # target_url = 'http://www.jianshu.com/contact'
    # target_url = 'http://www.jianshu.com/p/dd27230a0d95'
    # ts = ThreadSpider(concurrency=10, delay=1, max_depth=5,
    #                   keyword_list=['天文', '地理', '黑客', '亲人', '北京', '内在'], db_name='db/db.sqlite')
    ts = TornadoSpider(concurrency=10, delay=1, max_depth=5,
                      keyword_list=['天文', '地理', '黑客', '亲人', '北京', '内在'], db_name='db/db.sqlite')
    ts.run(target_url)
    print(time.time() - t1)
