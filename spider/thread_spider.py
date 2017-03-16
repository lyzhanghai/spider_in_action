# -*- coding:utf-8 -*-

import time
from Queue import Queue
from threading import Thread
from traceback import print_exc
from urlparse import urlparse

import requests

from base_spider import BaseSpider


class Worker(Thread):

    def __init__(self, spider):
        super(Worker, self).__init__()
        self.spider = spider
        self.daemon = True
        self.start()

    def run(self):
        while True:
            url, cur_depth = self.spider.queue_for_get.get()
            try:
                if not self.spider.running:
                    continue
                if url in self.spider.done_set:     # 极有可能重复入队列，此处判断
                    continue
                resp = self.spider.session.get(url, timeout=10)
                print('*=* ' * 10, url)
                self.spider.done_set.add(url)       # 加入已爬取队列
                time.sleep(self.spider.delay)
                href_list = self.spider.extract_href_list(resp.text, cur_depth)
                need_crawl_list = self.spider.deal_href_list(href_list)
                for item in need_crawl_list:
                    href, h_depth = item
                    if href not in self.spider.queue_set:
                        print(item)
                        self.spider.queue_set.add(href)     # 解决重复入队列问题
                        self.spider.queue_for_put.put((href, h_depth))
            except Exception as e:
                self.spider.fail_set.add(url)
                print(self.spider.fail_set)
                print_exc()
            finally:
                self.spider.queue_for_get.task_done()


class ThreadSpider(BaseSpider):

    def __init__(self, main_domain=None, domain=[], concurrent=10, max_depth=5, delay=1, keyword_list=[]):
        super(ThreadSpider, self).__init__(main_domain=main_domain, max_depth=max_depth,
                                           keyword_list=keyword_list, domain=domain)
        self.queue_for_get = Queue(concurrent)
        self.queue_for_put = Queue()
        self.session = requests.session()
        self.session.headers = {
            'user-agent': self.USER_AGENT
        }
        self.delay = delay
        for _ in range(concurrent):
            Worker(self)

    def add_task(self, url):
        params = urlparse(target_url)
        d = params.netloc
        if self.main_domain is None:
            self.main_domain = url
        self.domain_set.add(d)
        self.queue_for_get.put((url, 0))

    def wait_completion(self):
        while self.running:
            url, cur_depth = self.queue_for_put.get()
            self.queue_for_get.put((url, cur_depth))
        self.queue_for_get.join()   # todo: ctrl+c 终止不了此处的等待


if __name__ == '__main__':
    import time
    t1 = time.time()
    target_url = 'http://www.jianshu.com/'
    ts = ThreadSpider(concurrent=1)
    ts.add_task(target_url)
    # time.sleep(60)
    ts.wait_completion()
    print(time.time() - t1)
