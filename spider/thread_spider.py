# -*- coding:utf-8 -*-

import time
import os
from Queue import Queue, Empty
import threading
from threading import Thread
from traceback import print_exc
from urlparse import urlparse
import logging
import datetime

import requests

from base_spider import BaseSpider, CrawlJob


class Worker(Thread):

    def __init__(self, task_queue, result_queue):
        super(Worker, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True
        self.start()

    def run(self):
        while True:
            try:
                func, crawl_job = self.task_queue.get()
                func(crawl_job)
            except Exception as e:
                crawl_job.failed_flag = True
                print_exc()
            finally:
                self.result_queue.put(crawl_job)
                self.task_queue.task_done()


class ThreadSpider(BaseSpider):

    def __init__(self, main_domain=None, domain=[], concurrent=10, max_depth=5, max_retries=3, delay=1, keyword_list=[]):
        super(ThreadSpider, self).__init__(main_domain=main_domain, max_depth=max_depth,
                                           keyword_list=keyword_list, domain=domain)
        self.task_queue = Queue(concurrent)     # 发布任务用
        self.result_queue = Queue()                # 线程返回结果用
        self.session = requests.session()
        self.session.headers = {
            'user-agent': self.USER_AGENT
        }
        self.delay = delay
        self.max_retries = max_retries
        for _ in range(concurrent):
            Worker(self.task_queue, self.result_queue)

    def add_task(self, url):
        params = urlparse(target_url)
        d = params.netloc
        if self.main_domain is None:
            self.main_domain = url      # 设置主域名  todo：主域名称呼不对，若一开始初始化将主域名设置为别的，则所有合成的url都将是错的
        self.domain_set.add(d)
        crawl_job = CrawlJob(url)
        self.task_queue.put((self.crawl, crawl_job))

    def crawl(self, crawl_job):
        url = crawl_job.url
        if not self.running:
            return
        if url in self.done_set:     # 此处判断
            return
        resp = self.session.get(url, timeout=10)
        print('*=* ' * 10, url)
        self.done_set.add(url)       # 加入已爬取队列
        crawl_job.text = resp.text
        time.sleep(self.delay)

    def run(self):
        while self.running:
            if self.task_queue.empty():
                break
            crawl_job = self.result_queue.get()
            if crawl_job.failed_flag:
                if crawl_job.failed_num <= self.max_retries:
                    crawl_job.failed_num += 1
                    crawl_job.failed_flag = False
                    crawl_job.next_url = []
                    self.task_queue.put((self.crawl, crawl_job))
            else:
                cur_depth = crawl_job.depth
                cur_depth += 1
                if cur_depth <= self.max_depth:
                    href_list = self.extract_href_list(crawl_job.text)
                    need_crawl_list = self.deal_href_list(href_list)
                    for url in need_crawl_list:
                        crawl_job.next_url.append(url)
                    for i, url in enumerate(crawl_job.next_url):
                        if url not in self.queue_set:
                            print(i, '^%$', url, cur_depth, len(crawl_job.next_url), self.task_queue.maxsize)
                            new_job = CrawlJob(url, cur_depth)
                            self.task_queue.put((self.crawl, new_job))
                            self.queue_set.add(url)
                            # print(len(self.queue_set))
        self.task_queue.join()


if __name__ == '__main__':
    import time
    t1 = time.time()
    target_url = 'http://www.jianshu.com/'
    ts = ThreadSpider(concurrent=1, delay=1, max_depth=5)
    ts.add_task(target_url)
    # time.sleep(60)
    ts.run()
    print(time.time() - t1)
