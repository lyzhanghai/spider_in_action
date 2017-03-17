# -*- coding:utf-8 -*-

import traceback
from urlparse import urlparse

from tornado import httpclient, gen, queues, ioloop
from base_spider import BaseSpider, CrawlJob


class TornadoSpider(BaseSpider):
    def __init__(self, main_domain=None, domain=[], concurrency=10, max_depth=5,
                 max_retries=3, delay=1, keyword_list=[], db_name='db/db.sqlite'):
        super(TornadoSpider, self).__init__(main_domain=main_domain, max_depth=max_depth,
                                            keyword_list=keyword_list, domain=domain, db_name=db_name)
        self.max_depth = max_depth
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.delay = delay
        self.start_url = ''
        self.headers = {
            'user-agent': self.USER_AGENT
        }

    @gen.coroutine
    def crawl(self, crawl_job):
        try:
            url = crawl_job.url
            response = yield httpclient.AsyncHTTPClient().fetch(url, request_timeout=10,
                                                                headers=self.headers)    # todo: session headers
            # print(type(response.body))
            # todo: 搞清楚python2的编码到底是怎么一回事
            crawl_job.text = response.body.decode('utf-8') if isinstance(response.body, str) \
                else response.body
            print('=-= ' * 5, url, len(crawl_job.text))
        except Exception as e:
            crawl_job.failed_flag = True
            print(crawl_job.url)
            traceback.print_exc()
        finally:
            if self.running:
                print('hahah')
                yield gen.sleep(self.delay)

    @gen.coroutine
    def main(self):
        q = queues.Queue()

        @gen.coroutine
        def fetch_url():
            crawl_job = yield q.get()
            # print(crawl_job)
            try:
                if crawl_job.url in self.done_set:
                    return
                if not self.running:
                    return

                yield self.crawl(crawl_job)
                self.done_set.add(crawl_job.url)

                if crawl_job.failed_flag:
                    if crawl_job.failed_num <= self.max_retries:
                        crawl_job.failed_num += 1
                        crawl_job.failed_flag = False
                        crawl_job.next_url = []
                        q.put(crawl_job)
                    else:
                        crawl_job.failed_reason = 'over max retries'
                        self.deal_failed(crawl_job)
                else:
                    cur_depth = crawl_job.depth
                    cur_depth += 1
                    self.save2db(crawl_job.url, crawl_job.text)
                    if cur_depth <= self.max_depth:
                        href_list = self.extract_href_list(crawl_job.text)
                        need_crawl_list = self.deal_href_list(href_list)
                        for i, url in enumerate(need_crawl_list):
                            if url not in self.queue_set and self.running:
                                # print(i, '^%$', url, cur_depth, len(need_crawl_list), q.qsize())
                                new_job = CrawlJob(url, cur_depth, self.delay)
                                q.put(new_job)
                                self.queue_set.add(url)
            except Exception as e:
                print(crawl_job)
                print('abc '*5, crawl_job.url, crawl_job.text)
                traceback.print_exc()
                crawl_job.failed_reason = traceback.format_exc()
                self.deal_failed(crawl_job)

            finally:
                q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        crawl_job = CrawlJob(self.start_url, delay=self.delay)
        self.queue_set.add(self.start_url)
        q.put(crawl_job)
        for _ in range(self.concurrency):
            worker()
        yield q.join()

    def run(self, url):
        self.start_url = url
        params = urlparse(url)
        d = params.netloc
        if self.main_domain is None:
            # todo: main_domain => base_url
            self.main_domain = url  # 设置主域名  todo：主域名称呼不对，若一开始初始化将主域名设置为别的，则所有合成的url都将是错的
        self.domain_set.add(d)
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self.main)
        self.close()
