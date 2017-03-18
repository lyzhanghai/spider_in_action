# -*- coding:utf-8 -*-

import logging
import traceback
from urlparse import urlparse

from tornado import httpclient, gen, queues, ioloop

from base_spider import BaseSpider, CrawlJob
from setting import logger_name

logger = logging.getLogger(logger_name)


class TornadoSpider(BaseSpider):
    def __init__(self, domain=[], concurrency=10, max_depth=5,
                 max_retries=3, delay=1, keyword_list=[], db_name='db/db.sqlite'):
        super(TornadoSpider, self).__init__(max_depth=max_depth, keyword_list=keyword_list,
                                            domain=domain, db_name=db_name)
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
            request = httpclient.HTTPRequest(url=url, method='GET', headers=self.headers, \
                                             connect_timeout=4, request_timeout=6, follow_redirects=True)
            logger.info('{} start crawl'.format(url))
            response = yield httpclient.AsyncHTTPClient().fetch(request)
            logger.info('{} end crawl'.format(url))
            # todo: 搞清楚python2的编码到底是怎么一回事
            crawl_job.text = response.body.decode('utf-8') if isinstance(response.body, str) \
                else response.body
        except Exception as e:
            crawl_job.failed_flag = True
            logger.warn('{} crawl failed, fialed num: {}'.format(crawl_job.url, crawl_job.failed_num))
            logger.exception(e)
        finally:
            if self.running:
                yield gen.sleep(self.delay)

    @gen.coroutine
    def main(self):
        q = queues.Queue()
        logger.info('TornadoSpider crawl start')

        @gen.coroutine
        def fetch_url():
            crawl_job = yield q.get()
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
                        self.done_set.remove(crawl_job.url)
                        logger.debug('{} reput to task queue'.format('url'))
                    else:
                        crawl_job.failed_reason = 'over max retries'
                        self.deal_failed(crawl_job)
                        logger.debug('{} over max retries'.format('url'))
                else:
                    cur_depth = crawl_job.depth
                    cur_depth += 1
                    self.save2db(crawl_job.url, crawl_job.text)
                    if cur_depth <= self.max_depth:
                        href_list = self.extract_href_list(crawl_job.text)
                        need_crawl_list = self.deal_href_list(href_list)
                        for i, url in enumerate(need_crawl_list):
                            if url not in self.queue_set and self.running:
                                new_job = CrawlJob(url, cur_depth, self.delay)
                                q.put(new_job)
                                self.queue_set.add(url)
                                logger.debug('{} put to task queue'.format('url'))
            except Exception as e:
                crawl_job.failed_reason = traceback.format_exc()
                logger.warn('{} analyze failed'.formt(crawl_job.url))
                self.deal_failed(crawl_job)
                logger.exception(e)
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
        logger.info('TornadoSpider crawl done')

    def run(self, url):
        self.start_url = url
        params = urlparse(url)
        d = params.netloc
        self.base_url = url  # 设置基础url
        self.domain_set.add(d)
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self.main)
        self.close()
