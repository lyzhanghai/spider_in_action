# -*- coding:utf-8 -*-

"""
基于tornado的爬虫
"""

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
        self.max_depth = max_depth  # 爬行深度
        self.concurrency = concurrency  # 并发数
        self.max_retries = max_retries  # 最大重试次数
        self.delay = delay  # 爬完后休眠时间
        self.start_url = ''     # 入口url，在run函数中设置
        self.headers = {
            'user-agent': self.USER_AGENT
        }

    @gen.coroutine
    def crawl(self, crawl_job):
        """
        爬取crawl_job.url
        :param crawl_job: 爬取任务
        :return: None
        """
        try:
            url = crawl_job.url     # 取得url
            request = httpclient.HTTPRequest(url=url, method='GET', headers=self.headers, \
                                             connect_timeout=4, request_timeout=6, follow_redirects=True)
            logger.info('{} start crawl'.format(url))
            response = yield httpclient.AsyncHTTPClient().fetch(request)    # 爬取
            logger.info('{} end crawl'.format(url))
            # todo: 搞清楚python2的编码到底是怎么一回事
            crawl_job.text = response.body.decode('utf-8') if isinstance(response.body, str) \
                else response.body      # 保存网页内容
        except Exception as e:
            crawl_job.failed_flag = True    # 任务失败标志位置True
            logger.warn('{} crawl failed, fialed num: {}'.format(crawl_job.url, crawl_job.failed_num))
            logger.exception(e)
        finally:
            if self.running:    # 如果程序还在运行，每次爬取完毕就休眠一会
                yield gen.sleep(self.delay)

    @gen.coroutine
    def main(self):
        q = queues.Queue()
        logger.info('TornadoSpider crawl start')

        @gen.coroutine
        def fetch_url():
            crawl_job = yield q.get()   # 从队列中获得一个爬行任务
            try:
                if crawl_job.url in self.done_set:  # 如果已爬过，退出
                    return
                if not self.running:    # 如果程序停止，ctrl+c，running会变成False，以后的爬取任务在这里忽略
                    return

                yield self.crawl(crawl_job)     # 爬取
                self.done_set.add(crawl_job.url)    # 把url存入完成url集合中

                if crawl_job.failed_flag:   # 任务失败
                    if crawl_job.failed_num <= self.max_retries:    # 小于最大重试次数
                        crawl_job.failed_num += 1   # 重试次数+1
                        crawl_job.failed_flag = False   # 重置失败标志
                        q.put(crawl_job)    # 重新布置这个爬行任务
                        self.done_set.remove(crawl_job.url)     # 从已下载集合中移除这个url
                        logger.debug('{} reput to task queue'.format(crawl_job.url))
                    else:
                        crawl_job.failed_reason = 'over max retries'
                        self.deal_failed(crawl_job)     # 处理超过最大重试次数的重试任务
                        logger.debug('{} over max retries'.format(crawl_job.url))
                else:
                    cur_depth = crawl_job.depth     # 当前深度
                    cur_depth += 1      # 下一级深度
                    self.save2db(crawl_job.url, crawl_job.text)     # 处理内容
                    if cur_depth <= self.max_depth:     # 如果当前深度不大于最大爬取深度
                        href_list = self.extract_href_list(crawl_job.text)  # 抽取所有href
                        need_crawl_list = self.deal_href_list(href_list)    # 过滤，合成新url
                        for i, url in enumerate(need_crawl_list):
                            if url not in self.queue_set and self.running:
                                new_job = CrawlJob(url, cur_depth, self.delay)
                                q.put(new_job)      # 将新爬行任务放入队列中
                                self.queue_set.add(url)     # 将新爬行url放入集合中
                                logger.debug('{} put to task queue'.format('url'))
            except Exception as e:
                crawl_job.failed_reason = traceback.format_exc()    # 记录失败原因
                logger.warn('{} analyze failed'.formt(crawl_job.url))
                self.deal_failed(crawl_job)     # 处理失败任务（存入数据库）
                logger.exception(e)
            finally:
                q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        crawl_job = CrawlJob(self.start_url, delay=self.delay)  # 第一个爬行任务
        self.queue_set.add(self.start_url)      # 将新爬行url放入集合中
        q.put(crawl_job)    # 将异地个爬行任务放入队列中
        for _ in range(self.concurrency):     # 启动并发协程
            worker()

        yield q.join()  # 等待queue中的任务运行完
        logger.info('TornadoSpider crawl done')

    def run(self, url):
        """
        开始爬取
        :param url: 入口url
        :return: None
        """
        self.start_url = url
        params = urlparse(url)
        d = params.netloc     # 解析域名，例如www.jianshu.com
        self.base_url = url  # 设置基础url
        self.domain_set.add(d)  # 将域名加入过滤集合中
        io_loop = ioloop.IOLoop.current()   # 取得时间循环
        io_loop.run_sync(self.main)     # 运行时间循环
        self.close()        # 关闭数据库连接
