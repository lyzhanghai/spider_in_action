# -*- coding:utf-8 -*-

"""
基于thread的爬虫
"""

import logging
import time
from Queue import Queue, Empty
from threading import Thread
from urlparse import urlparse
import logging
import traceback

import requests

from base_spider import BaseSpider, CrawlJob
from setting import logger_name

logger = logging.getLogger(logger_name)


class Worker(Thread):
    """
    爬行线程
    """

    def __init__(self, task_queue, result_queue):
        super(Worker, self).__init__()
        self.task_queue = task_queue    # 爬行任务队列，通过这个队列将下载任务传到后台线程
        self.result_queue = result_queue    # 结果队列，通过这个队列将下载结果传回主线程
        self.daemon = True  # 设置为后台线程
        self.start()    # 启动线程

    def run(self):
        while True:
            try:
                func, crawl_job = self.task_queue.get()     # 取得处理函数与爬取任务
                func(crawl_job)     # 爬取
            except Exception as e:
                crawl_job.failed_flag = True    # 若失败，将失败标志置True
                logger.warn('{} crawl failed, fialed num: {}'.format(crawl_job.url, crawl_job.failed_num))
                logger.exception(e)
            finally:
                self.result_queue.put(crawl_job)    # 将爬取结果传回主线程
                self.task_queue.task_done()
                time.sleep(crawl_job.delay)    # 每次爬取完毕就休眠一会


class ThreadSpider(BaseSpider):

    def __init__(self, base_url=None, domain=[], concurrency=10, max_depth=5,
                 max_retries=3, delay=1, keyword_list=[], db_name='db/db.sqlite'):
        super(ThreadSpider, self).__init__(max_depth=max_depth, keyword_list=keyword_list,
                                           domain=domain, db_name=db_name)
        self.task_queue = Queue(concurrency)     # 发布任务用
        self.result_queue = Queue()                # 线程返回结果用
        self.session = requests.session()       # 初始化一个session
        self.session.headers = {
            'user-agent': self.USER_AGENT
        }
        self.delay = delay      # 爬取完毕延时间隔
        self.max_retries = max_retries  # 最大尝试次数
        for _ in range(concurrency):    # 启动线程池
            Worker(self.task_queue, self.result_queue)

    def crawl(self, crawl_job):
        url = crawl_job.url     # 获得爬取url
        if not self.running:    # 程序停止运行，直接返回
            return
        if url in self.done_set:     # 已爬取过，直接返回
            return
        logger.info('{} start crawl'.format(url))
        resp = self.session.get(url, timeout=10)    # 爬取
        logger.info('{} end crawl'.format(url))
        self.done_set.add(url)       # 加入已爬取队列
        crawl_job.text = resp.text  # 加载内容

    def run(self, url):
        params = urlparse(url)
        d = params.netloc       # 解析出基础
        self.base_url = url      # 设置基础url
        self.domain_set.add(d)
        crawl_job = CrawlJob(url, delay=self.delay)
        self.queue_set.add(url)
        self.task_queue.put((self.crawl, crawl_job))
        logger.info('ThreadSpider crawl start')
        time.sleep(self.delay)      # 防止接下来没取到结果直接退出
        while self.running:
            try:
                crawl_job = self.result_queue.get(timeout=self.delay)   # 获得爬取任务结果
                if crawl_job.failed_flag:   # 任务失败
                    if crawl_job.failed_num <= self.max_retries:    # 小于最大重试次数
                        crawl_job.failed_num += 1   # 重试次数+1
                        crawl_job.failed_flag = False   # 重置失败标志
                        self.task_queue.put((self.crawl, crawl_job))    # 重新布置这个爬行任务
                        logger.debug('{} reput to task queue'.format('url'))
                    else:
                        crawl_job.failed_reason = 'over max retries'
                        self.deal_failed(crawl_job)      # 处理超过最大重试次数的重试任务
                        logger.debug('{} over max retries'.format('url'))
                else:
                    cur_depth = crawl_job.depth     # 当前深度
                    cur_depth += 1      # 下一级深度
                    self.save2db(crawl_job.url, crawl_job.text)
                    if cur_depth <= self.max_depth:     # 如果当前深度不大于最大爬取深度
                        href_list = self.extract_href_list(crawl_job.text)  # 抽取所有href
                        need_crawl_list = self.deal_href_list(href_list)    # 过滤，合成新url
                        for i, url in enumerate(need_crawl_list):
                            if url not in self.queue_set and self.running:
                                new_job = CrawlJob(url, cur_depth, self.delay)
                                self.task_queue.put((self.crawl, new_job))      # 将新爬行任务放入队列中
                                self.queue_set.add(url)      # 将新爬行url放入集合中
                                logger.debug('{} put to task queue'.format('url'))
            except Empty as e1:
                # 没任务，raise Empty异常
                logger.exception(e1)
                time.sleep(self.delay)
                if self.task_queue.empty():     # 再次确认没有爬行任务
                    logger.info('ThreadSpider crawl done')
                    break
            except Exception as e:
                logger.warn('{} analyze failed'.formt(crawl_job.url))
                logger.exception(e)
                crawl_job.failed_reason = traceback.format_exc()    # 记录错误原因
                self.deal_failed(crawl_job)     # 处理失败任务（保存到数据库中）

        # self.task_queue.join()    # 这句话会阻塞主线程，导致ctrl+c无法退出
        self.close()

