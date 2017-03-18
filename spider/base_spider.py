# -*- coding:utf-8 -*-

"""
爬虫基类
定义了一些公用的处理方法
"""

import os
import time
import logging
import signal
from urlparse import urlparse, urljoin
import re
import sqlite3
import sys

from lxml import etree
from lxml.html.clean import Cleaner
import esm

from setting import logger_name

logger = logging.getLogger(logger_name)

if sys.version < '3':
    reload(sys)
    sys.setdefaultencoding('utf-8')


class BaseSpider(object):
    """
    提供一些基本的分析功能
    """

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) " \
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"

    def __init__(self, base_url=None, domain=[], max_depth=5, keyword_list=[], db_name='db/db.sqlite'):
        self.done_set = set()   # 已爬取过的url集合
        self.queue_set = set()  # 已经入过queue的url集合
        self.fail_set = set()   # 爬取失败的url集合
        self.save2db_set = set()   # 存入数据库的url集合
        self.max_depth = max_depth  # 最大爬行深度
        self.base_url = base_url    # 用于合成的基本url
        self.domain_set = set()     # 用于过滤的域名集合
        self.running = True         # 正在运行标志
        self.html_cleaner = Cleaner()   # 用于清洗js,css
        self.html_cleaner.javascript = True
        self.html_cleaner.style = True
        self.esm_index = esm.Index()    # 用于在文本中快速查找多个关键词
        db_exists = False
        if os.path.exists(db_name):     # 判断db是否存在
            db_exists = True
        self.db_conn = sqlite3.connect(db_name)
        if not db_exists:       # db不存在则创建数据库并创建表
            cursor = self.db_conn.cursor()
            with open('db/create_table.sql', 'r') as f:
                for sql in f.read().split(';'):
                    cursor.execute(sql)
            cursor.close()
            self.db_conn.commit()
        for keyword in keyword_list:    # 查询器中添加关键词
            self.esm_index.enter(keyword)
        self.esm_index.fix()    # 变异查询器
        signal.signal(signal.SIGINT, self.handle_sig)   # 劫持ctrl+c你好
        signal.signal(signal.SIGTERM, self.handle_sig)
        for d in domain:    # 若有其他需要爬取的域名，在这里加入domain_set
            self.domain_set.add(d)

    def clear_sharp(self, sstr):
        """去掉#号及其右边的内容"""
        if sstr.find('#') != -1:
            return sstr[0: sstr.rfind('#')]     # todo: 用urldefrag代替这个
        return sstr

    def deal_failed(self, crawl_job):
        """将失败信息存入数据库"""
        self.fail_set.add(crawl_job.url)
        cursor = self.db_conn.cursor()
        sql = 'insert into failed_url (url, content, failed_reason) values (?, ?, ?)'
        failed_reason = crawl_job.failed_reason if isinstance(crawl_job.failed_reason, str) \
                else crawl_job.failed_reason.decode()
        cursor.execute(sql, (crawl_job.url, crawl_job.text, failed_reason))
        cursor.close()
        self.db_conn.commit()

    def need_crawl(self, href):
        """
        检查href是否应该爬取, 检查事项: 1.域名
        2.是否是图片,css,js链接(排除jpg,png,css,js,ico后缀的),
          貌似可以用域名过滤掉, 一般cdn资源域名和内容域名不一样
        3.检测是否是合法链接, 有可能需要合成链接
        4.http://xxxxx#yyy, 应该去掉#yyy
        :param href: str, 待检查的链接
        :return:  bool, True-应该爬, False-不应该爬
                  str, 合成后的url
        """
        url = href
        if url.find('javascript:void(') != -1:
            return False, url
        params = urlparse(url)
        d = params.netloc
        if d != "" and d not in self.domain_set:
            return False, url
        if d != "":
            return True, self.clear_sharp(url)
        else:
            return True, self.clear_sharp(urljoin(self.base_url, url))

    def deal_href_list(self, href_list):
        """
        处理从正文里抽取出来的html链接数组, 检查是否该爬
        :param href_list: list, 待处理的链接数组
        :return: set: 应该爬取的set
        """
        href_list_trans = set()
        for href in href_list:
            b, url = self.need_crawl(href)
            if b:
                href_list_trans.add(url)
        return href_list_trans

    def extract_href_list(self, html_text):
        """
        从html里抽取href链接数组
        :param html_text: str, 待处理的html
        :return: list: 从html里抽取出来的href列表
        """
        data = etree.HTML(html_text)
        return data.xpath('//*/@href')

    def save2db(self, url, content):
        data = etree.HTML(self.html_cleaner.clean_html(content))
        sstr = data.xpath('string(.)')
        sstr = re.sub('\s+', ' ', sstr)
        query_result = self.esm_index.query(sstr)
        if len(query_result) > 0:
            cursor = self.db_conn.cursor()
            # print(sstr)
            # print(query_result)
            temp_dict = {}
            for item in query_result:
                keyword = item[-1].decode('utf-8')
                temp_dict[keyword] = url    # 用字典去重
            for k, v in temp_dict.items():
                sql = 'insert into keyword2url (keyword, url) values (?, ?)'
                cursor.execute(sql, (k, v))
            sql = 'insert into content (url, content) values (?, ?)'
            cursor.execute(sql, (url, sstr))
            cursor.close()
            self.db_conn.commit()
            self.save2db_set.add(url)
            return True
        return False

    def handle_sig(self, signum, frame):
        """第一次ctrl+c处理函数：等待正在运行的任务执行完毕后结束运行"""
        self.running = False
        logger.error('pid={}, got signal: {}, stopping...'.format(os.getpid(), signum))
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)

    def handle_sig_force(self, signum, frame):
        """第二次ctrl+c处理函数：强制结束所有线程"""
        logger.error('pid={}, got signal: {} again, forcing exit'.format(os.getpid(), signum))
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

    def close(self):
        """关闭数据库链接"""
        self.db_conn.close()


class CrawlJob(object):
    """
    爬行任务，这个类含有一个爬行任务的所有内容
    """

    def __init__(self, url, depth=0, delay=0.1):
        self.url = url
        self.depth = depth
        self.failed_flag = False
        self.text = ''
        self.failed_num = 0
        self.delay = delay
        self.failed_reason = ''

