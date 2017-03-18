# -*- coding:utf-8 -*-

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
        self.done_set = set()   # 已爬取过的set
        self.queue_set = set()  # 已经入过queue的set
        self.fail_set = set()   # 爬取失败的set
        self.max_depth = max_depth
        self.base_url = base_url
        self.domain_set = set()
        self.running = True
        self.html_cleaner = Cleaner()
        self.html_cleaner.javascript = True
        self.html_cleaner.style = True
        self.esm_index = esm.Index()
        db_exists = False
        if os.path.exists(db_name):
            db_exists = True
        self.db_conn = sqlite3.connect(db_name)
        if not db_exists:
            cursor = self.db_conn.cursor()
            with open('db/create_table.sql', 'r') as f:
                for sql in f.read().split(';'):
                    cursor.execute(sql)
            cursor.close()
            self.db_conn.commit()
        for keyword in keyword_list:
            self.esm_index.enter(keyword)
        self.esm_index.fix()
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        for d in domain:
            self.domain_set.add(d)

    def clear_sharp(self, sstr):
        """去掉#号及其右边的内容"""
        if sstr.find('#') != -1:
            return sstr[0: sstr.rfind('#')]     # todo: 用urldefrag代替这个
        return sstr

    def deal_failed(self, crawl_job):
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
        if url in self.done_set:
            return False, url
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

    def handle_sig(self, signum, frame):
        self.running = False
        logging.error('pid={}, got signal: {}, stopping...'.format(os.getpid(), signum))
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)

    def handle_sig_force(self, signum, frame):
        logging.error('pid={}, got signal: {} again, forcing exit'.format(os.getpid(), signum))
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

    def close(self):
        self.db_conn.close()


class CrawlJob(object):

    def __init__(self, url, depth=0, delay=0.1):
        self.url = url
        self.depth = depth
        self.failed_flag = False
        self.text = ''
        self.failed_num = 0
        self.delay = delay
        self.failed_reason = ''

