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


if sys.version < '3':
    reload(sys)
    sys.setdefaultencoding('utf-8')

class BaseSpider(object):
    """
    提供一些基本的分析功能
    """

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) " \
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"

    def __init__(self, main_domain=None, domain=[], max_depth=5, keyword_list=[], db_name='db.sqlite'):
        self.done_set = set()   # 已爬取过的set
        self.queue_set = set()  # 已经入过queue的set
        self.fail_set = set()   # 爬取失败的set
        self.max_depth = max_depth
        self.main_domain = main_domain
        self.domain_set = set()
        self.running = True
        self.db_conn = sqlite3.connect(db_name)
        self.html_cleaner = Cleaner()
        self.html_cleaner.javascript = True
        self.html_cleaner.style = True
        signal.signal(signal.SIGINT, self.handle_sig)
        signal.signal(signal.SIGTERM, self.handle_sig)
        for d in domain:
            self.domain_set.add(d)

    def clear_sharp(self, sstr):
        """去掉#号及其右边的内容"""
        # return sstr
        if sstr.find('#') != -1:
            return sstr[0: sstr.rfind('#')]
        return sstr

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
            return True, self.clear_sharp(urljoin(self.main_domain, url))

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
        # return href_list

    def extract_href_list(self, html_text):
        """
        从html里抽取href链接数组
        :param html_text: str, 待处理的html
        :return: list: 从html里抽取出来的href列表
        """
        data = etree.HTML(html_text)
        return data.xpath('//*/@href')

    def save2db(self, url, content):
        cursor = self.db_conn.cursor()
        data = etree.HTML(self.html_cleaner.clean_html(content))
        sstr = data.xpath('string(.)')
        sstr = re.sub('\s+', ' ', sstr)
        # print(sstr)
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
        # self.running = False
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
        self.next_url = []
        self.delay = delay

