# -*- coding:utf-8 -*-

import os
import time
import logging
import signal
from urlparse import urlparse, urljoin

from lxml import etree
import re


class BaseSpider(object):
    """
    提供一些基本的分析功能
    """

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) " \
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"

    def __init__(self, main_domain=None, domain=[], max_depth=5, keyword_list=[]):
        self.done_set = set()   # 已爬取过的set
        self.queue_set = set()  # 已经入过queue的set
        self.fail_set = set()   # 爬取失败的set
        self.max_depth = max_depth
        self.main_domain = main_domain
        self.domain_set = set()
        self.running = True
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
        检查href是否应该爬取, 检查事项: 1.域名, 2.是否爬取过, 3.爬取深度是否超限,
        4.是否是图片,css,js链接(排除jpg,png,css,js,ico后缀的),
          貌似可以用域名过滤掉, 一般cdn资源域名和内容域名不一样
        5.检测是否是合法链接, 有可能需要合成链接
        6.http://xxxxx#yyy, 应该去掉#yyy
        :param href: str, 待检查的链接
        :return:  bool, True-应该爬, False-不应该爬
                  str, 合成后的url
        """
        url, cur_depath = href
        if cur_depath > self.max_depth:
            return False, (url, cur_depath)
        if url in self.done_set:
            return False, (url, cur_depath)
        if url.find('javascript:void(') != -1:
            return False, (url, cur_depath)
        params = urlparse(url)
        d = params.netloc
        if d != "" and d not in self.domain_set:
            return False, (url, cur_depath)
        if d != "":
            return True, (self.clear_sharp(url), cur_depath)
        else:
            return True, (self.clear_sharp(urljoin(self.main_domain, url)), cur_depath)

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

    def extract_href_list(self, html_text, cur_depth):
        """
        从html里抽取href链接数组
        :param html_text: str, 待处理的html
        :param cur_depth: int, 这个url所处的深度
        :return: list: 从html里抽取出来的href列表
        """
        cur_depth += 1
        data = etree.HTML(html_text)
        return [(item, cur_depth) for item in data.xpath('//*/@href')]

    def handle_sig(self, signum, frame):
        print(';'*200)
        self.running = False
        logging.error('pid={}, got signal: {}, stopping...'.format(os.getpid(), signal.Signals(signum).name))
        signal.signal(signal.SIGINT, self.handle_sig_force)
        signal.signal(signal.SIGTERM, self.handle_sig_force)

    def handle_sig_force(self, signum, frame):
        # self.running = False
        logging.error('pid={}, got signal: {} again, forcing exit'.format(os.getpid(), signal.Signals(signum).name))
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

