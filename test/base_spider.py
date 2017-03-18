# -*- coding:utf-8 -*-

"""
测试BaseSpider
"""

import unittest
from spider.base_spider import BaseSpider


class TestBaseSpider(unittest.TestCase):

    def setUp(self):
        self.bs = BaseSpider(domain=['www.jianshu.com'])

    def test_clear_sharp(self):
        clear_url = self.bs.clear_sharp('http://www.jianshu.com/p/65c26fc2f5ad#comment')
        assert clear_url == 'http://www.jianshu.com/p/65c26fc2f5ad'

    def test_extract_href_list(self):
        with open('test.html', 'r') as f:
            content = f.read()
        href_list = self.bs.extract_href_list(content)
        assert len(href_list) == 55

    def test_deal_href_list(self):
        with open('test.html', 'r') as f:
            content = f.read()
        href_list = self.bs.extract_href_list(content)
        href_list = self.bs.deal_href_list(href_list)
        assert len(href_list) == 31

    def tearDown(self):
        self.bs.close()
