# -*- coding:utf-8 -*-

"""
测试命令
python cli.py -u http://www.jianshu.com -k=爱国,敬业,友善,和谐,富强
"""

import logging
import logging.config
import sys

import click
import yaml

from spider.thread_spider import ThreadSpider
from spider.tornado_spider import TornadoSpider
from setting import logger_name


@click.command()
@click.option('--url', '-u', prompt="crawl entry", type=str, help="crawl entry")
@click.option('--depth', '-d', default=2, type=int, help="crawl depth")
@click.option('--type', '-t', default='thread', type=click.Choice(['thread', 'tornado']),
              help="spider type")
@click.option('--concurrency', '-c', default=10, type=int, help="concurrency number")
@click.option('--delay', '-e', default=1, type=float, help="spider delay time")
@click.option('--dbfile', '-f', default='db/db.sqlite', type=str, help="sqlite file path")
@click.option('--key', '-k', default='', type=str,
              help="when key in content, save to sqlite, multi word split by ','")
@click.option('--loglevel', '-l', default='1', type=click.Choice(['1', '2', '3', '4', '5']),
              help="loglevel: 1-DEUBG, 2-INFO, 3-WARN, 4-ERROR, 5-CRITICAL")
def run(url, depth, type, concurrency, delay, dbfile, key, loglevel):
    with open('logging.yaml', 'r') as f:    # 读取级别配置
        config = yaml.load(f)
        logging.config.dictConfig(config)

    logger = logging.getLogger(logger_name)
    logger.setLevel(int(loglevel) * 10)     # 设置日志级别

    logger.info('{} spider start crawl {}, depth: {}, concurrency: {}, delay: {}s, key: {}, dbfile: {}'
                .format(type, url, depth, concurrency, delay, key, dbfile))
    k_list = key.split(',')
    # sys.exit(0)
    if type == 'thread':
        ts = ThreadSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                          keyword_list=k_list, db_name=dbfile)
    else:
        ts = TornadoSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                           keyword_list=k_list, db_name=dbfile)
    ts.run(url)


if __name__ == '__main__':
    run()
