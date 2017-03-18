# -*- coding:utf-8 -*-

"""
爬虫命令入口文件

python cli.py --help
Usage: cli.py [OPTIONS]

Options:
  -u, --url TEXT               crawl entry
  -d, --depth INTEGER          crawl depth
  -t, --type [thread|tornado]  spider type
  -c, --concurrency INTEGER    concurrency number
  -e, --delay FLOAT            spider delay time
  -f, --dbfile TEXT            sqlite file path
  -k, --key TEXT               when key in content, save to sqlite, multi word
                               split by ','
  -l, --loglevel [1|2|3|4|5]   loglevel: 1-DEUBG, 2-INFO, 3-WARN, 4-ERROR,
                               5-CRITICAL
  --help                       Show this message and exit.

测试命令
python cli.py -u http://www.jianshu.com -k=爱国,敬业,友善,和谐,富强
python cli.py -u https://www.toutiao.com/ -k=黑客,数据,命运,亲情,抗争 -t tornado -d 5
"""

import logging
import logging.config
import threading
import time
import sys

import click
import yaml

from spider.thread_spider import ThreadSpider
from spider.tornado_spider import TornadoSpider
from setting import logger_name, interval_time


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
    """
    爬虫命令入口
    :param url: 爬取入口url
    :param depth: 爬取深度
    :param type: 爬虫类型，从 thread，tornado 中选一个，前者基于线程，后者基于协程
    :param concurrency: 并发数
    :param delay: 每次爬完延迟时间
    :param dbfile: sqlite 文件位置
    :param key: 关键词，多个用,隔开，若网页内容含有关键词，则存储到sqlite
    :param loglevel: 日志等级 1-DEUBG, 2-INFO, 3-WARN, 4-ERROR, 5-CRITICAL
    :return: None
    """
    # 读取并加载日志配置
    with open('logging.yaml', 'r') as f:
        config = yaml.load(f)
        logging.config.dictConfig(config)

    logger = logging.getLogger(logger_name)
    logger.setLevel(int(loglevel) * 10)  # 设置日志级别

    # 将运行参数打印到日志中
    logger.info('{} spider start crawl {}, depth: {}, concurrency: {}, delay: {}s, key: {}, dbfile: {}'
                .format(type, url, depth, concurrency, delay, key, dbfile))
    k_list = key.split(',')
    # sys.exit(0)   # 测试命令行用
    if type == 'thread':    # 根据type选择爬虫类型
        spider = ThreadSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                              keyword_list=k_list, db_name=dbfile)
    else:
        spider = TornadoSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                               keyword_list=k_list, db_name=dbfile)
    # 另启一个线程显示爬虫状态
    t = threading.Thread(target=refresh, args=(interval_time, spider))
    t.daemon = True     # 设置为后台线程
    t.start()           # 启动爬虫状态显示线程
    spider.run(url)     # 开始爬取


def refresh(interval, spider):
    """
    定期更新当前爬虫状态函数
    :param interval: 更新状态时间间隔
    :param spider: 爬虫
    :return: None
    """
    while True:
        click.clear()
        click.echo("{}: {}".format('已下载数量', len(spider.done_set)))
        click.echo("{}: {}".format('  目标数量', len(spider.queue_set)))
        click.echo("{}: {}".format('  失败数量', len(spider.fail_set)))
        click.echo("{}: {}".format('  保存数量', len(spider.save2db_set)))
        time.sleep(interval)


if __name__ == '__main__':
    run()
