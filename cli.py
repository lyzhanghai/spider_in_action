# -*- coding:utf-8 -*-

"""
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
    with open('logging.yaml', 'r') as f:  # 读取级别配置
        config = yaml.load(f)
        logging.config.dictConfig(config)

    logger = logging.getLogger(logger_name)
    logger.setLevel(int(loglevel) * 10)  # 设置日志级别

    logger.info('{} spider start crawl {}, depth: {}, concurrency: {}, delay: {}s, key: {}, dbfile: {}'
                .format(type, url, depth, concurrency, delay, key, dbfile))
    k_list = key.split(',')
    # sys.exit(0)   # 测试命令行用
    if type == 'thread':
        spider = ThreadSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                              keyword_list=k_list, db_name=dbfile)
    else:
        spider = TornadoSpider(concurrency=concurrency, delay=delay, max_depth=depth,
                               keyword_list=k_list, db_name=dbfile)
    t = threading.Thread(target=refresh, args=(1, spider))
    t.daemon = True
    t.start()
    spider.run(url)


def refresh(interval, spider):
    while True:
        click.clear()
        click.echo("{}: {}".format('已下载数量', len(spider.done_set)))
        click.echo("{}: {}".format('  目标数量', len(spider.queue_set)))
        click.echo("{}: {}".format('  失败数量', len(spider.fail_set)))
        click.echo("{}: {}".format('  保存数量', len(spider.save2db_set)))
        time.sleep(interval)


if __name__ == '__main__':
    run()
