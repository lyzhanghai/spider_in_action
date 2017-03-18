# -*- coding:utf-8 -*-

import logging
import logging.config

import click
import yaml

from spider.thread_spider import ThreadSpider
from spider.tornado_spider import TornadoSpider
from setting import logger_name


@click.command()
@click.option('--loglevel', '-l', default='1', type=click.Choice(['1', '2', '3', '4', '5']),
              help="loglevel: 1-DEUBG, 2-INFO, 3-WARN, 4-ERROR, 5-CRITICAL")
def run(loglevel):
    with open('logging.yaml', 'r') as f:    # 读取级别配置
        config = yaml.load(f)
        logging.config.dictConfig(config)

    logger = logging.getLogger(logger_name)
    logger.setLevel(int(loglevel) * 10)     # 设置日志级别

    target_url = 'http://www.jianshu.com/'
    ts = ThreadSpider(concurrency=10, delay=1, max_depth=5,
                      keyword_list=['天文', '地理', '黑客', '亲人', '北京', '内在'], db_name='db/db.sqlite')
    ts.run(target_url)


if __name__ == '__main__':
    run()
