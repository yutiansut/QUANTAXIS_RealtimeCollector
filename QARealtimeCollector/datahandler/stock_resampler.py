# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    stockResampler.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/03/03 22:19:37 by zhongjy1992       #+#    #+#              #
#    Updated: 2020/03/06 14:06:35 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import datetime
import json
import logging
import multiprocessing
import os
import threading
import time

import click
import pandas as pd
from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread

from utils.common import create_empty_stock_df, tdx_stock_bar_resample_parallel, util_is_trade_time, \
    get_file_name_by_date, logging_csv

logger = logging.getLogger(__name__)


class QARTCStockBarResampler(QA_Thread):
    """
        应启动一个线程单独重采样1min的数据，然后按需求根据1min的数据重新采样为多周期数据
        若有一个内存数据库，则可以把数据先写入数据库，然后再根据订阅读取进行拉取(redis, mongo？)
    """

    def __init__(self, frequency='5min', date: datetime.datetime = None, log_dir='./log'):
        """
        暂时不支持单个股票重采样
        :param frequency:
        """
        super().__init__()
        logger.info("QA实时股票Bar重采样,初始化...周期: %s" % frequency)
        if isinstance(frequency, float):
            self.frequency = int(frequency)
        elif isinstance(frequency, str):
            _frequency = frequency.replace('min', '')
            if str.isnumeric(_frequency):
                self.frequency = int(_frequency)
            else:
                logger.error("不支持的周期 unknownFrequency: %s" % frequency)
                return
        elif isinstance(frequency, int):
            self.frequency = frequency
        else:
            logger.error("不支持的周期 unknownFrequency: %s" % frequency)
            return

        self.market_data = None

        # 接收stock tick 数据
        self.sub = subscriber(
            host=eventmq_ip, exchange='realtime_stock_min')
        self.sub.callback = self.on_message_callback
        self.stock_sub = subscriber_routing(host=eventmq_ip, exchange='QARealtime_Market', routing_key='stock')
        self.stock_sub.callback = self.on_stock_subscribe_message_callback
        # 发送重采样的数据
        self.pub = publisher(host=eventmq_ip, exchange='realtime_stock_{}_min'.format(self.frequency))
        self.count = 0
        self.code_list = []
        cur_time = datetime.datetime.now() if date is None else date
        self.cur_year = cur_time.year
        self.cur_month = cur_time.month
        self.cur_day = cur_time.day
        # 多进程计算
        self.cpu_count = multiprocessing.cpu_count() - 1
        self.log_dir = log_dir
        threading.Thread(target=self.sub.start, daemon=True).start()
        threading.Thread(target=self.stock_sub.start, daemon=True).start()


    def publish_msg(self, text):
        self.pub.pub(text)

    def on_stock_subscribe_message_callback(self, channel, method, properties, data):
        data = json.loads(data)
        if data['topic'].lower() == 'subscribe':
            logger.info('股票重采样,新的订阅: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.subscribe_callback(item)
            else:
                self.subscribe_callback(new_ins)
        if data['topic'].lower() == 'unsubscribe':
            logger.info('股票重采样,取消订阅: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.unsubscribe_callback(item)
            else:
                self.unsubscribe_callback(new_ins)

    def subscribe_callback(self, code):
        if code not in self.code_list:
            self.code_list.append(code)
            # initial time series data
            # date=datetime.datetime(2019, 5, 9)
            self.market_data = pd.concat([
                self.market_data, create_empty_stock_df(code, date=datetime.datetime(self.cur_year, self.cur_month,
                                                                                     self.cur_day))
            ])
            logger.info("当日数据初始化中,%s" % code)
        pass

    def unsubscribe_callback(self, item):
        # remove code from market data
        pass

    def on_message_callback(self, channel, method, properties, body):
        context = pd.read_msgpack(body)
        # merge update
        if self.market_data is None:
            # self.market_data = context
            pass
        else:
            logger.info("Before market_data, concat and update start, 合并市场数据")
            cur_time = datetime.datetime.now()
            self.market_data.update(context)
            end_time = datetime.datetime.now()
            cost_time = (end_time - cur_time).total_seconds()
            logger.info("Before market_data, concat and update end, 合并市场数据, 耗时,cost: %s s" % cost_time)
            logger.info(self.market_data.to_csv(float_format='%.3f'))
            filename = get_file_name_by_date('stock.market.%s.csv', self.log_dir)
            # 不追加，复写
            logging_csv(self.market_data, filename, index=True, mode='w')

        # group by code and resample
        try:
            cur_time = datetime.datetime.now()
            bar_data: pd.DataFrame = tdx_stock_bar_resample_parallel(
                self.market_data[self.market_data.close > 0], self.frequency, jobs=self.cpu_count
            )
            end_time = datetime.datetime.now()
            cost_time = (end_time - cur_time).total_seconds()
            logger.info("数据重采样耗时,cost: %s" % cost_time)
            logger.info("发送重采样数据中start")
            self.publish_msg(bar_data.to_msgpack())
            logger.info("发送重采样数据完毕end")

            logger.info(bar_data.to_csv(float_format='%.3f'))
            filename = get_file_name_by_date('stock.bar.%s.csv', self.log_dir)
            # 不追加，复写
            logging_csv(bar_data, filename, index=True, mode='w')
            del bar_data
        except Exception as e:
            logger.error("failure股票重采样数据. " + e.__str__())
        finally:
            logger.info("重采样计数 count : %s" % self.count)
        self.count += 1
        del context

    def run(self):
        while True:
            # 9:15 - 11:31 and 12：58 - 15:00 获取
            cur_time = datetime.datetime.now()
            if util_is_trade_time(cur_time):  # 如果在交易时间
                time.sleep(0.2)
            else:
                time.sleep(1)


@click.command()
# @click.argument()
@click.option('-F', '--frequency', default='5min', help='calculate frequency', type=click.STRING)
@click.option('-log', '--logfile', help="log file path", type=click.Path(exists=False))
@click.option('-log_dir', '--log_dir', help="log path", type=click.Path(exists=False))
def main(frequency: str, logfile: str = None, log_dir: str = None):
    try:
        from utils.logconf import update_log_file_config
        logfile = 'stock.resample.log' if logfile is None else logfile
        logging.config.dictConfig(update_log_file_config(logfile))
    except Exception as e:
        print(e.__str__())
    # TODO suuport codelist file
    QARTCStockBarResampler(frequency=frequency, log_dir=log_dir.replace('~', os.path.expanduser('~'))).run()


if __name__ == '__main__':
    main()
