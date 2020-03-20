# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    stockBarCollector.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/10/01 22:07:05 by zhongjy1992       #+#    #+#              #
#    Updated: 2020/03/07 13:10:45 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import datetime
import json
import logging
import os
import threading
import time

import click
from QAPUBSUB.consumer import subscriber_routing
from QAPUBSUB.producer import publisher
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_min_adv, QA_fetch_stock_day_adv, QA_fetch_index_day_adv
from QUANTAXIS.QAUtil.QADate_trade import QA_util_get_pre_trade_date
from pandas import concat, DataFrame, DatetimeIndex

from QARealtimeCollector.utils.QATdx_adv import QA_Tdx_Executor
# from utils.TdxAdv import QA_Tdx_Executor
from QARealtimeCollector.utils.common import util_is_trade_time, get_file_name_by_date, logging_csv

logger = logging.getLogger(__name__)


class QARTCStockBar(QA_Tdx_Executor):
    # TODO tdx的问题请自行修正，此处只是给出一个分钟bar的采集分发重采样的思路
    # TODO 股票订阅请按文档中说明进行http请求
    def __init__(self, delay=10.5, date: datetime.datetime = None, log_dir='./log', debug=False):
        super().__init__(name='QA_REALTIME_COLLECTOR_STOCK_BAR', thread_num=None, timeout=0.5)
        cur_time = datetime.datetime.now() if date is None else date
        # set qa_tdx_excutor is debug mode
        self.debug = debug
        self.cur_year = cur_time.year
        self.cur_month = cur_time.month
        self.cur_day = cur_time.day
        self.isRequesting = False
        self.delay = delay  # 数据获取请求间隔
        self.code_list = []
        self.sub = subscriber_routing(host=eventmq_ip, exchange='QARealtime_Market', routing_key='stock')
        self.sub.callback = self.callback
        self.pub = publisher(host=eventmq_ip, exchange='realtime_stock_min')
        self.log_dir = log_dir
        self.pre_market_data = None
        self.last_update_time = cur_time
        threading.Thread(target=self.sub.start, daemon=True).start()
        logger.info("QA_REALTIME_COLLECTOR_STOCK_BAR INIT, delay %s" % self.delay)

    def subscribe_callback(self, code):
        """
        订阅回调
        :param code:
        :return:
        """
        if not isinstance(code, str):
            logger.error('not string , %s' % code)
            return
        today = datetime.datetime(self.cur_year, self.cur_month, self.cur_day).isoformat()[:10]
        end_date = QA_util_get_pre_trade_date(cursor_date=today, n=1)[:10]
        if code not in self.code_list:
            self.code_list.append(code)
            # ETF or Stock, 获取前天的收盘价格
            logger.info("try fetch %s ,%s" % (code, end_date))
            if code.startswith('5') or code.startswith('1'):
                _data = QA_fetch_index_day_adv(code, end_date, end_date)
            else:
                _data = QA_fetch_stock_day_adv(code, end_date, end_date)
            if _data is not None:
                self.pre_market_data = concat([self.pre_market_data, _data.data.reset_index()])
                logger.info("fetch %s" % _data.data.to_csv(header=False))
        # initial data from server
        # self.get_history_data(code, frequency="1min")

    def unsubscribe_callback(self, code):
        """
        取消订阅回调
        :param code:
        :return:
        """
        self.code_list.remove(code)

    def publish_msg(self, msg):
        self.pub.pub(msg)

    def callback(self, a, b, c, data):
        """
        监听订阅信息的回调处理
        :param a:
        :param b:
        :param c:
        :param data:
        :return:
        """
        data = json.loads(data)
        if data['topic'].lower() == 'subscribe':
            logger.info('stock bar collector service receive new subscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.subscribe_callback(item)
            else:
                self.subscribe_callback(new_ins)
        elif data['topic'].lower() == 'unsubscribe':
            logger.info('stock bar collector service receive new unsubscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.unsubscribe_callback(item)
            else:
                self.unsubscribe_callback(new_ins)

    def get_data(self, frequency="1min", lens=5):
        """
        调用tdx获取数据
        :param frequency:
        :param lens: increasing data len , default: 获取当前及上一bar
        :return:
        """
        cur_time = datetime.datetime.now()
        data = self.get_security_bar_concurrent(self.code_list, frequency, lens)
        if len(data) > 0:
            self.last_update_time = datetime.datetime.now()
        end_time = datetime.datetime.now()
        cost_time = (end_time - cur_time).total_seconds()
        logger.info("request请求数据完成，耗时, cost: %s 秒" % cost_time)
        return concat(data, sort=False).drop_duplicates()

    def get_history_data(self, code_list, frequency="1min", n=1):
        """
        获取历史数据
        :param code_list:
        :param frequency: k线数据级别
        :param n: (当天)前n个交易日 n = QA_util_get_trade_gap(start_date, today_)
        :return:
        """
        # TODO 历史数据部分应放在策略计算，而不是数据采集部分
        # TODO get history bar data
        # TODO 调用QA_fetch_stock_min_adv(code, start, end) 从数据库获取数据
        today = datetime.datetime(self.cur_year, self.cur_month, self.cur_day).isoformat()[:10]
        start_date = QA_util_get_pre_trade_date(cursor_date=today, n=n)[:10]
        end_date = QA_util_get_pre_trade_date(cursor_date=today, n=1)[:10]
        # start='2019-05-08', end='2019-05-09' means start from 2019-05-08 9:30 and end to 2019-05-09 15:00
        data = None
        try:
            data = QA_fetch_stock_min_adv(code_list, start=start_date, end=end_date)
        except Exception as e:
            logger.error("fetch stock min history data failure. " + e.__str__())

        if data is not None:
            for code in data.code.to_list():
                qa_data = data.select_code(code)
                if qa_data is not None:
                    # TODO 规定标准columns
                    self.publish_msg(qa_data.data.to_msgpack())
        else:
            lens = 0  # initial data len
            if frequency in ['5', '5m', '5min', 'five']:
                lens = 48 * n
            elif frequency in ['1', '1m', '1min', 'one']:
                lens = 240 * n
            elif frequency in ['15', '15m', '15min', 'fifteen']:
                lens = 16 * n
            elif frequency in ['30', '30m', '30min', 'half']:
                lens = 8 * n
            elif frequency in ['60', '60m', '60min', '1h']:
                lens = 4 * n
            lens = 20800 if lens > 20800 else lens
            # TODO 如果获取失败则在线获取 参考save stock min
            # data = self.get_security_bar_concurrent(code_list, frequency, lens)
            # TODO 规定标准columns
            # self.publish_msg(qa_data.data.to_msgpack())
        pass

    def update_date(self, date: datetime.datetime = None):
        # TODO auto update every day
        cur_time = datetime.datetime.now() if date is None else date
        self.cur_year = cur_time.year
        self.cur_month = cur_time.month
        self.cur_day = cur_time.day

    def length(self):
        """
        返回当前订阅列表的大小
        :return:
        """
        return len(self.code_list)

    def update_data_job(self):
        cur_time = datetime.datetime.now()
        context = self.get_data()
        if "code" not in context.columns or "datetime" not in context.columns:
            logger.info("the requested data has no columns name like 'code'")
            return
        if context.shape[0] == 0:
            logger.info("the requested data has no rows")
            return
        # 修正tdx在11:30的数据的时间直接跳至13:00的问题
        # if isinstance(context.code[0], str):
        context.datetime = context.datetime.apply(lambda x: datetime.datetime.fromisoformat(
                x.replace('13:00', '11:30')))
        # TODO tdx实时获取可能存在非正常的数据: 1.非交易时间错误 2.OHLC振幅超过上以交易日的10%
        # Fixed: 1.非交易时间错误
        if "year" in context.columns:
            context = context[
                (context.year == self.cur_year) & (context.month == self.cur_month) & (
                        context.day <= self.cur_day)]
        # 自动补充0开头的完整股票代码
        # context["code"] = context["code"].apply(fill_stock_code)
        # TODO 过滤振幅异常的数据
        context = context.merge(self.pre_market_data[['code', 'close']], on='code', suffixes=('', '_y'))
        # 异常的数据
        _context = context[
            (
                    (context.open / context.close_y - 1).abs() >= 0.101
            ) & (
                    (context.high / context.close_y - 1).abs() >= 0.101
            ) & (
                    (context.low / context.close_y - 1).abs() >= 0.101
            ) & (
                    (context.close / context.close_y - 1).abs() >= 0.101
            )
            ]
        if _context.shape[0] > 0:
            logger.info("异常数据输出START")
            logger.info(_context.to_csv())
            logger.info("异常数据输出END")
        # 过滤异常数据
        context = context[
            (
                    (context.open / context.close_y - 1).abs() < 0.101
            ) & (
                    (context.high / context.close_y - 1).abs() < 0.101
            ) & (
                    (context.low / context.close_y - 1).abs() < 0.101
            ) & (
                    (context.close / context.close_y - 1).abs() < 0.101
            )
            ]
        # 转换日期数据格式 datetime data type from str to Timestamp('2019-10-24 13:00:00', freq='1T')
        context["datetime"] = DatetimeIndex(context.datetime).to_list()
        context = context.drop([
            "year", "month", "day", "hour", "minute", "close_y"], axis=1
        ).reset_index(drop=True).set_index(["datetime", "code"]).sort_index()
        # TODO context.groupby(code)
        end_time = datetime.datetime.now()
        self.last_update_time = end_time
        cost_time = (end_time - cur_time).total_seconds()
        logger.info("clean数据初步清洗, 耗时, cost: %s 秒" % cost_time)
        # 数据原始记录输出到csv
        logger.info(context.to_csv(float_format='%.3f'))
        filename = get_file_name_by_date('stock.collector.%s.csv', self.log_dir)
        logging_csv(context, filename, index=True)
        self.publish_msg(context.to_msgpack())  # send with maspack
        del context

    def run(self):
        # 循环定时获取数据
        count = 0
        while 1:
            # code list not empty
            count += 1
            logger.info("stock bar collector service requested data start. count %s" % count)
            if self.length() <= 0:
                logger.info("code list is empty")
                time.sleep(1)
                continue
            self.isRequesting = True
            # 9:15 - 11:31 and 12：58 - 15:00 获取
            cur_time = datetime.datetime.now()
            _pass = (cur_time - self.last_update_time).total_seconds()
            if  self.debug or util_is_trade_time(cur_time):  # 如果在交易时间
                if  _pass > 55:
                    logger.warning("超时未收到更新数据")
                self.update_data_job()
            else:
                logger.info('current time %s not in trade time' % cur_time.isoformat())

            logger.info("stock bar collector service requested data end. count %s" % count)
            self.isRequesting = False
            time.sleep(self.delay)


@click.command()
# @click.argument()
@click.option('-t', '--delay', default=20.5, help="fetch data interval, float", type=click.FLOAT)
@click.option('-log', '--logfile', help="log file path", type=click.Path(exists=False))
@click.option('-log_dir', '--log_dir', help="log path", type=click.Path(exists=False))
def main(delay: float = 20.5, logfile: str = None, log_dir: str = None):
    try:
        from QARealtimeCollector.utils.logconf import update_log_file_config
        logfile = 'stock.collector.log' if logfile is None else logfile
        logging.config.dictConfig(update_log_file_config(logfile))
    except Exception as e:
        print(e.__str__())
    QARTCStockBar(delay=delay, log_dir=log_dir.replace('~', os.path.expanduser('~')), debug=False).start()


if __name__ == "__main__":
    # normal
    main()
