# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    QATdx_adv.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/03/06 13:42:58 by zhongjy1992       #+#    #+#              #
#    Updated: 2020/03/06 13:43:02 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2019 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime
import os
import queue
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Timer

import click
import pandas as pd
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_tradetime
from QUANTAXIS.QAUtil.QASetting import DATABASE, stock_ip_list
from QUANTAXIS.QAUtil.QASql import QA_util_sql_mongo_sort_ASCENDING
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from pytdx.hq import TdxHq_API

from QARealtimeCollector.utils.common import get_file_name_by_date, logging_csv
logger = logging.getLogger(__name__)

"""
准备做一个多连接的连接池执行器Executor
当持续获取数据/批量数据的时候,可以减小服务器的压力,并且可以更快的进行并行处理
"""


class QA_Tdx_Executor(QA_Thread):
    def __init__(self, thread_num=2, timeout:float=1, sleep_time=1, *args, **kwargs):
        super().__init__(name='QATdxExecutor')
        self.thread_num = thread_num
        self._queue = queue.Queue(maxsize=200)
        self.api_no_connection = TdxHq_API()
        self._api_worker = Thread(
            target=self.api_worker, args=(), name='API Worker')
        self._api_worker.start()
        self.timeout = timeout
        self.executor = ThreadPoolExecutor(self.thread_num)
        self.sleep_time = sleep_time

    def __getattr__(self, item):
        try:
            api = self.get_available()
            func = api.__getattribute__(item)

            def wrapper(*args, **kwargs):
                res = self.executor.submit(func, *args, **kwargs)
                self._queue.put(api)
                return res
            return wrapper
        except:
            return self.__getattr__(item)

    def _queue_clean(self):
        self._queue = queue.Queue(maxsize=200)

    def _test_speed(self, ip, port=7709):

        api = TdxHq_API(raise_exception=True, auto_retry=False)
        _time = datetime.datetime.now()
        # print(self.timeout)
        try:
            with api.connect(ip, port, time_out=1):
                # api.get_security_list(0, 1)
                # res = api.get_security_list(0, 1)
                # print(res)
                # print(len(res))
                if len(api.get_security_list(0, 1)) > 800:
                    return (datetime.datetime.now() - _time).total_seconds()
                else:
                    return datetime.timedelta(9, 9, 0).total_seconds()
        except:
            return datetime.timedelta(9, 9, 0).total_seconds()

    def get_market(self, code):
        code = str(code)
        if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
            return 1
        return 0

    def get_frequence(self, frequence):
        if frequence in ['day', 'd', 'D', 'DAY', 'Day']:
            frequence = 9
        elif frequence in ['w', 'W', 'Week', 'week']:
            frequence = 5
        elif frequence in ['month', 'M', 'm', 'Month']:
            frequence = 6
        elif frequence in ['Q', 'Quarter', 'q']:
            frequence = 10
        elif frequence in ['y', 'Y', 'year', 'Year']:
            frequence = 11
        elif str(frequence) in ['5', '5m', '5min', 'five']:
            frequence = 0
        elif str(frequence) in ['1', '1m', '1min', 'one']:
            frequence = 8
        elif str(frequence) in ['15', '15m', '15min', 'fifteen']:
            frequence = 1
        elif str(frequence) in ['30', '30m', '30min', 'half']:
            frequence = 2
        elif str(frequence) in ['60', '60m', '60min', '1h']:
            frequence = 3

        return frequence

    @property
    def ipsize(self):
        return len(self._queue.qsize())

    @property
    def api(self):
        return self.get_available()

    def get_available(self):

        if self._queue.empty() is False:
            return self._queue.get_nowait()
        else:
            Timer(0, self.api_worker).start()
            return self._queue.get()

    def api_worker(self):
        # data = []
        if self._queue.qsize() < 80:
            for item in stock_ip_list:
                if self._queue.full():
                    break
                _sec = self._test_speed(ip=item['ip'], port=item['port'])
                if _sec < self.timeout*3:
                    try:
                        self._queue.put(TdxHq_API(heartbeat=False).connect(
                            ip=item['ip'], port=item['port'], time_out=self.timeout*2))
                    except:
                        pass
        else:
            self._queue_clean()
            Timer(0, self.api_worker).start()
        Timer(300, self.api_worker).start()

    def _singal_job(self, context, id_, code, time_out=0.7):
        try:
            _api = self.get_available()
            # self.api_no_connection.to_df 为了调用 to_df的方法
            __data = context.append(self.api_no_connection.to_df(_api.get_security_quotes(
                [(self._select_market_code(x), x) for x in code[80 * id_:80 * (id_ + 1)]])))
            __data['datetime'] = datetime.datetime.now()
            self._queue.put(_api)  # 加入注销
            return __data
        except:
            return self.singal_job(context, id_)

    def get_realtime(self, code):
        context = pd.DataFrame()

        code = [code] if isinstance(code, str) is str else code
        try:
            for id_ in range(int(len(code) / 80) + 1):
                context = self._singal_job(context, id_, code)

            data = context[['datetime', 'last_close', 'code', 'open', 'high', 'low', 'price', 'cur_vol',
                            's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
                            'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
                            'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5']]
            data['datetime'] = data['datetime'].apply(lambda x: str(x))
            return data.set_index('code', drop=False, inplace=False)
        except:
            return None

    def get_realtime_concurrent(self, code):
        code = [code] if isinstance(code, str) is str else code

        try:
            data = {self.get_security_quotes([(self.get_market(
                x), x) for x in code[80 * pos:80 * (pos + 1)]]) for pos in range(int(len(code) / 80) + 1)}
            return (pd.concat([self.api_no_connection.to_df(i.result()) for i in data]), datetime.datetime.now())
        except:
            pass

    def get_security_bar_concurrent(self, code_list, _type, lens):
        """

        :param code_list:
        :param _type:
        :param lens:
        :return: [Dataframe, df, df]
        """
        try:
            context = []
            for item in code_list:
                _code = str(item)
                _request_time = datetime.datetime.now().isoformat(sep=' ', timespec='seconds')
                # print(_code, lens)
                _data = {self.get_security_bars(self.get_frequence(_type), self.get_market(_code), _code, 0, lens)}
                data = pd.concat([
                    pd.DataFrame(i.result()) for i in _data if i is not None
                ]).assign(code=_code, update=_request_time)
                # data = pd.concat([
                #     self.api_no_connection.to_df(i.result()) for i in _data if i is not None
                # ]).assign(code=_code, update=_request_time)
                context.append(data)
                # record the dta
                filename = get_file_name_by_date('stock.pytdx.%s.csv', os.path.join(os.path.expanduser('~'), './log/'))
                logging_csv(data, filename, index=False, mode='a')
            return context
        except:
            raise Exception

    def _get_security_bars(self, context, code, _type, lens):
        try:
            _api = self.get_available()
            _code = str(code) if not isinstance(code, str) else code
            for i in range(1, int(lens / 800) + 2):
                context.extend(_api.get_security_bars(self.get_frequence(
                    _type), self.get_market(_code), _code, (i - 1) * 800, 800))
            self._queue.put(_api)
            return context
        except Exception as e:
            logger.error('pytdx get bars failure' + e.__str__())
            return self._get_security_bars(context, code, _type, lens)

    def get_security_bar(self, code, _type, lens):
        code = [code] if isinstance(code, str) is str else code
        context = []
        try:
            for item in code:
                context = self._get_security_bars(context, item, _type, lens)
            return context
        except Exception as e:
            raise e

    def save_mongo(self, data, client=DATABASE):
        database = DATABASE.get_collection(
            'realtime_{}'.format(datetime.date.today()))

        database.insert_many(QA_util_to_json_from_pandas(data))

    def run(self):

        sleep = int(self.sleep_time)
        _time1 = datetime.datetime.now()
        database = DATABASE.get_collection(
            'realtime_{}'.format(datetime.date.today()))
        database.create_index([('code', QA_util_sql_mongo_sort_ASCENDING)])
        database.create_index([('datetime', QA_util_sql_mongo_sort_ASCENDING)])

        from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_block_adv
        code = QA_fetch_stock_block_adv().code

        while True:
            _time = datetime.datetime.now()
            if QA_util_if_tradetime(_time):  # 如果在交易时间
                data = self.get_realtime_concurrent(code)

                data[0]['datetime'] = data[1]
                self.save_mongo(data[0])

                logger.debug('Cost Time {}'.format(
                    (datetime.datetime.now() - _time).total_seconds()))
                time.sleep(sleep)
                logger.debug('Connection Pool NOW LEFT {} Available IP'.format(
                    self._queue.qsize()))
                logger.debug('Program Last Time {}'.format(
                    (datetime.datetime.now() - _time1).total_seconds()))
            else:
                logger.warning('Not Trading time {}'.format(_time))
                time.sleep(sleep)


def get_bar(timeout=1, sleep=1):
    sleep = int(sleep)
    _time1 = datetime.datetime.now()
    from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_block_adv
    code = QA_fetch_stock_block_adv().code
    x = QA_Tdx_Executor(timeout=float(timeout))

    while True:
        _time = datetime.datetime.now()
        if QA_util_if_tradetime(_time):  # 如果在交易时间
            data = x.get_security_bar_concurrent(code, 'day', 1)

            logger.debug('Cost Time {}'.format(
                (datetime.datetime.now() - _time).total_seconds()))
            time.sleep(sleep)
            logger.debug('Connection Pool NOW LEFT {} Available IP'.format(
                x._queue.qsize()))
            logger.debug('Program Last Time {}'.format(
                (datetime.datetime.now() - _time1).total_seconds()))

            return data
        else:
            logger.warning('Not Trading time {}'.format(_time))
            time.sleep(sleep)


def get_day_once():

    _time1 = datetime.datetime.now()
    from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_block_adv
    code = QA_fetch_stock_block_adv().code
    x = QA_Tdx_Executor()
    return x.get_security_bar_concurrent(code, 'day', 1)


@click.command()
@click.option('--timeout', default=0.2, help='timeout param')
@click.option('--sleep', default=1, help='sleep step')
def bat(timeout=0.2, sleep=1):
    QA_Tdx_Executor(timeout=timeout, sleep_time=sleep).start()


if __name__ == '__main__':
    QA_Tdx_Executor().start()
