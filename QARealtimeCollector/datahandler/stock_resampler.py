# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    stock_resampler.py                                 :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/10/01 15:00:56 by zhongjy1992       #+#    #+#              #
#    Updated: 2019/10/02 21:29:57 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import datetime
import json
import pandas as pd
import threading
import time

from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QARealtimeCollector.setting import eventmq_ip
from QARealtimeCollector.datahandler.realtime_resampler import NpEncoder


def tdx_bar_data_stock_resample(min_data, period=5):
    """
    1min 分钟线采样成 1,5,15,30,60,120 级别的分钟线
    TODO 240时间戳有问题
    :param min_data:
    :param period:
    :return:
    """
    min_data = min_data.reset_index()
    if 'datetime' not in min_data.columns:
        return None

    if isinstance(period, float):
        period = int(period)
    elif isinstance(period, str):
        period = period.replace('min', '')
    elif isinstance(period, int):
        pass
    _period = '%sT' % period
    # TODO 确认时间格式 yyyy-mm-dd HH:MM:SS
    min_data.datetime = min_data.datetime.apply(datetime.datetime.fromisoformat)
    # 9:30 - 11:30
    min_data_morning = min_data.set_index(
        "datetime").loc[datetime.time(9, 30):datetime.time(11, 30)].reset_index()
    min_data_morning.index = pd.DatetimeIndex(
        min_data_morning.datetime).to_period('T')
    # 13:00 - 15:00
    min_data_afternoon = min_data.set_index(
        "datetime").loc[datetime.time(13, 00):datetime.time(15, 00)].reset_index()
    min_data_afternoon.index = pd.DatetimeIndex(
        min_data_afternoon.datetime).to_period('T')

    _conversion = {
        'code' : 'first',
        'open' : 'first',
        'high' : 'max',
        'low'  : 'min',
        'close': 'last',
    }
    if 'vol' in min_data.columns:
        _conversion["vol"] = "sum"
    elif 'volume' in min_data.columns:
        _conversion["volume"] = "sum"
    if 'amount' in min_data.columns:
        _conversion['amount'] = 'sum'
    _base = 0
    if period > 60:
        _base = 60
    res = pd.concat([
        min_data_morning.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=30+_base).apply(_conversion).dropna(),
        min_data_afternoon.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=_base).apply(_conversion).dropna()
    ])
    return res.reset_index().set_index(["datetime", "code"]).sort_index()


class QARTCStockBarResampler(QA_Thread):
    """
        应启动一个线程单独重采样1min的数据，然后按需求根据1min的数据重新采样为多周期数据
        若有一个内存数据库，则可以把数据先写入数据库，然后再根据订阅读取进行拉取(redis, mongo？)
    """

    def __init__(self, frequency='5min'):
        """
        暂时不支持单个股票重采样
        :param frequency:
        """
        super().__init__()
        if isinstance(frequency, float):
            self.frequency = int(frequency)
        elif isinstance(frequency, str):
            _frequency = frequency.replace('min', '')
            if str.isnumeric(_frequency):
                self.frequency = int(_frequency)
            else:
                print("unknown frequency: %s" % frequency)
                return
        elif isinstance(frequency, int):
            self.frequency = frequency
        else:
            print("unknown frequency: %s" % frequency)
            return

        self.market_data = None

        # 接收stock tick 数据
        self.sub = subscriber(
            host=eventmq_ip, exchange='realtime_stock_min')
        self.sub.callback = self.stock_min_callback
        # 发送重采样的数据
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_stock_{}_min'.format(self.frequency))
        threading.Thread(target=self.sub.start).start()

        print("QA_REALTIME_COLLECTOR_STOCK_BAR_RESAMPLER INIT, frequency: %s" % self.frequency)

    def unsubscribe(self, item):
        # remove code from market data
        pass

    def stock_min_callback(self, a, b, c, data):
        latest_data = json.loads(str(data, encoding='utf-8'))
        # print("latest data", latest_data)
        latest_data = [pd.DataFrame.from_records(i) for i in latest_data if i is not None]
        if len(latest_data) == 0:
            print(datetime.datetime.now(), "no data")
            return
        context = pd.concat(latest_data)
        # merge update
        if self.market_data is None:
            self.market_data = context
        else:
            self.market_data.update(context)
        # print(self.market_data)
        # group by code and resample
        bar_data: pd.DataFrame = self.market_data.groupby(['code'], group_keys=False).apply(
            tdx_bar_data_stock_resample, self.frequency)
        # print(bar_data)

        # if time is xx:00:00 -> xx:06:00, may be update two bar(xx:05:00, xx:10:00)
        # client just use dataframe update
        diff_data: pd.DataFrame = bar_data.groupby(['code'], group_keys=False).tail(2)
        # print(diff_data)
        try:
            res = diff_data.reset_index().to_dict()
            self.pub.pub(json.dumps(res, cls=NpEncoder))
        except Exception as e:
            print("reset index failure", e, diff_data.index, diff_data.columns)

    def run(self):
        while True:
            print(datetime.datetime.now(), "stock resampler is running")
            time.sleep(1)


if __name__ == '__main__':
    QARTCStockBarResampler().start()
