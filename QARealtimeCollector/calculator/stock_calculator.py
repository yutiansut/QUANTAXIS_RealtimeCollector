# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    stock_calculator.py                                :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/10/02 13:44:50 by zhongjy1992       #+#    #+#              #
#    Updated: 2019/10/02 21:41:13 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import json
import pandas as pd
import threading
import time

from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher, publisher_topic
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QARealtimeCollector.setting import eventmq_ip
from QARealtimeCollector.datahandler.realtime_resampler import NpEncoder
from QUANTAXIS import QA_indicator_BOLL

class RTCCaluator(QA_Thread):
    # 只写了个样例框架
    def __init__(self, code_list: list, frequency='60min', strategy="HS300Enhance", init_data=None):
        """

        :param code_list:
        :param indicator_fun:
        :param frequency:
        :param strategy:
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
        self.market_data = init_data
        self.stock_code_list = code_list
        self.strategy = strategy

        # 接收stock 重采样的数据
        self.sub = subscriber(
            host=eventmq_ip, exchange='realtime_stock_{}_min'.format(self.frequency))
        self.sub.callback = self.stock_min_callback
        # 发送stock indicator result
        self.pub = publisher_topic(
            host=eventmq_ip, exchange='realtime_stock_calculator_{}_{}_min'.format(self.strategy, self.frequency))
        threading.Thread(target=self.sub.start).start()

        print("REALTIME_STOCK_CACULATOR INIT, strategy: %s frequency: %s" % (self.strategy, self.frequency))

    def unsubscribe(self, item):
        # remove code from market data
        pass

    def stock_min_callback(self, a, b, c, data):
        latest_data = json.loads(str(data, encoding='utf-8'))
        # print("latest data", latest_data)
        context = pd.DataFrame(latest_data)

        # merge update
        if self.market_data is None:
            self.market_data = context
        else:
            self.market_data.update(context)
        # print(self.market_data)

        # calculate indicator
        ind = self.market_data.groupby(['code']).apply(QA_indicator_BOLL)
        res = ind.join(self.market_data).dropna().round(2)
        res.set_value(index=res[res['LB'] >= res.close].index, col='buyorsell', value=1)  # 买入信号
        res.set_value(index=res[res['UB'] < res.close].index, col='buyorsell', value=-1)  # 卖出信号
        res['change'] = res['buyorsell'].diff()  # 计算指标信号是否反转
        res = res.groupby('code').tail(1)  # 取最新的信号
        # Buy信号的股票池
        res_buy: pd.DataFrame = res[res.change > 0].reset_index()
        # res_buy_code_list = res_buy['code']
        print("calculator.buy", res_buy)
        # Sell信号的股票池
        res_sell: pd.DataFrame = res[res.change < 0].reset_index()
        # res_sell_code_list = res_sell['code']
        print("calculator.sell", res_sell)

        self.pub.pub(json.dumps(res_buy.to_dict(), cls=NpEncoder), routing_key="calculator.buy")
        self.pub.pub(json.dumps(res_sell.to_dict(), cls=NpEncoder), routing_key="calculator.sell")

    def run(self):
        import datetime
        while True:
            print(datetime.datetime.now(), "realtime stock calculator is running")
            time.sleep(1)


if __name__ == '__main__':
    import QUANTAXIS as QA
    from QUANTAXIS import SUM
    code_list = ['000001', '000002']  # TODO HS300 STOCK CODE LIST
    start_date = '2019-09-29'
    end_date = '2019-09-30'
    # TODO 若遇上当天除权除息可能出现计算错误
    # TODO should resample and the data format is same to the mq_min_data
    init_min_data = QA.QA_fetch_stock_min_adv(code_list, start_date, end_date)
    if init_min_data is not None:
        init_min_data = init_min_data.data
    from QUANTAXIS import QA_indicator_BOLL
    RTCCaluator(
        code_list=code_list, frequency='5min', strategy="HS300Enhance", init_data=init_min_data
    ).start()
