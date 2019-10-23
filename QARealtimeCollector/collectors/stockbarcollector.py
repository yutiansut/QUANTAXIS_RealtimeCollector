# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    stockbarcollector.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/10/01 22:07:05 by zhongjy1992       #+#    #+#              #
#    Updated: 2019/10/01 22:07:05 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import json
import threading
import datetime

from QAPUBSUB.consumer import subscriber_routing
from QAPUBSUB.producer import publisher
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAFetch.QATdx_adv import QA_Tdx_Executor

class QARTCStockBar(QA_Tdx_Executor):
    def __init__(self, delay=30.5):
        super().__init__(name='QA_REALTIME_COLLECTOR_STOCK_BAR')
        # 数据获取请求间隔
        self.isOK = True
        self.delay = delay
        self.codelist = []
        self.sub = subscriber_routing(host=eventmq_ip,
                                      exchange='QARealtime_Market', routing_key='stock')
        self.sub.callback = self.callback
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_stock_min'
        )
        print("QA_REALTIME_COLLECTOR_STOCK_BAR INIT, delay %s" % self.delay)
        threading.Thread(target=self.sub.start, daemon=True).start()

    def subscribe(self, code):
        """继续订阅

        Arguments:
            code {[type]} -- [description]
        """
        if code not in self.codelist:
            self.codelist.append(code)

    def unsubscribe(self, code):
        self.codelist.remove(code)

    def callback(self, a, b, c, data):
        data = json.loads(data)
        if data['topic'].lower() == 'subscribe':
            print('stock bar collector service receive new subscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.subscribe(item)
            else:
                self.subscribe(new_ins)
        if data['topic'].lower() == 'unsubscribe':
            print('stock bar collector service receive new unsubscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            if isinstance(new_ins, list):
                for item in new_ins:
                    self.unsubscribe(item)
            else:
                self.unsubscribe(new_ins)

    def get_data(self):
        # 获取当前及上一bar
        lens = 2  # TODO default 2
        data = self.get_security_bar_concurrent(self.codelist, "1min", lens)
        # print(data)
        self.pub.pub(json.dumps(data))

    def run(self):
        # 循环定时获取数据
        import time
        while 1:
            print(self.codelist, self.isOK)
            if len(self.codelist) > 0 and self.isOK:
                print(datetime.datetime.now(), " : stock bar collector service requested data")
                self.isOK = False
                self.get_data()
                self.isOK = True
                time.sleep(self.delay)
            else:
                time.sleep(1)


if __name__ == "__main__":
    QARTCStockBar().start()
