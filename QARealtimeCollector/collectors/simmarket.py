#
import json
import time

from QAPUBSUB.producer import publisher_routing
from QUANTAXIS_RandomPrice import get_random_price
from QARealtimeCollector.setting import eventmq_ip, mongo_ip


class QARTC_RandomTick():
    def __init__(self, code, date, price, interval):
        self.code = code
        self.date = date
        self.price = price
        self.interval = interval

        self.pub = publisher_routing(
            exchange='tick', routing_key=code, host=eventmq_ip, user='admin', password='admin')

    @property
    def data(self):
        return get_random_price(self.price, self.code, self.date)

    def start(self):
        for _, item in self.data.iterrows():
            print(item.to_dict())
            time.sleep(self.interval)
            self.pub.pub(
                json.dumps(item.to_dict()), routing_key=self.code)
