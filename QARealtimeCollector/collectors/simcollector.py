import click
import datetime
import json

from QAPUBSUB import consumer, producer
from QARealtimeCollector.setting import eventmq_ip, mongo_ip
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info


class QARTC_CTPTickCollector():
    def __init__(self, code, subexchange='tick'):
        self.data = {}
        self.is_send = False
        self.last_volume = 0

        self.pro = producer.publisher(exchange='bar_1min_{}'.format(
            code), user='admin', password='admin', host=eventmq_ip)
        self.pro_realtimemin = producer.publisher(exchange='realtime_min_{}'.format(
            code), user='admin', password='admin', host=eventmq_ip)
        self.c = consumer.subscriber_routing(
            exchange='tick', routing_key=code, user='admin', password='admin', host=eventmq_ip)

        print('start ctptick collector {}'.format(code))

    def create_new(self, new_tick):

        time = '{}-{}-{} '.format(str(new_tick['TradingDay'])[0:4], str(new_tick['TradingDay'])[4:6], str(new_tick['TradingDay'])
                                  [6:8]) + new_tick['UpdateTime'] + str('%.6f' % (new_tick['UpdateMillisec']/1000))[1:]
        print(time)
        self.data[new_tick['InstrumentID']] = {'open': new_tick['LastPrice'],
                                               'high': new_tick['LastPrice'],
                                               'low': new_tick['LastPrice'],
                                               'close': new_tick['LastPrice'],
                                               'code': str(new_tick['InstrumentID']).upper(),
                                               'datetime': time,
                                               'volume': new_tick['Volume']-self.last_volume}

    def update_bar(self, new_tick):
        time = '{}-{}-{} '.format(str(new_tick['TradingDay'])[0:4], str(new_tick['TradingDay'])[4:6], str(new_tick['TradingDay'])
                                  [6:8]) + new_tick['UpdateTime'] + str('%.6f' % (new_tick['UpdateMillisec']/1000))[1:]
        old_data = self.data[new_tick['InstrumentID']]
        old_data['close'] = new_tick['LastPrice']
        old_data['high'] = old_data['high'] if old_data['high'] > new_tick['LastPrice'] else new_tick['LastPrice']
        old_data['low'] = old_data['low'] if old_data['low'] < new_tick['LastPrice'] else new_tick['LastPrice']
        old_data['datetime'] = time
        old_data['volume'] = new_tick['Volume'] - self.last_volume
        self.data[new_tick['InstrumentID']] = old_data
        return old_data

    def publish_bar(self, InstrumentID):
        QA_util_log_info('=================================')
        QA_util_log_info('publish bar')
        QA_util_log_info('=================================')
        print(self.data)
        self.pro.pub(json.dumps(self.data[InstrumentID]))
        self.is_send = True

    def publish_realtime(self, data):
        QA_util_log_info('=================================')
        QA_util_log_info('publish realtime')
        QA_util_log_info('=================================')
        print(data)
        self.pro_realtimemin.pub(json.dumps(data))

    def upcoming_data(self, new_tick):

        curtime = '{}-{}-{} '.format(str(new_tick['TradingDay'])[0:4], str(new_tick['TradingDay'])[4:6], str(new_tick['TradingDay'])
                                     [6:8]) + new_tick['UpdateTime'] + str('%.6f' % (new_tick['UpdateMillisec']/1000))[1:]
        time = curtime

        print('{} === get update tick {}'.format(time, new_tick))
        if new_tick['UpdateTime'][-2:] == '00' and new_tick['UpdateMillisec'] == 0:

            old_data=self.update_bar(new_tick)
            self.last_volume=new_tick['Volume']
            self.publish_bar(new_tick['InstrumentID'])
            self.publish_realtime(old_data)

            self.data[new_tick['InstrumentID']]={}
            self.data[new_tick['InstrumentID']]['datetime']=time

        elif new_tick['UpdateTime'][-2:] == '00' and new_tick['UpdateMillisec'] == 500:
            if self.is_send:
                self.is_send=False
            else:
                self.publish_bar(new_tick['InstrumentID'])
            self.create_new(new_tick)
            self.publish_realtime(self.data[new_tick['InstrumentID']])
            QA_util_log_info(self.data)
        else:
            try:
                self.update_bar(new_tick)
            except:
                self.create_new(new_tick)
            self.publish_realtime(self.data[new_tick['InstrumentID']])

    def callback(self, a, b, c, body):
        self.upcoming_data(json.loads(body))

    def start(self):
        self.c.callback=self.callback

        self.c.start()
