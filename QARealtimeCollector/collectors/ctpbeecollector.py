import datetime
import json

from QAPUBSUB import consumer, producer
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info
from QARealtimeCollector.setting import mongo_ip, eventmq_ip, market_data_password, market_data_user
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread


class QARTC_CtpBeeCollector(QA_Thread):
    """这是接收重采样部分

    Returns:
        [type] -- [description]
    """

    def __init__(self, code):
        super().__init__()
        self.data = {}
        self.min5_data = {}

        self.pro = producer.publisher(host=eventmq_ip, exchange='bar_1min_{}'.format(code),
                                      user=market_data_user, password=market_data_password)
        self.pro_realtimemin = producer.publisher(host=eventmq_ip, exchange='realtime_min_{}'.format(
            code), user=market_data_user, password=market_data_password)
        self.is_send = False
        self.last_volume = 0
        self.c = consumer.subscriber_routing(host=eventmq_ip,
                                             exchange='CTPX', routing_key=code, user=market_data_user, password=market_data_password)

    def create_new(self, new_tick):
        """
        {'gateway_name': 'ctp', 'symbol': 'au2004', 'exchange': 'SHFE', 
        'datetime': '2019-07-02 23:40:19.500000', 'name': '黄金2004', 
        'volume': 918, 'last_price': 318.35, 'last_volume': 0, 
        'limit_up': 325.95, 'limit_down': 300.9, 'open_interest':4940.0, 
        'average_price': 315256.2091503268, 'preSettlementPrice': 313.45, 
        'open_price': 314.0, 'high_price': 318.35, 'low_price': 313.9, 
        'pre_close': 314.05, 'bid_price_1': 318.25, 'bid_price_2': 0, 'bid_price_3': 0, 
        'bid_price_4': 0, 'bid_price_5': 0, 'ask_price_1': 318.45, 'ask_price_2': 0, 
        'ask_price_3': 0, 'ask_price_4': 0, 'ask_price_5': 0, 'bid_volume_1': 6, 
        'bid_volume_2': 0, 'bid_volume_3': 0, 'bid_volume_4': 0, 'bid_volume_5': 0, 
        'ask_volume_1': 3, 'ask_volume_2': 0, 'ask_volume_3': 0, 'ask_volume_4': 0, 
        'ask_volume_5': 0, 'vt_symbol': 'au2004.SHFE'}
        """
        # time = '{}-{}-{} '.format(new_tick['ActionDay'][0:4], new_tick['ActionDay'][4:6], new_tick['ActionDay']
        #                           [6:8]) + new_tick['datetime'] + str('%.6f' % (new_tick['UpdateMillisec']/1000000))[1:]
        self.data[new_tick['symbol']] = {'open': new_tick['last_price'],
                                         'high': new_tick['last_price'],
                                         'low': new_tick['last_price'],
                                         'close': new_tick['last_price'],
                                         'code': str(new_tick['symbol']).upper(),
                                         'datetime': new_tick['datetime'],
                                         'volume': new_tick['volume']-self.last_volume}

    def update_bar(self, new_tick):

        time = new_tick['datetime']
        old_data = self.data[new_tick['symbol']]
        # print(old_data)
        old_data['close'] = new_tick['last_price']
        old_data['high'] = old_data['high'] if old_data['high'] > new_tick['last_price'] else new_tick['last_price']
        old_data['low'] = old_data['low'] if old_data['low'] < new_tick['last_price'] else new_tick['last_price']
        old_data['datetime'] = time
        old_data['volume'] = new_tick['volume'] - self.last_volume
        self.data[new_tick['symbol']] = old_data
        return old_data

    def publish_bar(self, symbol):
        QA_util_log_info('=================================')
        QA_util_log_info('publish')
        QA_util_log_info('=================================')
        print(self.data[symbol])
        self.pro.pub(json.dumps(self.data[symbol]))
        self.is_send = True

    def upcoming_data(self, new_tick):
        curtime = new_tick['datetime']
        time = curtime
        if curtime[11:13] in ['00', '01', '02',
                              '09', '10', '11',
                              '13', '14', '15',
                              '21', '22', '23']:

            try:
                if new_tick['datetime'][17:19] == '00' and len(new_tick['datetime']) == 19:
                    # print(True)
                    old_data = self.update_bar(new_tick)
                    self.last_volume = new_tick['volume']
                    self.publish_bar(new_tick['symbol'])
                    self.pro_realtimemin.pub(json.dumps(old_data))
                    self.data[new_tick['symbol']] = {}
                    self.data[new_tick['symbol']]['datetime'] = time

                elif new_tick['datetime'][17:19] == '00' and len(new_tick['datetime']) > 19:
                    if self.is_send:
                        self.is_send = False
                    else:
                        self.publish_bar(new_tick['symbol'])

                    QA_util_log_info('xxx')
                    self.create_new(new_tick)
                    self.pro_realtimemin.pub(json.dumps(
                        self.data[new_tick['symbol']]))
                    QA_util_log_info(self.data)
                else:
                    try:
                        self.update_bar(new_tick)
                    except:
                        self.create_new(new_tick)
                    self.pro_realtimemin.pub(json.dumps(
                        self.data[new_tick['symbol']]))
            except Exception as e:
                print(e)

    def callback(self, a, b, c, body):
        self.upcoming_data(json.loads(body))

    def run(self):
        self.c.callback = self.callback
        self.c.start()


if __name__ == '__main__':
    pass
    # import click
    # @click.command()
    # @click.option('--code', default='au1910')
    # def handler(code):
    #     r = QARealtimeCollector_CtpBeeCollector(code)

    #     r.start()

    # handler()
