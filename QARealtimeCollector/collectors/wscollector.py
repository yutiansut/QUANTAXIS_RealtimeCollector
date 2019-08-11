#
from QAPUBSUB.producer import publisher_routing
from QAPUBSUB.consumer import subscriber_routing
from QUANTAXIS.QAEngine import QA_Thread
from QA_OTGBroker import on_pong, on_message, on_error, subscribe_quote, on_close, login, peek
import websocket
import threading
import click
import time
import json
import pymongo
from QARealtimeCollector.util import fix_dict
from QARealtimeCollector.setting import  mongo_ip, eventmq_ip

class QARTC_WsCollector(QA_Thread):
    def __init__(self):

        super().__init__()
        self.ws = websocket.WebSocketApp('wss://openmd.shinnytech.com/t/md/front/mobile',
                                         on_pong=on_pong,
                                         on_message=self.on_message,
                                         on_error=on_error,
                                         on_close=on_close)

        def _onopen(ws):
            def run():
                ws.send(peek())
            threading.Thread(target=run, daemon=False).start()

        self.quoteclient = pymongo.MongoClient(host=mongo_ip).QAREALTIME.realtimeQuote
        self.ws.on_open = _onopen
        self.data = {}
        self.subscribe_list = ['SHFE.rb1910', 'DCE.j1909']
        self.sub = subscriber_routing(host=eventmq_ip, exchange='QARealtime_Market', routing_key='future')
        self.sub.callback = self.callback
        threading.Thread(target=self.ws.run_forever,
                         name='market_websock', daemon=False).start()
        threading.Thread(target=self.sub.start,
                         name='market_subscriber', daemon=True).start()

    def on_message(self, message):
        print(message)
        message = json.loads(message)
        if 'data' in message.keys():
            data = message['data'][0]
            if 'quotes' in data.keys():
                data = data['quotes']
                for items in data.keys():
                    try:
                        item = items.replace('.', '_')
                        if item not in self.data.keys():
                            self.data[item] = data[items]
                        else:
                            for keys in data[items].keys():
                                self.data[item][keys] = data[items][keys]
                        self.data[item]['instrument_id'] = item
                        self.quoteclient.update_one({'instrument_id': item},
                                                    {'$set': self.data[item]}, upsert=True)
                    except Exception as e:
                        print(e)

        self.ws.send(peek())

    def callback(self, a, b, c, data):
        data = json.loads(data)
        if data['topic'] == 'subscribe':
            new_ins = data['code'].replace('_', '.').split(',')
            import copy

            old = len(self.subscribe_list)
            self.subscribe_list.extend(new_ins)
            self.subscribe_list = list(
                set(self.subscribe_list))
            if old < len(self.subscribe_list):
                self.ws.send(subscribe_quote(','.join(self.subscribe_list)))

    def run(self):
        time.sleep(2)
        self.ws.send(subscribe_quote('SHFE.rb1910,DCE.j1909'))
        while True:
            time.sleep(1)


if __name__ == "__main__":
    QARTC_WsCollector().start()
