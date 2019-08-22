from QUANTAXIS.QAFetch.QATdx_adv import QA_Tdx_Executor
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAARP.QAUser import QA_User
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
import threading
from QAPUBSUB.consumer import subscriber_routing
from QAPUBSUB.producer import publisher_routing
from qarealtimecollector.setting import eventmq_ip


class QARTC_Stock(QA_Tdx_Executor):
    def __init__(self, username, password):
        super().__init__(name='QAREALTIME_COLLECTOR_STOCK')
        self.user = QA_User(username=username, password=password)
        self.sub = subscriber_routing(host=eventmq_ip,
                                      exchange='QARealtime_Market', routing_key='stock')
        self.sub.callback = self.callback
        self.pub = publisher_routing(
            host=eventmq_ip, exchange='stocktransaction')
        threading.Thread(target=self.sub.start, daemon=True).start()

    def subscribe(self, code):
        """继续订阅

        Arguments:
            code {[type]} -- [description]
        """
        self.user.sub_code(code)

    def unsubscribe(self, code):
        self.user.unsub_code(code)

    def callback(self, a, b, c, data):
        data = json.loads(data)
        if data['topic'] == 'subscribe':
            print('receive new subscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.user.sub_code(item)
            else:
                self.user.sub_code(new_ins)
        if data['topic'] == 'unsubscribe':
            print('receive new unsubscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.user.unsub_code(item)
            else:
                self.user.unsub_code(new_ins)

    def get_data(self):
        data, time = self.get_realtime_concurrent(
            self.user.subscribed_code['stock_cn'])
        print(QA_util_to_json_from_pandas(data))

    def run(self):
        while 1:
            self.get_data()
            import time
            time.sleep(1)


if __name__ == "__main__":
    r = QARTC_Stock('yutiansut', '940809')
    r.subscribe('000001')
    r.subscribe('000002')
    r.start()

    r.subscribe('600010')

    import json
    import time
    time.sleep(2)
    publisher_routing(exchange='QARealtime_Market', routing_key='stock').pub(json.dumps({
        'topic': 'subscribe',
        'code': '600012'
    }), routing_key='stock')

    r.unsubscribe('000001')
