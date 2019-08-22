from QAPUBSUB.consumer import subscriber
from QAPUBSUB.producer import publisher_routing
import json
import pandas as pd
# 订阅操作

req = publisher_routing(exchange='QARealtime_Market', routing_key='stock')

req.pub(json.dumps({'topic': 'subscribe', 'code': '000007'}),routing_key='stock')


# 接受并分析


def callback(a, b, c, data):
    data = json.loads(data)

    print(pd.DataFrame(data).set_index(['code']).loc[:,['ask1', 'ask_vol1', 'price', 'bid1','bid_vol1']])


sub = subscriber(exchange='stocktransaction')

sub.callback = callback

sub.start()