#
from QAPUBSUB.consumer import subscriber
from QAPUBSUB.producer import publisher
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAData.data_resample import QA_data_futuremin_resample, QA_data_futuremin_resample_series
from QUANTAXIS.QAUtil.QADate_trade import QA_util_future_to_tradedatetime
from QARealtimeCollector.setting import eventmq_ip
import json
import pandas as pd
import numpy as np
import threading
import time


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return str(obj)
        else:
            return super(NpEncoder, self).default(obj)


class QARTC_Resampler(QA_Thread):
    def __init__(self, code='rb1910', freqence='60min'):
        super().__init__()
        self.code = code
        self.freqence = freqence
        self.sub = subscriber(
            host=eventmq_ip, exchange='realtime_min_{}'.format(self.code))
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_{}_{}'.format(self.freqence, self.code))
        self.sub.callback = self.callback
        self.market_data = []
        self.dt = None
        threading.Thread(target=self.sub.start).start()

    def callback(self, a, b, c, data):
        lastest_data = json.loads(str(data, encoding='utf-8'))
        # print(lastest_data['datetime'])
        if self.dt != lastest_data['datetime'][15:16] or len(self.market_data) < 1:
            self.dt = lastest_data['datetime'][15:16]
            #print('new')
            self.market_data.append(lastest_data)
        else:
            #print('update')
            self.market_data[-1] = lastest_data
        df = pd.DataFrame(self.market_data)
        df = df.assign(datetime=pd.to_datetime(df.datetime), code=self.code, position=0,
                       tradetime=df.datetime.apply(QA_util_future_to_tradedatetime)).set_index('datetime')
        # print(df)
        res = QA_data_futuremin_resample(df, self.freqence)
        # print(res)
        # print(res.iloc[-1].to_dict())
        self.pub.pub(json.dumps(res.iloc[-1].to_dict(), cls=NpEncoder))

    def run(self):
        while True:
            # print(pd.DataFrame(self.data))
            time.sleep(1)


if __name__ == "__main__":
    QARTC_Resampler().start()
