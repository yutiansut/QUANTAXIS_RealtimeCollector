#
from QAPUBSUB.consumer import subscriber
from QAPUBSUB.producer import publisher
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAData.data_resample import QA_data_futuremin_resample, QA_data_futuremin_resample_series
from QUANTAXIS.QAUtil.QADate_trade import  QA_util_future_to_tradedatetime
from QARealtimeCollector.setting import eventmq_ip
import json
import pandas as pd
import threading
import time


class resample_pub(QA_Thread):
    def __init__(self, ):
        super().__init__()
        self.sub = subscriber(host=eventmq_ip, exchange='realtime_min_rb1910')
        self.pub = publisher(host=eventmq_ip, exchange='realtime_60min_rb1910')
        self.sub.callback = self.callback
        self.market_data = []
        self.dt = None
        threading.Thread(target=self.sub.start).start()

    def callback(self, a, b, c, data):
        #print(data)
        lastest_data = json.loads(str(data, encoding='utf-8'))
        if self.dt != lastest_data['datetime'][-12:-10] or len(self.market_data) < 1:
            self.dt = lastest_data['datetime'][-12:-10]
            self.market_data.append(lastest_data)
        else:
            self.market_data[-1] = lastest_data
        #self.market_dat.append(json.loads(data))
        #print(pd.DataFrame(self.market_data))
        df = pd.DataFrame(self.market_data)
        df = df.assign(datetime=pd.to_datetime(df.datetime), code='rb1910', position=0, tradetime = df.datetime.apply(QA_util_future_to_tradedatetime)).set_index('datetime')
        print(df)
        res = QA_data_futuremin_resample(df, '60min')
        print(res)
        print(res.iloc[-1].to_dict())
        self.pub.pub(json.dumps(res.iloc[-1].to_dict()))
        

    def run(self):
        while True:
            #print(pd.DataFrame(self.data))
            time.sleep(1)

if __name__ == "__main__":
    resample_pub().start
