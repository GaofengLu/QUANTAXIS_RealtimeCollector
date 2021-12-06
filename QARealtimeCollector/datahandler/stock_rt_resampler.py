from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAData.data_resample import QA_data_stockmin_resample
from QUANTAXIS.QAUtil.QADate_trade import QA_util_future_to_tradedatetime
from QARealtimeCollector.setting import eventmq_ip
import json
import pandas as pd
import numpy as np
import threading
import time

#直接从 l1 数据采样成>1分钟级别的数据


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
    def __init__(self, code='rb1910', freqence='60min', model='tb'):
        super().__init__()
        self.code = code
        self.freqence = freqence
        self.sub = subscriber_routing(
            host=eventmq_ip, exchange='stocktransaction', routing_key=self.code)
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_{}_{}'.format(self.freqence, self.code))
        self.sub.callback = self.callback
        self.market_data = []
        self.minute_data = {}
        self.first_minute_volume = 0   #整分钟时候的成交额
        self.first_minute_turnover = 0
        self.dt = None
        self.model = model
    
    def create_new_minute_data(self, new_tick):
        """
        {"open": 19.31, "close": 19.37, "high": 20.99, "low": 19.19, 
        "buy": 19.46, "sell": 19.47, "turnover": 41746115, "bid1_volume": 6300, 
        "bid1": 19.46, "bid2_volume": 2700, "bid2": 19.45, "bid3_volume": 22600, 
        "bid3": 19.44, "bid4_volume": 15800, "bid4": 19.43, "bid5_volume": 40200, 
        "bid5": 19.42, "ask1_volume": 8200, "ask1": 19.47, "ask2_volume": 37000, 
        "ask2": 19.48, "ask3_volume": 5838, "ask3": 19.49, "ask4_volume": 8200, 
        "ask4": 19.5, "ask5_volume": 1200, "ask5": 19.51, "code": "000001", 
        "servertime": "10:26:24", "datetime": "2021-09-15 10:26:24", 
        "price": 19.46, "vol": 817046697.19}
        """
        #整分钟时候的tick数据
        self.minute_data = {'open': new_tick['price'],
                                         'high': new_tick['price'],
                                         'low': new_tick['price'],
                                         'close': new_tick['price'],
                                         'code': new_tick['code'],
                                         'turnover': 0,
                                         'datetime': new_tick['datetime'],
                                         'volume': 0}
        self.first_minute_volume = new_tick['volume']
        self.first_minute_turnover = new_tick['turnover']
        print("新的分钟数据{} : {}".format(new_tick['datetime'], new_tick['price']))


    def update_min_data(self, new_tick):
        #一个一个的更新对比
        old_data = self.minute_data
        old_data['close'] = new_tick['price']
        old_data['high'] = old_data['high'] if old_data['high'] > new_tick['price'] else new_tick['price']
        old_data['low'] = old_data['low'] if old_data['low'] < new_tick['price'] else new_tick['price']
        old_data['datetime'] = new_tick['datetime']
        old_data['volume'] = new_tick['volume'] - self.first_minute_volume
        old_data['turnover'] = new_tick['turnover'] - self.first_minute_turnover
        self.minute_data = old_data
        
    def callback(self, a, b, c, data):
        lastest_data = json.loads(str(data, encoding='utf-8'))
        #print(lastest_data)
        if self.dt != lastest_data['datetime'][15:16] or len(self.market_data) < 1:
            self.dt = lastest_data['datetime'][15:16]
            print('new------------------')
            self.create_new_minute_data(lastest_data)
            self.market_data.append(self.minute_data)
        else:
            self.update_min_data(lastest_data)
            self.market_data[-1] = self.minute_data
            print('update----------------')
        df = pd.DataFrame(self.market_data)
        df = df.assign(datetime=pd.to_datetime(df.datetime), code=self.code, position=0,
                       tradetime=df.datetime).set_index('datetime')
        print(df)
        print('重采样后=======================================')
        res = QA_data_stockmin_resample(df, self.freqence)
        print(res)
        # print(res.iloc[-1].to_dict())
        self.pub.pub(json.dumps(
            res.reset_index().iloc[-1].to_dict(), cls=NpEncoder))

    def run(self):
        self.sub.start()


if __name__ == "__main__":
    QARTC_Resampler(code='sz000001', freqence='5min').start()
