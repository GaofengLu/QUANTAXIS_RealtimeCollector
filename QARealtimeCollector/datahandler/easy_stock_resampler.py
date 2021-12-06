import datetime
import json
import logging
import multiprocessing
import os
import threading
import time
from typing import ContextManager

import click
import pandas as pd
from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread

import sys
sys.path.append("..")


from QARealtimeCollector.utils.common import create_empty_stock_df, tdx_stock_bar_resample_parallel, util_is_trade_time, \
    get_file_name_by_date, logging_csv

logger = logging.getLogger(__name__)

#从 l1 数据采样成1分钟bar的数据

class EQARTC_Stock_Resampler(QA_Thread):
    def __init__(self, code, frequency='1min'):
        super().__init__()
        self.code = code
        self.frequency = frequency
        self.sub = subscriber_routing(
            host=eventmq_ip, exchange='stocktransaction', routing_key=code)
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_1min_{}'.format(self.code))
        self.sub.callback = self.on_tick_data_callback
        self.data = {}
        self.initiated = False
        self.last_minute = ''   #判断是否到另外一分钟了
        self.last_volume = 0   #整分钟时候的成交额
        self.last_turnover = 0

    def create_new(self, new_tick):
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
        self.data = {'open': new_tick['price'],
                                         'high': new_tick['price'],
                                         'low': new_tick['price'],
                                         'close': new_tick['price'],
                                         'code': new_tick['code'],
                                         'turnover': new_tick['turnover'],
                                         'datetime': new_tick['datetime'],
                                         'volume': new_tick['volume']}
        self.last_minute = new_tick['datetime'][14:16]
        self.last_volume = new_tick['volume']
        self.last_turnover = new_tick['turnover']
        print("新的分钟数据{} : {} {}".format(new_tick['datetime'], self.last_turnover, self.last_volume))


    def update_bar(self, new_tick):
        #一个一个的更新对比
        old_data = self.data
        old_data['close'] = new_tick['price']
        old_data['high'] = old_data['high'] if old_data['high'] > new_tick['price'] else new_tick['price']
        old_data['low'] = old_data['low'] if old_data['low'] < new_tick['price'] else new_tick['price']
        old_data['datetime'] = new_tick['datetime']
        old_data['volume'] = new_tick['volume']
        old_data['turnover'] = new_tick['turnover']

        self.data = old_data

    def upcoming_data(self, new_tick):
        curtime = new_tick['datetime']
        if curtime[11:13] in ['09', '10', '11', '13', '14', '15']:
            #11，12 位是小时
            #17，18 位是秒
            #14，16 位是分钟
            try:
                if self.initiated and new_tick['datetime'][14:16] != self.last_minute:
                    self.data['volume'] = new_tick['volume'] - self.last_volume
                    self.data['turnover'] = new_tick['turnover'] - self.last_turnover
                    self.pub.pub(json.dumps(self.data))
                    self.create_new(new_tick)
                else:
                    try:
                        self.update_bar(new_tick)
                    except:
                        self.create_new(new_tick)
                    self.initiated = True

            except Exception as e:
                print(e)

    def on_tick_data_callback(self, channel, method, properties, data):
        #重采样为分钟线数据
        #1. 先采样为 1min 的数据，如果要采样5min的，则基于1分钟的重采样
        l1_ticks_data = json.loads(data)
        self.upcoming_data(l1_ticks_data)
    
    def run(self):
        self.sub.start()


if __name__ == '__main__':
    EQARTC_Stock_Resampler(code='sz000001').start()
            