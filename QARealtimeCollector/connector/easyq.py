import datetime
import os
import queue
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Timer
import easyquotation as eq
import pandas as pd
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_tradetime
from QUANTAXIS.QAUtil.QASetting import DATABASE, stock_ip_list
from QUANTAXIS.QAUtil.QASql import QA_util_sql_mongo_sort_ASCENDING
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from QUANTAXIS import QA_fetch_stock_block_adv, QA_data_tick_resample_1min
from QARealtimeCollector.utils.common import get_file_name_by_date, logging_csv
logger = logging.getLogger(__name__)

class Easyq_Executor(QA_Thread):
    def __init__(self):
        super().__init__(name='Easyq_Executor')
        self.quotation = eq.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq'] 

    def get_market(self, code):
        code = str(code)
        if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
            return 1
        return 0

    def get_realtime(self, codes=None, prefix=False, full_market=False):
        try:
            if full_market:
                data = self.quotation.market_snapshot(prefix=False)
            else:
                data = self.quotation.stocks(codes, prefix=prefix)
            return data
        except:
            return None


if __name__ == '__main__':
    stock_list = QA_fetch_stock_block_adv().get_block('上证50').code
    data = Easyq_Executor().get_realtime(stock_list)
    print(data)

