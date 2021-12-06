import json
import threading
import datetime
from datetime import datetime as dt, timezone, timedelta, date
import datetime
import time as timer
import numba as nb
import easyquotation as eq

from QAPUBSUB.consumer import subscriber_routing
from QAPUBSUB.producer import publisher, publisher_routing
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from QARealtimeCollector.setting import eventmq_ip
from QARealtimeCollector.connector.easyq import Easyq_Executor

try:
    import QUANTAXIS as QA
    from QUANTAXIS.QAUtil.QAParameter import ORDER_DIRECTION
    from QUANTAXIS.QAUtil.QASql import QA_util_sql_mongo_sort_ASCENDING
    from QUANTAXIS.QAUtil.QADate_trade import (
        QA_util_if_tradetime,
        QA_util_get_pre_trade_date,
        QA_util_get_real_date,
        trade_date_sse
    )
    from QUANTAXIS.QAData.QADataStruct import (
        QA_DataStruct_Index_min, 
        QA_DataStruct_Index_day, 
        QA_DataStruct_Stock_day, 
        QA_DataStruct_Stock_min
    )
    from QUANTAXIS.QAIndicator.talib_numpy import *
    from QUANTAXIS.QAUtil.QADate_Adv import (
        QA_util_timestamp_to_str,
        QA_util_datetime_to_Unix_timestamp,
        QA_util_print_timestamp
    )
    from QUANTAXIS.QAUtil import (
        DATABASE,
        QASETTING,
        QA_util_log_info, 
        QA_util_log_debug, 
        QA_util_log_expection,
        QA_util_to_json_from_pandas
    )
except:
    print('PLEASE run "pip install QUANTAXIS" before call GolemQ.cli.sub modules')
    pass

class _const:
    class ConstError(TypeError):pass
    def __setattr__(self,name,value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" %name)
        if not name.isupper():
            raise self.ConstCaseError("const name '%s' is not all uppercase" % name)

        self.__dict__[name] = value
        
class EXCHANGE(_const):
    XSHG = 'XSHG'
    SSE = 'XSHG'
    SH = 'XSHG'
    XSHE = 'XSHE'
    SZ = 'XSHE'
    SZE = 'XSHE'


def normalize_code(symbol, pre_close=None):
    """
    归一化证券代码

    :param code 如000001
    :return 证券代码的全称 如000001.XSHE
    """
    if (not isinstance(symbol, str)):
        return symbol

    if (symbol.startswith('sz') and (len(symbol) == 8)):
        ret_normalize_code = '{}.{}'.format(symbol[2:8], EXCHANGE.SZ)
    elif (symbol.startswith('sh') and (len(symbol) == 8)):
        ret_normalize_code = '{}.{}'.format(symbol[2:8], EXCHANGE.SH)
    elif (symbol.startswith('00') and (len(symbol) == 6)):
        if ((pre_close is not None) and (pre_close > 2000)):
            # 推断是上证指数
            ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SH)
        else:
            ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SZ)
    elif ((symbol.startswith('399') or symbol.startswith('159') or \
        symbol.startswith('150')) and (len(symbol) == 6)):
        ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SH)
    elif ((len(symbol) == 6) and (symbol.startswith('399') or \
        symbol.startswith('159') or symbol.startswith('150') or \
        symbol.startswith('16') or symbol.startswith('184801') or \
        symbol.startswith('201872'))):
        ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SZ)
    elif ((len(symbol) == 6) and (symbol.startswith('50') or \
        symbol.startswith('51') or symbol.startswith('60') or \
        symbol.startswith('688') or symbol.startswith('900') or \
        (symbol == '751038'))):
        ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SH)
    elif ((len(symbol) == 6) and (symbol[:3] in ['000', '001', '002',
                                                 '200', '300'])):
        ret_normalize_code = '{}.{}'.format(symbol, EXCHANGE.SZ)
    elif symbol.startswith('XSHG'):
        ret_normalize_code = '{}.{}'.format(symbol[5:], EXCHANGE.SH)
    elif symbol.startswith('XSHE'):
        ret_normalize_code = '{}.{}'.format(symbol[5:], EXCHANGE.SZ)
    elif (symbol.endswith('XSHG') or symbol.endswith('XSHE')):
        ret_normalize_code = symbol
    else:
        print(u'normalize_code():', symbol)
        ret_normalize_code = symbol

    return ret_normalize_code


def formater_l1_tick(code:str, l1_tick:dict) -> dict:
    """
    处理分发 Tick 数据，新浪和tdx l1 tick差异字段格式化处理
    """
    '''
    if ((len(code) == 6) and code.startswith('00')):
        l1_tick['code'] = normalize_code(code, l1_tick['now'])
    else:
        l1_tick['code'] = normalize_code(code)
    '''
    l1_tick['code'] = code
    l1_tick['servertime'] = l1_tick['time']
    l1_tick['datetime'] = '{} {}'.format(l1_tick['date'], l1_tick['time'])
    l1_tick['price'] = l1_tick['now']
    del l1_tick['date']
    del l1_tick['time']
    del l1_tick['now']
    del l1_tick['name']
    #print(l1_tick)
    return l1_tick


def formater_l1_ticks(l1_ticks:dict, codelist:list=None, stacks=None, symbol_list=None) -> dict:
    """
    处理 l1 ticks 数据
    """
    if (stacks is None):
        l1_ticks_data = []
    else:
        l1_ticks_data = stacks

    for code, l1_tick_values in l1_ticks.items():
        #l1_tick = namedtuple('l1_tick', l1_ticks[code])
        #formater_l1_tick_jit(code, l1_tick)
        if (codelist is None) or \
            (code in codelist):
            l1_tick = formater_l1_tick(code, l1_tick_values)
            l1_ticks_data.append(l1_tick)

    return l1_ticks_data

class EQARTC_Stock(QA_Thread):
    def __init__(self):
        super().__init__()
        self.quotation = eq.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq'] 
        self.codelist = []
        self.sub = subscriber_routing(host=eventmq_ip,
                                      exchange='QARealtime_Market', routing_key='stock')
        self.sub.callback = self.callback
        self.pub = publisher_routing(
            host=eventmq_ip, exchange='stocktransaction', routing_key='')
        threading.Thread(target=self.sub.start, daemon=True).start()

    def subscribe(self, code):
        """继续订阅

        Arguments:
            code {[type]} -- [description]
        """
        if code not in self.codelist:
            self.codelist.append(code)

    def unsubscribe(self, code):
        self.codelist.remove(code)

    def callback(self, a, b, c, data):
        data = json.loads(data)
        if data['topic'] == 'subscribe':
            print('receive new subscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.subscribe(item)
            else:
                self.subscribe(new_ins)
        if data['topic'] == 'unsubscribe':
            print('receive new unsubscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.unsubscribe(item)
            else:
                self.unsubscribe(new_ins)

    def get_data(self):
        data = self.get_realtime(self.codelist)
        #data = QA_util_to_json_from_pandas(data.reset_index())
        #self.pub.pub(json.dumps(data))



    def run(self):
        sleep = 3
        _time1 = dt.now()
        get_once = True
        # 开盘/收盘时间
        end_time = dt.strptime(str(dt.now().date()) + ' 16:30', '%Y-%m-%d %H:%M')
        start_time = dt.strptime(str(dt.now().date()) + ' 09:15', '%Y-%m-%d %H:%M')
        day_changed_time = dt.strptime(str(dt.now().date()) + ' 01:00', 
                                    '%Y-%m-%d %H:%M')
        while (dt.now() < end_time):
            # 开盘/收盘时间
            end_time = dt.strptime(str(dt.now().date()) + ' 16:30', '%Y-%m-%d %H:%M')
            start_time = dt.strptime(str(dt.now().date()) + ' 09:15', '%Y-%m-%d %H:%M')
            day_changed_time = dt.strptime(str(dt.now().date()) + ' 01:00', 
                                        '%Y-%m-%d %H:%M')
            _time = dt.now()

            if QA_util_if_tradetime(_time) and \
                (dt.now() < day_changed_time):
                # 日期变更，写入表也会相应变更，这是为了防止用户永不退出一直执行
                print(u'当前日期更新~！ {} '.format(datetime.date.today()))
                #database = collections_of_today()
                print(u'Not Trading time 现在是中国A股收盘时间 {}'.format(_time))
                timer.sleep(sleep)
                continue

            if QA_util_if_tradetime(_time) or \
                (get_once):  # 如果在交易时间
                l1_ticks = self.quotation.market_snapshot(prefix=True)
                #l1_ticks = self.get_realtime(full_market=True)

                #l1_ticks = self.get_realtime(self.codelist)
                l1_ticks_data = formater_l1_ticks(l1_ticks)

                if (dt.now() < start_time) or \
                    ((len(l1_ticks_data) > 0) and \
                    (dt.strptime(l1_ticks_data[-1]['datetime'], 
                                '%Y-%m-%d %H:%M:%S') < dt.strptime(str(dt.now().date()) + ' 00:00', 
                                                                    '%Y-%m-%d %H:%M'))):
                    print(u'Not Trading time 现在是中国A股收盘时间 {}'.format(_time))
                    timer.sleep(sleep)
                    continue
                
                else:
                    #一个股票一个股票的pub出去
                    for tick in l1_ticks_data:
                        self.pub.pub(json.dumps(tick), routing_key=tick['code'])

                if (get_once != True):
                    print(u'Trading time now 现在是中国A股交易时间 {}\nProcessing ticks data cost:{:.3f}s'.format(dt.now(),
                        (dt.now() - _time).total_seconds()))
                if ((dt.now() - _time).total_seconds() < sleep):
                    timer.sleep(sleep - (dt.now() - _time).total_seconds())
                print('Program Last Time {:.3f}s'.format((dt.now() - _time1).total_seconds()))
                get_once = False
            else:
                print(u'Not Trading time 现在是中国A股收盘时间 {}'.format(_time))
                timer.sleep(sleep)

        # 每天下午5点，代码就会执行到这里，如有必要，再次执行收盘行情下载，也就是 QUANTAXIS/save X
        save_time = dt.strptime(str(dt.now().date()) + ' 17:00', '%Y-%m-%d %H:%M')
        if (dt.now() > end_time) and \
            (dt.now() < save_time):
            # 收盘时间 下午16:00到17:00 更新收盘数据
            # 我不建议整合，因为少数情况会出现 程序执行阻塞 block，
            # 本进程被阻塞后无人干预第二天影响实盘行情接收。
            # save_X_func()
            pass

        # While循环每天下午5点自动结束，在此等待13小时，大概早上六点结束程序自动重启
        print(u'While循环每天下午5点自动结束，在此等待13小时，大概早上六点结束程序自动重启，这样只要窗口不关，永远每天自动收取 tick')
        timer.sleep(40000)



if __name__ == "__main__":
    r = EQARTC_Stock()
    r.subscribe('000001')
    r.subscribe('000002')
    r.start()
    '''
    r.subscribe('600010')

    import json
    import time
    time.sleep(2)
    publisher_routing(exchange='QARealtime_Market', routing_key='stock').pub(json.dumps({
        'topic': 'subscribe',
        'code': '600012'
    }), routing_key='stock')

    r.unsubscribe('000001')
    '''


