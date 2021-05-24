# encoding='utf-8'
import json
import time
from threading import Thread
import pandas as pd
import requests
from tools.Config import symbols

# import talib

"""定义带返回值的线程类"""


class MyThread(Thread):
    def __init__(self, func, args):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


# 获取k线数据（现货）
def get_klinedata(platform, symbol, granularity):
    klinedata = []
    try:
        # [时间，开盘价，最高价，最低价，收盘价，交易量]
        if platform == "okex":  # 200条数据，时间粒度60,300,3600,86400……
            res = requests.get("https://www.okex.com/api/spot/v3/instruments/{}/candles?granularity={}".format(
                symbol.upper().replace("_", "-"), granularity), timeout=1)
            klinedata = json.loads(res.content.decode())[::-1]  # 时间升序排列
            for i in klinedata:
                t = i[0].replace("T", " ").replace(".000Z", "")
                timeStruct = time.strptime(t, "%Y-%m-%d %H:%M:%S")
                timeStamp = int(time.mktime(timeStruct)) + 60 * 60 * 8
                i[0] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeStamp))
        if platform == "huobi":  # 200条数据，时间粒度1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1mon, 1week, 1year
            huobi_granularity_dict = {60: "1min", 300: "5min", 900: "15min", 1800: "30min", 3600: "60min",
                                      14400: "4hour", 86400: "1day", 604800: "1week", 2592000: "mon",
                                      946080000: "1year"}
            res = requests.get("https://api.huobi.pro/market/history/kline?period={}&size=200&symbol={}".format(
                huobi_granularity_dict[granularity], symbol.replace("_", "")), timeout=1)
            data = json.loads(res.content.decode())["data"]
            klinedata = []
            for i in data:
                l = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i["id"])),
                    i["open"],
                    i["high"],
                    i["low"],
                    i["close"],
                    i["amount"]
                ]
                klinedata.append(l)
            klinedata = klinedata[::-1]
        if platform == "binance":  # 200条数据，时间粒度1m, 5m, 15m, 30m, 1h, 4h, 1d
            binance_granularity_dict = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h",
                                        14400: "4h", 86400: "1d"}
            res = requests.get("https://www.binancezh.cc/api/v3/klines?symbol={}&interval={}&limit=200".format(
                symbol.upper().replace("_", ""), binance_granularity_dict[granularity]), timeout=1)
            data = json.loads(res.content.decode())
            klinedata = []
            for i in data:
                l = [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(i[0])[:10]))),
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                ]
                klinedata.append(l)
    except:
        pass
    finally:
        symbol_klinedata = (symbol, klinedata)
    return symbol_klinedata


# 获取三个交易所的k线数据，返回不为空的那个
def get_all_klinedata(symbol, granularity):
    t1 = MyThread(get_klinedata, args=("okex", symbol, granularity))
    t2 = MyThread(get_klinedata, args=("binance", symbol, granularity))
    t3 = MyThread(get_klinedata, args=("huobi", symbol, granularity))
    t1.start()
    t2.start()
    t3.start()
    t1.join(timeout=10)
    t1.join(timeout=10)
    t1.join(timeout=10)
    klinedata1 = t1.get_result()
    klinedata2 = t2.get_result()
    klinedata3 = t3.get_result()
    if klinedata1 is not None:
        return "okex", klinedata1
    else:
        if klinedata2 is not None:
            return "binance", klinedata2
        else:
            if klinedata3 is not None:
                return "huobi", klinedata3
            else:
                return "", []


# 获取所有交易对的K线
def get_all_symbol_klinedata(platform):
    result_list = []
    thread_list = []
    for symbol in symbols:
        t = MyThread(get_klinedata, (platform, symbol, 3600,))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()
        result_list.append(t.get_result())
    return result_list


# 简单移动平均线
def MA(klinedata):
    dataframe = dict({'time': [i[0] for i in klinedata],
                      'low': [float(i[3]) for i in klinedata],
                      "close": [float(i[4]) for i in klinedata],
                      "volumn": [float(i[5]) for i in klinedata]})
    data = pd.DataFrame(dataframe)
    data['MA5'] = data['close'].rolling(5).mean()
    data['MA30'] = data['close'].rolling(30).mean()
    data['MA_sign'] = 0
    MA_position = data['MA5'] > data['MA30']
    data.loc[MA_position[(MA_position == True) & (MA_position.shift() == False)].index, 'MA_sign'] = 1  # 金叉
    data.loc[MA_position[(MA_position == False) & (MA_position.shift() == True)].index, 'MA_sign'] = 2  # 死叉
    return data


def chandelier_stop(klinedata, stopratio):
    dataframe = dict({'high': [float(i[2]) for i in klinedata],
                      'low': [float(i[3]) for i in klinedata],
                      "close": [float(i[4]) for i in klinedata],})
    df = pd.DataFrame(dataframe)
    stopPrice = []
    try:
        for i in range(0, len(df)):
            df.loc[df.index[i], 'TR'] = max((df['close'][i] - df['low'][i]),
                                            abs(df['high'][i] - df['close'].shift()[i]),
                                            abs(df['low'][i] - df['close'].shift()[i]))
        df['ATR'] = df['TR'].rolling(14).mean()
        df['HH'] = df['high'].rolling(22).max()
        df['LL'] = df['low'].rolling(22).min()
        df['stopPrice'] = df['HH'] - stopratio * df['ATR']
        stopPrice = df['stopPrice'].values.tolist()[-1]
    except:
        pass
    return stopPrice


if __name__ == "__main__":
    # symbol_klinedata = get_all_symbol_klinedata('huobi')
    # print(symbol_klinedata)
    symbol, klinedata = get_klinedata('huobi', 'waves_usdt', 3600)
    print(klinedata)
    if klinedata:
        stopPrice = chandelier_stop(klinedata, 2)
        print(stopPrice[-30])
