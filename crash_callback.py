# 总结策略思路:
# 开仓条件---监测所有币种，当一个币种5小时均线金叉30小时均线，且在金叉之前上一个死叉之后有出现成交量激增（由暴跌引起），开仓;
# 止盈条件：动态追踪止盈止损（止盈比例5个点，止盈回调1个点，止损比例1个点，止损回调0.5个点）

# 第一步：监控5小时均线与30小时均线，如果出现金叉，标记为1
# 第二步：标记上一个5小时均线与30小时均线死叉的时间
# 第三步：标记从死叉到金叉时间段中最低点出现的位置
# 第四步：求出死叉到最低点（不包含最低点）成交量（按1小时切片）的均值
# 第五步：判断最低点成交量/均值是否大于阈值（比如2.5）
# 第六步：如果满足条件（条件1：均线金叉，条件2：最低点成交量/均值大于阈值）,开仓
# 第七步：动态追踪止盈止损（止盈比例5个点，止盈回调1个点，止损比例1个点）
# 第八步：此策略不做跟单

# encoding='utf-8'
import json
import time
from threading import Thread
import numpy as np
import requests
from loggerConfig import logger
from tools.Config import pricelimit, Trade_url, Queryorder_url, Fee, Query_tradeprice_url, updateCover_url, amountlimit, \
    Cancel_url
from tools.Kline_analyze import MA, get_all_symbol_klinedata, get_klinedata, chandelier_stop
from tools.databasePool import r2, POOL
from tools.get_market_info import get_currentprice1


def cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, direction):
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        cancelbuyparams = {"direction": direction, "symbol": symbol, "platform": platform, "orderId": orderId,
                           "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        cancelres = requests.post(Cancel_url, data=cancelbuyparams)
        res = json.loads(cancelres.content.decode())
        if res["code"] == 1:
            cur.execute("update crashlist set status=2 where strategyId=%s and orderid=%s",
                        (strategyId, orderId))
        else:
            i = "用户{}策略{}撤销{}平台订单出错,原因{}".format(userUuid, strategyId, platform, res['message'])
            print(i)
            logger.info(i)
    except Exception as e:
        i = "用户{}策略{}撤销{}平台订单出错{}".format(userUuid, strategyId, platform, e)
        print(i)
        logger.info(i)
    finally:
        conn.commit()
        cur.close()
        conn.close()


def sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, amount, entryPrice):
    conn = POOL.connection()
    cur = conn.cursor()
    currentprice = get_currentprice1(platform, symbol)
    sell_flag = 0
    try:
        # 下卖单
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 折价卖出
        sellprice = round(currentprice * 0.99, pricelimit[symbol][platform])
        try:
            x, y = str(amount).split('.')
            amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except:
            pass
        sellparams = {"direction": 2, "amount": amount, "symbol": symbol, "platform": platform,
                      "price": sellprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                      "strategyId": strategyId, 'tradetype': 1}
        i = '用户{}子账户{}暴跌反弹策略{}交易对{}开始卖出,挂单价{}'.format(userUuid, apiAccountId, strategyId, symbol, sellprice)
        print(i)
        logger.info(i)
        res = requests.post(Trade_url, data=sellparams)
        resdict = json.loads(res.content.decode())
        print(resdict)
        orderId = resdict["response"]["orderid"]  # 获取订单id
        sellinsertsql = "INSERT INTO crashlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                        "orderid,order_amount,order_price,order_time," \
                        "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                        "%s,%s)"
        cur.execute(sellinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 2, orderId, amount, sellprice, ordertime, 0, 11, 1,))
        conn.commit()
        i = "用户{}子账户{}暴跌反弹策略{}交易对{}下卖单成功".format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        logger.info(i)
        time.sleep(3)
        # 3秒后查询卖单
        queryparams = {"direction": 2, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        print(querydict)
        status = querydict['response']['status']
        if status == 'closed':
            fee = Fee[platform]['sellfee']
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                numberDeal = queryresdict['response']['totalAmount']
                sellfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = sellprice
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = amount
                sellfee = round(numberDeal * tradeprice * fee, 8)
            profit = round((tradeprice - entryPrice - (tradeprice + entryPrice) * fee) * numberDeal, 8)
            profitRate = round(profit / (entryPrice * numberDeal), 8)
            updatesql = "update crashlist set trade_amount=%s,trade_price=%s,trade_time=%s,profit=%s," \
                        "profitRate=%s,status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql,
                        (numberDeal, tradeprice, tradetime, profit, profitRate, 1, sellfee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}暴跌反弹策略{}交易对{}卖单全部成交".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            logger.info(i)
            r2.hdel('Crash_label:{}'.format(strategyId), symbol)
            sell_flag = 1
        elif status == "open":
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}子账户{}暴跌反弹策略{}交易对{}下卖单时出错{}".format(userUuid, apiAccountId, strategyId, symbol, e)
        print(i)
        logger.error(i)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4}
        res1 = requests.post(updateCover_url, data=updateparams)  # 此处链接需更换
        print(res1)
        sell_flag = 0
    finally:
        cur.close()
        conn.close()
        return sell_flag


# 计算策略总收益与收益率
def sum_profit(userUuid, apiAccountId, strategyId):
    totalprofit = 0
    totalprofitRate = 0
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        sql = "select sum(profit),sum(profitRate) from crashlist where strategyId=%s and " \
              "direction=2 and status=1 "
        cur.execute(sql, (strategyId,))
        total_profit, total_profitrate = cur.fetchone()
        if total_profit and total_profitrate:
            totalprofit = float(total_profit)
            totalprofitRate = float(total_profitrate)
    except Exception as e:
        logger.error('用户{}子账户{}暴跌反弹策略{}在查询利润时出错{}'.format(userUuid, apiAccountId, strategyId, e))
    finally:
        cur.close()
        conn.close()
        return totalprofit, totalprofitRate


def gold_cross(klinedata):
    flag = 0
    if not klinedata:
        return flag
    MA_data = MA(klinedata)
    ma_sign_array = MA_data['MA_sign'].values.tolist()
    low_array = MA_data['low'].values.tolist()
    volumn_array = MA_data['volumn'].values.tolist()
    try:
        if ma_sign_array[-1] == 1 and volumn_array:
            dead_cross_position = ma_sign_array[::-1].index(2)
            low_array = low_array[-(dead_cross_position + 1):]
            volumn_array = volumn_array[-(dead_cross_position + 1):]
            low_min_position = low_array.index(min(low_array))
            if volumn_array[:low_min_position]:
                volumn_mean = np.mean(volumn_array[:low_min_position])
                if volumn_array[low_min_position] >= volumn_mean * 2.5:
                    flag = 1
    except Exception as e:
        print(e)
    finally:
        return flag


def get_candidate_symbols(platform, stopRatio):
    symbol_pool = []
    localtime = time.localtime(time.time())
    if localtime.tm_min % 20 == 0:
        symbol_klinedata = get_all_symbol_klinedata(platform)
        for i in symbol_klinedata:
            symbol, klinedata = i
            flag = gold_cross(klinedata)
            if flag:
                try:
                    stopprice = chandelier_stop(klinedata, stopRatio)
                    currentprice = klinedata[-1][4]
                    if currentprice > stopprice:
                        symbol_pool.append(symbol)
                    print("备选交易对{}".format(symbol))
                except Exception as e:
                    print(e)
    return symbol_pool


# 买入币种池中的交易对
def startBuy(strategydata, symbol):
    userUuid = strategydata['userUuid']
    strategyId = strategydata['strategyId']
    apiAccountId = strategydata['apiAccountId']
    amount = strategydata['amount']
    platform = strategydata["platform"]
    currentprice = get_currentprice1(platform, symbol)
    conn = POOL.connection()
    cur = conn.cursor()
    try:
        ordertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        buyprice = round(currentprice * 1.01, pricelimit[symbol][platform])
        buy_amount = amount / buyprice
        try:
            x, y = str(buy_amount).split('.')
            buy_amount = float(x + '.' + y[0:amountlimit[symbol][platform]])
        except:
            pass
        buyparams = {"direction": 1, "amount": buy_amount, "symbol": symbol, "platform": platform,
                     "price": buyprice, "userUuid": userUuid, "apiAccountId": apiAccountId, "source": 11,
                     "strategyId": strategyId, 'tradetype': 1}
        i = '用户{}子账户{}暴跌反弹策略{}开始买入交易对{},挂单价{}'.format(userUuid, apiAccountId, strategyId, symbol, buyprice)
        print(i)
        logger.info(i)
        res = requests.post(Trade_url, data=buyparams)
        resdict = json.loads(res.content.decode())
        orderId = resdict["response"]["orderid"]  # 获取订单id
        buyinsertsql = "INSERT INTO crashlist(strategyId,userUuid,apiAccountId,platform,symbol,direction," \
                       "orderid,order_amount,order_price,order_time," \
                       "status,uniqueId,tradetype) VALUES(%s, %s, %s, %s, %s, %s,%s,%s,%s, %s, %s, " \
                       "%s,%s) "
        cur.execute(buyinsertsql, (
            strategyId, userUuid, apiAccountId, platform, symbol, 1, orderId, buy_amount, buyprice, ordertime, 0, 11,
            1,))
        conn.commit()
        time.sleep(3)
        i = '用户{}子账户{}暴跌反弹策略{}买入{}下单成功'.format(userUuid, apiAccountId, strategyId, symbol)
        print(i)
        logger.info(i)
        # 3秒后查询买单
        queryparams = {"direction": 1, "symbol": symbol, "platform": platform, "orderId": orderId,
                       "apiAccountId": apiAccountId, "userUuid": userUuid, "source": 11, "strategyId": strategyId}
        queryres = requests.post(Queryorder_url, data=queryparams)
        querydict = json.loads(queryres.content.decode())
        status = querydict['response']['status']
        if status == 'closed':
            fee = Fee[platform]['buyfee']
            queryparams = {"platform": platform, "symbol": symbol, "orderId": orderId, "apiId": apiAccountId,
                           "userUuid": userUuid}
            res = requests.post(Query_tradeprice_url, data=queryparams)
            queryresdict = json.loads(res.content.decode())
            try:
                tradeprice = queryresdict['response']['avgPrice']
                tradetime = queryresdict['response']['createdDate']
                totalamount = float(queryresdict['response']['totalAmount'])
                totalfees = float(queryresdict['response']['totalFees'])
                numberDeal = totalamount - totalfees
                buyfee = round(numberDeal * tradeprice * fee, 8)
            except:
                tradeprice = buyprice
                tradetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                numberDeal = round(buy_amount * 0.99799, 8)
                buyfee = round(numberDeal * tradeprice * fee, 8)
            updatesql = "update crashlist set trade_amount=%s,trade_price=%s,trade_time=%s," \
                        "status=%s,fee=%s where " \
                        "strategyId=%s and orderid=%s"
            cur.execute(updatesql, (numberDeal, tradeprice, tradetime, 1, buyfee, strategyId, orderId))
            conn.commit()
            i = "用户{}子账户{}暴跌反弹策略{}买入{}全部成交".format(userUuid, apiAccountId, strategyId, symbol)
            print(i)
            logger.info(i)
            Crash_label = {'symbol': symbol, 'entryPrice': tradeprice, 'numberDeal': numberDeal}
            r2.hset('Crash_label:{}'.format(strategyId), symbol, json.dumps(Crash_label))
        elif status == "open":
            cancel_order(userUuid, apiAccountId, strategyId, platform, symbol, orderId, 2)
    except Exception as e:
        i = "用户{}子账户{}暴跌反弹策略{}买入{}时未成交，原因{}".format(userUuid, apiAccountId, strategyId, symbol, e)
        print(i)
        logger.error(i)
        # 调用java停止策略接口
        updateparams = {'strategyId': strategyId, "status": 4, }
        res1 = requests.post(updateCover_url, data=updateparams)
        print(res1)
    finally:
        cur.close()
        conn.close()


# 执行止盈卖单
def traceSell(strategydata, symbol):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    strategyId = strategydata['strategyId']
    platform = strategydata["platform"]
    stopRatio = strategydata['stopRatio']
    Crash_label = json.loads(r2.hget('Crash_label:{}'.format(strategyId), symbol))
    sell_amount = Crash_label['numberDeal']
    entryPrice = Crash_label['entryPrice']
    symbol, kline_data = get_klinedata(platform, symbol, 3600)
    if not kline_data:
        return
    stopPrice = chandelier_stop(kline_data, stopRatio)
    currentprice = get_currentprice1(platform, symbol)
    print("交易对{}当前吊灯止损价格为:{},行情价{}".format(symbol, stopPrice, currentprice))
    # 当价格触碰了吊灯止损价格
    if currentprice < stopPrice:
        print('{}当前行情价{}触碰了吊灯止损价，开始卖出'.format(symbol, currentprice))
        sell_flag = sell_symbol(userUuid, apiAccountId, strategyId, platform, symbol, sell_amount, entryPrice)
        if sell_flag:
            totalprofit, totalprofitRate = sum_profit(userUuid, apiAccountId, strategyId)
            i = "用户{}子账户{}暴跌反弹策略{}开始计算利润{}".format(userUuid, apiAccountId, strategyId, totalprofit)
            print(i)
            logger.info(i)
            params = {'strategyId': strategyId, 'profit': totalprofit, 'profitRate': totalprofitRate}
            res = requests.post(updateCover_url, data=params)  # 更新利润的链接需更改
            resdict = json.loads(res.content.decode())
            print(resdict)


def clear_remain(strategydata):
    userUuid = strategydata['userUuid']
    apiAccountId = strategydata['apiAccountId']
    platform = strategydata["platform"]
    strategyId = strategydata['strategyId']
    crash_list = r2.hvals('Crash_label:{}'.format(strategyId))
    crash_list = [json.loads(i) for i in crash_list]
    T_sell = []
    for i in crash_list:
        symbol = i['symbol']
        sell_amount = i['numberDeal']
        entryPrice = i['entryPrice']
        T_sell.append(Thread(target=sell_symbol,
                             args=(userUuid, apiAccountId, strategyId, platform, symbol, sell_amount, entryPrice,)))
    for t in T_sell:
        t.start()
    for t in T_sell:
        t.join()
    totalprofit, totalprofitRate = sum_profit(userUuid, apiAccountId, strategyId)
    print("用户{}子账户{}暴跌反弹策略{}当前利润为{}".format(userUuid, apiAccountId, strategyId, totalprofit))
    return totalprofit, totalprofitRate


if __name__ == "__main__":
    while True:
        try:
            strategy_list = r2.hvals("Crash_strategy")
            strategy_list = [json.loads(i) for i in strategy_list]
            T = []
            for strategy_info in strategy_list:
                strategyId = strategy_info['strategyId']
                platform = strategy_info['platform']
                maxPositionNum = strategy_info['maxPositionNum']
                stopRatio = float(strategy_info['stopRatio'])
                symbol_pool = get_candidate_symbols(platform, stopRatio)
                hold_pool = r2.hkeys('Crash_label:{}'.format(strategyId))
                for symbol in hold_pool:
                    T.append(Thread(target=traceSell, args=(strategy_info, symbol)))
                # 限制最大持仓交易对的数量，如最多买入5个
                if len(hold_pool) >= maxPositionNum:
                    continue
                # 依据最大持仓数限制买入交易对的数量
                to_buy = [s for s in symbol_pool if s not in hold_pool]
                if to_buy and len(hold_pool) + len(to_buy) > maxPositionNum:
                    to_buy = to_buy[:(maxPositionNum - len(hold_pool))]
                for symbol in to_buy:
                    T.append(Thread(target=startBuy, args=(strategy_info, symbol)))
            for t in T:
                t.start()
            for t in T:
                t.join()
        except Exception as e:
            print(e)
        finally:
            time.sleep(2)
