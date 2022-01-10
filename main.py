from time import sleep

import numpy as np
import pandas as pd
from pandas.io import sql
import sqlite3 as lite
import tushare as ts
from datetime import datetime, timedelta

from matplotlib import pyplot as plt

# def calBoll(priceList, length=22, factor=2):
#     tl = priceList[:length]
#     yl = priceList[1:length+1]
#     kma = (np.average(tl) - np.average(yl))/np.average(yl)
#     tp = priceList[0]
#     yp = priceList[1]
#     kp = (tp - yp)/tp
#     kstd = (np.std(tl) - np.std(yl))/np.std(yl)
#     btp = np.average(tl) + factor*np.std(tl)
#     bbtm = np.average(tl) - factor*np.std(tl)
#
#     return btp,bbtm,np.average(tl), tp, kp, kma, kstd

def updateDB(client, start, end, codes):
    df = pd.DataFrame()
    cnt = 1
    for code in codes:
        print(cnt)
        df_p = ts.pro_bar(ts_code=code, api=api, adj='qfq', start_date=start, end_date=end, retry_count=5)
        df = df.append(df_p)
        cnt+=1
    sql.to_sql(df[['ts_code', 'close', 'trade_date', 'vol']], name='stock_price', con=client, if_exists='append')

def calStock(code, clt, startDay, endDay):
    df = pd.read_sql_query('select close, vol from stock_price where ts_code=\'{}\' and trade_date between \'{}\' and \'{}\' order by trade_date '.format(code, startDay, endDay), clt)
    df['Normalized'] = df['close'].apply(lambda x: x/df['close'][0] * 5)
    df['Delta Rate'] = df['close'].rolling(window=2).apply(lambda x: (x.iloc[1]-x.iloc[0])/x.iloc[0] * 100)
    df['Vol Delta'] = df['vol'].rolling(window=2).apply(lambda x: (x.iloc[1]-x.iloc[0])/x.iloc[0] * 100)
    df['30 DAY MA'] = df['Normalized'].rolling(window=22).mean()
    df['5 DAY MA'] = df['Normalized'].rolling(window=5).mean()
    df['3 DAY MV'] = df['vol'].rolling(window=3).mean()
    df['30 DAY STD'] = df['Normalized'].rolling(window=22).std()
    df['Upper Band'] = df['30 DAY MA'] + 2*df['30 DAY STD']
    df['Lower Band'] = df['30 DAY MA'] - 2*df['30 DAY STD']
    df['Band Width'] = df['Upper Band'] - df['Lower Band']
    df['3 DAY Delta Rate'] = df['close'].rolling(window=3).apply(lambda x: x.iloc[-1]/x.iloc[0] * 100)
    df['3 DAY Delta Max'] = df['close'].rolling(window=4).apply(lambda x: x.iloc[1:].max()/x.iloc[0] * 100)
    df['BW Delta'] = df['Band Width'].rolling(window=2).apply(lambda x: x.iloc[1] - x.iloc[0])
    # df['2D BW Delta'] = df['BW Delta'].rolling(window=2).apply(lambda x: x.iloc[1] - x.iloc[0])
    return df

def toSinaCode(code):
    t = code.split('.')
    return t[1].lower()+ t[0]

if __name__ == '__main__':
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
    ts.set_token("42544fd5c79c396e1021b2324a2ae9b6a12dcb84d7a5c1a23bb2756d")
    api = ts.pro_api(timeout=2)
    client = lite.connect('/Users/yubangtai/stock.db')

    # get stock list
    stockDF = api.query('stock_basic', exchange='', list_status='L',
                          fields='ts_code,name')

    needed = list(filter(lambda pair: pair[0][0:2] in ('60','00','30') and 'ST' not in pair[1], stockDF.values))

    stockMap = {}
    for stock in needed:
        stockMap[stock[0]] = stock[1]
    today = datetime.today().strftime('%Y%m%d')
    startDay = datetime.today() - timedelta(120)
    startDay = startDay.strftime('%Y%m%d')
    startDay = today

    # updateDB(client, startDay, today, stockMap.keys())

    potential = []
    codes = []
    print('开始计算')
    #
    # for i in range(20210525, 20210529):
    #     cPlus = 0
    #     cMinus = 0
    #     tPlus = 0
    #     tMinus = 0
    #     for stock in stockMap.keys():
    #         df = calStock(code=stock, clt=client, startDay=startDay, endDay= str(i))
    #         if(len(df) >= 30 and df['Delta Rate'].iloc[-4] <= 2 and
    #                 abs(df['Normalized'].iloc[-4] - df['30 DAY MA'].iloc[-4])/df['Band Width'].iloc[-4] <= 0.1):
    #             if(df['3 DAY Delta Rate'].iloc[-1]) >= 105:
    #                 cPlus += 1
    #                 if df['BW Delta'].iloc[-4] < 0 and df['Band Width'].iloc[-8:-3].mean() <= 0.9 * df['Band Width'].iloc[-13:-8].mean() \
    #                         and df['Normalized'].iloc[-12:-3].max()/df['Normalized'].iloc[-4] >= 1.1:
    #                     tPlus += 1
    #             else:
    #                 cMinus += 1
    #                 if df['BW Delta'].iloc[-4] < 0 and df['Band Width'].iloc[-8:-3].mean() <= 0.9 * df['Band Width'].iloc[-13:-8].mean() \
    #                         and df['Normalized'].iloc[-12:-3].max()/df['Normalized'].iloc[-4] >= 1.1:
    #                     tMinus += 1
    #
    #     from empiricaldist import Pmf
    #     distr = Pmf()
    #     distr['P'] = 0.1446
    #     distr['M'] = 1 - distr['P']
    #     res = distr * [tPlus/cPlus, tMinus/cMinus]
    #     res.normalize()
    #     print(res)
    for stock in stockMap.keys():
        df = calStock(code=stock, clt=client, startDay='20210301', endDay= today)
        ## 靠近中轨
        if(len(df) >= 30 and df['Delta Rate'].iloc[-1] <= 2 and
            abs(df['Normalized'].iloc[-1] - df['30 DAY MA'].iloc[-1])/df['Band Width'].iloc[-1] <= 0.1):
            if df['BW Delta'].iloc[-1] < 0 and df['Band Width'].iloc[-5:].mean() <= 0.9 * df['Band Width'].iloc[-10:-5].mean() \
            and df['Normalized'].iloc[-9:].max()/df['Normalized'].iloc[-1] >= 1.1:
                df[['Normalized', 'Upper Band', 'Lower Band', '30 DAY MA']].plot(figsize=(10, 6))
                plt.title(stockMap[stock])
                plt.grid()
                plt.show()
                # potential.append(stockMap[stock])
                codes.append(stock.split('.')[0])

        ## 超过上轨
        # if(len(df) >= 30  and df['Normalized'].iloc[-1] / df['Upper Band'].iloc[-1]) > 1.05\
        #         and df['3 DAY MV'].iloc[-1] / df['3 DAY MV'].iloc[-2] > 1.2\
        #         and df['3 DAY Delta Rate'].iloc[-1] < 125\
        #         and df['Normalized'].iloc[-1] > max(df['Normalized'].iloc[-50:-1]):
        #     df[['Normalized', 'Upper Band', 'Lower Band', '30 DAY MA']].plot(figsize=(10, 6))
        #     plt.title(stockMap[stock])
        #     plt.grid()
        #     plt.show()
        #     potential.append(stock.split('.')[0])

            # codes.append(stock.split('.')[0])
    #         if(df['3 DAY Delta Rate'].iloc[-1]) >= 5 and \
    #                  df['Band Width'].iloc[-33:-18].mean() < 0.9 * df['Band Width'].iloc[-18:-3].mean() and \
    #                   (df['Normalized'].iloc[-4] - df['Normalized'].iloc[-9])/df['Normalized'].iloc[-9] <= -0.05 and \
    #                   df['BW Delta'].iloc[-4] < 0:
    #             df[['Normalized', 'Upper Band', 'Lower Band', '30 DAY MA']].iloc[:-3, :].plot(figsize=(10, 6))
    #             plt.title(stockMap[stock])
    #             plt.grid()
    #             plt.show()
    #             potential.append(stockMap[stock])
    #             codes.append(stock.split('.')[0])
        # if len(df) > 22:
            #趋近下界线
            # if -0.1 <= df['BW Delta'].iloc[-1] <= 0 and df['Normalized'].iloc[-1] - df['Lower Band'].iloc[-1] <= 0.1:
            #     df[['Normalized', 'Upper Band', 'Lower Band' ]].plot(figsize=(10, 6))
            #     plt.title(stockMap[stock])
            #     plt.grid()
            #     plt.show()
            #     potential.append(stockMap[stock])
            #     codes.append(stock)
            # #接近上界限
            # if (df['Normalized'].iloc[-1] - df['Upper Band'].iloc[-1])/df['Upper Band'].iloc[-1] >= 0.02 \
            #         and df['Band Width'].iloc[-1] > df['Band Width'].iloc[-2] \
            #         and df['Band Width'].iloc[-5:].mean() < df['Band Width'].iloc[-15:-5].mean():
            #     df[['Normalized', 'Upper Band', 'Lower Band']].plot(figsize=(10, 6))
            #     plt.title(stockMap[stock])
            #     plt.grid()
            #     plt.show()
            #     potential.append(stockMap[stock])
            #     codes.append(stock.split('.')[0])
            #回踩中轨(22日均线)，通道变宽
            # if abs((df['Normalized'].iloc[-1]-df['30 DAY MA'].iloc[-1])/df['Band Width'].iloc[-1] - 0.05) <= 0.15 and df['Delta Rate'].iloc[-1] > -6\
            #         and (df['Upper Band'].iloc[-1] - df['Lower Band'].iloc[-1]) / df['Lower Band'].iloc[-1] > 0.12 \
            #         and (df['Normalized'] - df['30 DAY MA']).iloc[-5:].min() > 0 \
            #         and df['Band Width'].iloc[-5:].mean() < df['Band Width'].iloc[-10:-5].mean() \
            #         and df['Band Width'].iloc[-11:].mean() > df['Band Width'].iloc[-22:-11].mean():
            #     df[['Normalized', 'Upper Band', 'Lower Band', '30 DAY MA']].plot(figsize=(10, 6))
            #     plt.title(stockMap[stock])
            #     plt.grid()
            #     plt.show()
            #     potential.append(stockMap[stock])
            #     codes.append(stock.split('.')[0])
    client.close()


    # df = pd.read_sql('select ts_code, trade_date, close from stock_price where ts_code = "002168.SZ"', client)
    # df = ts.pro_bar(ts_code='605133.SH', api=api, adj='qfq', start_date=startDay, end_date=today, retry_count=2)
    # df = df.sort_index(ascending=False)
    # df.index = range(0, len(df.index))
    #
    # df['30 DAY MA'] = df['close'].rolling(window=22).mean()
    # df['30 DAY STD'] = df['close'].rolling(window=22).std()
    # df['Upper Band'] = df['30 DAY MA'] + 2*df['30 DAY STD']
    # df['Lower Band'] = df['30 DAY MA'] - 2*df['30 DAY STD']
    # df['Band Width'] = df['Upper Band'] - df['Lower Band']
    # df['STD Delta'] = df['Band Width'].rolling(window=2).apply(lambda x: x.iloc[1] - x.iloc[0])
    # df['Price Delta'] = df['close'].rolling(window=2).apply(lambda x: x.iloc[1] - x.iloc[0])
