﻿# -*- coding: utf-8 -*-

import akshare as ak
import numpy as np  
import pandas as pd  
import math
import datetime
import os
import matplotlib.pyplot as plt
import openpyxl
import time, datetime
import xlsxwriter
from matplotlib.pyplot import MultipleLocator

def get_akshare_stock_financial(xlsfile,stock):
    try:
        shname='financial'
        isExist = os.path.exists(xlsfile)
        if not isExist:
#            stock_financial_abstract_df = ak.stock_financial_abstract(stock)
#            stock_financial_abstract_df.to_excel(xlsfile,sheet_name=shname)
            stock_financial_analysis_indicator_df = ak.stock_financial_analysis_indicator(symbol=stock)
            stock_financial_analysis_indicator_df.to_excel(xlsfile,sheet_name=shname)
            print("xfsfile:%s create" % (xlsfile))
        else:
            print("xfsfile:%s exist" % (xlsfile))
            #print(stock_financial_abstract_df)
    except IOError:
        print("Error get stock financial:%s" % stock )
    else:
        return xlsfile, shname

def get_akshare_stock_trade(xlsfile,stock):
    try:
        shname='trade'
        isExist = os.path.exists(xlsfile)
        if not isExist:
            stock_a_indicator_df = ak.stock_a_lg_indicator(stock)
            stock_a_indicator_df.to_excel(xlsfile,sheet_name=shname)
            print("xfsfile:%s create" % (xlsfile))
        else:
            print("xfsfile:%s exist" % (xlsfile))
    except IOError:
        print("Error get stock trade:%s" % stock )
    else:
        return xlsfile, shname

#def get_fin_number(strcounts):
#    if strcounts is np.nan:
#        return 0
#    else:
#        counts = float(strcounts[0:-1].replace(',',''))
#        return counts

def get_fin_date(time: str) -> str:
    try:
        # Check if time is already in required format
        datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        return time
    except ValueError:
        try:
            # Try to parse as date only and append time
            datetime.datetime.strptime(time, "%Y-%m-%d")
            return f"{time} 00:00:00"
        except ValueError:
            raise ValueError("Invalid date format. Expected 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'")
    
def get_fin_number(strcounts):
    if strcounts is np.nan:
        return 0
    else:
        counts = float(strcounts)
        return counts

def get_debt_number(fin,debt):
    return float(fin)*float(debt)/100

def get_mvalue_number(tradedf,date,datecolumn,mvcolumn):
    for i,r in tradedf.iterrows():
        if r[datecolumn] ==  date:
            return r[mvcolumn]
    return 0

def get_mvalue_number2(tradedf,date,datecolumn,mvcolumn,debt):
    #for tup in zip(tradedf['trade_date'], tradedf['total_mv']):
    #    if tup[0] ==  date:
    #        return float(tup[1])*10000
    if debt == 0:
        return 0

    intdate = get_time_stamp(date)
    for tup in zip(tradedf[datecolumn], tradedf[mvcolumn]):
        if get_time_stamp(tup[0]) <= intdate:
            return float(tup[1])*10000+debt
    return 0

def get_latest30_marketvalue(findf,fincolumn,debtcol,tradedf,datecolumn,mvcolumn):
    if 0 == findf[debtcol][0]:
        return (0,0)
    count = 0
    value = 0
    days = 30
    for tup in zip(tradedf[datecolumn], tradedf[mvcolumn]):
        value += float(tup[1])*10000
        count += 1

        if count >= days:
            break
    return (findf[fincolumn][0],value/count+findf[debtcol][0])



def get_time_stamp(date):
    time1 = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
    secondsFrom1970 = time.mktime(time1.timetuple())
    #print(secondsFrom1970)
    return secondsFrom1970



def calc_value_tobinsq(row):
    fintotal = np.sum(row[0::2])
    mvtotal = np.sum(row[1::2])
    if fintotal == 0:
        return 0
    else:
        return mvtotal/fintotal



def calc_global_mises_mean(ms_tobin_df,colum):
    mises = ms_tobin_df[colum]
    return np.mean(mises)



def calc_history_mises_mean(row,ms_tobin_df,colum):
    for tup in ms_tobin_df.itertuples():
        if tup[-3] == row:
            #return np.mean(ms_tobin_df[tup[0]:][colum])
            return np.mean(ms_tobin_df[tup[0]::-1][colum])
    return 0



def calc_stock_finmv_df(datepot,stock,filefolder):
    mises_stock_df = pd.DataFrame()
    latestmv = ''
    bget = False
    try:
        isExist = os.path.exists(filefolder)
        if not isExist:
            os.makedirs(filefolder)
            print("AkShareFile:%s create" % (filefolder))
        else:
            print("AkShareFile:%s exist" % (filefolder))

        fininpath = "%s/%s%s" % (filefolder, stock, '_fin_in.xlsx')
        tradeinpath = "%s/%s%s" % (filefolder, stock, '_trade_in.xlsx')

        # 总资产22,493,600,000.00元
        finpath, finsheet = get_akshare_stock_financial(fininpath, stock)
        #print("data of path:" + finpath + "sheetname:" + finsheet)
        # 总市值11，392，881.8488百万
        tradepath, tradesheet = get_akshare_stock_trade(tradeinpath, stock)
        #print("data of path:" + tradepath + "sheetname:" + tradesheet)


        
        stock_a_indicator_df = pd.read_excel(tradepath, tradesheet, converters={'trade_date': str, 'total_mv': str})[['trade_date', 'total_mv']]
#       stock_financial_abstract_df = pd.read_excel(finpath, finsheet, converters={'截止日期': str, '资产总计': str,'长期负债合计':str})[['截止日期', '资产总计','长期负债合计']]

        stock_financial_abstract_df = pd.read_excel(finpath, finsheet, converters={'日期': str, '总资产(元)': str,'资产负债率(%)':str})[['日期', '总资产(元)', '资产负债率(%)']]
        stock_financial_abstract_df = stock_financial_abstract_df.sort_values('日期', ascending=False)
        stock_financial_abstract_df = stock_financial_abstract_df.replace('--','0')

        starttime = 0;
        if stock_financial_abstract_df.empty or stock_a_indicator_df.empty:
            bget = False;
        else:

            starttime = time.time()
            #fin_date = datepot.split(' ')[0]
            #stock_financial_abstract_df = stock_financial_abstract_df[stock_financial_abstract_df['截止日期']< fin_date]
            #print('datepot',datepot)
            #print('stock_a_indicator_df1:', stock_a_indicator_df)
            datepotstart = (datetime.datetime.strptime(datepot,"%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=-10)).strftime("%Y-%m-%d %H:%M:%S")
            datepotend = (datetime.datetime.strptime(datepot,"%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
            #print(datepotstart,datepotend)
            stock_a_indicator_df = stock_a_indicator_df[(stock_a_indicator_df['trade_date'] >datepotstart) & (stock_a_indicator_df['trade_date'] < datepotend) ]

            #print('stock_a_indicator_df2:', stock_a_indicator_df)
            
            findatecol =  stock  +  'date'
            fintotalcol = stock  + 'finance'
            debttotalcol = stock + 'debt'
            mvtotalcol =  stock  +  'maket'

#           stock_financial_abstract_df[findatecol] = stock_financial_abstract_df.apply(lambda row: get_fin_date(row['截止日期']),axis=1)
#           stock_financial_abstract_df[fintotalcol] = stock_financial_abstract_df.apply(lambda row: get_fin_number(row['资产总计']),axis=1)
#           stock_financial_abstract_df[debttotalcol] = stock_financial_abstract_df.apply(lambda row: get_fin_number(row['长期负债合计']), axis=1)

            #不确定旧的stock_financial_abstract_df = pd.read_excel在这里是否补全了日期，目前是补全了
            stock_financial_abstract_df[findatecol] = stock_financial_abstract_df.apply(lambda row: get_fin_date(row['日期']),axis=1)
            stock_financial_abstract_df[fintotalcol] = stock_financial_abstract_df.apply(lambda row: get_fin_number(row['总资产(元)']),axis=1)
            stock_financial_abstract_df[debttotalcol] = stock_financial_abstract_df.apply(lambda row: get_debt_number(row['总资产(元)'],row['资产负债率(%)']), axis=1)


            stock_financial_abstract_df[mvtotalcol] = stock_financial_abstract_df.apply(lambda row: get_mvalue_number2(stock_a_indicator_df, row[findatecol],'trade_date','total_mv',row[debttotalcol]), axis=1)


            mises_stock_df = stock_financial_abstract_df[stock_financial_abstract_df[mvtotalcol] != 0][[findatecol,fintotalcol,mvtotalcol]]
            #滤除>50 的数据,民生2008年9月数据可能有问题
            mises_stock_df = mises_stock_df[(mises_stock_df[mvtotalcol] / mises_stock_df[fintotalcol]) < 50 ][[findatecol,fintotalcol,mvtotalcol]]
            #print('mises_stock_df:',       mises_stock_df)

            latestmv = get_latest30_marketvalue(stock_financial_abstract_df,fintotalcol,debttotalcol,stock_a_indicator_df,'trade_date','total_mv')
            bget = True;
        
        endtime = time.time()
        print("Time(s) read excel",endtime-starttime)

    except IOError:
        print("read error file:%s" % stock)
    finally:
        return bget, mises_stock_df, latestmv



def get_time_df(timepath):

    isExist = os.path.exists(timepath)
    if not isExist:
        print("time path not exist:%s" % (timepath))
        return pd.DataFrame()
    else:
        print("time path exist:%s" % (timepath))

    time_df =  pd.read_excel(timepath,"analy",index_col=[0],dtype=str)
    return time_df

def get_laststock_set(hs300,datadir):

    allset = set([stock for stock in hs300])

    print('沪深300个数',len(allset))

    existset = set()
    if os.path.exists(datadir):
        filelist = os.listdir(datadir)
        existset = set([stock.split('_')[0] for stock in filelist])

    lastset = allset - existset
    
    return allset,lastset

def get_legacy_misesq(legacypath,timelist):

    mises_global_df = pd.DataFrame()
    legacy_misesq_df = pd.read_excel(legacypath,'Sheet1',index_col=[0])
    
    
    dfindex = legacy_misesq_df.index.values.tolist()
    for rowNum in range(len(dfindex)):
        timeidx = dfindex[rowNum]
        if timeidx in timelist:
           legacyline = (legacy_misesq_df[timeidx:timeidx]).iloc[:,0:-5]
           #print(legacyline)
           mises_global_df = pd.concat([mises_global_df,legacyline])
    
    #print(mises_global_df.iloc[:, -1])
    lastset = set(timelist) - set(mises_global_df.index.values.tolist())
    lastlist = list(lastset);lastlist.sort()
    #print(lastlist)
    mises_increment_df = pd.DataFrame(index=lastlist)
    mises_global_df = pd.concat([mises_global_df,mises_increment_df])
    
    return lastlist,mises_global_df

def out_put_dataframe(mises_global_df):
    x_data  =  [ dt[2:] for dt in mises_global_df.index.values.tolist() ]
    y_data  =  mises_global_df['全局均值比'].tolist()
    y_data2 = mises_global_df['历史均值比'].tolist()

    plt.plot(x_data,y_data,color='red',linewidth=2.0,linestyle='--')
    plt.plot(x_data,y_data2,color='blue',linewidth=2.0,linestyle='--')
    plt.xticks(range(len(x_data)),x_data,rotation=270)
    plt.xlabel('time',fontsize=10)
    plt.ylabel('hisratio',fontsize=10)

    x_major_locator=MultipleLocator(1)
    y_major_locator=MultipleLocator(0.1)
    ax=plt.gca()
    ax.xaxis.set_major_locator(x_major_locator)
    ax.yaxis.set_major_locator(y_major_locator)
    miny = min(min(y_data),min(y_data2)) - 0.1
    maxy = max(max(y_data),max(y_data2)) + 0.1
    plt.ylim(miny, maxy)

    #plt.show()
    imagepath =  r'./misespig.png'
    plt.savefig(imagepath)


    outanalypath = r'./misesq.xlsx'
    workbook = xlsxwriter.Workbook(outanalypath)
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': True})
    headRows = 1
    headCols = 1
    dfindex = mises_global_df.index.values.tolist()
    for rowNum in range(len(dfindex)):
        worksheet.write_string(rowNum + headRows, 0, str(dfindex[rowNum]))


    for colNum in range(len(mises_global_df.columns)):
        xlColCont = mises_global_df[mises_global_df.columns[colNum]].tolist()
        worksheet.write_string(0, colNum+headCols, str(mises_global_df.columns[colNum]), bold)
        for rowNum in range(len(xlColCont)):
            worksheet.write_number(rowNum + headRows, colNum+headCols, xlColCont[rowNum])
    workbook.close()

    print("mises q value out in :" + outanalypath)


if __name__=='__main__':

    timepath =     r'./timeex.xlsx'
    legacypath =   r'./misesqbase.xlsx'
    
    mises_time_df = get_time_df(timepath)
    timelist = mises_time_df.index.values
    #mises_global_df = pd.DataFrame(index=timelist)

    potlist,mises_global_df = get_legacy_misesq(legacypath,timelist)
    #print(potlist,mises_global_df)
    #exit(0)

    starttime = time.time()
    for pot in potlist:
        print("time pot:",pot)
        hs300 = mises_time_df.loc[pot].values.tolist()
        datadir = './dataex/' + pot.split(' ')[0]
        stockset,lastset = get_laststock_set(hs300, datadir)
        if len(lastset) >0 :
            print("time %s stock data is not complete,set:%s" % (pot,lastset))
            #continue

        for stock in stockset:
            bget,mises_stock_df,latestmv = calc_stock_finmv_df(pot,stock,datadir)
            if bget is False:
                print("get empty DataFrame:%s" % stock)
                continue

            col_name = mises_stock_df.columns.tolist()
            for tup in mises_stock_df.itertuples():
                try:
                    if tup[1] == pot:
                        mises_global_df.loc[tup[1], col_name[1]] = tup[2]
                        mises_global_df.loc[tup[1], col_name[2]] = tup[3]
                except KeyError:
                    print("stock:%s,time:%s,location error" % (stock,tup[1]))
            if pot == potlist[-1]:
               fintotalcol = stock  + 'finance'
               mvtotalcol =  stock  +  'maket'
               try:
                  mises_global_df.loc[pot, fintotalcol] = latestmv[0]
                  mises_global_df.loc[pot, mvtotalcol] = latestmv[1]
               except KeyError:
                  print("stock:%s,time:%s,location error" % (stock,pot))

    endtime = time.time()
    print("Time(s) used",endtime-starttime)


    MisesqIndex = '米塞斯指数'
    mises_global_df[MisesqIndex] = mises_global_df.apply(lambda row: calc_value_tobinsq(row), axis=1)
    mises_mean = calc_global_mises_mean(mises_global_df,MisesqIndex)

    mises_global_df['全局均值']   = mises_global_df.apply(lambda row: mises_mean, axis=1)
    mises_global_df['全局均值比'] = mises_global_df.apply(lambda row: row[MisesqIndex]/mises_mean, axis=1)

    mises_global_df['历史均值'] = mises_global_df.apply(lambda row: calc_history_mises_mean(row[MisesqIndex],mises_global_df,MisesqIndex), axis=1)
    mises_global_df['历史均值比'] = mises_global_df.apply(lambda row: row[MisesqIndex]/row['历史均值'], axis=1)
    mises_global_df[np.isnan(mises_global_df)] = 0.;

    out_put_dataframe(mises_global_df)


