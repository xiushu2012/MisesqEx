# -*- coding: utf-8 -*-

import akshare as ak
import numpy as np  
import pandas as pd  
import math
import datetime
import os,shutil
import time, datetime



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
        return '',''
    else:
        return xlsfile, shname

def get_akshare_stock_trade(xlsfile,stock):
    try:
        shname='trade'
        isExist = os.path.exists(xlsfile)
        if not isExist:
            #stock_a_indicator_df = ak.stock_a_lg_indicator(stock)
            stock_a_indicator_df = ak.stock_a_indicator_lg(stock)
            stock_a_indicator_df.to_excel(xlsfile,sheet_name=shname)
            print("xfsfile:%s create" % (xlsfile))
        else:
            print("xfsfile:%s exist" % (xlsfile))
    except IOError:
        print("Error get stock trade:%s" % stock )
        return '',''
    else:
        return xlsfile, shname

def get_fin_number(strcounts):
    if strcounts is np.nan:
        return 0
    else:
        counts = float(strcounts[0:-1].replace(',',''))
        return counts

def get_fin_date(time):
    return time+" 00:00:00"

def gen_stock_filename(stock):
    finname = "%s%s" % (stock, '_fin_in.xlsx')
    tradename = "%s%s" % (stock, '_trade_in.xlsx')
    return finname,tradename

def get_time_df(timepath):

    isExist = os.path.exists(timepath)
    if not isExist:
        print("time path not exist:%s" % (timepath))
        return pd.DataFrame()
    else:
        print("time path exist:%s" % (timepath))

    time_df =  pd.read_excel(timepath,"analy",index_col=[0],dtype=str)
    return time_df

def get_stock_finmv_file(stock,filefolder):
    bget = False
    try:
        isExist = os.path.exists(filefolder)
        if not isExist:
            os.makedirs(filefolder)
            print("AkShareFile:%s create" % (filefolder))
        else:
            print("AkShareFile:%s exist" % (filefolder))

        fininname, tradeinname = gen_stock_filename(stock)
        fininpath = "%s/%s" % (filefolder, fininname)
        tradeinpath = "%s/%s" % (filefolder, tradeinname)


        finout ,  finsheet = get_akshare_stock_financial(fininpath, stock)
        tradeout, tradesheet = get_akshare_stock_trade(tradeinpath, stock)
        if ( finout | tradeout):
            bget = True
    except IOError:
        print("read error file:%s" % stock)
    finally:
        return bget



def get_laststock_set(hs300,datadir):

    allset = hs300
    print('沪深300个数',len(allset))

    existset = set()
    
    if os.path.exists(datadir):
        filelist = os.listdir(datadir)
        existset1 = set([stock.split('_')[0]  for stock in filelist if  'fin' in stock])
        existset2 = set([stock.split('_')[0]  for stock in filelist if  'trade' in stock])
        if len(existset1)-len(existset2)>0:
            existset = existset2
        else:
            existset = existset1
    #print("get_existstock_set",existset)
    lastset = allset - existset
    #print("get_laststock_set1",lastset)

    getset = set()
    if lastset :
        for stock in lastset:
            bget = get_stock_finmv_file(stock,datadir)
            if bget is False: 
                print("get DataFrame fail:%s,folder:%s" % (stock,datadir))

            else:
                print("get DataFrame ok:%s,folder:%s" % (stock,datadir))
                getset.add(stock)
            #time.sleep(1)

    lastset = lastset - getset
    print("get_laststock_set2",lastset)
    return allset,lastset


def copy_stock_set(datamx,hs300,destdir):
    bcopy = False
    finsrcpath =''
    tradesrcpath = ''
   
    try:
        isExist = os.path.exists(destdir)
        if not isExist:
            os.makedirs(destdir)
            print("copy_stock_set:%s create" % (destdir))
        else:
            print("copy_stock_set:%s exist" % (destdir))

        for stock in hs300:
            finname, tradename = gen_stock_filename(stock)
            fintopath = "%s/%s" % (destdir, finname)
            tradetopath = "%s/%s" % (destdir, tradename)

            finsrcpath = "%s/%s" % (datamx, finname)
            tradesrcpath = "%s/%s" % (datamx, tradename)

            shutil.copy(finsrcpath,fintopath)
            shutil.copy(tradesrcpath, tradetopath)
        bcopy = True
    except IOError:
        print(finsrcpath,tradesrcpath)
        print("copy error files:%s" % destdir)
    finally:
        return bcopy


def del_stock_set(datamx,hs300):
    bdel = False
    finsrcpath = ''
    tradesrcpath = ''

    try:
        isExist = os.path.exists(datamx)
        if not isExist:
            print("del_stock_set:%s not exist" % (datamx))
            return bdel

        for stock in hs300:
            finname, tradename = gen_stock_filename(stock)
            finsrcpath = "%s/%s" % (datamx, finname)
            tradesrcpath = "%s/%s" % (datamx, tradename)
            if os.path.exists(finsrcpath):
                os.remove(finsrcpath)
                print("del file:%s ok" % finsrcpath)
            else:
                print("del file:%s,not exist" % finsrcpath)


            if os.path.exists(tradesrcpath):
                os.remove(tradesrcpath)
                print("del files:%s,ok" % tradesrcpath)
            else:
                print("del files:%s,not exist" % tradesrcpath)

        bdel = True
    except IOError:
        print("del error files:%s,%s" % (finsrcpath,tradesrcpath))
    finally:
        return bdel

if __name__=='__main__':
    from sys import argv
    if len(argv) > 1:
        flag = argv[1]
    else:
        print("'python Misesq.py [inc|com]'")
        print("setp1 'Misesq.py inc' for clear   old   hs300 in timeex")
        print("setp2 'Misesq.py com' for repeate update hs300 in timeex")
        exit(1)

    timepath = r'./timeex.xlsx'
    datamx = r'./datamx'
    dataex = r'./dataex'
    mises_time_df = get_time_df(timepath)
    potlist = mises_time_df.index.values

    if flag=='inc':
        potlist = potlist[-1:]
        hs300 = mises_time_df.loc[potlist[0]].values.tolist()
        delset = ([it for it in hs300 if not math.isnan(float(it))])
        del_stock_set(datamx,delset)
        print('data update for increment',potlist)
    else:
        print("date update for completely", potlist)

    starttime = time.time()

    hisset =  set()
    print("hisset1",hisset)

    for pot in potlist:
        print("time pot:",pot)
        hs300 = mises_time_df.loc[pot].values.tolist()
        [hisset.add(it) for it in hs300 if not math.isnan(float(it))]

    print("hisset2",hisset)

    stockset,lastset = get_laststock_set(hisset, datamx)
    if len(lastset)>0:
        print("###### get data not complete,set:%s ######" % (lastset))
    else:
        print("###### get data complete ######")


    for pot in potlist:
        print("###time pot:",pot)
        hslist = mises_time_df.loc[pot].values.tolist()
        hs300 = [it for it in hslist if not math.isnan(float(it))]
        destdir = dataex + '/' + pot.split(' ')[0]
        copyok = copy_stock_set(datamx,hs300,destdir)
        if copyok is False:
            print("time %s copy data not complete" % (pot))
        else:
            print("time %s copy data  complete" % (pot))


    endtime = time.time()
    print("Time(s) used",endtime-starttime)


