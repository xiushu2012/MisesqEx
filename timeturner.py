# -*- coding: utf-8 -*-

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


def get_hs300_time_df(timepath):

    isExist = os.path.exists(timepath)
    if not isExist:
        print("time path not exist:%s" % (timepath))
        return pd.DataFrame()
    else:
        print("time path exist:%s" % (timepath))

    time_list = pd.read_excel(timepath, "analy")['date'].values.tolist()
    time_df = pd.DataFrame(index=time_list)
    return time_df

def get_hs300_data_df(hspath):

    isExist = os.path.exists(hspath)
    if not isExist:
        print("hspath path not exist:%s" % (hspath))
        return pd.DataFrame()
    else:
        print("hspath path exist:%s" % (hspath))

    hs300_df = pd.read_excel(hspath, 'data', converters={'变更日期': str, '成份证券代码': str,'成份证券简称': str, '变动方式': str})[['变更日期', '成份证券代码','成份证券简称', '变动方式']]
    return hs300_df


def out_put_df(result_df):
    outanalypath = './' +'timeex.xlsx'
    workbook = xlsxwriter.Workbook(outanalypath)
    worksheet = workbook.add_worksheet(name='analy')
    bold = workbook.add_format({'bold': True})
    headRows = 1
    headCols = 1
    dfindex = result_df.index.values.tolist()
    for rowNum in range(len(dfindex)):
        worksheet.write_string(rowNum + headRows, 0, str(dfindex[rowNum]))


    for colNum in range(len(result_df.columns)):
        xlColCont = result_df[result_df.columns[colNum]].tolist()
        worksheet.write_string(0, colNum+headCols, str(result_df.columns[colNum]), bold)
        for rowNum in range(len(xlColCont)):
            worksheet.write_string(rowNum + headRows, colNum+headCols, xlColCont[rowNum])
    workbook.close()
    print("result out in :" + outanalypath)
    

if __name__=='__main__':
    from sys import argv
    
    timepath = r'./time.xlsx'
    hsinoutpath = r'./300inout.xlsx'
    if len(argv) > 2:
        timepath = argv[1]
        hsinoutpath = argv[2]
    else:
        print("python timeturnner.py [timefile] [300inout]")

    hs300_time_df = get_hs300_time_df(timepath)
    hs300_data_df = get_hs300_data_df(hsinoutpath)

    hs300_timedata_df = pd.DataFrame()
    for time in hs300_time_df.index.values:
        codedict = {}
        hs300_selected_df = hs300_data_df[hs300_data_df['变更日期'] < time][['变更日期', '成份证券代码','成份证券简称', '变动方式']]
        for tup in hs300_selected_df.itertuples():
           code = tup[2]
           count = 0 
           if tup[4]=='调入':
               count = 1
           else:
               count = -1
           
           if code in codedict.keys():
               codedict[code] = codedict[code]+count
           else:
               codedict[code] = count

        selectdict = {};seq = 0
        for key in codedict.keys():
            if codedict[key] == 1:
              selectdict[seq] = key
              seq = seq + 1 
     
        new=pd.DataFrame(selectdict,index=[time])
        #hs300_timedata_df = hs300_timedata_df.append(new)
        hs300_timedata_df = pd.concat([hs300_timedata_df,new])
    hs300_timedata_df = hs300_timedata_df.where((hs300_timedata_df.notna()),'')
    print(hs300_timedata_df)
    out_put_df(hs300_timedata_df)


