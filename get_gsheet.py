from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import numpy as np
import pandas as pd
import pickle as pkl
import main  # 計算月份是否需要備份用
from google.oauth2.service_account import Credentials
import gspread
import time

scope = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
gs = gspread.authorize(creds)

def get_google_sheet(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''
    取得Google Sheet資料，並匯出成CSV
    Inputs:
    1. SCOPES: Google Sheet網址
    2. SPREADSHEET_ID: Google Sheet ID (中間那段)
    3. RANGE_NAME: 要抓取的範圍
    4. SHEET_NAME: 匯csv的檔名，若是False則不輸出csv，以return方式回傳
    '''

    ob_gsheet = gs.open_by_url(SCOPES).worksheet("OB Daily Tracker")
    values = pd.DataFrame(ob_gsheet.get_values()[4:])
    header = values.iloc[0]
    header[0] = "Unnamed: 1"
    header[1] = 'Unnamed: 2'
    values.rename(columns=header, inplace = True)
    values.drop(values.index[0], inplace = True)

    keep_col = []
    for v in values.columns[1:13]:
        if v != "":
            keep_col.append(v)

    values = values[keep_col]
    values = values.rename(columns={'Unnamed: 1': '', 'Unnamed: 2': ''})

    if SHEET_NAME:  # 要轉出CSV
        values.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False, encoding='utf_8_sig')
        print('Download {} data SUCCEED'.format(SHEET_NAME))
    else:
        return values


def get_google_sheet_commercial(range_name_list, SCOPES, SPREADSHEET_ID, SHEET_NAME=False):

    commercial_gsheet = gs.open_by_url(SCOPES)
    for i, range_name in enumerate(range_name_list):
        # values = get_google_sheet(SCOPES, SPREADSHEET_ID, range_name, False)
        values = pd.DataFrame(commercial_gsheet.worksheet(range_name).get_values()[1:])
        values.rename(columns=values.iloc[0], inplace = True)
        values.drop(values.index[0], inplace = True)
        values = values[["Date", "", "IB \n(pcs)"]]

        if i == 0:
            commercial = values
        else:
            commercial = commercial.append(values, ignore_index=True)
    commercial.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False, encoding='utf_8_sig')
    print('Download {} data SUCCEED'.format(SHEET_NAME))


def get_google_sheet_reject(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''reject data下載後，先將空白欄位刪去'''
    # values = get_google_sheet(SCOPES, SPREADSHEET_ID, RANGE_NAME, False)
    # values = pd.read_excel("tmp_input/2022年進貨相關問題 01 ~ 03 月.xlsx", sheet_name="拒收紀錄")

    reject_gsheet = gs.open_by_url(SCOPES).worksheet("拒收紀錄")
    values = pd.DataFrame(reject_gsheet.get_values())
    values.rename(columns=values.iloc[0], inplace = True)
    values.drop(values.index[0], inplace = True)

    values = values[values.columns[0:4]]
    values = values[values['Date'] != ""]
    
    values.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False)
    print('Download {} data SUCCEED'.format(SHEET_NAME))


def get_google_sheet_abnormal(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''
    1. abnormal data下載後，將其與his_abnormal資料合併後再匯出成csv
    2. 若月份是3/6/9/12月，則將當季資料備份
    '''

    abnormal_gsheet = gs.open_by_url(SCOPES).worksheet("倉庫回報表格")
    abs_raw = abnormal_gsheet.get_values()

    header_row_idx = 0
    while abs_raw[header_row_idx][0] != "日期":
        header_row_idx += 1  

    abnormal_header = abs_raw[header_row_idx]
    values = pd.DataFrame(abs_raw[header_row_idx + 1:])
    values.columns = abnormal_header

    values = values[["Inbound ID"]]
    values.drop(values.index[0], inplace = True)

    hist_data = pd.read_csv("Input/historical_data/his_abnormal.csv", encoding='utf_8_sig')

    # abnormal data下載後，將其與his_abnormal資料合併後再匯出成csv
    values = pd.concat([values, hist_data])
    values.drop_duplicates(inplace=True)
    values.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False, encoding='utf_8_sig')

    # 若月份是3/6/9/12月，則將當季資料備份
    if main.start_date.month % 3 != 0:  # 每季結束需備份資料至pickle中
        values.to_csv("Input/historical_data/his_abnormal.csv", encoding='utf_8_sig')
    print('Download {} data SUCCEED'.format(SHEET_NAME))

# get_gsheet.get_label_data(label_gdoc, label_scopes_dict, label_spreadsheet_id_dict, range_date.astype('str'), 'label')

def get_label_data(label_gdoc, label_scopes_dict, label_spreadsheet_id_dict, label_date_range, csv_name):
    '''
    抓取貼標資料，因貼標資料為每天一張工作表，不能直接使用get_google_sheet抓，要額外執行一些步驟
    Input:
    1. label_gdoc: Class Object, 串接get_google_sheet用
    2. label_scopes_dict: Dictionary, 每月貼標資料的Google Sheet
    3. label_spreadsheet_id_dict: Dictionary, 每月貼標資料的Google Sheet ID
    4. label_date_range: List, 欲抓取日期
    5. csv_name: 要匯出的csv檔名稱(若存在Class中會在get_google_sheet一直匯出，故寫在此函數中)
    Output: label_date_range內每天的貼標資料
    '''
    # Step 1. 抓取每天Label資料
    print(label_gdoc)
    print(label_scopes_dict)
    print(label_spreadsheet_id_dict)
    print(label_date_range)


    for i, date in enumerate(label_date_range):
        date_str = date[:4] + date[5:7] + date[8:10]  # get sheet name 'YYYYmmdd'
        label_gdoc.SCOPES = label_scopes_dict[date[:7]]
        label_gsheet = gs.open_by_url(label_gdoc.SCOPES).worksheet(date_str.replace("-", ""))

        day_label = pd.DataFrame(label_gsheet.get_values()[1:])
        if i == 0:
            label_data = day_label
        else:
            label_data = label_data.append(day_label, ignore_index=True)
        time.sleep(3)

    #     # label_data.rename(columns= header)
    # print(label_data)
    # print(label_data.index)
    # print(label_data.columns)

    label_data.to_csv('Input/api_data/{}.csv'.format(csv_name), index=False, encoding='utf_8_sig')
    print('Download {} data SUCCEED'.format(csv_name))

