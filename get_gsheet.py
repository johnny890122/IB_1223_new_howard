from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import numpy as np
import pandas as pd
import pickle as pkl
import main  # 計算月份是否需要備份用


def get_google_sheet(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''
    取得Google Sheet資料，並匯出成CSV
    Inputs:
    1. SCOPES: Google Sheet網址
    2. SPREADSHEET_ID: Google Sheet ID (中間那段)
    3. RANGE_NAME: 要抓取的範圍
    4. SHEET_NAME: 匯csv的檔名，若是False則不輸出csv，以return方式回傳
    '''
    # creds = None
    # if os.path.exists('token.pickle'):
    #     with open('token.pickle', 'rb') as token:
    #         creds = pkl.load(token)

    # # If there are no (valid) credentials available, let the user log in.
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    #         creds = flow.run_local_server(port=0)

    #     # Save the credentials for the next run
    #     with open('token.pickle', 'wb') as token:
    #         pkl.dump(creds, token)

    # service = build('sheets', 'v4', credentials=creds)
    # sheet = service.spreadsheets()
    # result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
    #                             range=RANGE_NAME).execute()
    # values = result.get('values', [])

    # 
    values = pd.read_excel(
        "tmp_input/OB Daily Tracker.xlsx", 
        sheet_name="OB Daily Tracker", 
        skiprows=[0,1,2,3]
    )
    keep_col = values.columns[1:13]
    values = values[keep_col]
    values = values.rename(columns={'Unnamed: 1': '', 'Unnamed: 2': ''})

    # if not values:
    #     print('No data found.')
    # else:
    #     values = pd.DataFrame(values[1:], columns=values[0])

    if SHEET_NAME:  # 要轉出CSV
        values.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False, encoding='utf_8_sig')
        print('Download {} data SUCCEED'.format(SHEET_NAME))
    else:
        return values


def get_google_sheet_commercial(range_name_list, SCOPES, SPREADSHEET_ID, SHEET_NAME=False):
    for i, range_name in enumerate(range_name_list):
        # values = get_google_sheet(SCOPES, SPREADSHEET_ID, range_name, False)
        values = pd.read_excel(
            "tmp_input/B2C S&OP Inbound_Outbound Tracking 的副本.xlsx", sheet_name=range_name, skiprows=[0], usecols=["Date", "Unnamed: 1", "IB \n(pcs)"]
        )

        if i == 0:
            commercial = values
        else:
            commercial = commercial.append(values, ignore_index=True)
    commercial.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False, encoding='utf_8_sig')
    print('Download {} data SUCCEED'.format(SHEET_NAME))


def get_google_sheet_reject(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''reject data下載後，先將空白欄位刪去'''
    # values = get_google_sheet(SCOPES, SPREADSHEET_ID, RANGE_NAME, False)
    values = pd.read_excel("tmp_input/2022年進貨相關問題 01 ~ 03 月.xlsx", sheet_name="拒收紀錄")
    values = values[values.columns[0:4]]
    values = values[values['Date'] != ""]
    
    values.to_csv('Input/api_data/{}.csv'.format(SHEET_NAME), index=False)
    print('Download {} data SUCCEED'.format(SHEET_NAME))


def get_google_sheet_abnormal(SCOPES, SPREADSHEET_ID, RANGE_NAME, SHEET_NAME=False):
    '''
    1. abnormal data下載後，將其與his_abnormal資料合併後再匯出成csv
    2. 若月份是3/6/9/12月，則將當季資料備份
    '''
    # values = get_google_sheet(SCOPES, SPREADSHEET_ID, RANGE_NAME, False)
    values = pd.read_excel("tmp_input/2022年進貨相關問題 01 ~ 03 月.xlsx", sheet_name="倉庫回報表格", skiprows=[0,1,2], usecols=["Inbound ID"])

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
        # label_gdoc.SCOPES = [label_scopes_dict[date[:7]]]
        # label_gdoc.SPREADSHEET_ID = [label_spreadsheet_id_dict[date[:7]]]
        # label_gdoc.RANGE_NAME = ['{}!A:G'.format(date_str)]
        day_label = pd.read_excel("tmp_input/新版貼標紀錄備份.xlsx", sheet_name=date_str.replace("-", ""))
        # day_label = get_google_sheet(*label_gdoc.trans())
        if i == 0:
            label_data = day_label
        else:
            label_data = label_data.append(day_label, ignore_index=True)

    label_data.to_csv('Input/api_data/{}.csv'.format(csv_name), index=False, encoding='utf_8_sig')
    print('Download {} data SUCCEED'.format(csv_name))
