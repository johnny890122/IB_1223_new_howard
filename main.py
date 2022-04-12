from pandas.core import indexing
from pandas.core.indexes.datetimes import date_range
import get_gsheet  # 用來讀取Google Sheet資料
import get_sql_data  # 用來讀取SQL資料
import assist_funcs  # 用來放一些比較不重要的小函數
import numpy as np
import pandas as pd
import datetime as dt
import socket
import os
import pickle
import time
import warnings
warnings.simplefilter("ignore")
socket.setdefaulttimeout(180)  # 超過180秒才會報超時

'''
程式碼說明 2021/08/23 Mic Tu
操作說明:
1. 於第46、47行輸入開始、結束日期，執行即可得到結果，開始日期必須是禮拜一
2. 於第48行輸入first_accumulated_backlog，用於Step 3計算IB metric用
3. 若計算weighted IPH的權重有改變，可更動第49~54行
4. 更改第88~125行的gsheet網址
5. 如果出現"socket.timeout: The read operation timed out"，代表抓gsheet超時，可以更改第15行延長報超時的時間

Input:
1. 指定日期: start/end
2. 在Input中放入以下資料:
    (1). new_weekly_report.csv: WMS資料
    (2). qc_qty.csv

Processin/Output:
1. Output/IB metric.xlsx，包括以下七張工作表
    (1). IB metric: Step 3
    (2). Productivity: Step 4
    (3). Ops time per order: Step 4
    (4). Daily SLA Breakdown: Step 5
    (5). Daily tracker IB+PY: Step 6
    (6). IB Performance: Step 7
    (7). SLA per hr_new D+3: Step 8
2. Pickle檔: 存在Input/historical_data中，包括以下種類檔案
    (1). W-1_AVG_{YYYY-MM-DD}_{productivity/sla/daily_tracker}: 儲存最後一週平均資料供下次使用
    (2). week_{YYYY-MM-DD}_{productivity/sla/daily_tracker}: 儲存最後一次輸入的半週資料供下次使用
'''

start = '2022-02-28'
end = '2022-03-31'
first_accumulated_backlog = 0  # 計算IB Metric使用
weighted = {'Arrival Check': 1625 / 31,
            'Counting': 1625 / 407,
            'QC': 1625 / 1625,
            'Labeling': 1625 / 396,
            'Received': 1625 / 500,
            'Putaway': 1625 / 44}

start_date = dt.datetime.strptime(start, '%Y-%m-%d')  # 開始日期
end_date = dt.datetime.strptime(end, '%Y-%m-%d')  # 結束日期
range_date = pd.date_range(start_date, end_date, freq='D')
range_week_start = pd.date_range(start_date, end_date, freq='7D').astype('str')
range_week_end = pd.date_range(start_date + dt.timedelta(days=6), end_date + dt.timedelta(days=6), freq='7D')
if range_week_end[-1] > end_date:
    range_week_end = range_week_end[:-1].astype("str").append(pd.Index([end]))

print(range_week_start)
print(range_week_end)
month_shortname_list = range_date.strftime("%b").unique().tolist()  # e.g. Jul, Jun

# 取得google sheet資料
class gdoc_information():
    '''
    用來抓取資料用，儲存欲抓取資料之網址、工作表名稱及匯出檔名
    1. SCOPES: 抓取資料的Google Sheet網址
    2. SPREADSHEET_ID: Google Sheet網址的中間文字
    3. RANGE_NAME: 要抓取的工作表名稱及範圍
    4. CSV_NAME: 匯出csv檔的名稱
    '''
    def __init__(self):
        self.SCOPES = []
        self.SPREADSHEET_ID = []
        self.RANGE_NAME = []
        self.CSV_NAME = []

    def trans(self):
        tmp = []
        tmp.extend(self.SCOPES)
        tmp.extend(self.SPREADSHEET_ID)
        tmp.extend(self.RANGE_NAME)
        tmp.extend(self.CSV_NAME)
        return tmp


# commercial: "B2C S&OP Inbound/Outbound Tracking 的副本"
commercial_gdoc = gdoc_information()
commercial_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/1C2ciUAdaUQE0SaWwiJr13M3FkBdu3oL5QzJKkzUVT3M']
commercial_gdoc.SPREADSHEET_ID = ['1C2ciUAdaUQE0SaWwiJr13M3FkBdu3oL5QzJKkzUVT3M']
commercial_gdoc_range_name_list = ['{} S&OP'.format(month_shortname) for month_shortname in month_shortname_list]
commercial_gdoc.CSV_NAME = ['commercial']

# OB daily tracker: OB Daily Tracker
ob_gdoc = gdoc_information()
ob_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/1aer6ti4q_4bjITONODo_kzLqF8Ge1T2QBj9wz7F1-Ag']
ob_gdoc.SPREADSHEET_ID = ['1aer6ti4q_4bjITONODo_kzLqF8Ge1T2QBj9wz7F1-Ag']
ob_gdoc.RANGE_NAME = ['OB Daily Tracker!B5:M']
ob_gdoc.CSV_NAME = ['OB_daily']

# 倉庫回報表格: "20XX年進貨相關問題 XX ~ XX 月"
abnormal_gdoc = gdoc_information()
abnormal_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/10ylvlT6KzZ9kQi4-VTmx5V7FIVOJs-MaSphuLqoAA5M']
abnormal_gdoc.SPREADSHEET_ID = ['10ylvlT6KzZ9kQi4-VTmx5V7FIVOJs-MaSphuLqoAA5M']
abnormal_gdoc.RANGE_NAME = ['倉庫回報表格!D3:D']  # 表頭有兩行，請從第二行表頭開始抓
abnormal_gdoc.CSV_NAME = ['abnormal']

# 拒收紀錄: "20XX年進貨相關問題 XX ~ XX 月"
reject_gdoc = gdoc_information()
reject_gdoc.SCOPES = abnormal_gdoc.SCOPES
reject_gdoc.SPREADSHEET_ID = abnormal_gdoc.SPREADSHEET_ID
reject_gdoc.RANGE_NAME = ['拒收紀錄!A:D']
reject_gdoc.CSV_NAME = ['reject']

# 貼標紀錄
label_gdoc = gdoc_information()
label_scopes_dict = {  # 抓label資料用
    '2022-02': 'https://docs.google.com/spreadsheets/d/1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8',
    '2022-03': 'https://docs.google.com/spreadsheets/d/1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8',
}
label_spreadsheet_id_dict = {  # 抓label資料用
    '2022-02': '1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8',
    '2022-03': '1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8',
}


if __name__ == '__main__':
    if start_date.weekday() != 0:  # 起始日一定要是星期一
        raise ValueError('起始日不是星期一，請重新輸入')
    else:
        '''
        # Step 1 抓取google sheet、SQL資料
        # '''
        time0 = time.time()
        get_gsheet.get_google_sheet_commercial(commercial_gdoc_range_name_list, *commercial_gdoc.trans())
        get_gsheet.get_google_sheet(*ob_gdoc.trans())
        get_gsheet.get_google_sheet_abnormal(*abnormal_gdoc.trans())
        get_gsheet.get_google_sheet_reject(*reject_gdoc.trans())
        get_gsheet.get_label_data(label_gdoc, label_scopes_dict, label_spreadsheet_id_dict, range_date.astype('str'), 'label')
        get_sql_data.get_hour_data(start, "hour_data")
        time1 = time.time()
        print('Step 1 抓取Google Sheet資料 SUCCEED    Spend {:.4f} seconds'.format(time1 - time0))

        '''
        讀入所有資料，並在new_weekly_report新增以下欄位，在Step3~8會陸續用到
        1. 回報異常
        2. 訂單Tag
        3. Arrival_Hour
        4. Putaway_minus_Arrival
        '''
        # reject = pd.read_csv("Input/api_data/reject.csv", parse_dates=["Date"], encoding="utf_8_sig")
        reject = pd.read_csv("Input/api_data/reject.csv", parse_dates=['Date'], date_parser=assist_funcs.dash_date_parser, encoding='utf_8_sig')
        ob_daily = pd.read_csv("Input/api_data/OB_daily.csv", encoding='utf_8_sig').rename(columns={'Unnamed: 0': 'Date'}).set_index('Date')
        # ob_daily.index = pd.to_datetime(ob_daily.index, errors='coerce').astype('str')
        commercial = pd.read_csv("Input/api_data/commercial.csv", parse_dates=['Date'], date_parser=assist_funcs.dash_date_parser, encoding='utf_8_sig')
        commercial = pd.read_csv("Input/api_data/commercial.csv", parse_dates=['Date'], encoding='utf_8_sig')
        qc_qty = pd.read_csv("Input/qc_qty.csv", parse_dates=['_col0'], encoding='utf_8_sig')
        hour_data = pd.read_csv("Input/api_data/hour_data.csv", parse_dates=['cdate'], encoding='utf_8_sig')
        hour_data = hour_data[(hour_data['cdate'] >= start_date) & (hour_data['cdate'] <= end_date)]
        label_data = pd.read_csv("Input/api_data/label.csv", parse_dates=['開始', '結束'], encoding='utf_8_sig')
        abnormal = pd.read_csv("Input/api_data/abnormal.csv", encoding='utf_8_sig')  # 得到整年資料
        new_weekly_report = pd.read_csv("Input/new_weekly_report.csv",
                                        parse_dates=["Inbound_Date", "Actual_arrived_time", "counting_Start", "counting_End", "QC_Start", "QC_End",
                                                     "Receive_start", "Receive_End", "Putaway_start", "Putaway_End", "Arrival_date", "Counting_date",
                                                     "QC_date", "Receive_date", "Putaway_date"], encoding='utf_8_sig')
        new_weekly_report['回報異常'] = new_weekly_report['po_inbound_id'].isin(set(abnormal['Inbound ID']))
        new_weekly_report['訂單Tag'] = np.where(
            new_weekly_report['order_complete'] == 'V', np.where(
                new_weekly_report['回報異常'], 'AB', np.where(
                    new_weekly_report['platform_num'] == '06', 'RI', np.where(
                        new_weekly_report['po_inbound_id'].map(dict(zip(reject['Inbound ID'], reject['實收(pcs)']))) == 0, 'X', 'NR'
                    )
                )
            ), np.nan
        )
        # Step 8 會用到
        new_weekly_report['Arrival_Hour'] = new_weekly_report['Actual_arrived_time'].dt.hour
        new_weekly_report['Putaway_minus_Arrival'] = (new_weekly_report['Putaway_date'] - new_weekly_report['Arrival_date']).dt.days

        time2 = time.time()
        print('Step 2 讀取csv與excel檔 SUCCEED        Spend {:.4f} seconds'.format(time2 - time1))

        # Step 3: IB metric
        def calculate_ib_metric():
            '''
            計算ib metric，共分成五個區段
            1. IB target orders: Column C~L
            2. IB actual arrived orders: Column M~Y
            3. Warehouse performance: Column Z~AS
            4. Backlog: Column AT~AW
            5. D+1 Performance: Column AX~BI
            '''
            # Step 3-1: IB target orders(Column C~L)
            # Column C
            target_target = commercial.rename(columns={'Unnamed: 1': 'Week', 'IB \n(pcs)': 'Target PCS'})
            target_target['Target PCS'] = target_target['Target PCS'].astype('int')
            target_target = target_target[(target_target['Date'] >= start_date) & (target_target['Date'] <= end_date)]  # 篩選日期區間
            target_target['Date'] = target_target['Date'].astype('str')

            # Column D~E
            target_pms_schduled = new_weekly_report.groupby(['Inbound_Date'])['expected_qty']\
                                                   .agg(['count', np.sum]).reset_index()\
                                                   .rename(columns={'Inbound_Date': 'Date', 'count': 'PMS Schduled SKU', 'sum': 'PMS Schduled PCS'})
            target_pms_schduled['Date'] = target_pms_schduled['Date'].astype('str')

            # Column G~H
            target_arrived = new_weekly_report.groupby(['Arrival_date'])['expected_qty']\
                                              .agg(['count', np.sum]).reset_index()\
                                              .rename(columns={'Arrival_date': 'Date', 'count': 'Arrived SKU', 'sum': 'Arrived PCS'})
            target_arrived['Date'] = target_arrived['Date'].astype('str')

            # 拒收: SKU只有實收=0的才算是拒收，但PCS還是算拒收(pcs)的加總
            # Column I
            target_reject_sku = reject[reject['實收(pcs)'] == 0]\
                .groupby(['Date'])['實收(pcs)']\
                .agg(['count']).reset_index()\
                .rename(columns={'count': '拒收 SKU'})
            target_reject_sku['Date'] = target_reject_sku['Date'].astype('str')

            # Column J
            target_reject_pcs = reject.groupby(['Date'])['拒收(pcs)']\
                                      .agg([np.sum]).reset_index()\
                                      .rename(columns={'sum': '拒收 PCS'})
            target_reject_pcs['Date'] = target_reject_pcs['Date'].astype('str')

            # 合併結果
            ib_metric = target_target.merge(target_pms_schduled, how='left', on='Date').fillna(0)

            # Column F
            ib_metric['PMS-target gap'] = np.where(
                ib_metric['Target PCS'].values == 0, np.nan,
                (ib_metric['PMS Schduled PCS'] / ib_metric['Target PCS'].values) - 1)

            ib_metric = ib_metric.merge(target_arrived, how='left', on='Date')
            ib_metric = ib_metric.merge(target_reject_sku, how='left', on='Date')
            ib_metric = ib_metric.merge(target_reject_pcs, how='left', on='Date')

            fillna_values = {'Arrived SKU': 0, 'Arrived PCS': 0, '拒收 SKU': 0, '拒收 PCS': 0}
            ib_metric.fillna(value=fillna_values, inplace=True)

            # Column K
            ib_metric['actual-target Projection gap'] = np.where(
                ib_metric['Target PCS'].values == 0, np.nan,
                ((ib_metric['Arrived PCS'].values + ib_metric['拒收 PCS'].values) / ib_metric['Target PCS'].values) - 1)

            # Column L
            ib_metric['Abs actual-target Projection gap'] = np.abs(ib_metric['actual-target Projection gap'].values)
            ib_metric.set_index('Date', inplace=True)

            # Step 3-2: IB actual arrived orders(Column M~Y)
            # Column M~N
            act_arrived_ot = new_weekly_report[new_weekly_report['Inbound_Date'].values == new_weekly_report['Arrival_date'].values]\
                .groupby(['Arrival_date'])['expected_qty']\
                .agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date', 'count': 'OT SKU', 'sum': 'OT PCS'})\
                .set_index('Date')
            act_arrived_ot.index = act_arrived_ot.index.astype('str')

            ib_metric = ib_metric.merge(act_arrived_ot, left_index=True, right_index=True, how='left')
            ib_metric['OT%'] = ib_metric['OT SKU'].values / ib_metric['PMS Schduled SKU'].values  # Column O

            # Column P
            act_arrived_if_pcs = new_weekly_report[new_weekly_report['expected_qty'].values == new_weekly_report['putaway_qty'].values]\
                .groupby(['Arrival_date'])['expected_qty']\
                .agg([np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date', 'sum': 'IF PCS'})\
                .set_index('Date')
            act_arrived_if_pcs.index = act_arrived_if_pcs.index.astype('str')

            ib_metric = ib_metric.merge(act_arrived_if_pcs, left_index=True, right_index=True, how='left')
            ib_metric['IF%'] = ib_metric['IF PCS'].values / ib_metric['Arrived PCS'].values  # Column Q

            # Column R
            act_arrived_ot_if = new_weekly_report[
                (new_weekly_report['Inbound_Date'].values == new_weekly_report['Arrival_date'].values) &
                (new_weekly_report['expected_qty'].values == new_weekly_report['putaway_qty'].values)]\
                .groupby(['Arrival_date'])['expected_qty']\
                .agg(['count']).reset_index()\
                .rename(columns={'Arrival_date': 'Date', 'count': 'OTIF order'})\
                .set_index('Date')
            act_arrived_ot_if.index = act_arrived_ot_if.index.astype('str')

            ib_metric = ib_metric.merge(act_arrived_ot_if, left_index=True, right_index=True, how='left')
            ib_metric['OTIF%'] = ib_metric['OTIF order'].values / ib_metric['PMS Schduled SKU'].values  # Column S

            # Column T&V
            act_arrived_normal = new_weekly_report[new_weekly_report['訂單Tag'].values == "NR"]\
                .groupby(['Arrival_date'])['expected_qty']\
                .agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date', 'count': 'Normal order', 'sum': 'Normal PCS'})\
                .set_index('Date')
            act_arrived_normal.index = act_arrived_normal.index.astype('str')

            ib_metric = ib_metric.merge(act_arrived_normal[['Normal order']], left_index=True, right_index=True, how='left')  # Column T
            ib_metric['Normal%'] = ib_metric['Normal order'].values / ib_metric['Arrived SKU'].values  # Column U
            ib_metric = ib_metric.merge(act_arrived_normal[['Normal PCS']], left_index=True, right_index=True, how='left')  # Column V

            # Column W&Y
            act_arrived_abnormal = new_weekly_report[new_weekly_report['訂單Tag'].values == "AB"]\
                .groupby(['Arrival_date'])['putaway_qty']\
                .agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date', 'count': 'Abnormal order', 'sum': 'Abnormal PCS'})\
                .set_index('Date')
            act_arrived_abnormal.index = act_arrived_abnormal.index.astype('str')

            ib_metric = ib_metric.merge(act_arrived_abnormal[['Abnormal order']], left_index=True, right_index=True, how='left')  # Column W
            ib_metric['Abnormal%'] = ib_metric['Abnormal order'].values / ib_metric['Arrived SKU'].values  # Column X
            ib_metric = ib_metric.merge(act_arrived_abnormal[['Abnormal PCS']], left_index=True, right_index=True, how='left')  # Column Y

            # Step 3-3 Warehouse performance(Column Z~AS)
            # Column Z~AC
            perf_dict = {
                'Counting': ['Counting_date', 'counting_qty', 'Counting PCS'],
                'QC': ['QC_date', 'QC_qty', 'QC PCS'],
                'Receive': ['Receive_date', 'recv_qty', 'Receive PCS'],
                'Putaway': ['Putaway_date', 'putaway_qty', 'Putaway PCS']
            }

            for key, value in perf_dict.items():
                key_perf = new_weekly_report.groupby([value[0]])[value[1]].agg([np.sum]).reset_index()\
                                            .rename(columns={value[0]: 'Date', 'sum': value[2]})\
                                            .set_index('Date')
                key_perf.index = key_perf.index.astype('str')
                ib_metric = ib_metric.merge(key_perf, how='left', left_index=True, right_index=True)

            # Column AD~AS
            # 計算是哪一天完成: 每天2:00AM~2:00PM為一天，因此要先將所數天數進行調整
            new_weekly_report['adj_Putaway_date'] = np.where(
                new_weekly_report['Putaway_End'].dt.hour <= 1,
                (new_weekly_report['Putaway_End'].dt.date - dt.timedelta(days=1)).astype('str'),
                new_weekly_report['Putaway_End'].dt.date.astype('str'))

            new_weekly_report['adj_Receive_date'] = np.where(
                new_weekly_report['Receive_End'].dt.hour <= 1,
                (new_weekly_report['Receive_End'].dt.date - dt.timedelta(days=1)).astype('str'),
                new_weekly_report['Receive_End'].dt.date.astype('str'))

            same_day_putaway = new_weekly_report[new_weekly_report['Arrival_date'].astype('str') == new_weekly_report['adj_Putaway_date']]
            same_day_receive = new_weekly_report[new_weekly_report['Arrival_date'].astype('str') == new_weekly_report['adj_Receive_date']]
            # Column AD~AG
            same_day_arrived = same_day_putaway\
                .groupby(['adj_Putaway_date'])['putaway_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'adj_Putaway_date': 'Date',
                                 'count': 'same day arrived-putaway order',
                                 'sum': 'same day arrived-putaway pcs'})\
                .set_index('Date')

            ib_metric = ib_metric.merge(same_day_arrived[['same day arrived-putaway order']], on='Date', how='left')  # Column AD
            ib_metric['same day arrived-putaway order%'] = np.where(  # Column AE
                ib_metric['Arrived SKU'].values == 0, np.nan, ib_metric['same day arrived-putaway order'].values / ib_metric['Arrived SKU'].values)
            ib_metric = ib_metric.merge(same_day_arrived[['same day arrived-putaway pcs']], on='Date', how='left')  # Column AF
            ib_metric['same day arrived-putaway pcs%'] = np.where(  # Column AG
                ib_metric['Arrived PCS'].values == 0, np.nan, ib_metric['same day arrived-putaway pcs'].values / ib_metric['Arrived PCS'].values)

            # Column AH~AK
            same_day_arrived_nr = same_day_putaway[same_day_putaway['訂單Tag'] == 'NR']\
                .groupby(['adj_Putaway_date'])['putaway_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'adj_Putaway_date': 'Date',
                                 'count': 'normal order same day putaway order',
                                 'sum': 'normal order same day putaway pcs'})\
                .set_index('Date')
            same_day_arrived_nr.index = same_day_arrived_nr.index.astype('str')

            ib_metric = ib_metric.merge(same_day_arrived_nr[['normal order same day putaway order']], on='Date', how='left')  # Column AH
            ib_metric['normal order same day putaway order%'] = np.where(  # Column AI
                ib_metric['Normal order'].values == 0, np.nan, ib_metric['normal order same day putaway order'].values / ib_metric['Normal order'].values)
            ib_metric = ib_metric.merge(same_day_arrived_nr[['normal order same day putaway pcs']], on='Date', how='left')  # Column AJ
            ib_metric['normal order same day putaway pcs%'] = np.where(  # Column AK
                ib_metric['Normal order'].values == 0, np.nan, ib_metric['normal order same day putaway pcs'].values / ib_metric['Normal PCS'].values)

            # Column AL~AO
            same_day_arrived_ab = same_day_putaway[same_day_putaway['訂單Tag'] == 'AB']\
                .groupby(['adj_Putaway_date'])['putaway_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'adj_Putaway_date': 'Date',
                                 'count': 'abnormal order same day putaway order',
                                 'sum': 'abnormal order same day putaway pcs'})\
                .set_index('Date')
            same_day_arrived_ab.index = same_day_arrived_ab.index.astype('str')

            ib_metric = ib_metric.merge(same_day_arrived_ab[['abnormal order same day putaway order']], on='Date', how='left')  # Column AL
            ib_metric['abnormal order same day putaway order%'] = np.where(  # Column AM
                ib_metric['Abnormal order'].values == 0, np.nan, ib_metric['abnormal order same day putaway order'].values / ib_metric['Abnormal order'].values)
            ib_metric = ib_metric.merge(same_day_arrived_ab[['abnormal order same day putaway pcs']], on='Date', how='left')  # Column AN
            ib_metric['abnormal order same day putaway pcs%'] = np.where(  # Column AO
                ib_metric['Abnormal PCS'].values == 0, np.nan, ib_metric['abnormal order same day putaway pcs'].values / ib_metric['Abnormal PCS'].values)

            # Column AP~AS
            normal_order_d_receive = same_day_receive[same_day_receive['訂單Tag'] == 'NR']\
                .groupby(['adj_Receive_date'])['expected_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'adj_Receive_date': 'Date',
                                 'count': 'normal order D receive order',
                                 'sum': 'normal order D receive pcs'})\
                .set_index('Date')
            normal_order_d_receive.index = normal_order_d_receive.index.astype('str')

            ib_metric = ib_metric.merge(normal_order_d_receive[['normal order D receive order']], on='Date', how='left')  # Column AP
            ib_metric['normal order D receive order%'] = np.where(  # Column AQ
                ib_metric['Normal order'].values == 0, np.nan, ib_metric['normal order D receive order'].values / ib_metric['Normal order'].values)

            ib_metric = ib_metric.merge(normal_order_d_receive[['normal order D receive pcs']], on='Date', how='left')  # Column AR
            ib_metric['normal order D receive pcs%'] = np.where(  # Column AS
                ib_metric['Normal PCS'].values == 0, np.nan, ib_metric['normal order D receive pcs'].values / ib_metric['Normal PCS'].values)

            # Step 3-4: Backlog(Column AT~AW)
            # Column AU
            backlog_net_daily_nr = np.where(
                ib_metric['normal order same day putaway order%'].values == 1, 0,
                ib_metric['Normal PCS'].values - ib_metric['normal order same day putaway pcs'].values)

            # Column AV
            backlog_net_daily_ab = ib_metric['Abnormal PCS'].values - ib_metric['abnormal order same day putaway pcs'].values
            # Column AT
            backlog_net_daily = backlog_net_daily_nr + backlog_net_daily_ab

            ib_metric = ib_metric.merge(pd.DataFrame({'Net Daily Backlog pcs': backlog_net_daily,
                                                      'Net Daily NR backlog pcs': backlog_net_daily_nr,
                                                      'Net Daily AB backog pcs': backlog_net_daily_ab},
                                                     index=ib_metric.index), on='Date', how='left')
            ib_metric.fillna(value={'Net Daily Backlog pcs': 0, 'Net Daily NR backlog pcs': 0, 'Net Daily AB backog pcs': 0}, inplace=True)

            # Column AW
            acc_arrival = new_weekly_report[new_weekly_report['訂單Tag'].values == "NR"]\
                .groupby(['Arrival_date'])['expected_qty'].agg(np.sum)
            acc_arrival.index = acc_arrival.index.astype('str')

            acc_putaway = new_weekly_report[new_weekly_report['訂單Tag'].values == "NR"]\
                .groupby(['Putaway_date'])['expected_qty'].agg(np.sum)
            acc_putaway.index = acc_putaway.index.astype('str')

            for date in ib_metric.index:
                if date not in acc_arrival:
                    acc_arrival[date] = 0
                if date not in acc_putaway:
                    acc_putaway[date] = 0

            def calculate_accumulated_backlog(date, yes_accumulated_backlog, net_nr_daily_backlog):
                '''
                計算Accumulated backlog
                Input:
                1. date: 計算日期，用於取得當日acc_arrival、acc_putaway資料
                2. last_accumulated_backlog: 前一天的accumulated_backlog
                3. net_nr_daily_backlog: 用於比較大小用
                計算規則:
                1. 若net_nr_daily_backlog == 0，則輸出0
                2. 計算當天acc_arrival - acc_putaway + yes_accumulated_backlog
                    (1) 若小於0則輸出0
                    (2) 小於net_nr_daily_backlog則輸出net_nr_daily_backlog
                    (3) 大於net_nr_daily_backlog則輸出計算結果
                '''
                if net_nr_daily_backlog == 0:
                    return 0
                else:
                    calculate_result = acc_arrival[date] - acc_putaway[date] + yes_accumulated_backlog
                    if calculate_result < 0:
                        return 0
                    if calculate_result < net_nr_daily_backlog:
                        return net_nr_daily_backlog
                    else:
                        return calculate_result

            accumulated_backlog = []
            for date, row in ib_metric.iterrows():
                i = 0
                if date == start:  # 第一天用上月最後一天的accumulated_backlog(first_accumulated_backlog)
                    accumulated_backlog.append(calculate_accumulated_backlog(date, first_accumulated_backlog, row['Net Daily NR backlog pcs']))
                    i += 1
                else:  # 用前一天的accumulated_backlog
                    accumulated_backlog.append(calculate_accumulated_backlog(date, accumulated_backlog[i - 1], row['Net Daily NR backlog pcs']))
                    i += 1
            ib_metric['Accumulated backlog'] = accumulated_backlog

            # Step 3-5 D+1 Performance(Column AX~BI)
            # Column AX~BA
            normal_order_D_1_comp = new_weekly_report[
                (new_weekly_report['訂單Tag'] == 'NR') &
                ((new_weekly_report['Putaway_date'] == new_weekly_report['Arrival_date']) |
                 (new_weekly_report['Putaway_date'] == (new_weekly_report['Arrival_date'] + dt.timedelta(days=1))))]\
                .groupby(['Arrival_date'])['expected_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date',
                                 'count': 'normal order D+1 comp. order',
                                 'sum': 'normal order D+1 comp. pcs'})\
                .set_index('Date')
            normal_order_D_1_comp.index = normal_order_D_1_comp.index.astype('str')

            # Column BB~BE
            normal_order_D_1_rece = new_weekly_report[
                (new_weekly_report['訂單Tag'] == 'NR') &
                ((new_weekly_report['Receive_date'] == new_weekly_report['Arrival_date']) |
                 (new_weekly_report['Receive_date'] == (new_weekly_report['Arrival_date'] + dt.timedelta(days=1))))]\
                .groupby(['Arrival_date'])['expected_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date',
                                 'count': 'normal order D+1 receive order',
                                 'sum': 'normal order D+1 receive pcs'})\
                .set_index('Date')
            normal_order_D_1_rece.index = normal_order_D_1_rece.index.astype('str')

            # Column BF~BI
            normal_order_D_2_comp = new_weekly_report[
                (new_weekly_report['訂單Tag'] == 'NR') &
                ((new_weekly_report['Putaway_date'] == new_weekly_report['Arrival_date']) |
                 (new_weekly_report['Putaway_date'] == (new_weekly_report['Arrival_date'] + dt.timedelta(days=1))) |
                 (new_weekly_report['Putaway_date'] == (new_weekly_report['Arrival_date'] + dt.timedelta(days=2))))]\
                .groupby(['Arrival_date'])['expected_qty'].agg(['count', np.sum]).reset_index()\
                .rename(columns={'Arrival_date': 'Date',
                                 'count': 'normal order D+2 comp. order',
                                 'sum': 'normal order D+2 comp. pcs'})\
                .set_index('Date')
            normal_order_D_2_comp.index = normal_order_D_2_comp.index.astype('str')

            D_perf_dict = [[normal_order_D_1_comp, 'normal order D+1 comp. order', 'normal order D+1 comp. pcs'],
                           [normal_order_D_1_rece, 'normal order D+1 receive order', 'normal order D+1 receive pcs'],
                           [normal_order_D_2_comp, 'normal order D+2 comp. order', 'normal order D+2 comp. pcs']]

            for value in D_perf_dict:
                ib_metric = ib_metric.merge(value[0][value[1]], on='Date', how='left')
                ib_metric['{}%'.format(value[1])] = np.where(
                    ib_metric['Normal order'].values == 0, np.nan, ib_metric[value[1]] / ib_metric['Normal order']
                )
                ib_metric = ib_metric.merge(value[0][value[2]], on='Date', how='left')
                ib_metric['{}%'.format(value[2])] = np.where(
                    ib_metric['Normal PCS'].values == 0, np.nan, ib_metric[value[2]] / ib_metric['Normal PCS']
                )
            return ib_metric

        ib_metric = calculate_ib_metric()
        time3 = time.time()
        print('Step 3 IB metric SUCCEED               Spend {:.4f} seconds'.format(time3 - time2))

        # Step 4: Productivity
        def calculate_productivity():
            '''
            計算productivity，共分成六個區段
            1. Working Hours計算
            2. throughput計算與合併
            3. 合併working hours和throughput，初步計算每天IPH
            4. 計算每週平均
            5. 合併每週資料
            6. 輸出pickle
            Output:
            1. productivity: 每天的productivity資料，之後計算Daily SLA Breakdown會用到
            2. productivity_result: 最終輸出excel檔的productivity資料
            '''
            # Step 4-1: Working Hours計算
            productivity_columns = ['Overall', 'Arrival Check', 'Counting', 'QC', 'Labeling', 'Received', 'Putaway', 'Abnormal']
            hour_data['cdate'] = hour_data['cdate'].astype('str')
            hour_data['working_code_l2'] = np.where(hour_data['working_code_l3'] == 'ABNORMAL', 'ABNORMAL', hour_data['working_code_l2'])

            working_hours = pd.crosstab(hour_data['cdate'], hour_data['working_code_l2'], hour_data['total_hour'], aggfunc=np.sum).fillna(0)
            working_hours.columns = working_hours.columns.str.capitalize()
            working_hours.rename(columns={'Arrival': 'Arrival Check', 'Qc': 'QC'}, inplace=True)
            working_hours['Overall'] = working_hours[['Arrival Check', 'Counting', 'QC', 'Labeling', 'Received', 'Putaway']].sum(axis=1)
            working_hours = working_hours[productivity_columns]

            # Step 4-2: throughput計算與合併
            throughput_overall = new_weekly_report.groupby(['Putaway_date'])['putaway_qty'].agg([np.sum])\
                .reset_index().rename(columns={'Putaway_date': 'Date', 'sum': 'Overall'})

            throughput_arrival_check = new_weekly_report.groupby(['Arrival_date'])['Arrival_date'].agg(['count'])\
                .reset_index().rename(columns={'Arrival_date': 'Date', 'count': 'Arrival Check'})

            throughput_counting = new_weekly_report.groupby(['Counting_date'])['counting_qty'].agg([np.sum])\
                .reset_index().rename(columns={'Counting_date': 'Date', 'sum': 'Counting'})

            throughput_qc = new_weekly_report.groupby(['QC_date'])['QC_qty'].agg([np.sum])\
                .reset_index().rename(columns={'QC_date': 'Date', 'sum': 'QC'})

            throughput_received = new_weekly_report.groupby(['Receive_date'])['recv_qty'].agg([np.sum])\
                .reset_index().rename(columns={'Receive_date': 'Date', 'sum': 'Received'})

            throughput_putaway = new_weekly_report.groupby(['Putaway_date'])['box_num'].agg([np.sum])\
                .reset_index().rename(columns={'Putaway_date': 'Date', 'sum': 'Putaway'})

            throughput_abnormal = new_weekly_report[new_weekly_report['訂單Tag'] == 'AB'].groupby(['Arrival_date'])['expected_qty'].agg([np.sum])\
                .reset_index().rename(columns={'Arrival_date': 'Date', 'sum': 'Abnormal'})

            # 將throughput合併
            throughput = throughput_arrival_check.merge(throughput_overall, on='Date', how='outer')\
                                                 .merge(throughput_counting, on='Date', how='outer')\
                                                 .merge(throughput_qc, on='Date', how='outer')\
                                                 .merge(throughput_received, on='Date', how='outer')\
                                                 .merge(throughput_putaway, on='Date', how='outer')\
                                                 .merge(throughput_abnormal, on='Date', how='outer')\
                                                 .fillna(0).set_index('Date')

            throughput['Labeling'] = (throughput['QC'] + throughput['Received']) / 2  # 計算Label(因無Labeling_date)
            throughput = throughput[throughput.index.isin(range_date)]  # 選指定範圍內的日期
            throughput = throughput[productivity_columns]  # 欄位重新排序
            throughput.index = throughput.index.astype('str')

            # Step 4-3 合併working hours和throughput，初步計算每天IPH
            productivity = throughput.merge(working_hours, left_index=True, right_index=True, suffixes=['_Throughput', '_Working hour'])
            for col in productivity_columns:
                productivity['{}_IPH'.format(col)] = np.where(
                    productivity['{}_Working hour'.format(col)].values == 0,
                    np.nan,
                    productivity['{}_Throughput'.format(col)].values / productivity['{}_Working hour'.format(col)].values)

            productivity['Overall_weighted_IPH'] = sum([productivity['{}_Throughput'.format(col)].values * weighted[col] for col in weighted.keys()]) / productivity['Overall_Working hour']
            productivity['進貨組IPH'] = productivity['Received_Throughput'].values / (productivity['Overall_Working hour'].values - productivity['Putaway_Working hour'].values)
            productivity['上架組IPH'] = productivity['Overall_Throughput'].values / productivity['Putaway_Working hour'].values
            productivity = productivity[['Overall_weighted_IPH'] +
                                        ['{col}_{type}'.format(col=col, type=type_) for col in productivity_columns for type_ in ['IPH', 'Throughput', 'Working hour']] +
                                        ['進貨組IPH', '上架組IPH']
                                        ].fillna(0).T

            # 把Input/week_{}_productivity.pickle中的productivity資料匯入，並將productivity原本那幾天的資料刪除
            productivity = assist_funcs.replace_to_old_data(productivity, 'productivity')
            productivity = productivity.reindex(sorted(productivity.columns), axis=1)
            
            # Step 4-4 計算每週平均
            productivity_week = []
            for week in range(len(range_week_start)):
                week_data = productivity.loc[:, range_week_start[week]: range_week_end[week]]

                # if week_data.shape[1] == 7:  # 有整週可算平均

                # 1. IPH & Overall_weighted_IPH:
                # (1)IPH: 七天的throughout加總 / 七天的working_hour加總
                # (2)Overall weighted IPH: 六種throughout加總 / 七天的Overall working_hour加總
                total_throughout = 0
                for col in productivity_columns:
                    if col not in ['Overall', 'Abnormal']:
                        total_throughout += week_data.loc['{}_Throughput'.format(col)].sum(axis=0) * weighted[col]
                    week_data.loc['{}_IPH'.format(col), 'W AVG'] = week_data.loc['{}_Throughput'.format(col)].sum(axis=0) / week_data.loc['{}_Working hour'.format(col)].sum(axis=0)
                week_data.loc['Overall_weighted_IPH', 'W AVG'] = total_throughout / week_data.loc['Overall_Working hour'].sum(axis=0)

                # 2. Throughput & Working hour: 週一至週六平均
                week_data['W AVG'] = np.where(week_data.index.str.contains('IPH'), week_data['W AVG'], week_data.iloc[:, :-2].mean(axis=1))
                if week == 0 :  # 沒有W-1週
                    if os.path.exists('Input/W-1_AVG_{}_prod.pickle'.format(week_data.columns[0])):  # 有上週平均資料
                        first_week_W_1_AVG = pd.read_pickle('Input/W-1_AVG_{}_prod.pickle'.format(week_data.columns[0]))
                        week_data['W-1 AVG'] = first_week_W_1_AVG[week_data.columns[0]]
                        week_data['Change'] = np.where(week_data['W-1 AVG'].values == 0, 0, week_data['W AVG'].values / week_data['W-1 AVG'].values - 1)
                    else:  # 'Input/W-1_AVG_prod.pickle'儲存的不是上週的資料，所以不能用
                        week_data['W-1 AVG'] = np.nan
                        week_data['Change'] = np.nan
                else:  # 有W-1週
                    # print("week-1: " + str(week-1))
                    # print("length: " + str(len(productivity_week)))
                    # print(productivity_week)
                    # exit()

                    week_data['W-1 AVG'] = productivity_week[week - 1]['W AVG'].values
                    week_data['Change'] = np.where(week_data['W-1 AVG'].values == 0, 0, week_data['W AVG'].values / week_data['W-1 AVG'].values - 1)
                # else:
                #     print(week_data.shape[1])
                #     print(week_data)
                #     exit()
                productivity_week.append(week_data)  # 每一週的資料都要加至productivity_week，不論是否有7天

            # Step 4-5 合併每週資料
            for week in range(len(range_week_start)):
                if week == 0:
                    productivity_result = productivity_week[week]
                else:
                    productivity_result = productivity_result.merge(productivity_week[week], left_index=True, right_index=True, suffixes=[' ', ' '])
            productivity_result.rename(columns={'W AVG ': 'W AVG', 'W-1 AVG ': 'W-1 AVG', 'Change ': 'Change'}, inplace=True)

            # Step 4-6: operating time per order
            ops_hours = working_hours[['Arrival Check', 'Counting', 'QC', 'Labeling', 'Received', 'Putaway']]\
                .fillna(0).rename(columns={'Arrival Check': 'Arrival', 'Received': 'Receive'})  # 名稱統一以利後續分析
            ops_type_list = ['Arrival', 'Counting', 'QC', 'Labeling', 'Receive', 'Putaway']

            def get_ops_time_per_order(type_):
                '''
                計算ops_time_per_order
                Input: type_
                Output: pd.DataFrame，包括工作時間{type_}_hrs、完成數量{type_}_cnt及每單完成小時數type_
                '''
                cnt = new_weekly_report.groupby(['{}_date'.format(type_)])['{}_date'.format(type_)].agg('count').to_frame(name='{}_cnt'.format(type_))
                cnt.index = cnt.index.astype('str')
                type_ops_time = ops_hours[[type_]].merge(cnt, how='left', left_index=True, right_index=True)\
                                                  .rename(columns={type_: '{}_hrs'.format(type_)})
                type_ops_time['{}_sla'.format(type_)] = type_ops_time['{}_hrs'.format(type_)].values / type_ops_time['{}_cnt'.format(type_)].values
                return type_ops_time

            for type_ in ops_type_list:
                if type_ == 'Labeling':
                    pass
                else:
                    type_ops_time = get_ops_time_per_order(type_)
                    if type_ == 'Arrival':
                        ops_time_per_order = type_ops_time.rename(columns={'{}_sla'.format(type_): type_})
                    else:
                        ops_time_per_order = ops_time_per_order.merge(type_ops_time, left_index=True, right_index=True)\
                                                               .rename(columns={'{}_sla'.format(type_): type_})

            ops_time_per_order['Labeling_hrs'] = ops_hours['Labeling'].values
            ops_time_per_order['Labeling_cnt'] = (ops_time_per_order['QC_cnt'].values + ops_time_per_order['Putaway_cnt'].values) / 2
            ops_time_per_order['Labeling'] = ops_time_per_order['Labeling_hrs'].values / ops_time_per_order['Labeling_cnt']
            ops_time_per_order = ops_time_per_order[ops_type_list].T

            # Step 4-7 輸出pickle
            assist_funcs.last_week_to_pickle(productivity_week, 'W AVG', 'productivity')

            return ops_time_per_order, productivity, productivity_result

        ops_time_per_order, productivity, productivity_result = calculate_productivity()
        time4 = time.time()
        print('Step 4 Productivity SUCCEED            Spend {:.4f} seconds'.format(time4 - time3))

        # Step 5: Daily SLA Breakdown
        def calculate_daily_sla():
            '''
            計算Daily SLA Breakdown，共分成六個區段
            1. 計算不包含週平均的sla初步結果
            2. 計算每週平均，存成list
            3. 合併每週資料
            4. 輸出pickle
            Output: sla_result
            '''
            # Step 5-1: 計算sla初步結果
            sla_data = new_weekly_report[(new_weekly_report['order_complete'] == 'V') & (new_weekly_report['訂單Tag'] == 'NR')]
            sla_data = sla_data[(sla_data['Arrival_to_counting_start'] != '未count') & (sla_data['after_QC_to_receive_start'] != '已到未驗')]  # 怕沒篩乾淨再重篩一次
            sla_data['Arrival_date'] = sla_data['Arrival_date'].astype('str')
            sla_data = sla_data[sla_data['Arrival_date'].isin(ops_time_per_order.columns)]

            # 新增欄位，計算Total hr、Arr-rec hr會用到
            sla_data['Overall SLA'] = ((sla_data['Putaway_End'] - sla_data['Actual_arrived_time']).dt.total_seconds() / 3600).astype('int')
            sla_data['Arr-rec SLA'] = ((sla_data['Receive_End'] - sla_data['Actual_arrived_time']).dt.total_seconds() / 3600).astype('int')

            # sla欄位名稱(key)對應到的要計算new_weekly_report每日平均的欄位(values)，None代表直接使用ops_time_per_order資料即可
            sla_col_dict = {
                'Arrival': None,
                'Arrival > Counting': 'Arrival_to_counting_start',
                'Counting': None,
                'Counting > QC': 'after_counting_to_QC_start',
                'QC': None,
                'Labeling': None,
                'QC > Labeling + Labeling > Receiving': 'after_QC_to_receive_start',
                'Receiving': 'receive_start_to_end',
                'Received > Putaway': 'after_receive_to_putaway_start',
                'Putaway': 'putaway_start_to_end',
                'Total hr': 'Overall SLA',
                'Arr-Rec hr': 'Arr-rec SLA'
            }

            sla = pd.DataFrame(index=range_date.astype('str'))
            oforder = sla_data.groupby(['Arrival_date'])['Arrival_date'].agg(['count']).rename(columns={'count': '#oforder'})
            sla = sla.merge(oforder, how='left', left_index=True, right_index=True)

            for key, value in sla_col_dict.items():
                if value is None:  # 直接使用ops_time_per_order資料
                    sla = sla.merge(ops_time_per_order.T[[key]], left_index=True, right_index=True)
                else:
                    sla_data[value] = sla_data[value].astype('float')
                    type_sla_data = sla_data.groupby(['Arrival_date'])[value].agg([np.mean])\
                                            .rename(columns={'mean': key})
                    sla = sla.merge(type_sla_data, how='left', left_index=True, right_index=True)

            sla['Waiting time'] = sla[['Arrival > Counting', 'Counting > QC', 'QC > Labeling + Labeling > Receiving', 'Received > Putaway']].sum(axis=1)
            sla['Operating time'] = sla[['Arrival', 'Counting', 'QC', 'Labeling', 'Receiving', 'Putaway']].sum(axis=1)
            sla = sla[['#oforder', 'Total hr', 'Arr-Rec hr', 'Waiting time', 'Operating time',
                       'Arrival', 'Arrival > Counting', 'Counting', 'Counting > QC', 'QC', 'Labeling',
                       'QC > Labeling + Labeling > Receiving', 'Receiving', 'Received > Putaway', 'Putaway']].fillna(0).T

            sla = assist_funcs.replace_to_old_data(sla, 'sla')

            # Step 5-2: 計算每週sla平均
            sla_week = []
            for week in range(len(range_week_start)):
                week_data = sla.loc[:, range_week_start[week]: range_week_end[week]]
                if week_data.shape[1] == 7:  # 有整週可算平均
                    week_data['W AVG'] = week_data.iloc[:, :-1].mean(axis=1)
                    if week == 0:  # 沒有W-1週
                        if os.path.exists('Input/W-1_AVG_{}_sla.pickle'.format(week_data.columns[0])):  # 有上週平均資料
                            first_week_W_1_AVG = pd.read_pickle('Input/W-1_AVG_{}_sla.pickle'.format(week_data.columns[0]))
                            week_data['W-1 AVG'] = first_week_W_1_AVG[week_data.columns[0]]
                            week_data['Change'] = np.where(week_data['W-1 AVG'].values == 0, 0, week_data['W AVG'].values / week_data['W-1 AVG'].values - 1)
                        else:
                            week_data['W-1 AVG'] = np.nan
                            week_data['Change'] = np.nan
                    else:  # 有W-1週
                        week_data['W-1 AVG'] = sla_week[week - 1]['W AVG'].values
                        week_data['Change'] = np.where(week_data['W-1 AVG'].values == 0, 0, week_data['W AVG'].values / week_data['W-1 AVG'].values - 1)
                sla_week.append(week_data)

            # Step 5-3 合併每週資料
            for week in range(len(range_week_start)):
                if week == 0:
                    sla_result = sla_week[week]
                else:
                    sla_result = sla_result.merge(sla_week[week], left_index=True, right_index=True, suffixes=[' ', ' '])
            sla_result.rename(columns={'W AVG ': 'W AVG', 'W-1 AVG ': 'W-1 AVG', 'Change ': 'Change'}, inplace=True)

            # Step 5-4: 輸出pickle
            assist_funcs.last_week_to_pickle(sla_week, 'W AVG', 'sla')

            return sla_result.T  # 最後記得要轉置

        sla_result = calculate_daily_sla()
        time5 = time.time()
        print('Step 5 Daily SLA SUCCEED               Spend {:.4f} seconds'.format(time5 - time4))

        # Step 6 Daily Tracker IB+PY
        def daily_tracker_ib_py():
            '''
            計算Daily tracker IB+PY，共分成六個區段
            1. 計算不包含週平均的daily_tracker初步結果
            2. 計算每週平均，存成list
            3. 合併每週資料
            4. 輸出pickle
            Output: tracker_result
            '''
            # Step 6-1 初步計算每日資料
            # Inbound orders
            daily_tracker = ib_metric[['Target PCS', '拒收 PCS', 'Arrived PCS',
                                       'Counting PCS', 'QC PCS', 'Receive PCS', 'Putaway PCS']]
            daily_tracker.rename(columns={'拒收 PCS': 'Reject', 'Arrived PCS': 'Actual Arrived', 'Counting PCS': 'Actual Counting',
                                          'QC PCS': 'Actual QC', 'Receive PCS': 'Actual Received', 'Putaway PCS': 'Actual putaway'}, inplace=True)
            daily_tracker['Actual arrived-Target Gap'] = np.where(
                daily_tracker['Target PCS'] == 0, np.nan,
                ((daily_tracker['Reject'] + daily_tracker['Actual Arrived']) / daily_tracker['Target PCS']) - 1)
            daily_tracker = daily_tracker[['Target PCS', 'Reject', 'Actual Arrived', 'Actual arrived-Target Gap', 'Actual Counting',
                                           'Actual QC', 'Actual Received', 'Actual putaway']]

            # Working Hour
            ib_py_hour = pd.crosstab(hour_data['cdate'], hour_data['working_code_l2'], values=hour_data['total_hour'], aggfunc=np.sum).fillna(0)
            ib_py_hour['站點打卡 IB hr'] = ib_py_hour[['ARRIVAL', 'COUNTING', 'LABELING', 'QC', 'RECEIVED']].sum(axis=1)
            ib_py_hour['站點打卡 PY hr'] = ib_py_hour['PUTAWAY']
            daily_tracker = daily_tracker.merge(ib_py_hour[['站點打卡 IB hr', '站點打卡 PY hr']], left_index=True, right_index=True)

            # Productivity
            daily_tracker = daily_tracker.merge(productivity.T[['Overall_weighted_IPH']], left_index=True, right_index=True)\
                                         .rename(columns={'Overall_weighted_IPH': 'Overall IPH'})
            daily_tracker = daily_tracker.T

            print(daily_tracker)

            # Step 6-2: 計算每週daily tracker平均
            tracker_week = []
            for week in range(len(range_week_start)):
                week_data = daily_tracker.loc[:, range_week_start[week]: range_week_end[week]]

                # if week_data.shape[1] == 7:  # 有整週可算平均

                week_data['AVG'] = week_data.iloc[:, :-1].mean(axis=1)
                week_data.loc['Overall IPH', 'AVG'] = week_data.iloc[-1:, 3:6].mean(axis=1).values  # Overall IPH平均只計算星期四~六
                if week == 0:  # 沒有W-1週
                    if os.path.exists('Input/W-1_AVG_{}_tracker.pickle'.format(week_data.columns[0])):  # 有上週平均資料
                        first_week_W_1_AVG = pd.read_pickle('Input/W-1_AVG_{}_tracker.pickle'.format(week_data.columns[0]))
                        week_data['W-1'] = first_week_W_1_AVG[week_data.columns[0]]
                        week_data['Change%'] = np.where(week_data['W-1'].values == 0, 0, week_data['AVG'].values / week_data['W-1'].values - 1)
                    else:
                        week_data['W-1'] = np.nan
                        week_data['Change%'] = np.nan
                else:  # 有W-1週
                    week_data['W-1'] = tracker_week[week - 1]['AVG'].values
                    week_data['Change%'] = np.where(week_data['W-1'].values == 0, 0, week_data['AVG'].values / week_data['W-1'].values - 1)
                tracker_week.append(week_data)

            # Step 6-3 合併每週資料
            for week in range(len(range_week_start)):
                if week == 0:
                    tracker_result = tracker_week[week]
                else:
                    tracker_result = tracker_result.merge(tracker_week[week], left_index=True, right_index=True, suffixes=[' ', ' '])
            tracker_result.rename(columns={'AVG ': 'AVG', 'W-1 ': 'W-1', 'Change% ': 'Change%'}, inplace=True)

            # Step 6-4: 輸出pickle
            assist_funcs.last_week_to_pickle(tracker_week, 'AVG', 'daily_tracker')

            return tracker_result.T

        tracker_result = daily_tracker_ib_py()
        time6 = time.time()
        print('Step 6 Daily Tracker IB+PY SUCCEED     Spend {:.4f} seconds'.format(time6 - time5))

        # Step 7 IB performance
        def get_ib_performance():
            '''
            將ib_metric, productivity, ob_daily的資料整理成IB Performance要的欄位
            Output: ib_performance
            '''
            ib_metric_perf_col = [  # IB performance需要欄位
                'Week', 'Arrived SKU', 'Arrived PCS', 'Normal PCS', 'Counting PCS', 'QC PCS', 'Receive PCS', 'Putaway PCS',
                'normal order same day putaway order%', 'normal order same day putaway pcs%', 'Accumulated backlog',
                'normal order D+1 receive order%', 'normal order D+1 comp. order%', 'actual-target Projection gap'
            ]
            prod_perf_col = ['Overall_weighted_IPH', 'Overall_Working hour']  # IB performance需要欄位
            rename_perf = {  # IB performance欄位名稱
                'Arrived SKU': 'IB actual arrived orders',
                'Arrived PCS': 'IB actual arrived pcs',
                'Normal PCS': 'IB Normal pcs',
                'Counting PCS': 'IB Counting pcs',
                'QC PCS': 'IB QC pcs',
                'Receive PCS': 'IB Receive pcs',
                'Putaway PCS': 'IB putaway pcs',
                'normal order same day putaway order%': 'NR comp. rate - order',
                'normal order same day putaway pcs%': 'NR comp. rate - pc',
                'Accumulated backlog': 'Accumulated NR backlog',
                'normal order D+1 receive order%': 'NR D+1 Receive Comp. rate',
                'normal order D+1 comp. order%': 'NR D+1 Comp. rate',
                'actual-target Projection gap': 'IB Projection gap',
                'Overall_weighted_IPH': 'IPH',
                'Overall_Working hour': '站點打卡hr',
                'Actual\nPiece\nGap': 'OB pcs gap'
            }
            output_col = [  # 輸出順序
                'Week', 'IB actual arrived orders', 'IB actual arrived pcs', 'IB Normal pcs', 'IB Counting pcs', 'IB QC pcs',
                'IB Receive pcs', 'IB putaway pcs', 'IPH', 'NR comp. rate - order', 'NR comp. rate - pc', 'Accumulated NR backlog',
                'NR D+1 Receive Comp. rate', 'NR D+1 Comp. rate', 'IB Projection gap', 'OB order gap', 'OB pcs gap',
                '排班工時hr', '實際需要hr', 'arrived+backlog需要hr', '站點打卡hr'
            ]

            ib_perf = ib_metric[ib_metric_perf_col]\
                .merge(productivity.T[prod_perf_col], how='left', left_index=True, right_index=True)\
                .merge(ob_daily[['Actual\nPiece\nGap']], how='left', left_index=True, right_index=True)\
                .rename(columns=rename_perf)
            ib_perf['OB order gap'] = np.nan
            ib_perf['排班工時hr'] = np.nan
            ib_perf['實際需要hr'] = np.nan
            ib_perf['arrived+backlog需要hr'] = np.nan
            ib_perf = ib_perf[output_col].reset_index().set_index(['Date', 'Week']).T
            return ib_perf

        ib_performance = get_ib_performance()
        time7 = time.time()
        print('Step 7 IB performance SUCCEED          Spend {:.4f} seconds'.format(time7 - time6))

        # Step 8 SLA per hr_new D+3
        def get_sla_per_hr():
            '''
            計算每天每個小時Arrival的數量，及在當天、一天內、兩天內、三天內Putaway的比率
            Output: sla_per_hr
            '''
            for day in range_date.astype('str'):
                # 計算每天Arrival每個小時的expected_qty: 用於計算dist用
                daily_arrival = new_weekly_report[new_weekly_report['Arrival_date'] == day]
                dist = daily_arrival\
                    .groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': (day, 'dist')})

                # 計算每天Arrival每個小時的expected_qty(訂單Tag == 'NR'): 用於計算百分比的分母及每天完成率使用
                daily_arrival_nr = daily_arrival[daily_arrival['訂單Tag'] == 'NR']
                nr = daily_arrival_nr.groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': 'NR'})

                # 計算每個小時Putaway_minus_Arrival的天數比例，從當天至差三天
                d_comp = daily_arrival_nr[daily_arrival_nr['Putaway_minus_Arrival'] == 0]\
                    .groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': (day, 'D comp%')})
                d_1_comp = daily_arrival_nr[daily_arrival_nr['Putaway_minus_Arrival'] <= 1]\
                    .groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': (day, 'D+1 comp%')})
                d_2_comp = daily_arrival_nr[daily_arrival_nr['Putaway_minus_Arrival'] <= 2]\
                    .groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': (day, 'D+2 comp%')})
                d_3_comp = daily_arrival_nr[daily_arrival_nr['Putaway_minus_Arrival'] <= 3]\
                    .groupby('Arrival_Hour')['expected_qty'].agg([np.sum]).rename(columns={'sum': (day, 'D+3 comp%')})

                # 將計算結果合併，並加上NR的總資料，用於計算比例使用
                sla_per_hr_day = dist.merge(d_comp, left_index=True, right_index=True, how='outer')\
                                     .merge(d_1_comp, left_index=True, right_index=True, how='outer')\
                                     .merge(d_2_comp, left_index=True, right_index=True, how='outer')\
                                     .merge(d_3_comp, left_index=True, right_index=True, how='outer')\
                                     .merge(nr, left_index=True, right_index=True, how='outer')
                sla_per_hr_day.loc["Total"] = sla_per_hr_day.sum()  # 每行加總

                # 將dist, D, D+1, D+2, D+3轉換成比例
                sla_per_hr_day.iloc[:-1, 0] = sla_per_hr_day.iloc[:-1, 0] / sla_per_hr_day.iloc[-1, 0]
                for n in range(1, 5):
                    sla_per_hr_day.iloc[:, n] = sla_per_hr_day.iloc[:, n] / sla_per_hr_day.iloc[:, -1]
                sla_per_hr_day.drop('NR', axis=1, inplace=True)
                sla_per_hr_day.columns = pd.MultiIndex.from_tuples(sla_per_hr_day.columns)

                # 合併成sla_per_hr
                if day == range_date.astype('str')[0]:
                    sla_per_hr = sla_per_hr_day
                else:
                    sla_per_hr = sla_per_hr.merge(sla_per_hr_day, left_index=True, right_index=True, how='outer')
            return sla_per_hr

        sla_per_hr = get_sla_per_hr()
        time8 = time.time()
        print('Step 8 SLA per hr_new D+3 SUCCEED      Spend {:.4f} seconds'.format(time8 - time7))

        # Step 9 匯出Excel
        # 先修改欄位名稱
        def column_prettify_ib_metric(ib_metric):
            ib_metric = ib_metric.reset_index().set_index(['Date', 'Week'])
            ib_metric.columns = [
                ('IB target orders', 'Target', 'pcs'),
                ('IB target orders', 'PMS Schduled', 'sku'),
                ('IB target orders', 'PMS Schduled', 'pcs'),
                ('IB target orders', 'PMS-target gap', 'pcs%'),
                ('IB target orders', 'Arrived', 'sku'),
                ('IB target orders', 'Arrived', 'pcs'),
                ('IB target orders', '拒收', 'sku'),
                ('IB target orders', '拒收', 'pcs'),
                ('IB target orders', 'actual-target\nProjection gap', 'pcs%'),
                ('IB target orders', 'ABS actual-target Projection gap', 'Daily'),
                ('IB actual arrived orders', 'OT', 'sku'),
                ('IB actual arrived orders', 'OT', 'pcs'),
                ('IB actual arrived orders', 'OT', '%'),
                ('IB actual arrived orders', 'IF', 'pcs'),
                ('IB actual arrived orders', 'IF', '%'),
                ('IB actual arrived orders', 'OTIF', 'order'),
                ('IB actual arrived orders', 'OTIF', '%'),
                ('IB actual arrived orders', 'Normal', 'order'),
                ('IB actual arrived orders', 'Normal', '%'),
                ('IB actual arrived orders', 'Normal', 'pcs'),
                ('IB actual arrived orders', 'Abnormal', 'order'),
                ('IB actual arrived orders', 'Abnormal', '%'),
                ('IB actual arrived orders', 'Abnormal', 'pcs'),
                ('Warehouse performance', 'Counting', 'pcs'),
                ('Warehouse performance', 'QC', 'pcs'),
                ('Warehouse performance', 'Receive', 'pcs'),
                ('Warehouse performance', 'Putaway', 'pcs'),
                ('Warehouse performance', 'same day arrived-putaway', 'order'),
                ('Warehouse performance', 'same day arrived-putaway', '%'),
                ('Warehouse performance', 'same day arrived-putaway', 'pcs'),
                ('Warehouse performance', 'same day arrived-putaway', '%'),
                ('Warehouse performance', 'normal order same day putaway', 'order'),
                ('Warehouse performance', 'normal order same day putaway', '%'),
                ('Warehouse performance', 'normal order same day putaway', 'pcs'),
                ('Warehouse performance', 'normal order same day putaway', '%'),
                ('Warehouse performance', 'abnormal order same day putaway', 'order'),
                ('Warehouse performance', 'abnormal order same day putaway', '%'),
                ('Warehouse performance', 'abnormal order same day putaway', 'pcs'),
                ('Warehouse performance', 'abnormal order same day putaway', '%'),
                ('Warehouse performance', 'normal order D receive', 'order'),
                ('Warehouse performance', 'normal order D receive', '%'),
                ('Warehouse performance', 'normal order D receive', 'pcs'),
                ('Warehouse performance', 'normal order D receive', '%'),
                ('Backlog', 'Net Daily Backlog', 'pcs'),
                ('Backlog', 'Net Daily NR backlog', 'pcs'),
                ('Backlog', 'Net Daily AB backog', 'pcs'),
                ('Backlog', 'Accumulated backlog', 'pcs'),
                ('D+1 performance', 'normal order D+1 comp.', 'order'),
                ('D+1 performance', 'normal order D+1 comp.', '%'),
                ('D+1 performance', 'normal order D+1 comp.', 'pcs'),
                ('D+1 performance', 'normal order D+1 comp.', '%'),
                ('D+1 performance', 'normal order D+1 receive', 'order'),
                ('D+1 performance', 'normal order D+1 receive', '%'),
                ('D+1 performance', 'normal order D+1 receive', 'pcs'),
                ('D+1 performance', 'normal order D+1 receive', '%'),
                ('D+1 performance', 'normal order D+2 comp.', 'order'),
                ('D+1 performance', 'normal order D+2 comp.', '%'),
                ('D+1 performance', 'normal order D+2 comp.', 'pcs'),
                ('D+1 performance', 'normal order D+2 comp.', '%'),
            ]
            ib_metric.columns = pd.MultiIndex.from_tuples(ib_metric.columns)
            return ib_metric

        def column_prettify_productivity(productivity_result):
            productivity_result.index = [
                ('Overall', 'weighted IPH'),
                ('Overall', 'IPH'),
                ('Overall', 'Throughput'),
                ('Overall', 'Working hour'),
                ('Arrival check', 'IPH(orders)'),
                ('Arrival check', 'Throughput'),
                ('Arrival check', 'Working hour'),
                ('Counting', 'IPH(pcs)'),
                ('Counting', 'Throughput'),
                ('Counting', 'Working hour'),
                ('QC', 'IPH(pcs)'),
                ('QC', 'Throughput'),
                ('QC', 'Working hour'),
                ('Labeling', 'IPH(pcs)'),
                ('Labeling', 'Throughput'),
                ('Labeling', 'Working hour'),
                ('Receiving', 'IPH(pcs)'),
                ('Receiving', 'Throughput'),
                ('Receiving', 'Working hour'),
                ('Putaway', 'IPH(boxes)'),
                ('Putaway', 'Throughput'),
                ('Putaway', 'Working hour'),
                ('Abnormal', 'IPH(pcs)'),
                ('Abnormal', 'Throughput'),
                ('Abnormal', 'Working hour'),
                ('', '進貨組IPH'),
                ('', '上架組IPH')
            ]
            productivity_result.index = pd.MultiIndex.from_tuples(productivity_result.index)
            return productivity_result

        def column_prettify_daily_tracker(tracker_result):
            tracker_result.columns = [
                ('Inbound orders', 'Target PCS'),
                ('Inbound orders', 'Reject'),
                ('Inbound orders', 'Actual Arrived'),
                ('Inbound orders', 'Actual arrived-Target Gap'),
                ('Inbound orders', 'Actual Counting'),
                ('Inbound orders', 'Actual QC'),
                ('Inbound orders', 'Actual Received'),
                ('Inbound orders', 'Actual putaway'),
                ('Working Hour', '站點打卡 IB hr'),
                ('Working Hour', '站點打卡 PY hr'),
                ('Productivity', 'Overall IPH')
            ]
            tracker_result.columns = pd.MultiIndex.from_tuples(tracker_result.columns)
            return tracker_result

        with pd.ExcelWriter('Output/IB metric.xlsx') as writer:
            column_prettify_ib_metric(ib_metric).to_excel(writer, sheet_name='IB metric', encoding="utf_8_sig")
            column_prettify_productivity(productivity_result).to_excel(writer, sheet_name='Productivity', encoding="utf_8_sig")
            ops_time_per_order.to_excel(writer, sheet_name='Ops time per order', encoding="utf_8_sig")
            sla_result.to_excel(writer, sheet_name='Daily SLA Breakdown', encoding="utf_8_sig")
            column_prettify_daily_tracker(tracker_result).to_excel(writer, sheet_name='Daily tracker IB+PY', encoding="utf_8_sig")
            ib_performance.to_excel(writer, sheet_name='IB Performance', encoding="utf_8_sig")
            sla_per_hr.to_excel(writer, sheet_name='SLA per hr_new D+3', encoding="utf_8_sig")

        time9 = time.time()
        print('Step 9 匯出Excel SUCCEED               Spend {:.4f} seconds'.format(time9 - time8))
