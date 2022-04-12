import datetime as dt
import pandas as pd
import numpy as np
import pickle
import os
import main  # 取得年份


def dash_date_parser(date):
    '''把M/D或Y/M/D轉成日期格式，例如7/9轉成datetime.datetime(2021, 7, 9)'''
    year, month, day = date.split(' ')[0].split("-")
    # if len(month_and_day) == 2:  # 沒有年份資料
    #     year = main.start_date.year
    #     month = int(month_and_day[0])
    #     day = int(month_and_day[1])
    # else:  # 有年份資料
    #     year = int(month_and_day[0])
    #     month = int(month_and_day[1])
    #     day = int(month_and_day[2])
    return dt.date(int(year), int(month), int(day))


def get_next_monday_str(date_str):
    '''
    得到本週的W AVG會做為哪週的W-1 AVG使用，即輸出下週一的日期(str)
    Input: str，本週週一日期
    Output: str，下週週一日期
    '''
    print(date_str)
    last_monday = dt.datetime.strptime(date_str, '%Y-%m-%d')  # 本次最後一週的星期一
    next_monday = dt.datetime.strftime(last_monday + dt.timedelta(days=7), '%Y-%m-%d')  # 下一週的星期一，轉成字串形式
    return next_monday


def replace_to_old_data(data, store_name):
    '''
    將pickle已存取第一週的一半資料取代現有資料
    Input:
    1. week_list: 要儲存資料的list，例如productivity_week, sla_week, tracker_week
    2. store_name: 要儲存的檔名
    Output: new_data取代後的資料
    '''
    if os.path.exists('Input/historical_data/week_{day}_{type_}.pickle'.format(day=main.start, type_=store_name)):
        pickle_data = pd.read_pickle('Input/historical_data/week_{day}_{type_}.pickle'.format(day=main.start, type_=store_name))
        data.drop(pickle_data.columns.tolist(), axis=1, inplace=True)
        new_data = pickle_data.merge(data, left_index=True, right_index=True)
        return new_data
    else:  # 沒有原始資料就不更動
        return data


def last_week_to_pickle(week_list, avg_name, store_name):
    '''
    將最後一週資料、最後一週平均資料存為pickle
    Input:
    1. week_list: 要儲存資料的list，例如productivity_week, sla_week, tracker_week
    2. avg_name: 各表中平均的名稱，例如'W AVG', 'AVG'
    3. store_name: 要儲存的檔名
    '''
    if avg_name in week_list[-1].columns:  # 最後一週有平均，代表只要抓「最後一週的W AVG」
        print(week_list[-1].columns[0])
        next_monday = get_next_monday_str(week_list[-1].columns[0])
        store_W_1_AVG = {next_monday: week_list[-1][avg_name]}

        with open('Input/historical_data/W-1_AVG_{day}_{type_}.pickle'.format(day=next_monday, type_=store_name), 'wb') as handle:
            pickle.dump(store_W_1_AVG, handle, protocol=pickle.HIGHEST_PROTOCOL)

    else:  # 最後一週沒有平均，代表只有一半的資料
        with open('Input/historical_data/week_{day}_{type_}.pickle'.format(day=week_list[-1].columns[0], type_=store_name), 'wb') as handle:  # 儲存半週的資料，並命名為當週名稱
            pickle.dump(week_list[-1], handle, protocol=pickle.HIGHEST_PROTOCOL)

        if len(week_list) > 1:  # 只有一週的話不用抓W-1 AVG，只有超過兩週才需要
            next_monday = get_next_monday_str(week_list[-2].columns[0])
            store_W_1_AVG = {next_monday: week_list[-2][avg_name]}

            with open('Input/historical_data/W-1_AVG_{day}_{type_}.pickle'.format(day=next_monday, type_=store_name), 'wb') as handle:
                pickle.dump(store_W_1_AVG, handle, protocol=pickle.HIGHEST_PROTOCOL)