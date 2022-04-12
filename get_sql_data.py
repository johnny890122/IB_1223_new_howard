import os
import datetime
import pandas as pd
import pymysql


def get_hour_data(start_date, sheet_name):
    '''
    抓取開始日至今的SQL Data
    input: start_date
    output: 刷站點紀錄，並存成hour_{start_date}.xlsx
    '''
    # SSL key & 資料庫連線
    conn = pymysql.connect(host='104.199.186.43',
                           port=3306,
                           user='eva.chou',
                           password='bWfFMDqiezj',
                           db='shopee24',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor,
                           ssl={'key': 'ssl/client-key.pem',
                                'cert': 'ssl/client-cert.pem',
                                'check_hostname': False})

    # sql script
    script = r'''
    SELECT
    a.cdate,
    a.working_code_l1,
    a.working_code_l2,
    a.working_code_l3,
    a.name,
    COUNT(DISTINCT a.hr_employee_info_id) AS workers,
    ROUND(SUM(total_hour)/60,1) AS total_hour

    FROM (
    SELECT a.cdate,
    a.working_code_l1,
    a.working_code_l2,
    a.working_code_l3,
    a.name,
    a.hr_employee_info_id,
    a.worker_name,
    a.created_at, a.ended_at,
    timestampdiff(minute, a.created_at, a.ended_at) AS total_hour

    FROM (
    SELECT DATE(DATE_ADD(a.created_at, INTERVAL 8 HOUR)) AS cdate,
    a.working_code_l1,
    a.working_code_l2,
    a.working_code_l3,
    b.name,
    a.hr_employee_info_id,
    a.worker_name,
    DATE_ADD(a.created_at, INTERVAL 8 HOUR) AS created_at,
    CASE WHEN DATE(DATE_ADD(a.ended_at, INTERVAL 8 HOUR)) > DATE(DATE_ADD(a.created_at, INTERVAL 8 HOUR))
    THEN DATE(DATE_ADD(a.created_at, INTERVAL 32 HOUR)) WHEN a.ended_at IS NULL
    THEN DATE(DATE_ADD(a.created_at, INTERVAL 32 HOUR))
    ELSE DATE_ADD(a.ended_at, INTERVAL 8 HOUR) END AS ended_at
    FROM shopee24.hr_working_log AS a
    LEFT JOIN shopee24.hr_working_code AS b ON CONCAT(a.working_code_l1, a.working_code_l2, a.working_code_l3) = CONCAT(b.working_code_l1, b.working_code_l2, b.working_code_l3)
    WHERE DATE(DATE_ADD(a.created_at, INTERVAL 8 HOUR)) >= {0}) AS a) AS a
    where a.working_code_l1="INBOUND" AND a.working_code_l3<>"BOX" GROUP BY a.cdate, a.working_code_l1, a.working_code_l2, a.working_code_l3, a.name
    '''

    sql_statement = script.format("'{}'".format(start_date))
    df_query = pd.read_sql_query(sql_statement, conn)
    df_query.to_csv('Input/api_data/{}.csv'.format(sheet_name), index=False)
    print('Download {} data SUCCEED'.format(sheet_name))
    conn.close()
    return df_query


if __name__ == '__main__':
    df = get_hour_data("2021-07-05")
    print(df)
