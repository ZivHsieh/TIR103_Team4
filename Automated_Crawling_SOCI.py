import requests
import pandas as pd
import time
import random  # 引入随机模块
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_綜合損益表(TYPEK, year, season, retries=3):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    parameter = {'firstin': '1', 'TYPEK': TYPEK,
                 'year': str(year), 'season': str(season)}

    for attempt in range(retries):
        try:
            res = requests.post(url, data=parameter)
            res.raise_for_status()  # 检查请求是否成功
            tables = pd.read_html(res.text)

            if len(tables) < 4:
                raise Exception("未找到足夠的表格")

            df = tables[3]  # 假设第4个表格是正确的
            df.insert(1, '年度', year)
            df.insert(2, '季別', season)
            print(f"抓取到的資料形狀: {df.shape}")
            return df
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            if attempt < retries - 1:
                wait_time = 2 ** attempt  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                raise Exception(f"无法抓取 {year} 年第 {season} 季的资料，已重试 {retries} 次")

def 自動抓取資料(TYPEK, start_year, start_season, **kwargs):
    current_year = datetime.now().year - 1911
    current_month = datetime.now().month

    if 1 <= current_month <= 3:
        current_season = 1
    elif 4 <= current_month <= 6:
        current_season = 2
    elif 7 <= current_month <= 9:
        current_season = 3
    else:
        current_season = 4

    all_data = pd.DataFrame()
    year = start_year
    season = start_season

    while (year < current_year) or (year == current_year and season <= current_season):
        print(f'正在抓取 {year} 年第 {season} 季的資料...')
        try:
            df = get_綜合損益表(TYPEK, year, season)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            print(f'{year} 年第 {season} 季的資料無法取得: {e}')

        # 每次请求后增加随机睡眠时间，避免频繁请求
        sleep_time = random.randint(5, 8)  # 生成随机的睡眠时间，范围在5到8秒
        print(f'等待 {sleep_time} 秒后再进行下一次请求...')
        time.sleep(sleep_time)

        if season < 4:
            season += 1
        else:
            season = 1
            year += 1

    # 儲存抓取的資料到 MySQL
    engine = create_engine('mysql+pymysql://airflow:airflow@mysql:3306/airflow')
    try:
        all_data.to_sql('projectdata', con=engine, if_exists='replace', index=False)
        print("資料匯入成功！")
    except Exception as e:
        print(f"發生錯誤：{e}")

    # 檢查匯入後的資料量
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM projectdata"))
        count = result.scalar()
        print(f"目前在 projectdata 表中的資料量為: {count} 筆")

default_args = {
    'start_date': datetime(2023, 10, 21),
    'retries': 1,
}

with DAG('fetch_financial_data_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_financial_data',
        python_callable=自動抓取資料,
        op_kwargs={'TYPEK': 'sii', 'start_year': 103, 'start_season': 1}
    )
