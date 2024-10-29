import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
from requests.exceptions import RequestException
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

def get_twse_dividend_history_for_today():
    try:
        # 獲取當天的日期，格式為 YYYYMMDD
        today = datetime.today().strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/exchangeReport/TWT49U?response=html&strDate={today}&endDate={today}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        # 嘗試重試機制
        retry_count = 3
        for i in range(retry_count):
            try:
                # 使用 requests.get() 獲取網頁內容
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                print("Successfully fetched HTML content.")

                # 將 HTML 原始內容輸出到日誌中
                with open('/tmp/twse_response.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)

                # 使用 BeautifulSoup 解析 HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')  # 找到第一個表格

                if table is None:
                    error_message = "No table found in the HTML response."
                    log_error_to_mysql(error_message, "dividend_rowdata", "35.221.134.31", "root", "rootpassword", "teamfour")
                    return None

                # 使用 pandas 讀取表格
                df = pd.read_html(str(table))[0]
                if not df.empty:
                    return df
                else:
                    error_message = "No data available in the parsed table."
                    log_error_to_mysql(error_message, "dividend_rowdata", "35.221.134.31", "root", "rootpassword", "teamfour")
                    return None
            except RequestException as e:
                print(f"Attempt {i+1} failed: {str(e)}")
                time.sleep(5)  # 等待 5 秒後重試

        # 如果重試次數用完，記錄錯誤
        error_message = "No table found in the HTML response after retries."
        log_error_to_mysql(error_message, "dividend_rowdata", "35.221.134.31", "root", "rootpassword", "teamfour")
        return None

    except requests.RequestException as e:
        error_message = f"Failed to get TWSE dividend history for today. Error: {str(e)}"
        log_error_to_mysql(error_message, "dividend_rowdata", "35.221.134.31", "root", "rootpassword", "teamfour")
        return None
    except ValueError as e:
        error_message = f"Failed to parse HTML table. Error: {str(e)}"
        log_error_to_mysql(error_message, "dividend_rowdata", "35.221.134.31", "root", "rootpassword", "teamfour")
        return None

def log_error_to_mysql(error_message: str, table_name: str, mysql_ip: str, user: str, password: str, database: str):
    try:
        # 建立 MySQL 連接字串
        connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:3306/{database}"
        engine = create_engine(connection_string)

        # 準備錯誤日誌的資料
        error_data = pd.DataFrame({
            'error_message': [error_message],
            'timestamp': [datetime.now()],
            'table_name': [table_name]
        })

        # 將錯誤日誌寫入到 error_log 表中
        error_data.to_sql(name='error_log', con=engine, if_exists='append', index=False)
        print(f"Error logged to error_log table: {error_message}")
    except Exception as e:
        # 如果記錄錯誤失敗，這裡打印錯誤，但不要讓這影響主要邏輯
        print(f"Failed to log error to MySQL. Error: {str(e)}")

def insert_into_mysql(df: pd.DataFrame, table_name: str, mysql_ip: str, user: str, password: str, database: str):
    try:
        # 將欄位名稱轉換為 MySQL 表中對應的英文名稱
        df.columns = [
            'ex_dividend_date', 'stock_code', 'stock_name', 'pre_ex_dividend_close_price',
            'ex_dividend_reference_price', 'rights_dividend_value', 'rights_or_dividend',
            'upper_limit_price', 'lower_limit_price', 'opening_reference_price',
            'dividend_deduction_reference_price', 'detailed_info', 'latest_report_period',
            'latest_report_net_value', 'latest_report_earnings'
        ]

        # 建立 MySQL 連接字串
        connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:3306/{database}"
        engine = create_engine(connection_string)

        # 將資料寫入 MySQL，若表已存在則追加
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, schema=None)
        print(f"Data inserted into table {table_name} successfully.")
    except SQLAlchemyError as e:
        # 捕捉 SQLAlchemy 的錯誤並記錄
        error_message = f"Failed to insert data into {table_name}. Error: {str(e)}"
        log_error_to_mysql(error_message, table_name, mysql_ip, user, password, database)
    except Exception as e:
        # 捕捉所有其他錯誤並記錄
        error_message = f"Unexpected error while inserting data into {table_name}. Error: {traceback.format_exc()}"
        log_error_to_mysql(error_message, table_name, mysql_ip, user, password, database)

def fetch_and_insert_data():
    # 設定 MySQL 參數
    mysql_ip = "35.221.134.31"
    user = "root"
    password = "rootpassword"
    database = "teamfour"
    table_name = "dividend_rowdata"

    try:
        # 取得當天的除權息資料
        dividend_history = get_twse_dividend_history_for_today()

        if dividend_history is None:
            # 沒有資料，則記錄完成並返回
            print("No dividend data found for today, task completed without data.")
            return

        # 插入資料到 MySQL
        insert_into_mysql(dividend_history, table_name, mysql_ip, user, password, database)
    except Exception as e:
        # 記錄任何失敗的狀況
        error_message = f"Failed to fetch and insert data. Error: {str(e)}"
        log_error_to_mysql(error_message, table_name, mysql_ip, user, password, database)

# Airflow DAG 定義
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 27, tzinfo=timezone('Asia/Taipei')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_twse_dividend_data',
    default_args=default_args,
    description='Fetch and insert daily TWSE dividend data',
    schedule_interval='@daily',
    catchup=False,
)

fetch_and_insert_task = PythonOperator(
    task_id='fetch_and_insert_data',
    python_callable=fetch_and_insert_data,
    dag=dag,
)
