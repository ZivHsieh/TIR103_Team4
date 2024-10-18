import requests
import pandas as pd
from io import StringIO
import time
import random
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()
def connect_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        user=os.getenv('DB_USER'),
        passwd=os.getenv('DB_PASSWORD'),
        charset=os.getenv('DB_CHARSET'),
        db=os.getenv('DB_NAME')
    )

def create_table_if_not_exists():
    conn = connect_db()
    with conn.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS bs (
            id VARCHAR(10),
            year INT,
            quarter INT,
            company_name VARCHAR(100),
            current_assets DECIMAL(20, 2),
            non_current_assets DECIMAL(20, 2),
            total_assets DECIMAL(20, 2),
            current_liabilities DECIMAL(20, 2),
            non_current_liabilities DECIMAL(20, 2),
            total_liabilities DECIMAL(20, 2),
            treasury_stock DECIMAL(20, 2),
            parent_equity DECIMAL(20, 2),
            joint_control_equity DECIMAL(20, 2),
            non_control_equity DECIMAL(20, 2),
            total_equity DECIMAL(20, 2),
            reference_net_value DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY `unique_id_year_quarter` (id, year, quarter)
        )
        """
        cursor.execute(sql)
    conn.commit()
    conn.close()

def safe_decimal(value):
    if value == '--' or value == 'N/A' or pd.isna(value):
        return 0
    try:
        return float(value)
    except ValueError:
        return 0

def insert_or_update_data(row, year, season):
    conn = connect_db()
    with conn.cursor() as cursor:
        sql = """
        INSERT INTO bs (
            id, year, quarter, company_name, current_assets,
            non_current_assets, total_assets, current_liabilities, non_current_liabilities,
            total_liabilities, treasury_stock, parent_equity, joint_control_equity,
            non_control_equity, total_equity, reference_net_value
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            company_name = VALUES(company_name),
            current_assets = VALUES(current_assets),
            non_current_assets = VALUES(non_current_assets),
            total_assets = VALUES(total_assets),
            current_liabilities = VALUES(current_liabilities),
            non_current_liabilities = VALUES(non_current_liabilities),
            total_liabilities = VALUES(total_liabilities),
            treasury_stock = VALUES(treasury_stock),
            parent_equity = VALUES(parent_equity),
            joint_control_equity = VALUES(joint_control_equity),
            non_control_equity = VALUES(non_control_equity),
            total_equity = VALUES(total_equity),
            reference_net_value = VALUES(reference_net_value)
        """

        cursor.execute(sql, (
            row.get('公司 代號', 'N/A'), year, season, row.get('公司名稱', 'N/A'),
            safe_decimal(row.get('流動資產', 'N/A')),
            safe_decimal(row.get('非流動資產', 'N/A')),
            safe_decimal(row.get('資產總計', 'N/A')),
            safe_decimal(row.get('流動負債', 'N/A')),
            safe_decimal(row.get('非流動負債', 'N/A')),
            safe_decimal(row.get('負債總計', row.get('負債總額', 'N/A'))),
            safe_decimal(row.get('庫藏股票', 'N/A')),
            safe_decimal(row.get('歸屬於母公司業主之權益合計', 'N/A')),
            safe_decimal(row.get('共同控制下前手權益', 'N/A')),
            safe_decimal(row.get('非控制權益', 'N/A')),
            safe_decimal(row.get('權益總計', row.get('權益總額', 'N/A'))),
            safe_decimal(row.get('每股參考淨值', 'N/A'))
        ))
    conn.commit()
    conn.close()

def get_bs(TYPEK, year, season, max_retries=3):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    parameter = {'firstin': '1', 'TYPEK': TYPEK, 'year': str(year), 'season': str(season)}

    for attempt in range(max_retries):
        try:
            res = requests.post(url, data=parameter)
            res.raise_for_status()

            tables = pd.read_html(StringIO(res.text))
            print(f"Found {len(tables)} tables")

            if len(tables) > 3:
                df = tables[3]
                print(df.columns)  # 打印列名，以便調試
            else:
                print(f"Unexpected table structure for Year {year}, Season {season}. Skipping...")
                return None

            required_columns = [
                '公司 代號', '公司名稱', '流動資產', '非流動資產', '資產總計', '流動負債',
                '非流動負債', ('負債總計', '負債總額'), '庫藏股票', '歸屬於母公司業主之權益合計',
                '共同控制下前手權益', '非控制權益', ('權益總計', '權益總額'), '每股參考淨值'
            ]

            missing_columns = []
            for col in required_columns:
                if isinstance(col, tuple):
                    if not any(c in df.columns for c in col):
                        missing_columns.append(col[0])
                elif col not in df.columns:
                    missing_columns.append(col)

            if missing_columns:
                print(f"Warning: Missing columns: {missing_columns} for Year {year}, Season {season}")
                print("Proceeding with available data...")

            for _, row in df.iterrows():
                insert_or_update_data(row, year, season)

            return df

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3))
            else:
                print(f"Unable to fetch data: Year {year}, Season {season}")
                return None

def main():
    create_table_if_not_exists()

    total_iterations = sum(4 if year != 113 else 2 for year in range(113, 107, -1))
    current_iteration = 0

    for year in range(113, 103, -1):
        if year == 113:
            seasons_to_fetch = range(1, 3)
        else:
            seasons_to_fetch = range(1, 5)

        for season in seasons_to_fetch:
            current_iteration += 1
            print(f"Progress: {current_iteration}/{total_iterations} - Processing Year {year}, Season {season}")

            get_bs('sii', year, season)

            time.sleep(random.uniform(2, 5))

    print("Data fetching and insertion into MySQL completed.")

if __name__ == "__main__":
    main()