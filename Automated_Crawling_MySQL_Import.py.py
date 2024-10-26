import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

def get_綜合損益表(TYPEK, year, season):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    parameter = { 'firstin': '1', 'TYPEK': TYPEK,
                  'year': str(year), 'season': str(season) }
    res = requests.post(url, data=parameter)
    df = pd.read_html(res.text)[3]  # 假設第4個表格是正確的
    
    df.insert(1, '年度', year)
    df.insert(2, '季別', season)
    return df

def 自動抓取資料(TYPEK, start_year, start_season):
    current_year = datetime.now().year - 1911  # 將西元年轉為民國年
    current_month = datetime.now().month

    # 計算當前季節
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
        
        # 更新季節和年度
        if season < 4:
            season += 1
        else:
            season = 1
            year += 1
    
    # 儲存抓取的資料到 CSV 文件
    all_data.to_csv('projectdata.csv', index=False)
    print("資料抓取完成並儲存至 projectdata.csv 檔案！")
    
    return all_data  # 返回 DataFrame 以便後續使用

# 日期格式轉換
def convert_date(date_str):
    try:
        year, month_day = date_str.split('年')
        month, day = month_day.split('月')
        year = str(int(year) + 1911)  # 將民國年轉換為西元年
        return f"{year}-{month.zfill(2)}-{day.strip('日').zfill(2)}"
    except ValueError:
        print(f"日期格式錯誤: {date_str}")
        return None  # 返回 None 以便後續處理

# 自動抓取從 113 年第 3 季開始之後的資料
df = 自動抓取資料('sii', 103, 1)

# 創建資料庫連接
engine = create_engine('mysql+pymysql://root:password@localhost/sampledata')

# 嘗試匯入資料到 MySQL
try:
    df.to_sql('projectdata', con=engine, if_exists='replace', index=False)
    print("資料匯入成功！")
except Exception as e:
    print(f"發生錯誤：{e}")

# 檢查匯入後的資料量
with engine.connect() as connection:
    result = connection.execute(text("SELECT COUNT(*) FROM projectdata"))
    count = result.scalar()
    print(f"目前在 projectdata 表中的資料量為: {count} 筆")

