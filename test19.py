import pandas as pd  # 匯入 pandas 套件，用於資料處理

# 將民國年轉換為西元年
def convert_to_gregorian(minguo_date):
    try:
        # 將民國年的格式分割成年份和剩餘部分
        year_str, rest = minguo_date.split("年", 1)  # 將字符串分割為年份和其他部分
        year = int(year_str) + 1911  # 將民國年轉換為西元年
        return f"{year}年{rest}"  # 返回轉換後的日期字串
    except:
        return minguo_date  # 如果發生錯誤，保持原來的格式不變

# 調整填息計算邏輯，正確遍歷交易日
def calculate_fill_dividend_with_epsilon_fixed(test_row, close_prices_df):
    stock_code = str(test_row['股票代號'])  # 取得當前行的股票代號
    
    # 檢查該股票代號是否存在於收盤價資料中
    if stock_code not in close_prices_df.columns:
        return None, None  # 如果不存在，返回 None
    
    ex_right_date = test_row['資料日期']  # 取得除權息日
    ex_right_price = pd.to_numeric(test_row['除權息前收盤價'], errors='coerce')  # 確保除權息前收盤價為浮點數

    epsilon = 0.01  # 設置容忍範圍，用於比較價格

    # 找到該股票的下一次除權息日
    next_ex_right_record = test_df[(test_df['股票代號'] == stock_code) & (test_df['資料日期'] > ex_right_date)]
    if not next_ex_right_record.empty:
        next_ex_right_date = next_ex_right_record['資料日期'].min()  # 取得最近的下一個除權息日
    else:
        next_ex_right_date = close_prices_df['Unnamed: 0'].max()  # 如果沒有，使用資料中的最後一天

    # 檢查除權息當天的收盤價
    first_day_row = close_prices_df[close_prices_df['Unnamed: 0'] == ex_right_date]
    
    if not first_day_row.empty:
        closing_price_first_day = pd.to_numeric(first_day_row[stock_code].values[0], errors='coerce')  # 取得除權息日的收盤價並轉換為浮點數
        print(f"除權息日 {ex_right_date} 的收盤價: {closing_price_first_day}")
        # 比較收盤價與除權息前收盤價
        if closing_price_first_day >= ex_right_price - epsilon:
            return True, 1  # 當天即填息，返回 True 和天數 1

    # 遍歷除權息日後的每個交易日
    relevant_rows = close_prices_df[(close_prices_df['Unnamed: 0'] > ex_right_date) &
                                    (close_prices_df['Unnamed: 0'] < next_ex_right_date)].sort_values(by='Unnamed: 0')  # 確保按日期正向排序
    
    trading_day_count = 1  # 從除權息日開始計算天數
    for index, row in relevant_rows.iterrows():  # 遍歷相關的行
        trading_date = row['Unnamed: 0']  # 取得當前交易日
        closing_price = pd.to_numeric(row[stock_code], errors='coerce')  # 取得收盤價並轉換為浮點數
        print(f"日期: {trading_date}, 收盤價: {closing_price}, 累計交易日: {trading_day_count + 1}")
        
        if pd.isna(closing_price):
            continue  # 如果收盤價是 NaN，跳過這一行
        
        trading_day_count += 1  # 增加交易日天數
        
        # 如果收盤價 >= 除權息前收盤價 - epsilon，則返回結果
        if closing_price >= ex_right_price - epsilon:
            print(f"在 {trading_date} 填息成功，總交易天數: {trading_day_count}")
            return True, trading_day_count

    return False, None  # 如果沒有填息，返回 False 和 None

# 讀取資料
close_prices_df = pd.read_csv('close_prices.csv', low_memory=False)  # 讀取收盤價資料
test_df = pd.read_csv('test.csv')  # 讀取測試資料

# 檢查讀取的資料
print("Close Prices DataFrame Shape:", close_prices_df.shape)  # 顯示收盤價資料的形狀 (行數, 列數)
print("Test DataFrame Shape:", test_df.shape)  # 顯示測試資料的形狀

# 將 '資料日期' 中的民國年轉換為西元年
test_df['資料日期'] = test_df['資料日期'].apply(convert_to_gregorian)  # 轉換資料日期

# 然後轉換為標準日期格式
test_df['資料日期'] = pd.to_datetime(test_df['資料日期'], format='%Y年%m月%d日')  # 將資料日期轉換為 datetime 格式

# 確保 'Unnamed: 0' 列轉換為日期格式
close_prices_df['Unnamed: 0'] = pd.to_datetime(close_prices_df['Unnamed: 0'], format='%Y-%m-%d')  # 將收盤日期轉換為 datetime 格式

# 初始化填息結果和交易天數
test_df['填息結果'] = None  # 初始化填息結果欄位
test_df['交易天數'] = None  # 初始化交易天數欄位

# 計算所有股票的填息結果
for index, row in test_df.iterrows():  # 遍歷每一行
    fill_result = calculate_fill_dividend_with_epsilon_fixed(row, close_prices_df)  # 計算填息結果
    if fill_result is not None:
        test_df.at[index, '填息結果'] = fill_result[0]  # 設置填息結果
        test_df.at[index, '交易天數'] = fill_result[1]  # 設置交易天數

# 顯示結果，按資料日期排序
test_df_sorted = test_df[['股票代號', '資料日期', '填息結果', '交易天數']].sort_values(by='資料日期')  # 按日期排序

print(test_df_sorted)  # 輸出結果

# 將結果儲存到本地檔案
test_df_sorted.to_csv('fill_dividend_results_all_stocks_with_epsilon_fixed.csv', index=False)  # 將結果儲存到 CSV 檔案
"""
pd.read_csv():

用途：從 CSV 檔案中讀取數據並轉換為 DataFrame。
範例：pd.read_csv('file.csv') 讀取名為 file.csv 的檔案。
DataFrame.apply():

用途：將指定的函數應用於 DataFrame 的每一行或每一列。
範例：df.apply(func, axis=1) 代表對每一行應用 func。
DataFrame.iterrows():

用途：逐行遍歷 DataFrame，返回行的索引和數據。
範例：for index, row in df.iterrows(): 迭代 DataFrame 的每一行。
pd.to_datetime():

用途：將字符串或數據列轉換為 datetime 格式。
範例：pd.to_datetime(df['date_column'], format='%Y-%m-%d') 將指定列轉換為 datetime。
DataFrame.sort_values():

用途：根據指定的列對 DataFrame 進行排序。
範例：df.sort_values(by='column_name') 以 column_name 進行排序。
pd.to_numeric():

用途：將列轉換為數字型別，並可以選擇處理錯誤。
範例：pd.to_numeric(df['column_name'], errors='coerce') 將無法轉換的值設為 NaN。
DataFrame.at[]:

用途：用於快速設置 DataFrame 中的特定值。
範例：df.at[index, 'column_name'] = value 將指定位置的
"""