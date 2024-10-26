import requests
import csv
import pandas as pd
url = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
res = requests.get(url)  

info = pd.DataFrame(res.json())
info.to_csv(r'./info.csv', index=0, encoding='utf-8-sig', header=None)