import requests
import csv
import pandas as pd
url = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
res = requests.get(url)  

info = pd.DataFrame(res.json())
info.to_csv(r'./info.csv', index=0, encoding='utf-8-sig', header=None)

# print(res.text)
# info_columns = [
#     "出表日期": "string",
#     "公司代號": "string",
#     "公司名稱": "string",
#     "公司簡稱": "string",
#     "外國企業註冊地國": "string",
#     "產業別": "string",
#     "住址": "string",
#     "營利事業統一編號": "string",
#     "董事長": "string",
#     "總經理": "string",
#     "發言人": "string",
#     "發言人職稱": "string",
#     "代理發言人": "string",
#     "總機電話": "string",
#     "成立日期": "string",
#     "上市日期": "string",
#     "普通股每股面額": "string",
#     "實收資本額": "string",
#     "私募股數": "string",
#     "特別股": "string",
#     "編制財務報表類型": "string",
#     "股票過戶機構": "string",
#     "過戶電話": "string",
#     "過戶地址": "string",
#     "簽證會計師事務所": "string",
#     "簽證會計師1": "string",
#     "簽證會計師2": "string",
#     "英文簡稱": "string",
#     "英文通訊地址": "string",
#     "傳真機號碼": "string",
#     "電子郵件信箱": "string",
#     "網址": "string",
#     "已發行普通股數或TDR原股發行股數": "string"
# ]

# with open('info.csv', mode='w', newline='') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=headers)
#     writer.writerheader()
#     writer.writerows(data)

