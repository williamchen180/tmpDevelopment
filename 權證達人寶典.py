
import xlrd
import pandas as pd
from collections import defaultdict


xls = pd.ExcelFile(r"權證達人寶典_NEWVOL_2022-06-29.xls")

summary = xls.parse(0)

name1 = list(summary.iloc[2])
name2 = list(summary.iloc[3])
new_name = []
for x, y in zip(name1, name2):
    new_name.append(x + y)

df: pd.DataFrame = summary.copy()

df.columns = new_name

df = df.iloc[4:, :]

df.set_index('權證代碼', inplace=True)

data = df.to_dict(orient='index')

# ['權證代碼', '權證名稱', '發行券商', '權證價格', '權證漲跌', '權證漲跌幅', '權證成交量', '權證買價', '權證賣價', '權證買賣價差', '溢價比率', '價內價外',
# '理論價格', '隱含波動率', '有效槓桿', '剩餘天數', '最新行使比例', '標的代碼', '標的名稱', '標的價格', '標的漲跌', '標的漲跌幅', '最新履約價', '最新界限價',
# '標的20日波動率', '標的60日波動率', '標的120日波動率', '權證DELTA', '權證GAMMA', '權證VEGA', '權證THETA', '內含價值', '時間價值', '流通在外估計張數',
# '流通在外增減張數', '上市日期', '到期日期', '最新發行量', '權證發行價', '認購/售類別']


# {'權證代碼': '03027Q', '權證名稱': '恒指麥證19售03', '發行券商': '麥格理', '權證價格': 0.43, '權證漲跌': 0, '權證漲跌幅': 0, '權證成交量': 0, '權證買價': 0.43,
# '權證賣價': 0.44, '權證買賣價差': 0.0167, '溢價比率': '-', '價內價外': '-', '理論價格': nan, '隱含波動率': '-', '有效槓桿': '-', '剩餘天數': 91, '最新行使比例': 0.0002,
# '標的代碼': 'HSIHK', '標的名稱': '恒生指數', '標的價格': '-', '標的漲跌': '-', '標的漲跌幅': '-', '最新履約價': 19000, '最新界限價': '-', '標的20日波動率': 0,
# '標的60日波動率': 0, '標的120日波動率': 0, '權證DELTA': '-', '權證GAMMA': '-', '權證VEGA': '-', '權證THETA': '-', '內含價值': nan, '時間價值': nan,
# '流通在外估計張數': 0, '流通在外增減張數': 0, '上市日期': '2022/03/29', '到期日期': '2022/09/28', '最新發行量': 20000, '權證發行價': 0.748, '認購/售類別': '認售'}

stock_warrant_map = defaultdict(list)

for x in data:
    code = data[x]['標的代碼']
    stock_warrant_map[code].append(x)

print(stock_warrant_map)



