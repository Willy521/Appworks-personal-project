import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from wordcloud import WordCloud
import pymysql
from dotenv import load_dotenv
from decouple import config
import matplotlib.font_manager as fm
import altair as alt

# 環境變數
load_dotenv()
password = config('DATABASE_PASSWORD')
password_bytes = password.encode('utf-8')

# 中文字體路徑
font_path = "/Users/chenweiting/Desktop/AppWorks_Personal_Project/PingFang.ttc"
font = fm.FontProperties(fname=font_path)

# 標題和副標題
st.title('Dashboard')
st.subheader('新北市2022/11-2023/02實價登錄')

# 載入 CSV 檔案
@st.cache_data
def load_data():
    df = pd.read_csv('/Users/chenweiting/Downloads/f_lvr_land_b.csv')
    return df

# 將 交易年月日 轉換為西元年
def convert_to_ad(date_str):
    try:
        year, month, day = int(date_str[:3]), int(date_str[3:5]), int(date_str[5:])
        return datetime(year + 1911, month, day)
    except ValueError:
        return None

# 讀取和預處理數據
original_df = load_data()
df = original_df.copy()
df['交易年月日'] = df['交易年月日'].apply(convert_to_ad)
df = df[df['交易年月日'].notna()]
df.set_index('交易年月日', inplace=True)

# 新增月份欄位
df['月份'] = df.index.month

# 下拉選單篩選條件
months = ['全部', '12月', '1月', '2月']
selected_month = st.selectbox('條件篩選欄', months)

if selected_month != '全部':
    selected_month_num = int(selected_month.strip('月'))
    filtered_df = df[df['月份'] == selected_month_num]
else:
    filtered_df = df

# 顯示 DataFrame
st.write(filtered_df)

# 繪製柱狀圖（總成交數量）
df_resampled = filtered_df.resample('D').size()
st.markdown('### 成交量柱狀圖')
st.bar_chart(df_resampled)

# 繪製折線圖（按鄉鎮市區分組）
grouped_df = filtered_df.groupby(['鄉鎮市區']).resample('D').size()
fig, ax = plt.subplots()

for name, group in grouped_df.groupby('鄉鎮市區'):
    ax.plot(group.index.get_level_values('交易年月日'), group.values, label=name)

# 將圖例放在 axes 的右側
ax.legend(title='Area', loc='center left', bbox_to_anchor=(1, 0.5), prop=font)


ax.set_xlabel('交易年月日', fontproperties=font, fontsize=10)
ax.set_ylabel('成交數量', fontproperties=font, fontsize=10)
ax.set_title('新北市預售屋成交數量', fontproperties=font)


# 設定 x 軸字體大小
plt.xticks(fontsize=5)
plt.xticks(rotation=45)

plt.tight_layout(rect=[0,0,0.75,1])  # 為圖例騰出空間
st.markdown('### 分區交易量')
st.pyplot(fig)





st.subheader('輿情分析')

# 连接到数据库并查询id=12的列
try:
    conn = pymysql.connect(
        host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
        port=3306,
        user='admin',
        password=password_bytes,
        database='estate_data_hub',
        charset='utf8mb4'
    )

    with conn.cursor() as cursor:
        sql = "SELECT keyword FROM keywords_table WHERE id=12"
        cursor.execute(sql)
        result = cursor.fetchone()  # 获取单个查询结果
        keyword_string = result[0] if result else ""

except Exception as e:
    print(f"发生错误: {e}")
    keyword_string = ""

finally:
    conn.close()

# 把 list_string 转换成一个 Python 列表
if keyword_string:
    keyword_list = keyword_string.strip("[]").split(", ")
else:
    keyword_list = []


# 生成文字雲
wordcloud = WordCloud(font_path='/PingFang.ttc',  # 指定中文字体的路径
                      width=800, height=400, background_color='white').generate(' '.join(keyword_list))
# 画图
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")

# 在Streamlit中显示
st.pyplot(plt)


st.subheader('面向分析')
# 添加一個選項
option = st.selectbox('選擇一個面向', ['經濟面', '社會面', '政策面'])
st.write('觀察',option,'與房市的關係')