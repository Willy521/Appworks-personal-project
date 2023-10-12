import streamlit as st

st.set_page_config(
    page_title="Estate Data Hub",
    layout="wide",
)
import matplotlib.pyplot as plt
from datetime import datetime
from wordcloud import WordCloud
import pymysql
from dotenv import load_dotenv
from decouple import config
import matplotlib.font_manager as fm
import pydeck as pdk
import altair as alt
from geopy.geocoders import Nominatim
import pandas as pd
import folium
from folium import Map, Marker
from geopy.geocoders import Nominatim
from streamlit_folium import folium_static
import seaborn as sns
import numpy as np
import plotly.express as px
from collections import OrderedDict
import plotly.graph_objects as go
import matplotlib.ticker as ticker
import yagmail
import os
import pymysql
import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import pymysql
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import matplotlib as mpl
import hashlib



# 環境變數
load_dotenv()
password = config('DATABASE_PASSWORD')
password_bytes = password.encode('utf-8')

# # 中文字體路徑
font_path = "./PingFang.ttc"
font = fm.FontProperties(fname=font_path)


# connect to db
def connect_db():
    try:
        conn = pymysql.connect(
            host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password=password_bytes,
            database='estate_data_hub',
            charset='utf8mb4'
        )
        print("Have connected to db")
        return conn
    except Exception as e:
        print(f"error: {e}")
        return None


# sidebar
with st.sidebar:
    st.title('Estate Data Hub')
    st.caption(
        '為了解析台灣的房地產市場，構建了一個專門的數據庫，核心宗旨是「讓數據為市場說話」。除了提供預售屋的實價登錄資料作為房地產市場核心的代表，還融合了多種市場影響因子，全面評估房市交易狀態。將幫助您獲得市場真實脈絡，為購房決策提供堅實的依據。')
    st.divider()

    st.subheader('導覽')
    add_radio = st.radio(
        "分頁",
        ("全台整體房市交易狀況", "區域交易熱點分析", "房市影響因子")
    )
    st.divider()
    st.subheader('特色功能')
    st.caption('1. **全台整體房市交易狀況**：')
    st.caption('首頁呈現與房價緊密相關的因子，追蹤其近期走勢及其對房價的潛在影響。也展示預售屋價格與六都交易情況，而文字雲部分則快速揭示市場熱點與公眾評價。')

    st.caption('2. **交易細節、市場分析和趨勢**：')
    st.caption('提供近年區域細節的實價登錄資訊，展示區域交易量與價格的排名，讓您了解您所在城市的分區交易狀況和熱點排行。')

    st.caption('3. **探索影響房價的核心因子**：')
    st.caption('您可以觀察歷史數據並從經濟、社會、政策三面探索房價影響因子。我們分析特定事件的相關性，並在儀表板首頁即時追蹤四大核心因子，提供您做出決策的依據。')

# Taiwanese Overall House Price
if add_radio == "全台整體房市交易狀況":
    st.header('房價關鍵因子即時追蹤')
    col1, col2, col3, col4 = st.columns(4)


    def fix_year_quarter(date_str):
        year, quarter = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{quarter}"


    def fetch_data(table_name, column_names, data_name, time_format='%Y年第%m季'):
        conn = connect_db()
        try:
            with conn.cursor() as cursor:
                sql = f"SELECT time_name, {data_name} FROM {table_name}"
                cursor.execute(sql)
                rows = cursor.fetchall()
        finally:
            conn.close()

        df = pd.DataFrame(rows, columns=['time_name', data_name])
        df = df[df[data_name] != 0]
        df['time_name'] = df['time_name'].apply(fix_year_quarter)
        df['time_name'] = pd.to_datetime(df['time_name'], format=time_format)
        df = df[df['time_name'] >= pd.to_datetime('now') - pd.DateOffset(years=5)]

        if len(df) >= 2:
            latest_data = df.iloc[-1][data_name]
            previous_data = df.iloc[-2][data_name]
            growth_rate = ((latest_data - previous_data) / abs(previous_data)) * 100 if previous_data != 0 else 0
        else:
            latest_data = df.iloc[-1][data_name] if len(df) == 1 else 0
            growth_rate = 0

        return latest_data, growth_rate, df


    def fix_year_month(date_str):
        year, month = date_str.split('/')
        year = int(year) + 1911  # 轉換民國年份到西元年份
        return f"{year}-{month}"


    def fetch_data_rates(table_name, column_names, data_name, time_format='%Y年第%m季'):
        conn = connect_db()
        try:
            with conn.cursor() as cursor:
                sql = f"SELECT period, {data_name} FROM {table_name}"
                cursor.execute(sql)
                rows = cursor.fetchall()
        finally:
            conn.close()

        df = pd.DataFrame(rows, columns=['period', data_name])
        df = df[df[data_name] != 0]

        # 根據所使用的日期格式轉換 period
        if time_format == '%Y年第%m季':
            df['period'] = df['period'].apply(fix_year_quarter)
        elif time_format == '%Y-%m':
            df['period'] = df['period'].apply(fix_year_month)
        # 如果還有其他日期格式，可以在這裡添加更多的條件分支

        df['period'] = pd.to_datetime(df['period'], format=time_format)
        df = df[df['period'] >= pd.to_datetime('now') - pd.DateOffset(years=5)]

        if len(df) >= 2:
            latest_data = float(df.iloc[-1][data_name])
            previous_data = float(df.iloc[-2][data_name])
            growth_rate = ((latest_data - previous_data) / abs(previous_data)) * 100 if previous_data != 0 else 0
        else:
            latest_data = float(df.iloc[-1][data_name] if len(df) == 1 else 0)
            growth_rate = 0

        return latest_data, growth_rate, df


    # 經濟成長率
    latest_data, growth_rate, df1 = fetch_data('economic_construction_cost', ['time_name', 'construction_index'],
                                               'construction_index', time_format='%Y年%m月')
    with col1:
        st.metric("建築成本", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df1['time_name'], df1['construction_index'], 'r-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')
        st.pyplot(fig)

    # 景氣燈號
    latest_data, growth_rate, df2 = fetch_data('economic_cycle_indicator', ['time_name', 'strategy_signal'],
                                               'strategy_signal', time_format='%Y年%m月')
    with col2:
        st.metric("景氣燈號", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df2['time_name'], df2['strategy_signal'], 'r-')
        ax.axis('off')
        ax.grid(False)
        fig.patch.set_facecolor('#0f1116')
        st.pyplot(fig)

    # 全台戶量(人/戶)
    latest_data, growth_rate, df3 = fetch_data('society_population_data', ['time_name', 'average_household_size'],
                                               'average_household_size', time_format='%Y年%m月')
    with col3:
        st.metric("全台戶量(人/戶)", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df3['time_name'], df3['average_household_size'], 'r-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')
        st.pyplot(fig)

    # 利率
    latest_data, growth_rate, df4 = fetch_data_rates('mortgage_interest_rates', ['period', 'rate'], 'rate',
                                                     time_format='%Y-%m')
    with col4:
        st.metric("五大銀行平均房貸利率", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df4['period'], df4['rate'], 'r-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')
        st.pyplot(fig)
    with st.expander("這些圖表的意義為何？"):
        st.write("""
此儀表板區塊呈現了與房價最具關聯的前四大因子，皆基於深度相關性分析所選。這些因子整合了最近五年的數據，並即時反映資料源的最新更新，確保您能追蹤其近期走勢及其對房價的潛在影響。藉由此設計，您不僅能瞭解近期市場趨勢，還能基於最新數據做出明智的決策。
        """)

    st.header('全國房屋指數(國泰房價指數)')
    # 連接數據庫並選取所有數據
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM house_cathay_index"
            cursor.execute(sql)
            result = cursor.fetchall()

        df = pd.DataFrame(result, columns=['id', 'period', 'city', 'index_value'])

        # 畫分區圖
        cities = df['city'].unique().tolist()
        selected_cities = st.multiselect('選擇城市', cities, default=cities)

        if selected_cities:
            df_filtered = df[df['city'].isin(selected_cities)]

            # 讓使用者選擇時間範圍
            periods = sorted(df['period'].unique().tolist())
            start_period, end_period = st.select_slider(
                '選擇時間範圍',
                options=periods,
                value=(periods[0], periods[-1])
            )

            # 使用選定的時間範圍進一步過濾 df_filtered
            df_filtered = df_filtered[df_filtered['period'].between(start_period, end_period)]

            df_pivot = df_filtered.pivot(index='period', columns='city', values='index_value')
            # st.line_chart(df_pivot) # 原始大小

            # 可調整大小
            fig = px.line(df_pivot.reset_index(), x='period', y=df_pivot.columns,
                          labels={'value': '指數', 'variable': '城市', 'period': '時間'},
                          width=1200, height=550)  # 這裡調整 width 和 height

            # st.subheader('國泰房價指數')
            st.plotly_chart(fig)
        else:
            st.warning("請至少選擇一個城市。")

        # 使用 st.expander 來添加一個可展開和折疊的區塊。
        # 用另一個 expander 來提供額外的解釋或資訊。
        with st.expander("這些圖表的意義為何？"):
            st.write("""
                國泰房價指數為國泰建設與政治大學台灣房地產研究中心、不動產界學者合作編製，於每季發布研究成果(目前更新至112年第2季度)，主要是針對「預售及新屋物件」，為國內房地產主要參考指標之一。
            """)

    except Exception as e:
        st.error(f"錯誤: {e}")


    # finally:
    #     conn.close()

    # 近三季量能排行
    def parse_date(transaction_date_str):
        year = int(transaction_date_str[:3]) + 1911
        month = int(transaction_date_str[3:5])
        day = int(transaction_date_str[5:])
        return f"{year}-{month:02d}-{day:02d}"


    st.header('近四個月六都量能分析')

    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            sql = """
            SELECT city, transaction_date, COUNT(*) as transaction_count 
            FROM sprint3_demo 
            WHERE city IN ('臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市') 
            GROUP BY city, transaction_date
            """
            cursor.execute(sql)
            result = cursor.fetchall()

        df = pd.DataFrame(result, columns=['city', 'transaction_date', 'transaction_count'])

        # Convert transaction_date from the format '1111113' to '2022-11-13'
        df['transaction_date'] = df['transaction_date'].apply(parse_date)
        # Convert the string date to datetime format
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        # Filter data for the last three months (to get data for each month)
        latest_date = df['transaction_date'].max()
        three_months_ago = pd.Timestamp(latest_date) - pd.DateOffset(months=3)
        df_filtered = df[df['transaction_date'] > three_months_ago]

        # Create a 'month' column for grouping
        df_filtered['month'] = df_filtered['transaction_date'].dt.strftime('%Y-%m')

        # Group by city and month, then sum transaction counts
        df_grouped = df_filtered.groupby(['month', 'city'])['transaction_count'].sum().reset_index()

        # Define cities and months order
        cities = ['臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市']
        months = df_grouped['month'].unique()

        # Create the figure and axes
        fig = go.Figure()

        # Add bars for each city in each month
        for month in months:
            month_data = df_grouped[df_grouped['month'] == month]
            fig.add_trace(go.Bar(
                x=month_data['city'],
                y=month_data['transaction_count'],
                name=month,
                text=month_data['transaction_count'],
                textposition='inside'
            ))

        # Adjust the layout
        fig.update_layout(
            # title='近三個月六都量能比較',
            xaxis_title='城市',
            yaxis_title='戶',
            barmode='group',
            bargap=0.15,
            bargroupgap=0.1,
            width=1200,  # Adjust the width here
            height=500  # Adjust the height here
        )

        st.plotly_chart(fig)
        with st.expander("這些圖表的意義為何？"):
            st.write("""
                以月份切分，觀察各主要縣市的交易量不僅能鮮明地突顯每個縣市間的交易差異，也可以讓您明確掌握房地產的短期變化。這種差異讓您初步了解各地的交易狀況。有了這樣的基礎認識，您可以再深入探究單一城市內不同區域的具體交易動態。這不僅有助於識別哪些地區在特定時期較為活躍，還可以進一步預測未來的市場趨勢，成為您投資或購房決策的重要依據。
            """)


    except Exception as e:
        st.error(f"錯誤: {e}")

    st.header('輿情分析')
    # 连接到数据库并查询id=12的列
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            sql = "SELECT keyword FROM keywords_table WHERE id=22"
            cursor.execute(sql)
            result = cursor.fetchone()  # 获取单个查询结果
            keyword_string = result[0] if result else ""

    except Exception as e:
        print(f"error: {e}")
        keyword_string = ""
    finally:
        conn.close()

    # 把 list_string 转换成一个 Python 列表
    if keyword_string:
        keyword_list = keyword_string.strip("[]").split(", ")
    else:
        keyword_list = []

    # 生成文字雲
    wordcloud = WordCloud(font_path='./PingFang.ttc',  # 指定中文字体的路径 ./real_estate/PingFang.ttc
                          width=800, height=400, background_color='#0f1116').generate(' '.join(keyword_list))

    print(keyword_list)  # 查看关键词列表是否为空
    print(wordcloud)  # 查看wordcloud对象

    # 画图
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")

    # 在Streamlit中显示
    st.pyplot(plt)
    with st.expander("這些圖表的意義為何？"):
        st.write("""
            整理近一月的主要房市新聞，透過文字雲呈現市場焦點和熱門公眾話題，助您迅速掌握市場脈動。
        """)


# Trading Hotspots
elif add_radio == "區域交易熱點分析":


    # # st.title('區域交易熱點分析')
    # st.header('六都預售屋實價登錄')
    #
    # # 篩選城市
    # cities = ['臺北市', '新北市', '桃園市', '新竹市', '新竹縣', '臺中市', '臺南市', '高雄市']
    # selected_city = st.selectbox('條件篩選欄', cities)
    # st.subheader(f'{selected_city} 預售屋實價登錄')
    #
    #
    # # 將 交易年月日 轉換為西元年
    # def convert_to_ad(date_str):
    #     try:
    #         year, month, day = int(date_str[:3]), int(date_str[3:5]), int(date_str[5:])
    #         return datetime(year + 1911, month, day)
    #     except ValueError:
    #         return None
    #
    # conn = connect_db()
    # if conn:
    #     try:
    #         with conn.cursor() as cursor:
    #             # 加入 WHERE 子句來過濾特定城市的數據
    #             cursor.execute(
    #                 f'SELECT city, district, transaction_sign, address, build_case, buildings_and_number, transaction_date, total_price_NTD, unit_price_NTD_per_square_meter, berth_category, land_area, zoning, transaction_pen_number, shifting_level, total_floor_number, building_state, main_use, main_building_materials, building_area, pattern_room, pattern_hall, pattern_health, pattern_compartmented, has_management_organization, berth_area, berth_total_price_NTD, note, serial_number FROM real_estate WHERE city = "{selected_city}"')
    #             data = cursor.fetchall()
    #
    #
    #
    #             # Convert the data to a pandas DataFrame 可以自定義欄位名稱
    #             df = pd.DataFrame(data,
    #                               columns=['城市', '鄉鎮市區', '交易標的', '土地位置建物門牌', '建案名稱', '棟及號', '交易年月日', '總價元', '單價元平方公尺',
    #                                        '車位類別', '土地移轉總面積平方公尺', '都市土地使用分區', '交易筆棟數', '移轉層次', '總樓層數', '建物型態', '主要用途',
    #                                        '主要建材', '建物移轉總面積平方公尺', '建物現況格局-房', '建物現況格局-廳', '建物現況格局-衛', '建物現況格局-隔間',
    #                                        '有無管理組織', '車位移轉總面積平方公尺', '車位總價元', '備註', '編號', ])
    #
    #             # 將 交易年月日 轉換為西元日期
    #             df['交易年月日'] = df['交易年月日'].apply(convert_to_ad)
    #             df.dropna(subset=['交易年月日'], inplace=True)
    #             df['交易年月日'] = pd.to_datetime(df['交易年月日'])
    #
    #             # slider
    #             min_date = df['交易年月日'].min().to_pydatetime()
    #             max_date = df['交易年月日'].max().to_pydatetime()
    #             start_date, end_date = st.slider('選擇日期範圍', min_value=min_date, max_value=max_date,
    #                                              value=(min_date, max_date))
    #             filtered_df = df[(df['交易年月日'] >= start_date) & (df['交易年月日'] <= end_date)]
    #             st.write(filtered_df)
    #
    #             # 成交量柱狀圖
    #             # filtered_df.set_index('交易年月日', inplace=True)
    #             # df_resampled = filtered_df.resample('D').size()
    #             # st.subheader(f'{selected_city} 預售屋總體成交量分析')
    #             # st.bar_chart(df_resampled)
    #
    #             filtered_df.set_index('交易年月日', inplace=True)
    #             df_resampled = filtered_df.resample('D').size().reset_index()
    #             df_resampled.columns = ['交易年月日', '成交量']
    #
    #             fig = px.bar(df_resampled, x='交易年月日', y='成交量')
    #
    #             fig.update_layout(
    #                 height=500,
    #                 width=1100,
    #                 xaxis_title="時間",
    #                 yaxis_title="戶",
    #                 plot_bgcolor='#0f1116',  # 背景色
    #                 paper_bgcolor='#0f1116',  # 畫布背景色
    #                 font=dict(color='white')  # 文字顏色
    #             )
    #
    #             st.plotly_chart(fig)
    #
    #             # 分區折線圖
    #             filtered_df['鄉鎮市區'].fillna('未知', inplace=True)  # 鄉鎮區有nan值
    #             unique_areas = filtered_df['鄉鎮市區'].unique()
    #             selected_areas = st.multiselect('選擇鄉鎮市區', options=unique_areas.tolist(), default=unique_areas.tolist())
    #
    #             # 使用保留的 '日期' 列來創建折線圖的 DataFrame
    #             line_chart_df = filtered_df.groupby(['交易年月日', '鄉鎮市區']).size().reset_index(name='交易量')
    #             line_chart_df = line_chart_df.pivot(index='交易年月日', columns='鄉鎮市區', values='交易量').fillna(0)
    #
    #             colors = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
    #                       '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
    #                       '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
    #                       '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']
    #
    #             # 使用plotly繪製折線圖
    #             fig = px.line(line_chart_df, x=line_chart_df.index, y=selected_areas, color_discrete_sequence=colors)
    #
    #             # 客製化圖形
    #             fig.update_layout(
    #                 height=500,
    #                 width=1200,
    #                 xaxis_title="時間",
    #                 yaxis_title="戶",
    #                 plot_bgcolor='#0f1116',  # 背景色
    #                 paper_bgcolor='#0f1116',  # 畫布背景色
    #                 font=dict(color='white')  # 文字顏色
    #             )
    #
    #             # 在streamlit上顯示圖形
    #             st.plotly_chart(fig)
    #
    #
    #             # 圓餅圖表示各個建案的交易次數佔比
    #
    #             # 基於所選的城市，提供區域的下拉式選單
    #             districts = df['鄉鎮市區'].unique()
    #             selected_district = st.selectbox(f'選擇 {selected_city} 的區域', options=districts.tolist())
    #             st.subheader(f'{selected_city} {selected_district} 建案銷售佔比')
    #
    #             # 根據選定的城市和區域進行篩選
    #             filtered_df = df[(df['城市'] == selected_city) & (df['鄉鎮市區'] == selected_district)]
    #
    #             pie_df = filtered_df.groupby('建案名稱').size().reset_index(name='交易次數')
    #             pie_df = pie_df.sort_values('交易次數', ascending=False)
    #
    #             pie_fig = px.pie(pie_df, values='交易次數', names='建案名稱')
    #
    #             pie_fig.update_layout(
    #                 height=500,
    #                 width=1200,
    #                 font=dict(color='white'),  # 文字顏色
    #                 plot_bgcolor='#0f1116',  # 背景色
    #                 paper_bgcolor='#0f1116',  # 畫布背景色
    #             )
    #
    #             # 在streamlit上顯示圓餅圖
    #             st.plotly_chart(pie_fig)
    #
    #     except Exception as e:
    #         st.error(f"Error while fetching data: {e}")
    # 定義一個函數來計算數據的哈希值，以便作為快取的鍵
    def compute_hash(data):
        return hashlib.sha256(str(data).encode()).hexdigest()


    # 定義一個函數來將數據保存到快取中
    @st.cache_data
    def load_data(selected_city):
        conn = connect_db()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'SELECT city, district, transaction_sign, address, build_case, buildings_and_number, transaction_date, total_price_NTD, unit_price_NTD_per_square_meter, berth_category, land_area, zoning, transaction_pen_number, shifting_level, total_floor_number, building_state, main_use, main_building_materials, building_area, pattern_room, pattern_hall, pattern_health, pattern_compartmented, has_management_organization, berth_area, berth_total_price_NTD, note, serial_number FROM real_estate WHERE city = "{selected_city}"')
                    data = cursor.fetchall()
                    return data
            except Exception as e:
                st.error(f"Error while fetching data: {e}")


    # 篩選城市
    cities = ['臺北市', '新北市', '桃園市', '新竹市', '新竹縣', '臺中市', '臺南市', '高雄市']
    selected_city = st.selectbox('條件篩選欄', cities)
    st.subheader(f'{selected_city} 預售屋實價登錄')

    # 從快取中加載數據
    data = load_data(selected_city)


    # 將 交易年月日 轉換為西元年
    def convert_to_ad(date_str):
        try:
            year, month, day = int(date_str[:3]), int(date_str[3:5]), int(date_str[5:])
            return datetime(year + 1911, month, day)
        except ValueError:
            return None


    # Convert the data to a pandas DataFrame 可以自定義欄位名稱
    df = pd.DataFrame(data,
                      columns=['城市', '鄉鎮市區', '交易標的', '土地位置建物門牌', '建案名稱', '棟及號', '交易年月日', '總價元', '單價元平方公尺',
                               '車位類別', '土地移轉總面積平方公尺', '都市土地使用分區', '交易筆棟數', '移轉層次', '總樓層數', '建物型態', '主要用途',
                               '主要建材', '建物移轉總面積平方公尺', '建物現況格局-房', '建物現況格局-廳', '建物現況格局-衛', '建物現況格局-隔間',
                               '有無管理組織', '車位移轉總面積平方公尺', '車位總價元', '備註', '編號', ])

    # 將 交易年月日 轉換為西元日期
    df['交易年月日'] = df['交易年月日'].apply(convert_to_ad)
    df.dropna(subset=['交易年月日'], inplace=True)
    df['交易年月日'] = pd.to_datetime(df['交易年月日'])

    # slider
    min_date = df['交易年月日'].min().to_pydatetime()
    max_date = df['交易年月日'].max().to_pydatetime()
    start_date, end_date = st.slider('選擇日期範圍', min_value=min_date, max_value=max_date,
                                     value=(min_date, max_date))
    filtered_df = df[(df['交易年月日'] >= start_date) & (df['交易年月日'] <= end_date)]
    st.write(filtered_df)

    # 成交量柱狀圖
    filtered_df.set_index('交易年月日', inplace=True)
    df_resampled = filtered_df.resample('D').size().reset_index()
    df_resampled.columns = ['交易年月日', '成交量']

    fig = px.bar(df_resampled, x='交易年月日', y='成交量')

    fig.update_layout(
        height=500,
        width=1100,
        xaxis_title="時間",
        yaxis_title="戶",
        plot_bgcolor='#0f1116',  # 背景色
        paper_bgcolor='#0f1116',  # 畫布背景色
        font=dict(color='white')  # 文字顏色
    )

    st.plotly_chart(fig)

    # 分區折線圖
    filtered_df['鄉鎮市區'].fillna('未知', inplace=True)  # 鄉鎮區有nan值
    unique_areas = filtered_df['鄉鎮市區'].unique()
    selected_areas = st.multiselect('選擇鄉鎮市區', options=unique_areas.tolist(), default=unique_areas.tolist())

    # 使用保留的 '日期' 列來創建折線圖的 DataFrame
    line_chart_df = filtered_df.groupby(['交易年月日', '鄉鎮市區']).size().reset_index(name='交易量')
    line_chart_df = line_chart_df.pivot(index='交易年月日', columns='鄉鎮市區', values='交易量').fillna(0)

    colors = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
              '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
              '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
              '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']

    # 使用plotly繪製折線圖
    fig = px.line(line_chart_df, x=line_chart_df.index, y=selected_areas, color_discrete_sequence=colors)

    # 客製化圖形
    fig.update_layout(
        height=500,
        width=1200,
        xaxis_title="時間",
        yaxis_title="戶",
        plot_bgcolor='#0f1116',  # 背景色
        paper_bgcolor='#0f1116',  # 畫布背景色
        font=dict(color='white')  # 文字顏色
    )

    # 在streamlit上顯示圖形
    st.plotly_chart(fig)

    # 圓餅圖表示各個建案的交易次數佔比

    # 基於所選的城市，提供區域的下拉式選單
    districts = df['鄉鎮市區'].unique()
    selected_district = st.selectbox(f'選擇 {selected_city} 的區域', options=districts.tolist())
    st.subheader(f'{selected_city} {selected_district} 建案銷售佔比')

    # 根據選定的城市和區域進行篩選
    filtered_df = df[(df['城市'] == selected_city) & (df['鄉鎮市區'] == selected_district)]

    pie_df = filtered_df.groupby('建案名稱').size().reset_index(name='交易次數')
    pie_df = pie_df.sort_values('交易次數', ascending=False)

    pie_fig = px.pie(pie_df, values='交易次數', names='建案名稱')

    pie_fig.update_layout(
        height=600,
        width=1200,
        font=dict(color='white'),  # 文字顏色
        plot_bgcolor='#0f1116',  # 背景色
        paper_bgcolor='#0f1116',  # 畫布背景色
    )

    # 在streamlit上顯示圓餅圖
    st.plotly_chart(pie_fig)




    # --------------- 地圖 --------
    # 先創一個新表格然後取100個新增座標

    from geopy.geocoders import Nominatim

    import time

    # # 建立資料庫連接
    # conn = pymysql.connect(
    #     host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
    #     port=3306,
    #     user='admin',
    #     password=password_bytes,  # 確保 password_bytes 變量在此處是可用的
    #     database='estate_data_hub',
    #     charset='utf8mb4'
    # )
    #
    # cursor = conn.cursor()  # 定義 cursor
    #
    # # 使用Nominatim服務
    # geolocator = Nominatim(user_agent="geoapi")
    #
    # try:
    #     # 創建新表格，如果不存在的話
    #     cursor.execute("""
    #         CREATE TABLE IF NOT EXISTS house_price_map (
    #             id INT,
    #             district VARCHAR(255),
    #             transaction_sign VARCHAR(255),
    #             address VARCHAR(255),
    #             transaction_date DATE,
    #             build_case VARCHAR(255),
    #             latitude DOUBLE,
    #             longitude DOUBLE,
    #             PRIMARY KEY (id)
    #         )
    #     """)
    #     conn.commit()
    #
    #     # 取出地址並進行地理編碼，並插入新表
    #     cursor.execute("SELECT id, district, transaction_sign, address, transaction_date, build_case FROM real_estate LIMIT 1000")
    #     rows = cursor.fetchall()
    #     for row in rows:
    #         location = geolocator.geocode(row[3], timeout=10)  # row[3] 應該是address
    #         if location:
    #             print(f"ID: {row[0]}, Address: {row[3]}, Latitude: {location.latitude}, Longitude: {location.longitude}")
    #             new_row = row[:6] + (location.latitude, location.longitude)
    #             cursor.execute("""
    #                 INSERT IGNORE INTO house_price_map (id, district, transaction_sign, address, transaction_date, build_case, latitude, longitude)
    #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #             """, new_row)
    #             conn.commit()
    #         time.sleep(4)
    # except Exception as e:
    #     print("Error: unable to fetch data", e)
    # finally:
    #     cursor.close()  # It's good practice to close the cursor when it's no longer needed.
    #     conn.close()

    # query demo data
    # st.subheader('區域熱點')

    #
    # conn = connect_db()
    # cursor = conn.cursor()  # 定義 cursor
    #
    # # 從數據庫中獲取數據
    # cursor.execute("SELECT latitude, longitude, address, build_case FROM house_price_map")
    # rows = cursor.fetchall()
    #
    # # 數據格式化
    # data = pd.DataFrame(rows, columns=['lat', 'lon', 'address', 'build_case'])
    #
    # st.write(data)  # 或者 st.table(data) 以表格的形式顯示
    # # st.table(data)
    #
    # # 定義 tooltip
    # tooltip = {
    #     "html": "<b>Value:</b>{elevationValue}",
    #     "style": {"backgroundColor": "steelblue", "color": "white"}
    # }
    #
    # # 3D 地圖的層
    # layer = pdk.Layer(
    #     "HexagonLayer",
    #     # 'ColumnLayer',
    #     data=data,
    #     get_position='[lon, lat]',
    #     radius=200,
    #     elevation_scale=8,
    #     elevation_range=[0, 1000],
    #     pickable=True,
    #     extruded=True,
    # )
    #
    # # 3D 地圖的視圖
    # # view_state = pdk.ViewState(latitude=25.0330, longitude=121.5654, zoom=11, pitch=45)
    # view_state = pdk.ViewState(latitude=25.0118, longitude=121.4559, zoom=11, pitch=45)
    #
    # # 在 Streamlit 應用中渲染 3D 地圖
    # # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))
    # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))
    #
    # conn = connect_db()
    # cursor = conn.cursor()  # 定義 cursor
    #
    # # 從數據庫中獲取數據
    # cursor.execute("SELECT latitude, longitude, address, build_case FROM house_price_map")
    # rows = cursor.fetchall()
    #
    # # 數據格式化
    # data = pd.DataFrame(rows, columns=['lat', 'lon', 'address', 'build_case'])
    #
    # # 定義 tooltip
    # tooltip = {
    #     "html": "<b>Address:</b>{build_case}",
    #     "style": {"backgroundColor": "steelblue", "color": "white"}
    # }
    #
    # # 3D 地圖的層
    # layer = pdk.Layer(
    #     # "HexagonLayer",
    #     'ColumnLayer',
    #     data=data,
    #     get_position='[lon, lat]',
    #     radius=200,
    #     elevation_scale=80,
    #     elevation_range=[0, 1000],
    #     get_elevation='elevation',  # 如果 build_case 是数值，您可以直接将其用作 elevation
    #     # get_fill_color='[255, 165, 0, 140]',  # 设置颜色为橙色，最后的 140 是透明度
    #     get_fill_color='[build_case * 15, 165, 0]',
    #     pickable=True,
    #     extruded=True,
    # )
    #
    # # 3D 地圖的視圖
    # view_state = pdk.ViewState(latitude=25.0330, longitude=121.5654, zoom=11, pitch=45)
    #
    # # 在 Streamlit 應用中渲染 3D 地圖
    # # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))
    # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))


elif add_radio == "房市影響因子":
    st.title('房價影響因子')
    st.subheader('時間趨勢 & 相關性關係圖')
    conn = connect_db()


    # 格式轉換
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"


    # 格式轉換
    def fix_period(period_str):
        year, quarter = period_str.split('Q')
        year = int(year)
        if year < 1000:
            year += 1911

        month_map = {
            '1': '01',
            '2': '04',
            '3': '07',
            '4': '10',
        }
        return f"{year}-{month_map[quarter]}-01"


    # 格式轉換
    def fix_period_mortgage(period_str):
        year, month = period_str.split('/')
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}-{month}-01"


    try:
        # Read the first DataFrame
        with conn.cursor() as cursor:
            cursor.execute("SELECT time_name, strategy_signal FROM economic_cycle_indicator")
            rows = cursor.fetchall()
            df1 = pd.DataFrame(rows, columns=['time_name', 'strategy_signal'])
            df1.rename(columns={"strategy_signal": "景氣信號燈"}, inplace=True)
            df1 = df1[df1['景氣信號燈'] != 0]
            df1['time_name'] = df1['time_name'].apply(fix_year)
            df1['time_name'] = pd.to_datetime(df1['time_name'], format='%Y年%m月')

        # Read the second DataFrame
        df2 = pd.read_sql("SELECT time_name, economic_growth_rate as `經濟年成長率(%)` FROM economic_gdp_indicator", conn)
        df2['year'] = df2['time_name'].str.split('年').str[0].astype(int) + 1911
        df2['quarter'] = df2['time_name'].str.split('年').str[1]
        df2.loc[df2['quarter'] == '第4季', 'year'] = df2['year'] + 1
        df2['month'] = df2['quarter'].map({'第1季': '04', '第2季': '07', '第3季': '10', '第4季': '01'})
        df2['date'] = pd.to_datetime(df2['year'].astype(str) + '-' + df2['month'] + '-01', format='%Y-%m-%d',
                                     errors='coerce')
        filtered_df2 = df2[df2['經濟年成長率(%)'] != 0]

        # 國泰房屋指數
        df3 = pd.read_sql("SELECT period, index_value as `國泰房價指數(全國)` FROM house_cathay_index WHERE city = '全國'",
                          conn)
        df3['date'] = df3['period'].apply(fix_period)
        df3['date'] = pd.to_datetime(df3['date'], format='%Y-%m-%d')

        # 營造指數
        df4 = pd.read_sql("SELECT time_name, construction_index as `營造工程總指數` FROM economic_construction_cost", conn)
        df4['time_name'] = df4['time_name'].apply(fix_year)  # 修正year
        df4['date'] = pd.to_datetime(df4['time_name'], format='%Y年%m月')

        # 利率
        df5 = pd.read_sql("SELECT period, rate as `五大銀行平均房貸利率(%)` FROM mortgage_interest_rates", conn)
        df5['date'] = df5['period'].apply(fix_period_mortgage)
        df5['date'] = pd.to_datetime(df5['date'], format='%Y-%m-%d')
        df5['五大銀行平均房貸利率(%)'] = df5['五大銀行平均房貸利率(%)'].astype(float)

        # 總人口數, 戶數, 戶量
        df6 = pd.read_sql(
            "SELECT time_name, population_count as `全台人口數(人)`, household_count as `全台戶數(戶)`, average_household_size as `全台戶量(人/戶)` FROM society_population_data",
            conn)
        df6['time_name'] = df6['time_name'].apply(fix_year)  # 修正year
        df6['date'] = pd.to_datetime(df6['time_name'], format='%Y年%m月')
    finally:
        conn.close()

    # Merge DataFrames on date columns
    merged_df = pd.merge(df1, filtered_df2, left_on='time_name', right_on='date', how='inner')
    merged_df = pd.merge(merged_df, df3, on='date', how='inner')
    merged_df = pd.merge(merged_df, df4, on='date', how='inner')
    merged_df = pd.merge(merged_df, df5, on='date', how='inner')
    merged_df = pd.merge(merged_df, df6, on='date', how='inner', suffixes=('', '_df6'))

    # 使用slider篩選日期
    # min_date = merged_df['date'].min().to_pydatetime()
    # max_date = merged_df['date'].max().to_pydatetime()
    # start_date, end_date = st.slider('選擇日期範圍', min_date, max_date, [min_date, max_date])
    # filtered_df = merged_df[(merged_df['date'] >= start_date) & (merged_df['date'] <= end_date)]
    # st.table(merged_df) 全部放在同一個table

    numeric_vars = ['國泰房價指數(全國)', '營造工程總指數', '景氣信號燈', '經濟年成長率(%)', '五大銀行平均房貸利率(%)', '全台人口數(人)', '全台戶數(戶)', '全台戶量(人/戶)']
    leftcol, rightcol = st.columns([1, 1])
    with leftcol:
        xvar = st.selectbox("X 變量", numeric_vars)
    with rightcol:
        # yvar = st.selectbox("Y variable", numeric_vars, index=len(numeric_vars) - 6)
        yvar = st.selectbox("Y 變量", numeric_vars)

    # 如果沒有可供繪圖的數值變量，則顯示警告並停止
    if len(numeric_vars) < 1:
        st.warning("沒有找到可供繪圖的數值列。")
        st.stop()

    # 兩個變量的趨勢圖
    leftcol, rightcol = st.columns([1, 1])


    # 人口數要變Million
    def millions_formatter(x, pos):
        return f'{x / 1e6:.1f}M'


    with leftcol:
        plt.figure(figsize=(10, 6))
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        ax_left = sns.lineplot(x='date', y=xvar, data=merged_df)  # 替換 'YourTimeColumn' 為您的時間列名
        # 檢查是否繪製人口數量
        if xvar == "全台人口數(人)" or xvar == "全台戶數(戶)":
            ax_left.yaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title(f'{xvar} 趨勢圖', color='white', fontproperties=font)
        plt.xlabel('時間', color='white', fontproperties=font)
        plt.ylabel(xvar, color='white', fontproperties=font)
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)

    with rightcol:
        plt.figure(figsize=(10, 6))
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        ax_right = sns.lineplot(x='date', y=yvar, data=merged_df)  # 替換 'YourTimeColumn' 為您的時間列名
        # 檢查是否繪製人口數量
        if yvar == "全台人口數(人)" or yvar == "全台戶數(戶)":
            ax_right.yaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title(f'{yvar} 趨勢圖', color='white', fontproperties=font)
        plt.xlabel('時間', color='white', fontproperties=font)
        plt.ylabel(yvar, color='white', fontproperties=font)
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)

    # st.subheader('相關性關係圖表')
    leftcol, centercol, rightcol = st.columns([1.3, 3.5, 1])
    # 兩個變量的相關性圖表
    with centercol:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.scatterplot(x=xvar, y=yvar, data=merged_df)
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        sns.regplot(x=xvar, y=yvar, data=merged_df, scatter=False, color='red')

        # 檢查x軸是否是人口數
        if xvar == "全台人口數(人)" or xvar == "全台戶數(戶)":
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))
        # 檢查y軸是否是人口數
        if yvar == "全台人口數(人)" or yvar == "全台戶數(戶)":
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

        plt.xlabel(xvar)
        plt.ylabel(yvar)
        plt.gca().set_facecolor('#0f1116')
        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title('相關性', color='white', fontproperties=font)
        plt.xlabel(xvar, color='white', fontproperties=font)
        plt.ylabel(yvar, color='white', fontproperties=font)
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)
    with st.expander("如何解讀這些圖表？"):
        st.write("""
           「房價影響因子」頁面目的在提供一個深入探討各種因素對房價影響的平台。我們整合了來自經濟、社會和政策面的資料並且透過視覺化數據呈現，我們可以直觀地觀察這些因子如何隨時間演變，並試圖了解它們與房價之間可能的關聯。

「時間趨勢圖」使使用者能夠直觀地看到各個因子隨著時間的變化趨勢，幫助我們理解這些因子在過去的歷史表現。「相關性關係圖」則展現了兩個選定因子之間的可能關聯。特別地，紅色的回歸線有助於我們揭示這些因子之間的線性關係，進一步預測它們對房價的潛在影響。

當我們確定了與房價具有高度相關性的指標後，這些因子將會被整合到我們的主要儀表板中，以持續追踪其對房價的影響。這不僅讓我們能夠更加精確地掌握房價動態，更希望透過數據驅動的方法優化整個決策的過程。

關於未來展望與參與方式，隨著時間的進行，我們會繼續增強資料來源和分析的深度，以提供更全面的視角。我們歡迎使用者提供寶貴的反饋和建議。
        """)
        user_name = st.text_input("名稱：")
        user_email = st.text_input("電子郵件：")
        user_feedback = st.text_area("回饋：")

        if st.button("提交"):
            GMAIL_USER = os.getenv("GMAIL_USER")
            GMAIL_APP_PASS = os.getenv("GMAIL_APP_PASS")

            # Send the feedback as an email
            yag = yagmail.SMTP(GMAIL_USER, GMAIL_APP_PASS)
            subject = "New Feedback from " + user_name
            content = [f"From: {user_name} <{user_email}>", f"Feedback: {user_feedback}"]
            yag.send(GMAIL_USER, subject, content)
            st.success("回饋已提交，謝謝你！")

    with st.expander("這些數據告訴我們哪些趨勢？"):
        st.write('')
