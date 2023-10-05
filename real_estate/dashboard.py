import pandas as pd
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
import pydeck as pdk
import streamlit as st
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


# 環境變數
load_dotenv()
password = config('DATABASE_PASSWORD')
password_bytes = password.encode('utf-8')

# # 中文字體路徑
font_path = "./PingFang.ttc"
font = fm.FontProperties(fname=font_path)

# widemode
st.set_page_config(layout="wide")


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
        print(f"发生错误: {e}")
        return None


# sidebar
with st.sidebar:
    st.title('Estate Data Hub')
    # st.subheader('Real Transactions, Genuine Data')
    st.caption(
        'Introducing our Taiwan-centric real estate data dashboard designed to let "data lead the conversation."')
    st.divider()
    st.subheader('Navigator')
    add_radio = st.radio(
        "Data Pages",
        ("Taiwanese Overall House Price", "Trading Hotspots", "Impact Factors")
    )
    st.divider()
    st.subheader('Features')
    st.caption('1. Data-First Approach: Encouraging decisions rooted in accurate information.')
    st.caption('2. Rich Database: Covers transaction details, market trends, and supply-demand metrics.')
    st.caption(
        '3. Holistic Monitoring: Tracks external factors like financial policies and global economic indicators.')

# Taiwanese Overall House Price
if add_radio == "Taiwanese Overall House Price":
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
        df = df[df['time_name'] >= pd.to_datetime('now') - pd.DateOffset(years=10)]

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
    latest_data, growth_rate, df1 = fetch_data('economic_gdp_indicator', ['time_name', 'economic_growth_rate'], 'economic_growth_rate')
    with col1:
        st.metric("經濟成長率", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df1['time_name'], df1['economic_growth_rate'], 'r-')
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

    # 建築成本
    latest_data, growth_rate, df3 = fetch_data('economic_construction_cost', ['time_name', 'construction_index'],
                                               'construction_index', time_format='%Y年%m月')
    with col3:
        st.metric("建築成本", f"{latest_data}", f"{growth_rate:.2f}%")
        fig, ax = plt.subplots(figsize=(5.33, 2))
        ax.plot(df3['time_name'], df3['construction_index'], 'r-')
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
    with st.expander("See the explanation of the index"):
        st.write("""
            This dashboard homepage prominently features four key factors, serving as a sophisticated reflection of the current real estate market trends. These selected factors stand out as the top four items exhibiting the highest correlation to housing prices within the existing dataset. By meticulously analyzing these pivotal items, users can gain invaluable insights and closely observe the directional flow of the housing market. The revelation of these correlated factors aids users in making informed and strategic decisions, allowing them to navigate through the intricate realm of real estate with enhanced clarity and confidence.
        """)

    st.subheader('全國房屋指數(國泰房價指數)')
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
                          labels={'value': 'Index Value', 'variable': 'City', 'period': 'Period'},
                          width=1200, height=500)  # 這裡調整 width 和 height

            # st.subheader('國泰房價指數')
            st.plotly_chart(fig)
        else:
            st.warning("請至少選擇一個城市。")

        # 使用 st.expander 來添加一個可展開和折疊的區塊。
        # 用另一個 expander 來提供額外的解釋或資訊。
        with st.expander("See the explanation of the index"):
            st.write("""
                The chart above shows the housing index for the selected cities.
                You can select or deselect cities from the multiselect box to view the corresponding trend chart.
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


    st.subheader('近四個月六都量能排行')

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
        cities = ['新北市','臺北市', '桃園市', '臺中市', '臺南市', '高雄市']
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
            xaxis_title='City',
            yaxis_title='Numbers',
            barmode='group',
            bargap=0.15,
            bargroupgap=0.1,
            width = 1200,  # Adjust the width here
            height = 500  # Adjust the height here
        )

        st.plotly_chart(fig)
        with st.expander("See the explanation of the index"):
            st.write("""
                The chart above shows the housing index for the selected cities.
                You can select or deselect cities from the multiselect box to view the corresponding trend chart.
            """)


    except Exception as e:
        st.error(f"錯誤: {e}")


















    st.subheader('輿情分析')

    # 连接到数据库并查询id=12的列
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            sql = "SELECT keyword FROM keywords_table WHERE id=21"
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


# Trading Hotspots
elif add_radio == "Trading Hotspots":
    st.title('Trading Hotspots')

    # 篩選城市
    cities = ['新北市', '台北市', '桃園市', '台中市', '新竹市']
    selected_city = st.selectbox('條件篩選欄', cities)
    st.subheader(f'{selected_city} 預售屋實價登錄')


    @st.cache_data
    def load_data():
        df = pd.read_csv('/Users/chenweiting/Downloads/f_lvr_land_b.csv')  # 之後改資料源
        df['交易年月日'] = df['交易年月日'].apply(convert_to_ad)
        df.dropna(subset=['交易年月日'], inplace=True)
        df['交易年月日'] = pd.to_datetime(df['交易年月日'])
        return df

    # 將 交易年月日 轉換為西元年
    def convert_to_ad(date_str):
        try:
            year, month, day = int(date_str[:3]), int(date_str[3:5]), int(date_str[5:])
            return datetime(year + 1911, month, day)
        except ValueError:
            return None

    df = load_data()
    min_date = df['交易年月日'].min().to_pydatetime()
    max_date = df['交易年月日'].max().to_pydatetime()
    start_date, end_date = st.slider('選擇日期範圍', min_value=min_date, max_value=max_date, value=(min_date, max_date))
    filtered_df = df[(df['交易年月日'] >= start_date) & (df['交易年月日'] <= end_date)]
    st.write(filtered_df)

    filtered_df.set_index('交易年月日', inplace=True)
    df_resampled = filtered_df.resample('D').size()
    st.subheader(f'{selected_city} 預售屋總體成交分析')
    st.bar_chart(df_resampled)

    # 選取區域並繪製區域交易量折線圖
    # 選取 "鄉鎮市區" 並繪製對應的交易量折線圖
    filtered_df['鄉鎮市區'].fillna('未知', inplace=True)  # 鄉鎮區有nan值
    unique_areas = filtered_df['鄉鎮市區'].unique()
    selected_areas = st.multiselect('選擇鄉鎮市區', options=unique_areas.tolist(), default=unique_areas.tolist())

    # 使用保留的 '日期' 列來創建折線圖的 DataFrame
    line_chart_df = filtered_df.groupby(['交易年月日', '鄉鎮市區']).size().reset_index(name='交易量')
    line_chart_df = line_chart_df.pivot(index='交易年月日', columns='鄉鎮市區', values='交易量').fillna(0)

    # start_date, end_date = st.slider('選擇日期範圍', min_value=min_date, max_value=max_date, value=(min_date, max_date))

    # 根據選定的 "鄉鎮市區" 繪製折線圖
    st.subheader(f'{selected_city} 預售屋分區成交分析')
    st.line_chart(line_chart_df[selected_areas])





    # --------------- 地圖 --------
    # 先創一個新表格然後取100個新增座標

    from geopy.geocoders import Nominatim
    import pymysql
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
    st.subheader('區域熱點')
    import streamlit as st
    import pandas as pd
    import numpy as np
    import pydeck as pdk
    import pymysql

    conn = connect_db()
    cursor = conn.cursor()  # 定義 cursor

    # 從數據庫中獲取數據
    cursor.execute("SELECT latitude, longitude, address, build_case FROM house_price_map")
    rows = cursor.fetchall()

    # 數據格式化
    data = pd.DataFrame(rows, columns=['lat', 'lon', 'address', 'build_case'])

    st.write(data)  # 或者 st.table(data) 以表格的形式顯示

    # 定義 tooltip
    tooltip = {
        "html": "<b>Value:</b>{elevationValue}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }


    # 3D 地圖的層
    layer = pdk.Layer(
        "HexagonLayer",
        # 'ColumnLayer',
        data=data,
        get_position='[lon, lat]',
        radius=200,
        elevation_scale=8,
        elevation_range=[0, 1000],
        pickable=True,
        extruded=True,
    )

    # 3D 地圖的視圖
    # view_state = pdk.ViewState(latitude=25.0330, longitude=121.5654, zoom=11, pitch=45)
    view_state = pdk.ViewState(latitude=25.0118, longitude=121.4559, zoom=11, pitch=45)


    # 在 Streamlit 應用中渲染 3D 地圖
    # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))
















    conn = connect_db()
    cursor = conn.cursor()  # 定義 cursor

    # 從數據庫中獲取數據
    cursor.execute("SELECT latitude, longitude, address, build_case FROM house_price_map")
    rows = cursor.fetchall()

    # 數據格式化
    data = pd.DataFrame(rows, columns=['lat', 'lon', 'address', 'build_case'])


    # 定義 tooltip
    tooltip = {
        "html": "<b>Address:</b>{build_case}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }


    # 3D 地圖的層
    layer = pdk.Layer(
        # "HexagonLayer",
        'ColumnLayer',
        data=data,
        get_position='[lon, lat]',
        radius=200,
        elevation_scale=80,
        elevation_range=[0, 1000],
        get_elevation='elevation',  # 如果 build_case 是数值，您可以直接将其用作 elevation
        # get_fill_color='[255, 165, 0, 140]',  # 设置颜色为橙色，最后的 140 是透明度
        get_fill_color='[build_case * 15, 165, 0]',
        pickable=True,
        extruded=True,
    )

    # 3D 地圖的視圖
    view_state = pdk.ViewState(latitude=25.0330, longitude=121.5654, zoom=11, pitch=45)

    # 在 Streamlit 應用中渲染 3D 地圖
    # st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))


elif add_radio == "Impact Factors":
    st.title('Impact Factors')
    # st.subheader('景氣信號燈')
    #
    # # connect to db
    # conn = connect_db()
    #
    # try:
    #     with conn.cursor() as cursor:
    #         # 执行 SQL 查询
    #         sql = "SELECT time_name, strategy_signal FROM economic_cycle_indicator"
    #         cursor.execute(sql)
    #
    #         # 获取结果
    #         rows = cursor.fetchall()
    # finally:
    #     conn.close()
    #
    #
    # # 将结果转换为 DataFrame
    # df = pd.DataFrame(rows, columns=['time_name', 'strategy_signal'])
    # # 过滤掉 'strategy_signal' 为 0 的行
    # df = df[df['strategy_signal'] != 0]
    #
    #
    # # 修复年份并转换为日期格式
    # def fix_year(date_str):
    #     year, month = date_str.split('年', 1)
    #     year = int(year)
    #     if year < 1000:
    #         year += 1911
    #     return f"{year}年{month}"
    #
    #
    # df['time_name'] = df['time_name'].apply(fix_year)
    # df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    #
    # # 绘制图形
    # st.line_chart(df.set_index('time_name'))
    #
    # st.subheader('經濟年成長率')
    #
    # # 連接數據庫，讀取數據（請根據您的環境替換這部分）
    # conn = connect_db()
    # sql = "SELECT time_name, economic_growth_rate FROM economic_gdp_indicator"  # 請修改SQL查詢以適合您的表格
    # df = pd.read_sql(sql, conn)
    #
    # # 定義一個字典來映射季度到月份
    # quarter_to_month = {
    #     '第1季': '04',
    #     '第2季': '07',
    #     '第3季': '10',
    #     '第4季': '01'
    # }
    #
    # # 將 'time_name' 列拆分成年份和季度
    # df['year'] = df['time_name'].str.split('年').str[0].astype(int) + 1911
    # df['quarter'] = df['time_name'].str.split('年').str[1]
    #
    # # 對於第4季，需要將年份加1
    # df.loc[df['quarter'] == '第4季', 'year'] = df['year'] + 1
    #
    # # 將季度映射到月份
    # df['month'] = df['quarter'].map(quarter_to_month)
    #
    # # 組合年份和月份，並創建新的日期列
    # df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'] + '-01', format='%Y-%m-%d', errors='coerce')
    #
    # # 過濾掉 'economic_growth_rate', 'gdp_million_nyd' 等等為 0 的行
    # filtered_df = df[(df['economic_growth_rate'] != 0)]
    # # 然後，使用過濾後的 DataFrame 繪圖
    # st.line_chart(filtered_df.set_index('date')['economic_growth_rate'])
    # # 使用Streamlit繪製線圖
    # # st.line_chart(df.set_index('date')['economic_growth_rate'])
    #
    # st.subheader('營建成本')
    # # 数据库连接信息
    # conn = pymysql.connect(
    #     host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
    #     port=3306,
    #     user='admin',
    #     password=password_bytes,  # 确保password_bytes变量在此处是可用的
    #     database='estate_data_hub',
    #     charset='utf8mb4'
    # )
    #
    # try:
    #     with conn.cursor() as cursor:
    #         # 执行SQL查询
    #         sql = "SELECT time_name, construction_index FROM economic_construction_cost"  # 请替换为您的实际表名
    #         cursor.execute(sql)
    #
    #         # 获取结果
    #         rows = cursor.fetchall()
    #
    # finally:
    #     conn.close()
    #
    # # 将结果转换为DataFrame
    # df = pd.DataFrame(rows, columns=['time_name', 'construction_index'])
    # df = df[df['construction_index'] != 0]
    #
    #
    # # 修复年份并转换为日期格式
    # def fix_year(date_str):
    #     year, month = date_str.split('年', 1)
    #     year = int(year)
    #     if year < 1000:
    #         year += 1911
    #     return f"{year}年{month}"
    #
    #
    # df['time_name'] = df['time_name'].apply(fix_year)
    # df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    #
    # # 绘制图形
    # st.line_chart(df.set_index('time_name'))
    #
    # st.subheader('全台人口數量&戶數')
    # # connect to db
    # conn = pymysql.connect(
    #     host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
    #     port=3306,
    #     user='admin',
    #     password=password_bytes,
    #     database='estate_data_hub',
    #     charset='utf8mb4'
    # )
    # try:
    #     with conn.cursor() as cursor:
    #         # 執行 SQL 查詢
    #         sql = """SELECT time_name, household_count, population_count, average_household_size
    #                      FROM society_population_data"""  # 請替換成您實際的表名
    #         cursor.execute(sql)
    #
    #         # 獲取結果
    #         rows = cursor.fetchall()
    #
    # finally:
    #     conn.close()
    #
    # # 將結果轉換為 DataFrame
    # df = pd.DataFrame(rows, columns=['time_name', 'household_count', 'population_count', 'average_household_size'])
    #
    # # 修復年份並轉換為日期格式
    # def fix_year(date_str):
    #     year, month = date_str.split('年', 1)
    #     year = int(year)
    #     if year < 1000:
    #         year += 1911
    #     return f"{year}年{month}"
    #
    #
    # df['time_name'] = df['time_name'].apply(fix_year)
    # df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    # df = df[df['household_count'] != 0]
    #
    # # 繪製圖形
    # st.line_chart(df.set_index('time_name')[['household_count', 'population_count']])
    #
    # st.subheader('戶量(人/戶)')
    # # connect to db
    # conn = connect_db()
    # try:
    #     with conn.cursor() as cursor:
    #         # 執行 SQL 查詢
    #         sql = """SELECT time_name,average_household_size
    #                      FROM society_population_data"""  # 請替換成您實際的表名
    #         cursor.execute(sql)
    #
    #         # 獲取結果
    #         rows = cursor.fetchall()
    #
    # finally:
    #     conn.close()
    #
    # # 將結果轉換為 DataFrame
    # df = pd.DataFrame(rows, columns=['time_name', 'average_household_size'])
    #
    # # 修復年份並轉換為日期格式
    # def fix_year(date_str):
    #     year, month = date_str.split('年', 1)
    #     year = int(year)
    #     if year < 1000:
    #         year += 1911
    #     return f"{year}年{month}"
    #
    #
    # df['time_name'] = df['time_name'].apply(fix_year)
    # df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    # df = df[df['average_household_size'] != 0]
    #
    # # 繪製圖形
    # st.line_chart(df.set_index('time_name')[['average_household_size']])








    # 景氣信號燈 v.s. 經濟年成長率
    conn = connect_db()
    # Fix year and convert to datetime
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"

    try:
        # Read the first DataFrame
        with conn.cursor() as cursor:
            cursor.execute("SELECT time_name, strategy_signal FROM economic_cycle_indicator")
            rows = cursor.fetchall()
            df1 = pd.DataFrame(rows, columns=['time_name', 'strategy_signal'])
        df1 = df1[df1['strategy_signal'] != 0]
        df1['time_name'] = df1['time_name'].apply(fix_year)
        df1['time_name'] = pd.to_datetime(df1['time_name'], format='%Y年%m月')

        # Read the second DataFrame
        df2 = pd.read_sql("SELECT time_name, economic_growth_rate FROM economic_gdp_indicator", conn)
        df2['year'] = df2['time_name'].str.split('年').str[0].astype(int) + 1911
        df2['quarter'] = df2['time_name'].str.split('年').str[1]
        df2.loc[df2['quarter'] == '第4季', 'year'] = df2['year'] + 1
        df2['month'] = df2['quarter'].map({'第1季': '04', '第2季': '07', '第3季': '10', '第4季': '01'})
        df2['date'] = pd.to_datetime(df2['year'].astype(str) + '-' + df2['month'] + '-01', format='%Y-%m-%d',
                                     errors='coerce')
        filtered_df2 = df2[df2['economic_growth_rate'] != 0]
    finally:
        conn.close()

    # Merge DataFrames on date columns
    merged_df = pd.merge(df1, filtered_df2, left_on='time_name', right_on='date', how='inner')

    # make dashboard 1002
    # numeric_vars = ['景氣信號燈', '經濟年成長率', '營建成本', '全台人口數量&戶數', '戶量(人/戶)','五大銀行平均房貸利率' ]  # 這裡的變量列表可以替換為你自己的變量列表
    numeric_vars = ['strategy_signal', 'economic_growth_rate', 'construction cost', 'Total population','Number of households in Taiwan', 'Household size (people per household)', 'Average mortgage interest rate of the top five banks']  # 有些數據還沒有進來
    leftcol, rightcol = st.columns([1, 1])
    with leftcol:
        xvar = st.selectbox("X variable", numeric_vars)
    with rightcol:
        # yvar = st.selectbox("Y variable", numeric_vars, index=len(numeric_vars) - 6)
        yvar = st.selectbox("Y variable", numeric_vars)

    # 如果沒有可供繪圖的數值變量，則顯示警告並停止
    if len(numeric_vars) < 1:
        st.warning("沒有找到可供繪圖的數值列。")
        st.stop()

    st.subheader('Trend Charts')
    # 兩個變量的趨勢圖
    leftcol, rightcol = st.columns([1, 1])
    # xvar = st.selectbox("X variable", numeric_vars)
    # yvar = st.selectbox("Y variable", numeric_vars, index=len(numeric_vars) - 1)

    with leftcol:
        # xvar = st.selectbox("X 變量", numeric_vars)
        # xvar = st.selectbox("X 變量", numeric_vars, key='xvar_selectbox')
        plt.figure(figsize=(10, 6))
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        sns.lineplot(x='date', y=xvar, data=merged_df)  # 替換 'YourTimeColumn' 為您的時間列名
        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title(f'{xvar} Trend Chart', color='white')
        plt.xlabel('date', color='white')
        plt.ylabel(yvar, color='white')
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)

    with rightcol:
        # yvar = st.selectbox("Y 變量", numeric_vars, index=len(numeric_vars) - 1)
        # yvar = st.selectbox("Y 變量", numeric_vars, index=len(numeric_vars) - 1, key='yvar_selectbox')
        plt.figure(figsize=(10, 6))
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        sns.lineplot(x='date', y=yvar, data=merged_df)  # 替換 'YourTimeColumn' 為您的時間列名
        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title(f'{yvar} Trend Chart', color='white')
        plt.xlabel('date', color='white')
        plt.ylabel(yvar, color='white')
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)

    st.subheader('Correlation Chart')
    # leftcol, rightcol = st.columns([1, 1])
    leftcol, centercol, rightcol = st.columns([1.3, 3.5, 1])
    # 兩個變量的相關性圖表
    with centercol:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=xvar, y=yvar, data=merged_df)
        sns.set(rc={'axes.facecolor': '#0f1116', 'figure.facecolor': '#0f1116'})
        sns.regplot(x=xvar, y=yvar, data=merged_df, scatter=False, color='red')
        plt.xlabel(xvar)
        plt.ylabel(yvar)
        plt.gca().set_facecolor('#0f1116')
        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title('Correlation', color='white')
        plt.xlabel(xvar, color='white')
        plt.ylabel(yvar, color='white')
        plt.xticks(color='white')
        plt.yticks(color='white')
        st.pyplot(plt)

