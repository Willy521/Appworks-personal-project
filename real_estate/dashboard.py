import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from wordcloud import WordCloud
import pymysql
from dotenv import load_dotenv
from decouple import config
import matplotlib.font_manager as fm
import streamlit as st
import pydeck as pdk
import altair as alt
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim
import pydeck as pdk
import streamlit as st
import pandas as pd
import folium
from folium import Map, Marker
from geopy.geocoders import Nominatim
from streamlit_folium import folium_static
import seaborn as sns


# 環境變數
load_dotenv()
password = config('DATABASE_PASSWORD')
password_bytes = password.encode('utf-8')
#
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
    st.title('Navigator')
    st.header("Data Pages")
    add_radio = st.radio(
        "Data Pages",
        ("Taiwanese Overall House Price", "Trading Hotspots", "Impact Factors")
    )
    st.header("dd")
    st.title('Factors')








import numpy as np
# Taiwanese Overall House Price
if add_radio == "Taiwanese Overall House Price":

    # 模擬一些數據
    data1 = np.random.rand(10)
    data2 = np.random.rand(10)
    data3 = np.random.rand(10)
    data4 = np.random.rand(10)

    # 創建三個列
    col1, col2, col3, col4 = st.columns(4)

    # 在第一列中添加metric和趨勢圖
    with col1:
        st.metric("經濟成長率", "3.2 ％", "1.2%")
        fig, ax = plt.subplots(figsize=(4, 2))
        # ax.plot(data1, 'r-', linewidth=0.5)
        ax.plot(data1, 'r-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')  # 設置圖表背景為黑色
        ax.set_facecolor('black')
        st.pyplot(fig)

    with col2:
    # 在第二列中添加metric和趨勢圖
        st.metric("景氣燈號", "22", "-8%")
        fig, ax = plt.subplots(figsize=(4, 2))
        ax.plot(data2, 'g-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')  # 設置圖表背景為黑色
        ax.set_facecolor('black')
        st.pyplot(fig)

    # 在第三列中添加metric和趨勢圖
    with col3:
        st.metric("建築成本", "86", "4%")
        fig, ax = plt.subplots(figsize=(4, 2))
        ax.plot(data3, 'b-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')  # 設置圖表背景為黑色
        ax.set_facecolor('black')
        st.pyplot(fig)
    with col4:
        st.metric("建築成本", "86", "4%")
        fig, ax = plt.subplots(figsize=(4, 2))
        ax.plot(data3, 'b-')
        ax.axis('off')
        fig.patch.set_facecolor('#0f1116')  # 設置圖表背景為黑色
        ax.set_facecolor('black')
        st.pyplot(fig)



    # 標題和副標題
    st.title('Taiwanese Overall House Price')
    col1, col2, col3 = st.columns(3)
    col1.metric("經濟成長率", "3.2 ％", "1.2%")
    col2.metric("景氣燈號", "22", "-8%")
    col3.metric("建築成本", "86", "4%")

    options = st.multiselect(
        '選擇城市',
        ['台北', '新北市', '桃園市', '台中市', '台南市', '高雄市'],
        default=[]
    )

    st.write('You selected:', options)

    st.subheader('輿情分析')

    # 连接到数据库并查询id=12的列
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            sql = "SELECT keyword FROM keywords_table WHERE id=18"
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
                          width=800, height=400, background_color='black').generate(' '.join(keyword_list))

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
    cities = ['台北市', '新北市', '桃園市', '台中市', '新竹市']
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
    st.subheader('景氣信號燈')

    # connect to db
    conn = connect_db()

    try:
        with conn.cursor() as cursor:
            # 执行 SQL 查询
            sql = "SELECT time_name, strategy_signal FROM economic_cycle_indicator"
            cursor.execute(sql)

            # 获取结果
            rows = cursor.fetchall()
    finally:
        conn.close()


    # 将结果转换为 DataFrame
    df = pd.DataFrame(rows, columns=['time_name', 'strategy_signal'])
    # 过滤掉 'strategy_signal' 为 0 的行
    df = df[df['strategy_signal'] != 0]


    # 修复年份并转换为日期格式
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"


    df['time_name'] = df['time_name'].apply(fix_year)
    df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')

    # 绘制图形
    st.line_chart(df.set_index('time_name'))

    st.subheader('經濟年成長率')

    # 連接數據庫，讀取數據（請根據您的環境替換這部分）
    conn = connect_db()
    sql = "SELECT time_name, economic_growth_rate FROM economic_gdp_indicator"  # 請修改SQL查詢以適合您的表格
    df = pd.read_sql(sql, conn)

    # 定義一個字典來映射季度到月份
    quarter_to_month = {
        '第1季': '04',
        '第2季': '07',
        '第3季': '10',
        '第4季': '01'
    }

    # 將 'time_name' 列拆分成年份和季度
    df['year'] = df['time_name'].str.split('年').str[0].astype(int) + 1911
    df['quarter'] = df['time_name'].str.split('年').str[1]

    # 對於第4季，需要將年份加1
    df.loc[df['quarter'] == '第4季', 'year'] = df['year'] + 1

    # 將季度映射到月份
    df['month'] = df['quarter'].map(quarter_to_month)

    # 組合年份和月份，並創建新的日期列
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'] + '-01', format='%Y-%m-%d', errors='coerce')

    # 過濾掉 'economic_growth_rate', 'gdp_million_nyd' 等等為 0 的行
    filtered_df = df[(df['economic_growth_rate'] != 0)]
    # 然後，使用過濾後的 DataFrame 繪圖
    st.line_chart(filtered_df.set_index('date')['economic_growth_rate'])
    # 使用Streamlit繪製線圖
    # st.line_chart(df.set_index('date')['economic_growth_rate'])

    st.subheader('營建成本')
    # 数据库连接信息
    conn = pymysql.connect(
        host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
        port=3306,
        user='admin',
        password=password_bytes,  # 确保password_bytes变量在此处是可用的
        database='estate_data_hub',
        charset='utf8mb4'
    )

    try:
        with conn.cursor() as cursor:
            # 执行SQL查询
            sql = "SELECT time_name, construction_index FROM economic_construction_cost"  # 请替换为您的实际表名
            cursor.execute(sql)

            # 获取结果
            rows = cursor.fetchall()

    finally:
        conn.close()

    # 将结果转换为DataFrame
    df = pd.DataFrame(rows, columns=['time_name', 'construction_index'])
    df = df[df['construction_index'] != 0]


    # 修复年份并转换为日期格式
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"


    df['time_name'] = df['time_name'].apply(fix_year)
    df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')

    # 绘制图形
    st.line_chart(df.set_index('time_name'))

    st.subheader('全台人口數量&戶數')
    # connect to db
    conn = pymysql.connect(
        host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
        port=3306,
        user='admin',
        password=password_bytes,
        database='estate_data_hub',
        charset='utf8mb4'
    )
    try:
        with conn.cursor() as cursor:
            # 執行 SQL 查詢
            sql = """SELECT time_name, household_count, population_count, average_household_size 
                         FROM society_population_data"""  # 請替換成您實際的表名
            cursor.execute(sql)

            # 獲取結果
            rows = cursor.fetchall()

    finally:
        conn.close()

    # 將結果轉換為 DataFrame
    df = pd.DataFrame(rows, columns=['time_name', 'household_count', 'population_count', 'average_household_size'])

    # 修復年份並轉換為日期格式
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"


    df['time_name'] = df['time_name'].apply(fix_year)
    df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    df = df[df['household_count'] != 0]

    # 繪製圖形
    st.line_chart(df.set_index('time_name')[['household_count', 'population_count']])

    st.subheader('戶量(人/戶)')
    # connect to db
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            # 執行 SQL 查詢
            sql = """SELECT time_name,average_household_size 
                         FROM society_population_data"""  # 請替換成您實際的表名
            cursor.execute(sql)

            # 獲取結果
            rows = cursor.fetchall()

    finally:
        conn.close()

    # 將結果轉換為 DataFrame
    df = pd.DataFrame(rows, columns=['time_name', 'average_household_size'])

    # 修復年份並轉換為日期格式
    def fix_year(date_str):
        year, month = date_str.split('年', 1)
        year = int(year)
        if year < 1000:
            year += 1911
        return f"{year}年{month}"


    df['time_name'] = df['time_name'].apply(fix_year)
    df['time_name'] = pd.to_datetime(df['time_name'], format='%Y年%m月')
    df = df[df['average_household_size'] != 0]

    # 繪製圖形
    st.line_chart(df.set_index('time_name')[['average_household_size']])








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

    # Plot correlation chart with dark background
    st.subheader('相關性')
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='strategy_signal', y='economic_growth_rate', data=merged_df)
    sns.regplot(x='strategy_signal', y='economic_growth_rate', data=merged_df, scatter=False, color='red')
    plt.xlabel('Strategy Signal')
    plt.ylabel('Economic Growth Rate')
    plt.gca().set_facecolor('black')
    plt.grid(color='white', linestyle='--', linewidth=0.5)
    plt.title('Correlation Chart', color='black')
    plt.xticks(color='black')
    plt.yticks(color='black')
    st.pyplot(plt)

    numeric_vars = ['strategy_signal', 'economic_growth_rate']  # 定義可以被選擇的數字型變量

    # 如果没有可供绘图的数值变量，则显示警告并停止
    if len(numeric_vars) < 1:
        st.warning("No numeric columns found for plotting.")
        st.stop()

    leftcol, rightcol = st.columns([2, 1])

    # 在右列中，让用户选择x和y变量
    with rightcol:
        xvar = st.selectbox("X variable", numeric_vars)
        yvar = st.selectbox("Y variable", numeric_vars, index=len(numeric_vars) - 1)

    # 在左列中绘制用户选择的x和y变量的散点图和回归线
    with leftcol:
        st.subheader('相關性')
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=xvar, y=yvar, data=merged_df)
        sns.regplot(x=xvar, y=yvar, data=merged_df, scatter=False, color='red')
        plt.xlabel(xvar)
        plt.ylabel(yvar)
        plt.gca().set_facecolor('black')
        plt.grid(color='white', linestyle='--', linewidth=0.5)
        plt.title('Correlation Chart', color='black')
        plt.xticks(color='black')
        plt.yticks(color='black')
        st.pyplot(plt)
