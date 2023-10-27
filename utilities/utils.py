from decouple import config
import pymysql


# connect to db
# def connect_to_db():
#     host = config('HOST')
#     port = config('PORT')
#     user = config('USER')
#     database = config('DATABASE')
#     password = config('DATABASE_PASSWORD')
#     try:
#         conn = pymysql.connect(
#             host=host,
#             port=port,
#             user=user,
#             password=password,
#             database=database,
#             charset='utf8mb4'
#         )
#         print(f"have connected to MySQL")
#         return conn
#     except Exception as e:
#         print(f"failed to connect to MySQL: {e}")
#         return None


# connect to db
def connect_to_db(table):
    host = config('HOST')
    port = int(config('PORT'))
    user = config('USER')
    database = config('DATABASE')
    password = config('DATABASE_PASSWORD')
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user='admin',
            password=password,
            database=database,
            charset='utf8mb4'
            # connection_timeout=57600
        )
        print(f"{table}Have connected to db")
        return conn
    except Exception as e:
        print(f"error: {e}")
        return None