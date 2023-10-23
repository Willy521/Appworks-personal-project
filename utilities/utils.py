from decouple import config
import pymysql


# connect to db
def connect_to_db(table):
    host = config('HOST')
    port = config('PORT')
    user = config('USER')
    database = config('DATABASE')
    password = config('DATABASE_PASSWORD')
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
        print(f"{table} have connected to MySQL")
        return conn
    except Exception as e:
        print(f"{table} failed to connect to MySQL: {e}")
        return None


