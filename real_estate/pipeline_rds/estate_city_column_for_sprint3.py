from dotenv import load_dotenv
import pymysql
from decouple import config
from utilities.utils import connect_to_db

load_dotenv()
password = config('DATABASE_PASSWORD')
password_bytes = password.encode('utf-8')


def update_city_column(conn):
    try:
        cursor = conn.cursor()

        # Check if the city column exists
        cursor.execute("SHOW COLUMNS FROM real_estate LIKE 'city';")
        result = cursor.fetchone()

        # If city column does not exist, then add it
        if not result:
            cursor.execute("ALTER TABLE real_estate ADD city VARCHAR(255);")
            print("Added 'city' column to the table.")

        # Update city column based on address column
        cursor.execute("UPDATE real_estate SET city = SUBSTRING(address, 1, 3);")
        conn.commit()

        print("City column updated successfully!")
        cursor.close()
    except Exception as e:
        print(f"Error while updating city column: {e}")


def create_sprint3_demo_table(conn):
    try:
        cursor = conn.cursor()
        sql = """
        CREATE TABLE IF NOT EXISTS sprint3_demo AS
        SELECT id, city, district, address, zoning, transaction_date, build_case, buildings_and_number
        FROM real_estate;
        """
        cursor.execute(sql)
        print("Created 'sprint3_demo' table successfully!")
        cursor.close()
    except Exception as e:
        print(f"Error while creating 'sprint3_demo' table: {e}")


def main():
    conn = connect_to_db()
    if conn:
        update_city_column(conn)
        create_sprint3_demo_table(conn)
        conn.close()


if __name__ == "__main__":
    main()

