import time
import pymysql

while True:
    try:
        conn = pymysql.connect(
            host="mysql",
            user="user",
            password="password123",
            database="astraworld_db"
        )
        conn.close()
        print("MySQL ready!")
        break
    except pymysql.err.OperationalError:
        print("Waiting for MySQL...")
        time.sleep(2)