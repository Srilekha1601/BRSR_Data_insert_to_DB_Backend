# processing/db.py
import pymysql
import os

db_connection = pymysql.connect(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "Srilekha@1601"),
    database=os.getenv("DB_NAME", "brsr_v1"),
    cursorclass=pymysql.cursors.DictCursor
)
