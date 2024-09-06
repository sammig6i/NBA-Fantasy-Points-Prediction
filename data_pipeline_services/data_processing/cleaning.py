import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def connect_db():
  try:
    conn = psycopg2.connect(
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT"),
      database=os.getenv("DB_NAME")
    )
    return conn
  except Exception as err:
    print(f"Error while connecting to PostgreSQL: {err}")
    return None
  
# TODO load raw data, assign unique IDs to players and games

if __name__ == "__main__":
  connection = connect_db()