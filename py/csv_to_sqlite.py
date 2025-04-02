#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlite3
import os
import glob
from logger_setup import setup_logger

logger = setup_logger("csv_to_sqlite")
# In[3]:


# -- 機種テーブル
def create_machine_tabel(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS machines (
            machine_id INTEGER PRIMARY KEY,
            machine_name TEXT,
            UNIQUE(machine_name)
            )
    """)
    conn.commit()
    
# -- ホールテーブル
def create_hall_tabel(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS halls (
            hall_id INTEGER PRIMARY KEY,
            hall_name TEXT,
            UNIQUE(hall_name)
            )
    """)
    conn.commit()

# === テーブル作成 ===
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS slot_data (
            hall_id INTEGER,
            machine_name TEXT,
            unit_no INTEGER,
            date TEXT,
            start INTEGER,
            bb INTEGER,
            rb INTEGER,
            medals INTEGER,
            bb_rate INTEGER,
            rb_rate INTEGER,
            total_rate INTEGER,
            UNIQUE(date, unit_no)
        )
    """)
    conn.commit()


# In[4]:


# === CSV → DB 挿入 ===
expected_columns = [
    "hall_id",
    "machine_name",
    "unit_no",
    "date",
    "start",
    "bb",
    "rb",
    "medals",
    "bb_rate",
    "rb_rate",
    "total_rate",
]

def csv_to_df(csv_file, hall_id):
    date = os.path.basename(csv_file).split("_")[-1].replace(".csv", "")
    df = pd.read_csv(csv_file)
    df = df.rename(columns={
        "機種名": "machine_name",
        "台番号": "unit_no",
        "G数": "start",
        "差枚": "medals",
        "BB": "bb",
        "RB": "rb",
        "合成確率": "total_rate",
        "BB確率": "bb_rate",
        "RB確率": "rb_rate",
    })
    df["date"] = date
    df["hall_id"] = hall_id

    df = df[[col for col in expected_columns if col in df.columns]]
    df['medals'] = df['medals'].str.replace("+", "")

    # 数値変換用の共通処理
    def extract_digits(series, prefix_remove=None):
        series = series.astype(str).str.replace(",", "", regex=True)
        if prefix_remove:
            series = series.str.replace(prefix_remove, "", regex=True)
        series = series.str.extract(r"(\d+)").dropna().astype(int)
        return series

    df["start"] = extract_digits(df["start"])
    df["bb"] = extract_digits(df["bb"])
    df["rb"] = extract_digits(df["rb"])
    df["medals"] = extract_digits(df["medals"])
    df["total_rate"] = extract_digits(df["total_rate"], prefix_remove="1/")
    df["bb_rate"] = extract_digits(df["bb_rate"], prefix_remove="1/")
    df["rb_rate"] = extract_digits(df["rb_rate"], prefix_remove="1/")
    
    return df


# In[5]:


def df_to_data_base(df):
    conn = sqlite3.connect(DB_PATH)
    create_table(conn)

    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT OR IGNORE INTO slot_data (
                hall_id, machine_name, unit_no, date, start, bb, rb,
                medals, bb_rate, rb_rate, total_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            tuple(row[col] for col in expected_columns),
        )
    conn.commit()


# In[6]:


# === 実行 ===
def csv_to_sqlite(csv_folder, db_path, hall_id):
    conn = sqlite3.connect(db_path)
    create_table(conn)

    csv_files = glob.glob(os.path.join(csv_folder, "EXA_FIRST_2025*.csv"))
    for file in csv_files:
        logger.info(f"📥 インポート中: {file}")
        df = csv_to_df(file, hall_id)
        df_to_data_base(df)

    conn.close()
    logger.info("✅ 全CSVデータをDBに取り込みました。")
    
    # DB確認とJSON出力
    df = pd.read_sql("SELECT * FROM slot_data", sqlite3.connect(db_path))
    df = df.drop_duplicates(subset=["unit_no", "date"])
    df.to_json("C:/python/akmicWebApp/akmic-app_02/public/slot_data.json", orient="records", force_ascii=False)

if __name__ == "__main__":
    
    CSV_FOLDER = "csv"  # CSVファイルがあるフォルダ
    DB_PATH = "db/anaslo.db"
    HALL_ID = 101262

    csv_to_sqlite(CSV_FOLDER, DB_PATH, HALL_ID)


# In[ ]:




