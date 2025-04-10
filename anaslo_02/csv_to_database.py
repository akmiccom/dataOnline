import pandas as pd
import sqlite3
import os
import re
import glob
import shutil
import time


# ファイル名から都道府県・ホール名・日付を取得
def get_pref_hallName_date(csv_file):
    print(f"File Name： {csv_file}")
    filename = os.path.basename(csv_file)
    match = re.match(r"(.+?)_(.+?)_(\d{4}-\d{2}-\d{2})\.csv", filename)
    if not match:
        raise ValueError("ファイル名は『都道府県_ホール名_YYYY-MM-DD.csv』の形式である必要があります")

    prefecture = match.group(1)
    hall_name = match.group(2)
    date = match.group(3)

    print(f"📄 ファイル名から抽出： {prefecture}, {hall_name}, {date}")
    
    return prefecture, hall_name, date


# 都道府県とホール名からIDを取得
def get_hall_id_from_db(conn, hall_name, prefecture_name=None):
    cursor = conn.cursor()
    
    if prefecture_name:
        # 都道府県名からIDを取得
        cursor.execute("SELECT prefecture_id FROM prefectures WHERE name = ?", (prefecture_name,))
        result = cursor.fetchone()
        if not result:
            return None
        prefecture_id = result[0]
        # 都道府県ありの場合のホール検索
        cursor.execute("""
            SELECT hall_id FROM halls
            WHERE name = ? AND prefecture_id = ?
        """, (hall_name, prefecture_id))
    else:
        # 都道府県が指定されない場合（曖昧検索注意）
        cursor.execute("SELECT hall_id, name FROM halls WHERE name LIKE ?", ('%' + hall_name + '%',))

    
    result = cursor.fetchone()
    return result[0] if result else None


# データベース接続
def append_database(cursor, df, hall_id, hall_name, date):
    for _, row in df.iterrows():
        model_name = row["model_name"]
        unit_no = int(row["unit_no"])
        games = int(row["game"])
        medals = int(row["medals"])
        BB = int(row["BB"])
        RB = int(row["RB"])

    # 機種登録＆取得
        cursor.execute("INSERT OR IGNORE INTO models (name) VALUES (?)", (model_name,))
        cursor.execute("SELECT model_id FROM models WHERE name = ?", (model_name,))
        model_id = cursor.fetchone()[0]

    # 出玉データを登録（重複回避）
        cursor.execute("""
        INSERT OR IGNORE INTO results (
            hall_id, model_id, unit_no, date, game, BB, RB, medals
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (hall_id, model_id, unit_no, date, games, BB, RB, medals))
    
    print(f"✅ {hall_name}, {date}: results テーブルに登録しました。")


def csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    csv_files = glob.glob(f'{CSV_PATH}*.csv')
    if not csv_files:
        print("CSVファイルが見つかりません")
        return
    for csv_file in csv_files:
        # CSVファイル名から都道府県・ホール名・日付を取得
        prefecture, hall_name, date = get_pref_hallName_date(csv_file)
        df = pd.read_csv(csv_file)

        hall_id = get_hall_id_from_db(conn, hall_name, prefecture_name=prefecture)
        append_database(cursor, df, hall_id, hall_name, date)
        
        archive_path = os.path.join(ARCHIVE_PATH, os.path.basename(csv_file))
        shutil.move(csv_file, archive_path)
        print(f"📦 CSVファイルをアーカイブへ移動しました → {archive_path}\n")
        
        time.sleep(0.1)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    
    DB_PATH = "anaslo_02/anaslo_02.db"
    CSV_PATH = "anaslo_02/csv/"
    ARCHIVE_PATH = "anaslo_02/archive/"
    
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)