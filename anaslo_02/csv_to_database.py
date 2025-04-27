import pandas as pd
import sqlite3
import os
import re
import glob
import shutil
import time
from logger_setup import setup_logger
from config import LOG_PATH, DB_PATH, CSV_PATH, ARCHIVE_PATH

logger = setup_logger("datebase_to_gspread", log_file=LOG_PATH)


# ファイル名から都道府県・ホール名・日付を取得
def get_pref_hallName_date(csv_file):
    logger.info(f"File Name： {csv_file}")
    filename = os.path.basename(csv_file)
    match = re.match(r"(.+?)_(.+?)_(\d{4}-\d{2}-\d{2})\.csv", filename)
    if not match:
        raise ValueError("ファイル名は『都道府県_ホール名_YYYY-MM-DD.csv』の形式である必要があります")

    prefecture = match.group(1)
    hall_name = match.group(2)
    date = match.group(3)

    logger.info(f"📄 ファイル名から抽出： {prefecture}, {hall_name}, {date}")
    
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
    
    logger.info(f"✅ {hall_name}, {date}: results テーブルに登録しました。")


def csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    csv_files = glob.glob(f'{CSV_PATH}*.csv')
    if not csv_files:
        logger.info("CSVファイルが見つかりません")
        return
    for csv_file in csv_files:
        # CSVファイル名から都道府県・ホール名・日付を取得
        prefecture, hall_name, date = get_pref_hallName_date(csv_file)
        df = pd.read_csv(csv_file)

        hall_id = get_hall_id_from_db(conn, hall_name, prefecture_name=prefecture)
        
        try:
            append_database(cursor, df, hall_id, hall_name, date)
            logger.info(f"✅ データベースに追加成功: {os.path.basename(csv_file)}")

        except Exception as e:
            logger.error(f"❌ データベース追加失敗: {os.path.basename(csv_file)} → エラー内容: {e}")
            continue  # このファイルをスキップして次へ

        # アーカイブに移動
        archive_path = os.path.join(ARCHIVE_PATH, os.path.basename(csv_file))
        shutil.move(csv_file, archive_path)
        logger.info(f"📦 CSVファイルをアーカイブへ移動しました → {archive_path}")
        
        time.sleep(0.2) # プログラミングっぽく

    conn.commit()
    conn.close()


if __name__ == "__main__":
    
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)