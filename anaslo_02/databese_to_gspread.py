from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import numpy as np
import sqlite3
import datetime
from utils import connect_to_spreadsheet
from logger_setup import setup_logger
from config import LOG_PATH, DB_PATH, JSONF, spreadSheet_ids


logger = setup_logger("datebase_to_gspread", log_file=LOG_PATH)


def search_hall_and_load_data(search_word, query):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    logger.info(f"🚀 DB接続: {DB_PATH}")
    cursor.execute(
        "SELECT hall_id, name FROM halls WHERE name LIKE ?", ("%" + search_word + "%",)
    )
    results = cursor.fetchall()
    # 結果表示
    if results:
        logger.info(f"🔍 '{search_word}' を含むホール名の検索結果:")
        for hall_id, hall_name in results:
            logger.info(f" - hall_name: {hall_name}, hall_id: {hall_id}")
    else:
        logger.error(f"❌ '{search_word}' を含むホール名は見つかりませんでした。")

    df_from_db = pd.read_sql_query(query, conn, params=(hall_name,))
    conn.close()
    logger.info(f"🛑 DB接続終了: {DB_PATH}")
    
    return df_from_db


def preprocess_result_df(df):
    df["date"] = pd.to_datetime(df["date"])
    df.drop(columns=["result_id", "hall_id", "model_id"], inplace=True)
    df = df[["hall_name", "date", "model_name", "unit_no", "game", "BB", "RB", "medals"]]
    df["BB"] = df["BB"].replace(0, np.nan)
    df["RB"] = df["RB"].replace(0, np.nan)
    df["BB_rate"] = (df["game"] / df["BB"]).round(1)
    df["RB_rate"] = (df["game"] / df["RB"]).round(1)
    df["Total_rate"] = (df["game"] / (df["BB"] + df["RB"])).round(1)
    df["day"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["weekday"] = df["date"].dt.weekday
    
    logger.info(f"🧹 データ前処理完了: {df.shape[0]} rows")
    
    return df


def get_medals_summary(df, start_date, end_date, model_name):
    # RB_RATE
    df_tmp = df[
        (df["model_name"] == model_name)
        & (df["date"].dt.date <= start_date)
        & (df["date"].dt.date >= end_date)
    ].copy()

    medals = df_tmp.pivot_table(
        index=["model_name", "unit_no"],
        columns="date",
        values="medals",
        aggfunc="sum",
        margins=True,
        margins_name="Total",
    )
    medals.drop(labels="Total", level=0, inplace=True)
    medals["Rank"] = medals["Total"].rank(method="min", ascending=True).astype(int)
    medals.columns = pd.MultiIndex.from_product([["MEDALS"], medals.columns])
    
    logger.info(f"🎯 メダル集計完了: {model_name}")

    return medals


def write_medals_summary_to_spreadsheet(df, spreadsheet, sheet_name, get_medals_summary):

    logger.info(f"📆 本日より3日前のデータを追加します: {sheet_name}")

    MODELS = [
        "マイジャグラーV",
        "ゴーゴージャグラー3",
        "アイムジャグラーEX-TP",
        "ファンキージャグラー2",
    ]

    today = datetime.date.today()
    start_date = today + datetime.timedelta(days=-1)
    end_date = today + datetime.timedelta(days=-3)
    logger.info(f"   📍 基準日: {start_date}, 終了日: {end_date}")

    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()

    next_row = 1
    for model in MODELS:
        medals = get_medals_summary(df, start_date, end_date, model)
        set_with_dataframe(sheet, medals, row=next_row, include_index=True)
        existing = get_as_dataframe(sheet, evaluate_formulas=True)
        next_row += medals.shape[0] + 5
        logger.info(f"   ✅ 追加完了: {model}")

    sheet.update_cell(1, 1, today.strftime("UPDATED: %Y-%m-%d"))
    
    logger.info(f"💾 シート更新完了: {sheet_name}")


if __name__ == "__main__":
    
    # 検索キーワードよりホール名取得
    SEARCH_WORD = "EXA FIRST"
    SHEET_NAME = "MEDALS_nDAYS_AGO"
    SPREADSHEET_ID = spreadSheet_ids[SEARCH_WORD]
    if SEARCH_WORD not in spreadSheet_ids:
        raise ValueError(f"{SEARCH_WORD} のスプレッドシートIDが見つかりません")

    # Table name 取得
    query = """
    -- 出玉データにホール名と機種名を結合して取得
    SELECT
        r.*, 
        h.name AS hall_name,     -- ホール名を追加
        m.name AS model_name     -- 機種名を追加
    FROM results r
    JOIN halls h ON r.hall_id = h.hall_id  -- ホールと結合
    JOIN models m ON r.model_id = m.model_id  -- 機種と結合
    WHERE h.name = ?  -- 指定ホールのみ
    AND m.name LIKE '%ジャグラー%'  -- ジャグラー系機種に限定
    ORDER BY r.date DESC, r.unit_no ASC;
    """

    spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
    df_from_db = search_hall_and_load_data(SEARCH_WORD, query)
    df = preprocess_result_df(df_from_db)
    write_medals_summary_to_spreadsheet(df, spreadsheet, SHEET_NAME, get_medals_summary)
