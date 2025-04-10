from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import numpy as np
import sqlite3
import datetime


DB_PATH = r"C:\python\dataOnline\anaslo_02\anaslo_02.db"

spreadSheet_ids = {
    "EXA FIRST": "10-B_vV1pvUzXmvGAiHhODGJgCloOsAmqSO9HvXpk_T8",
    "アスカ狭山店": "179nJF0NvLng7xPKsd_NX2pJBXsDNsO8SJhOvUAvFk2I",
    }

# 検索キーワードよりホール名取得
SEARCH_WORD = "EXA FIRST"
SPREADSHEET_ID = spreadSheet_ids[SEARCH_WORD]

# スプレッドシート認証設定
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
jsonf = r"C:\python\dataOnline\anaslo_02\spreeadsheet-347321-ff675ab5ccbd.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SPREADSHEET_ID)

# # Table name 取得
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(
    "SELECT hall_id, name FROM halls WHERE name LIKE ?", ("%" + SEARCH_WORD + "%",)
)
results = cursor.fetchall()

# 結果表示
if results:
    print(f"🔍 '{SEARCH_WORD}' を含むホール名の検索結果:")
    for hall_id, hall_name in results:
        print(f" - hall_name: {hall_name}, hall_id: {hall_id}")
else:
    print(f"❌ '{SEARCH_WORD}' を含むホール名は見つかりませんでした。")

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

df = pd.read_sql_query(query, conn, params=(hall_name,))
df["date"] = pd.to_datetime(df["date"])
df.drop(columns=["result_id", "hall_id", "model_id"], inplace=True)
df = df[["hall_name", "date", "model_name", "unit_no", "game", "BB", "RB", "medals"]]
df["BB_rate"] = (df["game"] / df["BB"]).round(1)
df["RB_rate"] = (df["game"] / df["RB"]).round(1)
df["Total_rate"] = (df["game"] / (df["BB"] + df["RB"])).round(1)
df["day"] = df["date"].dt.day
df["month"] = df["date"].dt.month
df["weekday"] = df["date"].dt.weekday

conn.close()


def rb_rate_n_days_ago(start_date, end_date, model_name):
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

    return medals


if __name__ == "__main__":

    SHEET_NAME = "nDAYS_AGO"

    today = datetime.date.today()
    start_date = today + datetime.timedelta(days=-1)
    end_date = today + datetime.timedelta(days=-3)

    models = [
        "マイジャグラーV",
        "ゴーゴージャグラー3",
        "アイムジャグラーEX-TP",
        "ファンキージャグラー2",
    ]

    sheet = spreadsheet.worksheet(SHEET_NAME)
    sheet.clear()

    next_row = 1
    for model in models:
        medals = rb_rate_n_days_ago(start_date, end_date, model)
        set_with_dataframe(sheet, medals, row=next_row, include_index=True)
        existing = get_as_dataframe(sheet, evaluate_formulas=True)
        next_row += medals.shape[0] + 5