# ============================
# detabase_to_gspread.py
# ============================
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
from dateutil.relativedelta import relativedelta
from utils import connect_to_spreadsheet
from logger_setup import setup_logger
from config import LOG_PATH, DB_PATH, JSONF, SPREADSHEET_IDS


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


def grape_calculator_myfive(game, bb, rb, medals, cherry=True):
    bb_medals = 239.25
    rb_medals = 95.25
    replay_rate = 0.411
    if cherry:
        cherry_rate_high = 0.04228
    else:
        cherry_rate_high = 0.05847
    denominator_inner = (
        -medals
        - (
            game * 3
            - (
                bb * bb_medals
                + rb * rb_medals
                + game * replay_rate
                + game * cherry_rate_high
            )
        )
    ) / 8
    grape_rate = (game / denominator_inner) - ((game / denominator_inner) * 2)

    return grape_rate


def assign_area(unit_no, json_file_path):
    with open(json_file_path, "r", encoding="utf-8") as f:
        area_map = json.load(f)
    for rule in area_map:
        if rule["start"] <= unit_no <= rule["end"]:
            return rule["area"]
    return "その他"


def preprocess_result_df(df, json_path):
    # データ前処理
    df["date"] = pd.to_datetime(df["date"])
    df.drop(columns=["result_id", "hall_id", "model_id"], inplace=True)
    df = df[
        ["hall_name", "date", "model_name", "unit_no", "game", "BB", "RB", "medals"]
    ]
    df["BB"] = df["BB"].replace(0, np.nan)
    df["RB"] = df["RB"].replace(0, np.nan)
    df["BB_rate"] = (df["game"] / df["BB"]).round(1)
    df["RB_rate"] = (df["game"] / df["RB"]).round(1)
    df["Total_rate"] = (df["game"] / (df["BB"] + df["RB"])).round(1)
    df["Grape_rate"] = grape_calculator_myfive(
        df["game"], df["BB"], df["RB"], df["medals"], cherry=True
    ).round(2)
    df["day"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["weekday"] = df["date"].dt.weekday
    df["unit_last"] = df["unit_no"].astype(str).str[-1]
    df["area"] = df["unit_no"].apply(lambda x: assign_area(x, json_path))

    logger.info(f"🧹 データ前処理完了: {df.shape[0]} rows")
    logger.info(f"データサイズ: {df.shape[0]} x {df.shape[1]}")
    model_list = df["model_name"].unique()
    logger.debug(f"データベースからのデータには以下のモデルが含まれています")
    for i, model in enumerate(model_list):
        logger.debug(f"{i}: {model}")

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


def medals_summary_to_gspread(
    df, model_list, spreadsheet, get_medals_summary, sheet_name
):
    logger.info(f"📆 本日より7日前のデータを追加します: {sheet_name}")

    today = datetime.date.today()
    start_date = today + datetime.timedelta(days=-1)
    end_date = today + datetime.timedelta(days=-7)
    logger.info(f"   📍 基準日: {start_date}, 終了日: {end_date}")

    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()
    next_row = 1
    for model in model_list:
        medals = get_medals_summary(df, start_date, end_date, model)
        set_with_dataframe(sheet, medals, row=next_row, include_index=True)
        existing = get_as_dataframe(sheet, evaluate_formulas=True)
        next_row += medals.shape[0] + 5
        logger.info(f"   ✅ 追加完了: {model}")
    sheet.update_cell(1, 1, today.strftime("UPDATED: %Y-%m-%d"))
    logger.info(f"💾 シート更新完了: {sheet_name}")


def extract_and_merge_model_data(df, model_name):
    period = 30
    today = datetime.date.today()
    start_date = today - relativedelta(days=1)
    end_date = start_date - relativedelta(days=period)

    # 対象期間のモデルのデータを抽出
    logger.info(f"🔍 モデル: {model_name} のデータを処理中...")
    df_filtered = df.copy()
    df_filtered = df_filtered[
        (df_filtered["date"].dt.date <= start_date)
        & (df_filtered["date"].dt.date >= end_date)
    ]
    df_filtered = df_filtered[(df_filtered["model_name"] == model_name)]
    if df_filtered.empty:
        logger.warning(f"⚠️ モデル: {model_name} の期間中のデータが見つかりませんでした")
        return pd.DataFrame()

    # 各種ピボットテーブル
    pivot_targets = ["medals", "game", "RB_rate", "Total_rate", "Grape_rate"]
    pivot_results = {}
    for col in pivot_targets:
        table = df_filtered.pivot_table(
            index=["area", "model_name", "unit_no"],
            columns="date",
            values=col,
            aggfunc="sum",
        )
        # 日付列を反転・スライス
        pivot_results[col] = table.iloc[:, 7:].iloc[:, ::-1]

    medals = pivot_results["medals"]
    game = pivot_results["game"]
    rb_rate = pivot_results["RB_rate"]
    total_rate = pivot_results["Total_rate"]
    grape_rate = pivot_results["Grape_rate"]

    # 7日間累積とランク
    rolling_7d_sum = (
        medals.iloc[:, ::-1]
        .rolling(window=7, min_periods=1)
        .sum()
        .iloc[:, ::-1]
        .iloc[:, :-6]
    )
    rolling_7d_sum.columns = [
        f"{col.strftime('%y-%m-%d')}_7d_sum" for col in rolling_7d_sum.columns
    ]
    rolling_7d_sum = rolling_7d_sum.iloc[:, ::-1]
    rolling_7d_rank = (
        rolling_7d_sum.rank(method="min", ascending=True)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .astype(int)
    )
    rolling_7d_rank.columns = [
        c.replace("sum", "rank") for c in rolling_7d_rank.columns
    ]
    rolling_7d_sum = rolling_7d_sum.iloc[:, ::-1]
    rolling_7d_rank = rolling_7d_rank.iloc[:, ::-1]

    # MultiIndex化（ラベル付け）
    labeled_tables = [
        ("RANK", rolling_7d_rank),
        ("7D_sum", rolling_7d_sum),
        ("MEDALS", medals),
        ("GAME", game),
        ("RB_RATE", rb_rate),
        ("TOTAL_RATE", total_rate),
        ("GRAPE_RATE", grape_rate),
    ]

    # ラベルを MultiIndex に付ける
    for label, df_table in labeled_tables:
        df_table.columns = pd.MultiIndex.from_product([[label], df_table.columns])

    # 列を交互に整列して統合・NaN除去
    interleaved_cols = [
        col
        for col_group in zip(*(df.columns for _, df in labeled_tables))
        for col in col_group
    ]
    merged = pd.concat([df for _, df in labeled_tables], axis=1)[interleaved_cols]
    merged = merged[~merged.iloc[:, 2].isna()]  # 前日がNaNの行は削除

    # エリアごとに空行挿入して整形
    merged_by_area = pd.DataFrame()
    for area in merged.index.get_level_values("area").unique():
        area_merged = merged.xs(area, level="area", drop_level=False)
        if not area_merged.empty:
            empty_index = pd.MultiIndex.from_tuples(
                [("", " ", " ")], names=merged.index.names
            )
            empty_row = pd.DataFrame(
                [[""] * area_merged.shape[1]],
                index=empty_index,
                columns=area_merged.columns,
            )
            merged_by_area = pd.concat([merged_by_area, area_merged, empty_row])

    # インデックス削除
    merged_by_area = merged_by_area.droplevel("area")
    logger.info(f"✅ モデル: {model_name} の処理完了（行数: {len(merged_by_area)}）")

    return merged_by_area


def extract_merge_all_model_date(process_func, df, model_list):

    merged_by_model = pd.DataFrame()
    for model in model_list:
        merged_by_area = extract_and_merge_model_data(df, model)
        # モデル間の区切り用空行追加して結合
        if not merged_by_area.empty:
            empty_index = pd.MultiIndex.from_tuples(
                [(" ", " ")], names=merged_by_area.index.names
            )
            empty_row = pd.DataFrame(
                [[""] * merged_by_area.shape[1]],
                index=empty_index,
                columns=merged_by_area.columns,
            )
            merged_by_model = pd.concat(
                [merged_by_model, merged_by_area, empty_row], axis=0
            )
    return merged_by_model


def merge_all_model_date_to_gspread(df, spreadsheet, sheet_name):
    today = datetime.date.today()
    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()
    set_with_dataframe(sheet, df, include_index=True)
    sheet.update_cell(1, 1, today.strftime("UPDATED: %Y-%m-%d"))
    logger.info(f"💾 シート更新完了: {sheet_name}")


if __name__ == "__main__":

    # 検索キーワードよりホール名取得
    SEARCH_WORD = "EXA FIRST"
    SHEET_NAME_RANK = "7日差枚ランキング"
    SHEET_NAME_COMPARE = "7日差枚と結果の比較"
    MODEL_LIST = [
        "マイジャグラーV",
        "アイムジャグラーEX-TP",
        "ゴーゴージャグラー3",
        "ファンキージャグラー2",
        "ミスタージャグラー",
    ]
    SPREADSHEET_ID = SPREADSHEET_IDS[SEARCH_WORD]
    if SEARCH_WORD not in SPREADSHEET_IDS:
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

    AREA_MAP_PATH = r"C:\python\dataOnline\anaslo_02\json\exa_area_map.json"

    spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
    df_from_db = search_hall_and_load_data(SEARCH_WORD, query)
    df = preprocess_result_df(df_from_db, AREA_MAP_PATH)
    # medals_summary_to_gspread(df, MODEL_LIST, spreadsheet, get_medals_summary, sheet_name=SHEET_NAME_RANK)

    merged_by_model = extract_merge_all_model_date(extract_and_merge_model_data, df, MODEL_LIST)
    merge_all_model_date_to_gspread(merged_by_model, spreadsheet, sheet_name=SHEET_NAME_COMPARE)
    
    # df.to_csv("for_df_check.csv", index=False, encoding="utf-8-sig")
    # logger.info(f"📁 保存完了: for_df_check.csv")
    # output_path = f"merged_by_model.csv"
    # merged_by_model.to_csv(output_path, encoding="utf_8_sig")
    # logger.info(f"📁 保存完了: {output_path}")
