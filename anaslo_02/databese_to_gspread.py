# ============================
# detabase_to_gspread.py
# ============================
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import gspread
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
import os

from dateutil.relativedelta import relativedelta
from utils import connect_to_spreadsheet, get_or_create_worksheet
from logger_setup import setup_logger
from config import LOG_PATH, DB_PATH, SPREADSHEET_IDS, MODEL_LIST, today


logger = setup_logger("datebase_to_gspread", log_file=LOG_PATH)


def search_hall_and_load_data(HALL_NAME, query):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    logger.info(f"🚀 DB接続: {DB_PATH}")
    cursor.execute(
        "SELECT hall_id, name FROM halls WHERE name LIKE ?", ("%" + HALL_NAME + "%",)
    )
    results = cursor.fetchall()
    # 結果表示
    if results:
        hall_id, hall_name = results[0]
        logger.info(f"🔍 該当ホール: {hall_name}（ID: {hall_id}）")
    else:
        logger.error(f"❌ '{HALL_NAME}' を含むホール名は見つかりませんでした。")
        conn.close()
        return pd.DataFrame()

    df_from_db = pd.read_sql_query(query, conn, params=(hall_name,))
    conn.close()
    logger.info(f"🛑 DB接続終了: {DB_PATH}")

    return df_from_db


def dataFrame_to_gspread(df, spreadsheet, sheet_name):
    today = datetime.date.today()
    get_or_create_worksheet(spreadsheet, sheet_name, df.shape[0]+5, df.shape[1]+3)
    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()
    set_with_dataframe(sheet, df, include_index=True)
    sheet.update_cell(1, 1, today.strftime("UPDATED: %Y-%m-%d"))
    logger.info(f"💾 シート更新完了: {sheet_name}")


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

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(0)

    logger.info(f"🧹 データ前処理完了: {df.shape[0]} rows")
    logger.info(f"データサイズ: {df.shape[0]} x {df.shape[1]}")
    model_list = df["model_name"].unique()
    logger.debug(f"データベースからのデータには以下のモデルが含まれています")
    for i, model in enumerate(model_list):
        logger.debug(f"{i}: {model}")

    return df


def create_pivot_table(
    df,
    start_date,
    end_date,
    pivot_targets,
    index_targets,
    columns_targets,
    day_target=None,
    date_reverse=False,
):
    df_filtered = df.copy()
    df_filtered = df_filtered[
        (df_filtered["date"].dt.date <= start_date)
        & (df_filtered["date"].dt.date >= end_date)
    ]
    
    if day_target is not None:
        df_filtered = df_filtered[df_filtered["day"] == day_target]

    pivot_results = {}
    for col in pivot_targets:
        table = df_filtered.pivot_table(
            index=index_targets,
            columns=columns_targets,
            values=col,
            aggfunc="sum",
            margins=True,
            margins_name="total",
        )
        if date_reverse:
            pivot_results[col] = table.iloc[:, ::-1]
        else:
            pivot_results[col] = table

    game = pivot_results["game"]
    medals = pivot_results["medals"]
    rb = pivot_results["RB"]
    bb = pivot_results["BB"]
    rb_rate = (game / rb).round(1)
    total_rate = (game / (bb + rb)).round(1)
    medal_rate = ((medals + game * 3) / (game * 3)).round(3)

    labeled_tables = [
        ("GAME", game),
        ("MEDALS", medals),
        ("RB_RATE", rb_rate),
        ("TOTAL_RATE", total_rate),
        ("MEDAL_RATE", medal_rate),
        ("BB", bb),
        ("RB", rb),
    ]

    # ラベルを MultiIndex に付ける
    for label, df_table in labeled_tables:
        df_table.columns = pd.MultiIndex.from_product([[label], df_table.columns])

    # 列を交互に整列して統合・NaN除去
    interleaved_cols = [
        col
        for pair in zip(
            game.columns,
            medals.columns,
            bb.columns,
            rb.columns,
            medal_rate.columns,
            rb_rate.columns,
            total_rate.columns,
        )
        for col in pair
    ]

    merged = pd.concat([game, medals, medal_rate, bb, rb, rb_rate, total_rate], axis=1)[
        interleaved_cols
    ]

    return merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate


def history_data_by_model(df, model_name, period_month=1):
    start_date = datetime.date.today()
    # end_date = start_date - relativedelta(months=period_month, days=start_date.day + 15)
    end_date = start_date - relativedelta(days=45)
    logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

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
    index_targets = ["area", "model_name", "unit_no"]
    columns_targets = ["date"]
    pivot_results = {}
    for col in pivot_targets:
        table = df_filtered.pivot_table(
            index=index_targets,
            columns=columns_targets,
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
    medal_rate = ((medals + game * 3) / (game * 3)).round(3)
    # メダル累積3日間・7日間
    rolling7 = (
        medals.iloc[:, ::-1].rolling(7, min_periods=7, axis=1).sum().iloc[:, ::-1]
    )
    rolling5 = (
        medals.iloc[:, ::-1].rolling(5, min_periods=3, axis=1).sum().iloc[:, ::-1]
    )
    rolling3 = (
        medals.iloc[:, ::-1].rolling(3, min_periods=3, axis=1).sum().iloc[:, ::-1]
    )
    # 7日間累積とランク
    medal_rank7 = (
        rolling7.rank(method="min", ascending=True)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .astype(int)
    )
    # 5日間累積とランク
    medal_rank5 = (
        rolling5.rank(method="min", ascending=True)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .astype(int)
    )
    # 3日間累積とランク
    medal_rank3 = (
        rolling3.rank(method="min", ascending=True)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .astype(int)
    )
    # 1日間累積とランク
    medal_rank1 = (
        medals.rank(method="min", ascending=True)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .astype(int)
    )

    # MultiIndex化（ラベル付け）
    labeled_tables = [
        ("7RANK", medal_rank7),
        ("5RANK", medal_rank5),
        ("3RANK", medal_rank3),
        ("1RANK", medal_rank1),
        ("7ROLLING", rolling7),
        ("5ROLLING", rolling5),
        ("3ROLLING", rolling3),
        ("MEDALS", medals),
        ("RATE_MEDAL", medal_rate),
        ("GAME", game),
        ("RB_RATE", rb_rate),
        ("TOTAL_RATE", total_rate),
        # ("GRAPE_RATE", grape_rate),
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
    merged = merged[~merged.iloc[:, 5].isna()]  # 前日のrollingがNaNの行は削除

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


def merge_history_by_model(process_func, df, model_list):
    merged_by_model = pd.DataFrame()
    for model_name in model_list:
        merged_by_area = history_data_by_model(df, model_name)
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


def medal_rate_by_unit(df):
    start_date = datetime.date.today()
    end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    pivot_targets = ["game", "medals", "BB", "RB"]
    index_targets = ["area", "unit_no"]
    columns_targets = ["day"]

    merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate = create_pivot_table(
        df,
        start_date,
        end_date,
        pivot_targets,
        index_targets,
        columns_targets,
        date_reverse=False,
    )

    target_rate = 1.05
    medal_rate[("MEDAL_RATE", f"count_{target_rate}+")] = (
        medal_rate.iloc[:, :-1] >= target_rate
    ).sum(axis=1)
    countif = (medal_rate.iloc[:-1, :] >= target_rate).sum(axis=0)
    medal_rate = pd.concat(
        [medal_rate, pd.DataFrame([countif], index=[(f"count_{target_rate}+", "")])],
        axis=0,
    )
    return medal_rate


def medal_rate_by_island(df):
    start_date = datetime.date.today()
    end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    pivot_targets = ["game", "medals", "BB", "RB"]
    index_targets = ["area"]
    colmns_targets = ["day"]
    merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate = create_pivot_table(
        df,
        start_date,
        end_date,
        pivot_targets,
        index_targets,
        colmns_targets,
        date_reverse=False,
    )
    return medal_rate


def medal_rate_by_model(df):
    start_date = datetime.date.today()
    end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    pivot_targets = ["game", "medals", "BB", "RB"]
    index_targets = ["model_name"]
    colmns_targets = ["day"]
    merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate = create_pivot_table(
        df,
        start_date,
        end_date,
        pivot_targets,
        index_targets,
        colmns_targets,
        date_reverse=False,
    )
    return medal_rate


def medal_rate_by_day(df, day_target):
    start_date = datetime.date.today()
    end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    pivot_targets = ["game", "medals", "BB", "RB"]
    index_targets = ["area", "unit_no"]
    columns_targets = ["date"]

    merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate = create_pivot_table(
        df,
        start_date,
        end_date,
        pivot_targets,
        index_targets,
        columns_targets,
        day_target,
        date_reverse=True,
    )

    merged.replace([np.inf, -np.inf, np.nan], None, inplace=True)
    return merged


if __name__ == "__main__":

    HALL_NAME = "EXA FIRST"
    # HALL_NAME = "第一プラザ坂戸1000"
    # HALL_NAME = "第一プラザ狭山店"
    HALL_NAME = "第一プラザみずほ台店"
    # HALL_NAME = "みずほ台uno"

    if HALL_NAME not in SPREADSHEET_IDS:
        raise ValueError(f"{HALL_NAME} のスプレッドシートIDが見つかりません")
    SPREADSHEET_ID = SPREADSHEET_IDS[HALL_NAME]

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

    AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/{HALL_NAME}_area_map.json"
    if not os.path.exists(AREA_MAP_PATH):
        AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/other_area_map.json"

    spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
    df_from_db = search_hall_and_load_data(HALL_NAME, query)
    df = preprocess_result_df(df_from_db, AREA_MAP_PATH)


    # # MODEL_RATE 用のピボット処理・出力
    # model_rate = medal_rate_by_model(df)
    # dataFrame_to_gspread(model_rate, spreadsheet, sheet_name="MODEL_RATE")

    # # ISLAND_RATE 用のピボット処理・出力
    # island_rate = medal_rate_by_island(df)
    # dataFrame_to_gspread(island_rate, spreadsheet, sheet_name="ISLAND_RATE")

    # # # UNIT_RATE 用のピボット処理・出力
    # unit_rate = medal_rate_by_unit(df)
    # dataFrame_to_gspread(unit_rate, spreadsheet, sheet_name="UNIT_RATE")
    
    # HISTORY 用のピボット処理・出力
    history = merge_history_by_model(history_data_by_model, df, MODEL_LIST)
    # dataFrame_to_gspread(history, spreadsheet, sheet_name="HISTORY")
    history.to_csv(f"{HALL_NAME}_history.csv", index=True)

    # # DAY_RATE 用のピボット処理・出力
    # for day_target in range(25, 26):
    #     marged_day = medal_rate_by_day(df, day_target)
    #     dataFrame_to_gspread(marged_day, spreadsheet, sheet_name=f"DAY{day_target}")
