# ============================
# detabase_to_gspread.py
# ============================
from gspread_dataframe import set_with_dataframe
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
import calendar
from dateutil.relativedelta import relativedelta
import os

from utils import connect_to_spreadsheet, get_or_create_worksheet
from logger_setup import setup_logger
from config import LOG_PATH, DB_PATH, SPREADSHEET_IDS, MODEL_LIST, GRAPE_CONSTANTS


logger = setup_logger("datebase_to_gspread", log_file=LOG_PATH)


def create_df_from_database(hall_name, start_date, end_date, model_name=None):
    # Table name 取得
    DB_PATH = r"C:\python\dataOnline\anaslo_02\db\anaslo_02.db"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = cursor.fetchall()
    # print(tables)

    cursor.execute(
        "SELECT hall_id, name FROM halls WHERE name LIKE ?", ("%" + hall_name + "%",)
    )
    results = cursor.fetchall()

    # 結果表示
    if results:
        hall_id, hall_name = results[0]
        print(f"🔍 '{hall_name}' を含むホール名が見つかりました。")
    else:
        print(f"❌ '{hall_name}' を含むホール名は見つかりませんでした。")

    query = """
        -- 出玉データにホール名と機種名を結合して取得
        SELECT
            r.*, 
            h.name AS hall_name,
            m.name AS model_name
        FROM results r
        JOIN halls h ON r.hall_id = h.hall_id
        JOIN models m ON r.model_id = m.model_id
        WHERE h.name = ?
        AND r.date BETWEEN ? AND ?
        """

    params = [hall_name, start_date, end_date]
    if model_name:
        query += " AND m.name LIKE ?"
        params.append(f"%{model_name}%")  # 部分一致にする

    query += " ORDER BY r.date DESC, r.unit_no ASC"

    df = pd.read_sql_query(query, conn, params=params)
    print(f"データサイズ: {df.shape[0]} x {df.shape[1]}")
    print(f"📅 検索期間: {start_date} ～ {end_date}", f"📅 抽出期間: {df.date.min()} ～ {df.date.max()}")
    print(f'含まれる日数 : {df["date"].nunique()}')

    return df


def dataFrame_to_gspread(df, spreadsheet, sheet_name):
    today = datetime.date.today()
    get_or_create_worksheet(spreadsheet, sheet_name, df.shape[0] + 5, df.shape[1] + 3)
    sheet = spreadsheet.worksheet(sheet_name)
    sheet.clear()
    set_with_dataframe(sheet, df, include_index=True)
    sheet.update_cell(1, 1, today.strftime("UPDATED: %Y-%m-%d"))
    logger.info(f"💾 シート更新完了: {sheet_name}")


def calc_grape_rate(row, constants, cherry=True):
    model = row["model_name"]
    if model not in constants:
        return None
    try:
        game = row["game"]
        bb = row["BB"]
        rb = row["RB"]
        medals = row["medals"]
        # 定数取得
        c = constants[model]
        cherry_rate = c["cherryOn"] if cherry else c["cherryOff"]
        # 分母計算式
        denominator = (
            -medals
            - (
                game * 3
                - (
                    bb * c["bb"]
                    + rb * c["rb"]
                    + game * c["replay"]
                    + game * cherry_rate
                )
            )
        ) / 8
        if denominator == 0:
            return None  # ゼロ除算防止
        grape = (game / denominator) - ((game / denominator) * 2)
        return round(grape, 2)

    except Exception as e:
        print(f"⚠️ Grape計算失敗: {model} → {e}")
        return None


def assign_area(unit_no, json_file_path):
    with open(json_file_path, "r", encoding="utf-8") as f:
        area_map = json.load(f)
    for rule in area_map:
        if rule["start"] <= unit_no <= rule["end"]:
            return rule["area"]
    return "その他"


# def preprocess_result_df(df, json_path):
#     # データ前処理
#     df["date"] = pd.to_datetime(df["date"])
#     df.drop(columns=["result_id", "hall_id", "model_id"], inplace=True)
#     df = df[
#         ["hall_name", "date", "model_name", "unit_no", "game", "BB", "RB", "medals"]
#     ]
#     df["BB"] = df["BB"].replace(0, np.nan)
#     df["RB"] = df["RB"].replace(0, np.nan)
#     df["BB_rate"] = (df["game"] / df["BB"]).round(1)
#     df["RB_rate"] = (df["game"] / df["RB"]).round(1)
#     df["Total_rate"] = (df["game"] / (df["BB"] + df["RB"])).round(1)
#     # df["Grape_rate"] = grape_calculator_myfive(
#     #     df["game"], df["BB"], df["RB"], df["medals"], cherry=True
#     # ).round(2)
#     df["Grape_rate"] = df.apply(
#         lambda row: calc_grape_rate(row, GRAPE_CONSTANTS), axis=1
#     )
#     df["day"] = df["date"].dt.day
#     df["month"] = df["date"].dt.month
#     df["weekday"] = df["date"].dt.weekday
#     df["unit_last"] = df["unit_no"].astype(str).str[-1]
#     df["area"] = df["unit_no"].apply(lambda x: assign_area(x, json_path))

#     df = df.replace([np.inf, -np.inf], np.nan)
#     df = df.fillna(0)

#     logger.info(f"🧹 データ前処理完了: {df.shape[0]} rows")
#     logger.info(f"データサイズ: {df.shape[0]} x {df.shape[1]}")
#     model_list = df["model_name"].unique()
#     logger.debug(f"データベースからのデータには以下のモデルが含まれています")
#     for i, model in enumerate(model_list):
#         logger.debug(f"{i}: {model}")

#     return df


def df_preprocessing(df, hall_name):
    json_path = f"C:/python/dataOnline/anaslo_02/json/{hall_name}_area_map.json"
    if not os.path.exists(json_path):
        json_path = f"C:/python/dataOnline/anaslo_02/json/other_area_map.json"
    print(f"データ前処理を行います")
    df_pre = df.copy()
    df_pre["date"] = pd.to_datetime(df_pre["date"])
    df_pre.drop(columns=["result_id", "hall_id", "model_id"], inplace=True)
    df_pre_columns = ["hall_name", "date", "model_name", "unit_no", "game", "BB", "RB", "medals"]
    df_pre = df_pre[df_pre_columns]
    df_pre["BB_rate"] = (df_pre["game"] / df_pre["BB"]).round(1)
    df_pre["RB_rate"] = (df_pre["game"] / df_pre["RB"]).round(1)
    # df_pre["Grape_rate"] = grape_calc_myfive(
    #     df_pre["game"], df_pre["BB"], df_pre["RB"], df_pre["medals"], cherry=True).round(2)
    
    df_pre["Grape_rate"] = df_pre.apply(lambda row: calc_grape_rate(row, GRAPE_CONSTANTS), axis=1)
    df_pre["Total_rate"] = (df_pre["game"] / (df_pre["BB"] + df_pre["RB"])).round(1)
    df_pre["month"] = df_pre["date"].dt.strftime("%Y-%m")
    df_pre["day"] = df_pre["date"].dt.day
    df_pre["weekday"] = df_pre["date"].dt.weekday
    df_pre["year"] = df_pre["date"].dt.year
    df_pre["unit_last"] = df_pre["unit_no"].astype(str).str[-1]

    df_pre["area"] = df_pre["unit_no"].apply(lambda x: assign_area(x, json_path))
    
    df_pre.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_pre = df_pre.fillna(0)

    model_list = list(df["model_name"].unique())
    # for i, model in enumerate(model_list):
    #     print(f"{i+1}: {model}", end=", ")

    return df_pre, model_list


def create_pivot_table(
    df,
    index,
    columns,
    pivots=["game", "medals", "BB", "RB"],
    reverse=False,
    margins=True,
    day_target=None,
):
    df_filtered = df.copy()
    # df_filtered = df_filtered[
    #     (df_filtered["date"].dt.date <= start_date)
    #     & (df_filtered["date"].dt.date >= end_date)
    # ]

    if day_target is not None:
        df_filtered = df_filtered[df_filtered["day"] == day_target]

    pivot_results = {}
    for col in pivots:
        table = df_filtered.pivot_table(
            index=index,
            columns=columns,
            values=col,
            aggfunc="sum",
            margins=True,
            margins_name="total",
        )
        if reverse:
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
    merged.replace([np.inf, -np.inf, np.nan], None, inplace=True)
    details = {
        "game": game,
        "medals": medals,
        "medal_rate": medal_rate,
        "bb": bb,
        "rb": rb,
        "rb_rate": rb_rate,
        "total_rate": total_rate,
    }

    return merged, details


# def history_data_by_model(df, model_name, period_month=1):
#     start_date = datetime.date.today()
#     # end_date = start_date - relativedelta(months=period_month, days=start_date.day + 15)
#     end_date = start_date - relativedelta(days=45)
#     logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

#     # 対象期間のモデルのデータを抽出
#     logger.info(f"🔍 モデル: {model_name} のデータを処理中...")
#     df_filtered = df.copy()
#     df_filtered = df_filtered[
#         (df_filtered["date"].dt.date <= start_date)
#         & (df_filtered["date"].dt.date >= end_date)
#     ]
#     df_filtered = df_filtered[(df_filtered["model_name"] == model_name)]
#     if df_filtered.empty:
#         logger.warning(f"⚠️ モデル: {model_name} の期間中のデータが見つかりませんでした")
#         return pd.DataFrame()

#     # 各種ピボットテーブル
#     pivot_targets = ["medals", "game", "RB_rate", "Total_rate", "Grape_rate"]
#     index_targets = ["area", "model_name", "unit_no"]
#     columns_targets = ["date"]
#     pivot_results = {}
#     for col in pivot_targets:
#         table = df_filtered.pivot_table(
#             index=index_targets,
#             columns=columns_targets,
#             values=col,
#             aggfunc="sum",
#         )
#         # 日付列を反転・スライス
#         pivot_results[col] = table.iloc[:, 7:].iloc[:, ::-1]

#     medals = pivot_results["medals"]
#     game = pivot_results["game"]
#     rb_rate = pivot_results["RB_rate"]
#     total_rate = pivot_results["Total_rate"]
#     grape_rate = pivot_results["Grape_rate"]
#     medal_rate = ((medals + game * 3) / (game * 3)).round(3)
#     # メダル累積3日間・7日間
#     rolling7 = (
#         medals.iloc[:, ::-1].rolling(7, min_periods=7, axis=1).sum().iloc[:, ::-1]
#     )
#     rolling5 = (
#         medals.iloc[:, ::-1].rolling(5, min_periods=3, axis=1).sum().iloc[:, ::-1]
#     )
#     rolling3 = (
#         medals.iloc[:, ::-1].rolling(3, min_periods=3, axis=1).sum().iloc[:, ::-1]
#     )
#     # 7日間累積とランク
#     medal_rank7 = (
#         rolling7.rank(method="min", ascending=True)
#         .fillna(0)
#         .replace([np.inf, -np.inf], 0)
#         .astype(int)
#     )
#     # 5日間累積とランク
#     medal_rank5 = (
#         rolling5.rank(method="min", ascending=True)
#         .fillna(0)
#         .replace([np.inf, -np.inf], 0)
#         .astype(int)
#     )
#     # 3日間累積とランク
#     medal_rank3 = (
#         rolling3.rank(method="min", ascending=True)
#         .fillna(0)
#         .replace([np.inf, -np.inf], 0)
#         .astype(int)
#     )
#     # 1日間累積とランク
#     medal_rank1 = (
#         medals.rank(method="min", ascending=True)
#         .fillna(0)
#         .replace([np.inf, -np.inf], 0)
#         .astype(int)
#     )

#     # MultiIndex化（ラベル付け）
#     labeled_tables = [
#         ("7RANK", medal_rank7),
#         ("5RANK", medal_rank5),
#         ("3RANK", medal_rank3),
#         ("1RANK", medal_rank1),
#         ("7ROLLING", rolling7),
#         ("5ROLLING", rolling5),
#         ("3ROLLING", rolling3),
#         ("MEDALS", medals),
#         ("RATE_MEDAL", medal_rate),
#         ("GAME", game),
#         ("RB_RATE", rb_rate),
#         ("TOTAL_RATE", total_rate),
#         ("GRAPE_RATE", grape_rate),
#     ]

#     # ラベルを MultiIndex に付ける
#     for label, df_table in labeled_tables:
#         df_table.columns = pd.MultiIndex.from_product([[label], df_table.columns])

#     # 列を交互に整列して統合・NaN除去
#     interleaved_cols = [
#         col
#         for col_group in zip(*(df.columns for _, df in labeled_tables))
#         for col in col_group
#     ]
#     merged = pd.concat([df for _, df in labeled_tables], axis=1)[interleaved_cols]
#     merged = merged[~merged.iloc[:, 5].isna()]  # 前日のrollingがNaNの行は削除

#     # エリアごとに空行挿入して整形
#     merged_by_area = pd.DataFrame()
#     for area in merged.index.get_level_values("area").unique():
#         area_merged = merged.xs(area, level="area", drop_level=False)
#         if not area_merged.empty:
#             empty_index = pd.MultiIndex.from_tuples(
#                 [("", " ", " ")], names=merged.index.names
#             )
#             empty_row = pd.DataFrame(
#                 [[""] * area_merged.shape[1]],
#                 index=empty_index,
#                 columns=area_merged.columns,
#             )
#             merged_by_area = pd.concat([merged_by_area, area_merged, empty_row])

#     # インデックス削除
#     merged_by_area = merged_by_area.droplevel("area")
#     logger.info(f"✅ モデル: {model_name} の処理完了（行数: {len(merged_by_area)}）")

#     return merged_by_area


# def merge_history_by_model(process_func, df, model_list):
#     merged_by_model = pd.DataFrame()
#     for model_name in model_list:
#         merged_by_area = history_data_by_model(df, model_name)
#         # モデル間の区切り用空行追加して結合
#         if not merged_by_area.empty:
#             empty_index = pd.MultiIndex.from_tuples(
#                 [(" ", " ")], names=merged_by_area.index.names
#             )
#             empty_row = pd.DataFrame(
#                 [[""] * merged_by_area.shape[1]],
#                 index=empty_index,
#                 columns=merged_by_area.columns,
#             )
#             merged_by_model = pd.concat(
#                 [merged_by_model, merged_by_area, empty_row], axis=0
#             )

#     return merged_by_model


def medal_rate_by_model(df):
    # start_date = datetime.date.today()
    # end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    # logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    index = ["model_name"]
    columns = ["day"]
    merged, details = create_pivot_table(df, index, columns, reverse=False, margins=True)
    model_rate = details["medal_rate"].copy()
    return model_rate


def medal_rate_by_island(df):
    # start_date = datetime.date.today()
    # end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    # logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    index = ["area"]
    columns = ["day"]
    merged, details = create_pivot_table(df, index, columns, reverse=False, margins=True)
    island_rate = details["medal_rate"].copy()
    return island_rate


def medal_rate_by_unit(df):
    # start_date = datetime.date.today()
    # end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    # logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    # pivot_targets = ["game", "medals", "BB", "RB"]
    index = ["area", "unit_no"]
    columns = ["day"]

    # merged, game, medals, medal_rate, bb, rb, rb_rate, total_rate = create_pivot_table(
    merged, details = create_pivot_table(
        df,
        # start_date,
        # end_date,
        # pivot_targets,
        index,
        columns,
        reverse=False,
        margins=True
    )
    unit_rate = details["medal_rate"].copy()
    target_rate = 1.05
    unit_rate[("MEDAL_RATE", f"count_{target_rate}+")] = (
        unit_rate.iloc[:, :-1] >= target_rate
    ).sum(axis=1)
    countif = (unit_rate.iloc[:-1, :] >= target_rate).sum(axis=0)
    unit_rate = pd.concat(
        [unit_rate, pd.DataFrame([countif], index=[(f"count_{target_rate}+", "")])],
        axis=0,
    )
    return unit_rate


def medal_rate_by_day(df, day_target):
    # start_date = datetime.date.today()
    # end_date = start_date - relativedelta(months=6, days=start_date.day - 1)
    # logger.info(f"📆 対象期間: {end_date} 〜 {start_date}")

    # pivot_targets = ["game", "medals", "BB", "RB"]
    index = ["area", "unit_no"]
    columns = ["date"]
    merged, details = create_pivot_table(df, index, columns, reverse=True, margins=True, day_target=day_target)

    merged.replace([np.inf, -np.inf, np.nan], None, inplace=True)
    return merged


if __name__ == "__main__":

    HALL_NAME = "EXA FIRST"
    # HALL_NAME = "第一プラザ坂戸1000"
    # HALL_NAME = "第一プラザ狭山店"
    # HALL_NAME = "第一プラザみずほ台店"
    # HALL_NAME = "みずほ台uno"
    
    model_name = "ジャグラー"
    
    add_spreadsheet = False

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
        
    
    # 日付範囲の設定
    today = datetime.date.today()
    last_month = today - relativedelta(months=1)
    month_ago = today - relativedelta(months=6)
    last_day = calendar.monthrange(last_month.year, last_month.month)[1]
    end_date = datetime.date(last_month.year, last_month.month, last_day)
    last_day = calendar.monthrange(month_ago.year, month_ago.month)[1]
    start_date = datetime.date(month_ago.year, month_ago.month, last_day)
    
    df_db = create_df_from_database(HALL_NAME, start_date, end_date, model_name=model_name)
    df, model_list = df_preprocessing(df_db, HALL_NAME)
    # df.to_csv(f"anaslo_02/out/{HALL_NAME}_df_preprocessing.csv", index=True)


    # MODEL_RATE 用のピボット処理・出力
    model_rate = medal_rate_by_model(df)
    model_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_model_rate.csv", index=True)

    # ISLAND_RATE 用のピボット処理・出力
    island_rate = medal_rate_by_island(df)
    island_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_island_rate.csv", index=True)
    

    # UNIT_RATE 用のピボット処理・出力
    unit_rate = medal_rate_by_unit(df)
    unit_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_unit_rate.csv", index=True)

    
    # DAY_RATE 用のピボット処理・出力
    for day_target in range(25, 26):
        marged_day = medal_rate_by_day(df, day_target)
        marged_day.to_csv(f"anaslo_02/out/{HALL_NAME}_marged_day.csv", index=True)
        
        if add_spreadsheet:
            spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
            dataFrame_to_gspread(marged_day, spreadsheet, sheet_name=f"DAY{day_target}")
    
    if add_spreadsheet:
        spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
        dataFrame_to_gspread(model_rate, spreadsheet, sheet_name="MODEL_RATE")
        dataFrame_to_gspread(island_rate, spreadsheet, sheet_name="ISLAND_RATE")
        dataFrame_to_gspread(unit_rate, spreadsheet, sheet_name="UNIT_RATE")
        

