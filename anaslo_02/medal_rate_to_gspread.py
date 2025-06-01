# ============================
# detabase_to_gspread.py
# ============================
from gspread_dataframe import set_with_dataframe
import pandas as pd
import numpy as np
from scipy.stats import poisson
import json
import sqlite3
import datetime
import calendar
from dateutil.relativedelta import relativedelta
import os

from utils import connect_to_spreadsheet, get_or_create_worksheet
from utils import MODEL_DATA_DICT
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


# 設定推定関数
def estimate_setting_probs(row, limit_games=5000):
    # JSONファイルの確率表を読み込む
    model = row["model_name"]
    games = row["game"]
    if games < limit_games or pd.isna(row["RB_rate"]) or pd.isna(row["Total_rate"]):
        return {f"設定{i}": 0.0 for i in range(1, 7)}
    use_grape = not pd.isna(row["Grape_rate"]) and row["Grape_rate"] != 0
    rb_actual = row["RB"]
    total_actual = row["BB"] + row["RB"]
    if use_grape:
        grape_actual = round(games / row["Grape_rate"])

    model_data = MODEL_DATA_DICT.get(model)
    if not model_data:
        return {f"設定{i}": 0.0 for i in range(1, 7)}

    likelihoods = {}
    for setting, values in model_data.items():
        try:
            rb_rate = float(values["RB_RATE"].split("/")[1])
            total_rate = float(values["TOTAL_RATE"].split("/")[1])
            grape_rate = float(values["GRAPE_RATE"].split("/")[1])
            expected_rb = games / rb_rate
            expected_total = games / total_rate
            expected_grape = games / grape_rate
            rb_likelihood = poisson.pmf(rb_actual, expected_rb)
            total_likelihood = poisson.pmf(total_actual, expected_total)
            # Grapeを使うかどうか
            if use_grape:
                grape_rate = float(values["GRAPE_RATE"].split("/")[1])
                expected_grape = games / grape_rate
                grape_likelihood = poisson.pmf(grape_actual, expected_grape)
                likelihood = rb_likelihood * total_likelihood * grape_likelihood
            else:
                likelihood = rb_likelihood * total_likelihood

            likelihoods[int(setting)] = likelihood
        except:
            continue
    total_sum = sum(likelihoods.values())
    if total_sum == 0:
        return {f"設定{i}": 0.0 for i in range(1, 7)}
    probs = {f"設定{s}": round(l / total_sum, 2) for s, l in likelihoods.items()}
    return probs


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
    df_pre["Grape_rate"] = df_pre.apply(lambda row: calc_grape_rate(row, GRAPE_CONSTANTS), axis=1)
    df_pre["Total_rate"] = (df_pre["game"] / (df_pre["BB"] + df_pre["RB"])).round(1)
    
    df_pre["month"] = df_pre["date"].dt.strftime("%Y-%m")
    df_pre["day"] = df_pre["date"].dt.day
    df_pre["weekday"] = df_pre["date"].dt.weekday
    df_pre["year"] = df_pre["date"].dt.year
    df_pre["unit_last"] = df_pre["unit_no"].astype(str).str[-1]
    df_pre["area"] = df_pre["unit_no"].apply(lambda x: assign_area(x, json_path))

    # 設定推定
    df_probs = df_pre.apply(estimate_setting_probs, axis=1, result_type="expand")
    df_pre["5more"] = df_probs[["設定5", "設定6"]].sum(axis=1)
    
    df_pre.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_pre = df_pre.fillna(0)

    model_list = list(df["model_name"].unique())

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
    df.to_csv(f"anaslo_02/out/{HALL_NAME}_df_preprocessing.csv", index=True)


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
        

