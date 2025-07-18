# ============================
# detabase_to_gspread.py
# ============================
import pandas as pd
import numpy as np
from scipy.stats import poisson
import json
import datetime
import os
from dateutil.relativedelta import relativedelta

from medal_rate_to_gspread import create_df_from_database, df_preprocessing, dataFrame_to_gspread
from utils import connect_to_spreadsheet
from logger_setup import setup_logger
from config import LOG_PATH, SPREADSHEET_IDS


logger = setup_logger("datebase_to_gspread", log_file=LOG_PATH)


def history_by_unit(df):
    merged_by_unit = pd.DataFrame()
    
    pivot_targets = ["medals", "game", "BB", "RB", "RB_rate", "Total_rate", "Grape_rate", "5more"]
    index_targets = ["area", "model_name", "unit_no"]
    columns_targets = ["date"]

    for model in df["model_name"].unique():
        
        # モデルごとにデータをフィルタリング
        df_model = df[df["model_name"] == model]
        pivot_results = {}
        for col in pivot_targets:
            table = df_model.pivot_table(
                index=index_targets,
                columns=columns_targets,
                values=col,
                aggfunc="sum",
            )
            pivot_results[col] = table
        
        medals = pivot_results["medals"]
        game = pivot_results["game"]
        bb = pivot_results["BB"]
        rb = pivot_results["RB"]
        rb_rate = pivot_results["RB_rate"]
        total_rate = pivot_results["Total_rate"]
        grape_rate = pivot_results["Grape_rate"]
        more5 = pivot_results["5more"]
        medal_rate = ((medals + game * 3) / (game * 3)).round(3)
        
        # ランキング列作成
        rolling7 = medals.T.rolling(7, min_periods=7).sum().T
        medal_rank7 = rolling7.rank(method="min", ascending=True)
        medal_rank7 = medal_rank7.fillna(0).replace([np.inf, -np.inf], 0).astype(int)
        rolling5 = medals.T.rolling(5, min_periods=5).sum().T
        medal_rank5 = rolling5.rank(method="min", ascending=True)
        medal_rank5 = medal_rank5.fillna(0).replace([np.inf, -np.inf], 0).astype(int)
        rolling3 = medals.T.rolling(3, min_periods=3).sum().T
        medal_rank3 = rolling3.rank(method="min", ascending=True)
        medal_rank3 = medal_rank3.fillna(0).replace([np.inf, -np.inf], 0).astype(int)
        medal_rank1 = medals.rank(method="min", ascending=True)
        medal_rank1 = medal_rank1.fillna(0).replace([np.inf, -np.inf], 0).astype(int)
        game_rank = game.rank(method="min", ascending=True)
        game_rank = game_rank.fillna(0).replace([np.inf, -np.inf], 0).astype(int)

        # MultiIndex化（ラベル付け）
        labeled_tables = [
            ("5MORE", more5),
            ("GRAPE_RATE", grape_rate),
            ("TOTAL_RATE", total_rate),
            ("RB_RATE", rb_rate),
            ("GAME", game),
            ("RATE_MEDAL", medal_rate),
            ("RB", rb),
            ("BB", bb),
            ("MEDALS", medals),
            ("3ROLLING", rolling3),
            # ("5ROLLING", rolling5),
            ("7ROLLING", rolling7),
            ("1RANK", medal_rank1),
            ("3RANK", medal_rank3),
            # ("5RANK", medal_rank5),
            ("7RANK", medal_rank7),
            ("GAME_RANK", game_rank),
        ]
        for label, df_table in labeled_tables:
            df_table.columns = pd.MultiIndex.from_product([[label], df_table.columns])

        # 列を交互に整列して統合・NaN除去・日付ソート・一部データ削除
        interleaved_cols = [
            col
            for col_group in zip(*(df.columns for _, df in labeled_tables))
            for col in col_group
        ]
        merged = pd.concat([df for _, df in labeled_tables], axis=1)[interleaved_cols]
        merged = merged.iloc[:, ::-1]
        merged = merged[~merged.iloc[:, 7].isna()]

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
        
        if not merged_by_area.empty:
            empty_index = pd.MultiIndex.from_tuples(
                [(" ", " ")], names=merged_by_area.index.names
            )
            empty_row = pd.DataFrame(
                [[""] * merged_by_area.shape[1]],
                index=empty_index,
                columns=merged_by_area.columns,
            )
            merged_by_unit = pd.concat(
                [merged_by_unit, merged_by_area, empty_row], axis=0
            )

    # 累計した最後の7日を削除
    merged_by_unit = merged_by_unit.iloc[:, :-len(labeled_tables)*6]
    merged_by_unit.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    return merged_by_unit





if __name__ == "__main__":
    
    HALL_LIST = [
        ("東京都", "スロットエランドール田無店", 1, 1),
    ]
    
    model_name = "ジャグラー"
    
    add_spreadsheet = True
    
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
    
    for PREF, HALL_NAME, DAYS_AGO, PERIOD in HALL_LIST:
        if HALL_NAME not in SPREADSHEET_IDS:
            raise ValueError(f"{HALL_NAME} のスプレッドシートIDが見つかりません")
        SPREADSHEET_ID = SPREADSHEET_IDS[HALL_NAME]

        AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/{HALL_NAME}_area_map.json"
        if not os.path.exists(AREA_MAP_PATH):
            AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/other_area_map.json"

        today = datetime.date.today()
        start_date = today - relativedelta(days=36)
        df_db = create_df_from_database(HALL_NAME, start_date, today, model_name=model_name)
        df, model_list = df_preprocessing(df_db, HALL_NAME)
        
        # HISTORY 用のピボット処理・出力
        merged_by_unit = history_by_unit(df)
        merged_by_unit.to_csv(f"anaslo_02/out/{HALL_NAME}_history_by_unit.csv", index=True)
        
        if add_spreadsheet:
            spreadsheet = connect_to_spreadsheet(SPREADSHEET_ID)
            dataFrame_to_gspread(merged_by_unit, spreadsheet, sheet_name="HIST_TEST")
        
