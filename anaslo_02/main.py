# ============================
# main.py
# ============================
from config import CSV_PATH, DB_PATH, ARCHIVE_PATH, LOG_PATH
from config import SPREADSHEET_IDS, today
import datetime
import time
import calendar
from random import randint
from dateutil.relativedelta import relativedelta

import scraper
from csv_to_database import csv_to_database
from medal_rate_to_gspread import medal_rate_by_unit, medal_rate_by_island
from medal_rate_to_gspread import medal_rate_by_model, medal_rate_by_day
from medal_rate_to_gspread import dataFrame_to_gspread
from medal_rate_to_gspread import create_df_from_database, df_preprocessing
from history_to_gspread import history_by_unit
from utils import upgrade_uc_if_needed, connect_to_spreadsheet, log_banner
from logger_setup import setup_logger

logger = setup_logger("main", log_file=LOG_PATH)

SCRAPER = True
TO_SPREADSHEET = True
HISTORY = True
MODEL_RATE  = True
ISLAND_RATE = True
UNIT_RATE = True
DAY_RATE = True


# ============================
# 全ホール定義：都道府県 + ホール名 + 日数範囲
# ============================


model_name = "ジャグラー"

HALL_LIST = [
    ("東京都", "EXA FIRST", 1, 1),
    ("東京都", "コンサートホールエフ成増", 1, 1),
    ("東京都", "楽園大山店", 1, 1),
    ("東京都", "マルハン大山店", 1, 1),
    ("東京都", "大山オーシャン", 1, 1),
    # ("東京都", "大山オーシャン", 51, 10),
    # ("東京都", "クラウンときわ台", 50, 10),
    # ("東京都", "ミリオン東武練馬13号店", 1, 10),
    # ("東京都", "ミリオン平和台16号店", 1, 10),
    
    ("茨城県", "レイト平塚", 1, 1),
    
    ("東京都", "スロットエランドール田無店", 1, 1),
    
    ("埼玉県", "パールショップともえ川越店", 1, 1),
    ("埼玉県", "ニューダイエイiii", 1, 1),
    ("埼玉県", "グランドオータ新座駅前店", 1, 1),
    ("埼玉県", "ニュークラウン川越2号店", 1, 1),
    
    # ("埼玉県", "第一プラザみずほ台店", 1, 1),
    # ("埼玉県", "みずほ台uno", 1, 1),
    # ("埼玉県", "sapみずほ台", 1, 1),
    # ("埼玉県", "ミリオン和光10号店", 1, 1),
    # ("埼玉県", "オータ志木駅前店", 1, 1),
    
    # ("埼玉県", "第一プラザ坂戸1000", 1, 1),
    # ("埼玉県", "第一プラザ狭山店", 1, 1),
    # ("埼玉県", "第一プラザ坂戸にっさい店", 1, 1),
    # ("埼玉県", "toho川越店", 1, 1),
    
    # ("東京都", "マルハン大山店", 125, 15),
    # ("茨城県", "レイト平塚", 130, 10),
]
# ============================


# SCRAPER = False
# TO_SPREADSHEET = False

# MODEL_RATE  = False
# ISLAND_RATE = False
# UNIT_RATE = False
# DAY_RATE = False
LAST_DIGIT_DAY = 9

# ============================

if SCRAPER:
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://www.google.com/")
    time.sleep(3)

for PREF, HALL_NAME, DAYS_AGO, PERIOD in HALL_LIST:
    log_banner(f"📊 {HALL_NAME} データ収集開始")
    if SCRAPER:
        driver.get("https://ana-slo.com/") # 初期ページを開く
        time.sleep(randint(2, 3))
        URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"
        for days_ago in range(DAYS_AGO, DAYS_AGO + PERIOD):
            scraper.scraper_for_data(
                driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL
            )
            time.sleep(randint(2, 3))

    if TO_SPREADSHEET:
        # スプレッドシートに接続
        spreadsheet = connect_to_spreadsheet(SPREADSHEET_IDS[HALL_NAME])
        
        # データベースからデータ取得と前処理
        csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)

        last_month = today - relativedelta(months=1)
        month_ago = today - relativedelta(months=6)
        last_day = calendar.monthrange(last_month.year, last_month.month)[1]
        end_date = datetime.date(last_month.year, today.month, today.day)
        last_day = calendar.monthrange(month_ago.year, month_ago.month)[1]
        start_date = datetime.date(month_ago.year, month_ago.month, last_day)
        
        df_db = create_df_from_database(HALL_NAME, start_date, end_date, model_name=model_name)
        df, model_list = df_preprocessing(df_db, HALL_NAME)
        
        # MODEL_RATE 用のピボット処理・出力
        if MODEL_RATE:
            model_rate = medal_rate_by_model(df)
            model_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_model_rate.csv", index=True)
            dataFrame_to_gspread(model_rate, spreadsheet, sheet_name="MODEL_RATE")

        # ISLAND_RATE 用のピボット処理・出力
        if ISLAND_RATE:
            island_rate = medal_rate_by_island(df)
            island_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_island_rate.csv", index=True)
            dataFrame_to_gspread(island_rate, spreadsheet, sheet_name="ISLAND_RATE")

        # UNIT_RATE 用のピボット処理・出力
        if UNIT_RATE:
            unit_rate = medal_rate_by_unit(df)
            unit_rate.to_csv(f"anaslo_02/out/{HALL_NAME}_unit_rate.csv", index=True)
            dataFrame_to_gspread(unit_rate, spreadsheet, sheet_name="UNIT_RATE")

        # # DAY_RATE 用のピボット処理・出力
        if DAY_RATE:
            for day_target in [LAST_DIGIT_DAY, 10+LAST_DIGIT_DAY, 20+LAST_DIGIT_DAY]:
                marged_day = medal_rate_by_day(df, day_target)
                marged_day.to_csv(f"anaslo_02/out/{HALL_NAME}_marged_day.csv", index=True)
                if TO_SPREADSHEET:
                    spreadsheet = connect_to_spreadsheet(SPREADSHEET_IDS[HALL_NAME])
                    dataFrame_to_gspread(marged_day, spreadsheet, sheet_name=f"DAY{day_target}")
                    
        # HISTORY 用のピボット処理・出力
        if HISTORY:
            start_date = today - relativedelta(months=0, days=38)
            if HALL_NAME == "パールショップともえ川越店":
                start_date = today - relativedelta(months=3, days=38)
            df_db = create_df_from_database(HALL_NAME, start_date, today, model_name=model_name)
            df, model_list = df_preprocessing(df_db, HALL_NAME)
            merged_by_unit = history_by_unit(df)
            merged_by_unit.to_csv(f"anaslo_02/out/{HALL_NAME}_history_by_unit.csv", index=True)
            dataFrame_to_gspread(merged_by_unit, spreadsheet, sheet_name="HISTORY")


logger.info(f"🎉 {HALL_NAME} データ収集終了")
logger.info("=" * 40)

if SCRAPER:
    driver.close()