# ============================
# main.py
# ============================
from config import CSV_PATH, DB_PATH, ARCHIVE_PATH, LOG_PATH
from config import QUERY, SPREADSHEET_IDS, MODEL_LIST
from config import today
import scraper
from csv_to_database import csv_to_database
from databese_to_gspread import search_hall_and_load_data, preprocess_result_df
from databese_to_gspread import merge_history_by_model, history_data_by_model
from databese_to_gspread import medal_rate_by_unit, medal_rate_by_island
from databese_to_gspread import medal_rate_by_model, medal_rate_by_day
from databese_to_gspread import dataFrame_to_gspread
from utils import upgrade_uc_if_needed, log_banner
from utils import connect_to_spreadsheet, get_existing_worksheet
from logger_setup import setup_logger
logger = setup_logger("main", log_file=LOG_PATH)


# ============================

PREF, HALL_NAME = "東京都", "EXA FIRST"
DAYS_AGO = 1
PERIOD = 1

# PREF, HALL_NAME = "東京都", "コンサートホールエフ成増"
# DAYS_AGO = 1
# PERIOD = 1

# PREF, HALL_NAME = "埼玉県", "第一プラザ坂戸1000"
# DAYS_AGO = 1
# PERIOD = 1

# PREF, HALL_NAME = "埼玉県", "第一プラザ狭山店"
# DAYS_AGO = 1
# PERIOD = 1

PREF, HALL_NAME = "埼玉県", "第一プラザみずほ台店"
DAYS_AGO = 1
PERIOD = 1

# PREF, HALL_NAME = "埼玉県", "みずほ台uno"
# DAYS_AGO = 1
# PERIOD = 200

# PREF, HALL_NAME = "埼玉県", "パールショップともえ川越店"
# PREF, HALL_NAME = "埼玉県", "パラッツォ川越店"

SCRAPER = False
TO_DATABESE = False
TO_SPREADSHEET = True


# ============================


log_banner(f"📊 {HALL_NAME} データ収集開始")

if SCRAPER:
    URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://www.google.com/")
    for days_ago in range(DAYS_AGO, DAYS_AGO + PERIOD):
        scraper.scraper_for_data(
            driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL
        )
    driver.close()

if TO_DATABESE:
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)

if TO_SPREADSHEET:    
    # データベースからデータを取得と前処理
    AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/{HALL_NAME}_area_map.json"
    df_from_db = search_hall_and_load_data(HALL_NAME, QUERY)
    df = preprocess_result_df(df_from_db, AREA_MAP_PATH)
    
    # スプレッドシートに接続
    spreadsheet = connect_to_spreadsheet(SPREADSHEET_IDS[HALL_NAME])
    
    # MODEL_RATE 用のピボット処理・出力
    model_rate = medal_rate_by_model(df)
    dataFrame_to_gspread(model_rate, spreadsheet, sheet_name="MODEL_RATE")

    # ISLAND_RATE 用のピボット処理・出力
    island_rate = medal_rate_by_island(df)
    dataFrame_to_gspread(island_rate, spreadsheet, sheet_name="ISLAND_RATE")

    # UNIT_RATE 用のピボット処理・出力
    unit_rate = medal_rate_by_unit(df)
    dataFrame_to_gspread(unit_rate, spreadsheet, sheet_name="UNIT_RATE")
    
    # HISTORY 用のピボット処理・出力
    history = merge_history_by_model(history_data_by_model, df, MODEL_LIST)
    dataFrame_to_gspread(history, spreadsheet, sheet_name="HISTORY")

    # DAY_RATE 用のピボット処理・出力
    for day_target in range(today.day - 1, today.day + 1):
        marged_day = medal_rate_by_day(df, day_target)
        dataFrame_to_gspread(marged_day, spreadsheet, sheet_name=f"DAY{day_target}")

logger.info(f"🎉 {HALL_NAME} データ収集終了")
logger.info("=" * 40)
