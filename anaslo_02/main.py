# ============================
# main.py
# ============================
from config import CSV_PATH, DB_PATH, ARCHIVE_PATH, LOG_PATH, spreadSheet_ids, QUERY
import scraper
from csv_to_database import csv_to_database
from databese_to_gspread import get_medals_summary, write_medals_summary_to_spreadsheet 
from databese_to_gspread import search_hall_and_load_data, preprocess_result_df
from utils import upgrade_uc_if_needed, connect_to_spreadsheet, log_banner
from logger_setup import setup_logger

logger = setup_logger("main", log_file=LOG_PATH)

log_banner("📊 ANA-SLO データ収集開始")



PREF = "東京都"
HALL_NAME = "EXA FIRST"

# PREF = "埼玉県"
# HALL_NAME = "アスカ狭山店"
# HALL_NAME = "パールショップともえ川越店"
# HALL_NAME = "第一プラザ狭山店"

DAYS_AGO = 1
PERIOD = 1
SHEET_NAME = "MEDALS_nDAYS_AGO"

SCRAPER = True
TO_DATABESE = True
TO_SPREADSHEET = True

if SCRAPER:
    URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://www.google.com/")
    for days_ago in range(DAYS_AGO, DAYS_AGO + PERIOD):
        scraper.scraper_for_data(
            driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL)

if TO_DATABESE:
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)

if TO_SPREADSHEET:
    df_from_db = search_hall_and_load_data(HALL_NAME, QUERY)
    df = preprocess_result_df(df_from_db)
    spreadsheet = connect_to_spreadsheet(spreadSheet_ids[HALL_NAME])
    write_medals_summary_to_spreadsheet(df, spreadsheet, SHEET_NAME, get_medals_summary)

logger.info("🎉 全ての処理が完了しました。")
