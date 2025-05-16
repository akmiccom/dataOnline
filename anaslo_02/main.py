# ============================
# main.py
# ============================
from config import CSV_PATH, DB_PATH, ARCHIVE_PATH, LOG_PATH
from config import QUERY, SPREADSHEET_IDS
import scraper
from csv_to_database import csv_to_database
from databese_to_gspread import search_hall_and_load_data, preprocess_result_df
from databese_to_gspread import get_medals_summary, medals_summary_to_gspread
from databese_to_gspread import extract_and_merge_model_data, extract_merge_all_model_date
from databese_to_gspread import merge_all_model_date_to_gspread
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

# PREF, HALL_NAME = "埼玉県", "パールショップともえ川越店"
# PREF, HALL_NAME = "埼玉県", "パラッツォ川越店"

SCRAPER = True
TO_DATABESE = True
TO_SPREADSHEET = True

# ============================


MODEL_LIST = [
    "マイジャグラーV",
    "ゴーゴージャグラー3",
    "アイムジャグラーEX-TP",
    "ファンキージャグラー2",
    "ミスタージャグラー",
    "ウルトラミラクルジャグラー",
    "ジャグラーガールズ",
    "ハッピージャグラーVIII",
]

SHEET_NAME_RANK = "RANKING"
SHEET_NAME_COMPARE = "HISTORY"

AREA_MAP_PATH = f"C:/python/dataOnline/anaslo_02/json/{HALL_NAME}_area_map.json"

log_banner(f"📊 {HALL_NAME} データ収集開始")

if SCRAPER:
    URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://www.google.com/")
    for days_ago in range(DAYS_AGO, DAYS_AGO + PERIOD):
        scraper.scraper_for_data(
            driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL
        )

if TO_DATABESE:
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)

if TO_SPREADSHEET:    
    spreadsheet = connect_to_spreadsheet(SPREADSHEET_IDS[HALL_NAME])
    ws = get_existing_worksheet(spreadsheet, SHEET_NAME_RANK)
    if ws is None:
        print("シートが見つかりません。処理を中止します。")
        exit()
    
    df_from_db = search_hall_and_load_data(HALL_NAME, QUERY)
    df = preprocess_result_df(df_from_db, AREA_MAP_PATH)
    medals_summary_to_gspread(
        df, MODEL_LIST, spreadsheet, get_medals_summary, sheet_name=SHEET_NAME_RANK
    )
    merged_by_model = extract_merge_all_model_date(
        extract_and_merge_model_data, df, MODEL_LIST
    )
    merge_all_model_date_to_gspread(
        merged_by_model, spreadsheet, sheet_name=SHEET_NAME_COMPARE
    )

logger.info(f"🎉 {HALL_NAME} データ収集終了")
logger.info("=" * 40)
