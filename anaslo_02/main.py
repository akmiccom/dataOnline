# ============================
# main.py
# ============================
from config import CSV_PATH, DB_PATH, ARCHIVE_PATH, LOG_PATH, AREA_MAP_PATH
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

SCRAPER = True
TO_DATABESE = True
TO_SPREADSHEET = True

PREF = "東京都"
HALL_NAME = "EXA FIRST"

# PREF = "埼玉県"
# HALL_NAME = "パールショップともえ川越店"
# HALL_NAME = "パラッツォ川越店"
# HALL_NAME = "第一プラザ狭山店"


MODEL_LIST = [
    "マイジャグラーV",
    "アイムジャグラーEX-TP",
    "ゴーゴージャグラー3",
    "ファンキージャグラー2",
    "ミスタージャグラー",
    "ウルトラミラクルジャグラー",
    "ジャグラーガールズ",
    # "ハッピージャグラーVIII",
]

DAYS_AGO = 1
PERIOD = 1
SHEET_NAME_RANK = "7日差枚ランキング"
SHEET_NAME_COMPARE = "7日差枚と結果の比較"


log_banner("📊 ANA-SLO データ収集開始")

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
    # medals_summary_to_gspread(
    #     df, MODEL_LIST, spreadsheet, get_medals_summary, sheet_name=SHEET_NAME_RANK
    # )
    merged_by_model = extract_merge_all_model_date(
        extract_and_merge_model_data, df, MODEL_LIST
    )
    merge_all_model_date_to_gspread(
        merged_by_model, spreadsheet, sheet_name=SHEET_NAME_COMPARE
    )

logger.info("🎉 ANA-SLO データ収集終了")
logger.info("=" * 40)
