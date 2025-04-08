# ============================
# main.py
# ============================
from config import DB_PATH, CSV_PATH, ARCHIVE_PATH
import scraper
from csv_to_database import csv_to_database
from utils import upgrade_uc_if_needed
from logger_setup import setup_logger

PREF = "東京都"
HALL_NAME = "exa-first"
URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"

DAYS_AGO = 40
RERIOD = 300

upgrade_uc_if_needed()

driver = scraper.start_google_chrome("https://www.google.com/")

for days_ago in range(DAYS_AGO, DAYS_AGO + RERIOD):
    scraper.scraper_for_data(
        driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL
    )

csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)
