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

# PREF = "埼玉県"
# HALL_NAME = "アスカ狭山店"
# HALL_NAME = "パールショップともえ川越店"
# HALL_NAME = "パラッツォ川越店"

SCRAPER = True
TO_DATABESE = True

DAYS_AGO = 1
PERIOD = 1

URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"

if SCRAPER:
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://www.google.com/")
    for days_ago in range(DAYS_AGO, DAYS_AGO + PERIOD):
        scraper.scraper_for_data(
            driver, days_ago, scraper.REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL)

if TO_DATABESE:
    csv_to_database(DB_PATH, CSV_PATH, ARCHIVE_PATH)

