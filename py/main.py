# ============================
# main.py
# ============================
import scraper
from time import sleep
from utils import upgrade_uc_if_needed
from logger_setup import setup_logger
from csv_to_sqlite import csv_to_sqlite
from config import REMOVE_ADS_SCRIPT, MAX_RETRIES, CSV_FOLDER, DB_PATH
from config import MAIN_URL, DATA, EXTRA_WORD

logger = setup_logger("main")

PREF = "東京都"
HALL_NAME = "exa-first"
HALL_ID = 101262

# PREF = "埼玉県"
# HALL_NAME = "パラッツォ川越店"
# HALL_ID = 100800
# HALL_NAME = "第一プラザ狭山店"
# HALL_ID = 999999

HALL_URL = f"{MAIN_URL}{DATA}/{PREF}/{HALL_NAME}{EXTRA_WORD}/"

DAYS_AGO = 1
PERIOD = 1

if __name__ == "__main__":
    
    upgrade_uc_if_needed()
    driver = scraper.start_google_chrome("https://google.com")

    for days_ago in range(DAYS_AGO, DAYS_AGO+PERIOD):
        logger.info("--- Scraping Start ---")
        
        driver.get(HALL_URL)
        driver.execute_script(REMOVE_ADS_SCRIPT)
        
        scraper.click_date_link(driver, days_ago, MAX_RETRIES=MAX_RETRIES)
        driver.execute_script(REMOVE_ADS_SCRIPT)

        date, hall_name = scraper.extract_date_and_hallname(driver)
        df = scraper.extract_and_save_model_data(driver, hall_name, date)
        
        logger.info(f"--- {date}: Scraping End---")
        sleep(5)

    driver.close()
    logger.info("--- Scraping End ---")
    
    ## csvファイル名とデータベース保存時のHallIDに関連性がないため、自動的に保存できない
    logger.info("--- 現在データベース保存は自動化できていません ---")
    # csv_to_sqlite(CSV_FOLDER, DB_PATH, HALL_ID)
    ## csvファイル名とデータベース保存時のHallIDに関連性がないため、自動的に保存できない
    
    