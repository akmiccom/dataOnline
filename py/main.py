# ============================
# main.py
# ============================
import scraper
from time import sleep
from utils import upgrade_uc_if_needed
from logger_setup import setup_logger
from csv_to_sqlite import csv_to_sqlite
from config import REMOVE_ADS_SCRIPT, MAX_RETRIES, HALL_URL, CSV_FOLDER, DB_PATH

logger = setup_logger("main")

DAYS_AGO = 20
PERIOD = 200
HALL_ID = 101262

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
    
    csv_to_sqlite(CSV_FOLDER, DB_PATH, HALL_ID)
    logger.info("--- Scraping End ---")