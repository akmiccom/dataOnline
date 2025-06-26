from start_chrome_driver import start_google_chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import re
from logger_setup import setup_logger
from config import LOG_PATH


REMOVE_ADS_SCRIPT = """
var ads = document.querySelectorAll('[id^="google_ads"], [class*="ads"], [class*="sponsored"]');
ads.forEach(ad => ad.remove());
"""

logger = setup_logger("scraper", log_file=LOG_PATH)

def click_date_link(driver, DAYS_AGO, MAX_RETRIES=3):
    """
    指定したDAYS_AGOの日付リンクをクリックし、ページ遷移を確認する
    :param driver: Selenium WebDriver
    :param DAYS_AGO: 過去の日数（1=最新の日付）
    :param MAX_RETRIES: 最大リトライ回数
    :return: 成功した場合はTrue、失敗した場合はFalse
    """
    retries = 0
    current_url = driver.current_url

    try:
        wait = WebDriverWait(driver, 10)
        date_links = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".table-data-cell a"))
        )

        if DAYS_AGO > len(date_links):
            logger.error(f"エラー: DAYS_AGO={DAYS_AGO} に対応するリンクがありません。")
            return False

        target_link = date_links[DAYS_AGO - 1]
        logger.info(f"🗓 Target Date: {target_link.text}")

        while retries < MAX_RETRIES:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});",
                target_link,
            )
            time.sleep(2)  # スクロール後の待機
            wait.until(EC.element_to_be_clickable(target_link))
            time.sleep(2)

            target_link.click()

            wait.until(EC.url_changes(current_url))
            time.sleep(2)

            if "google_vignette" in driver.current_url:
                retries += 1
                logger.info(f"⚠ Google Ads detected. Retrying... ({retries}/{MAX_RETRIES})")
                driver.back()  # 広告ページから戻る
                time.sleep(2)
                continue  # リトライ

            logger.info(f"✅ Navigation successful: {driver.current_url}")
            date, hall_name = extract_date_hall(driver.current_url)
            return date, hall_name

    except Exception as e:
        logger.error(f"エラー発生: {e}")

    logger.error("クリック失敗: リトライ回数超過")
    return False  # 失敗


def extract_date_hall(url):
    """
    URL から日付 (YYYY-MM-DD) とホール名を抽出
    :param url: 解析するURL
    :return: (date, hall_name) のタプル
    """
    pattern = r"https://ana-slo\.com/(\d{4})-(\d{1,2})-(\d{1,2})-(.+)-data/"
    match = re.search(pattern, url)

    if match:
        year, month, day, hall_name = match.groups()
        date = f"{year}-{int(month):02d}-{int(day):02d}"  # 月日をゼロ埋め
        hall_name = hall_name.replace(
            "-", "_"
        ).upper()  # "-" をスペースに置き換えて大文字に
        logger.info(f"🗓 Date extracted from URL: {date}")
        logger.info(f"🏢 Hall extracted from URL: {hall_name}")
        return date, hall_name
    else:
        return None, None  # マッチしない場合


def click_machine_by_name(driver):
    """
    指定した機種名を検索し、該当する要素をクリック
    :param driver: Selenium WebDriver
    :param search_machine: 検索する機種名
    :return: クリック成功: True / 失敗: False
    """
    try:
        title = driver.title
        date_pattern = r"\d{4}/\d{2}/\d{2}"
        date_match = re.search(date_pattern, title)
        date = date_match.group().replace("/", "-") if date_match else None
        hall_pattern = r"\d{4}/\d{2}/\d{2} (.+?) データまとめ"
        hall_match = re.search(hall_pattern, title)
        hall_name = hall_match.group(1) if hall_match else None

        logger.info(f"🗓 Date confirmed on page: {date}")
        logger.info(f"🏢 Hall confirmed on page: {hall_name}")

        return date, hall_name

    except Exception as e:
        logger.error(f"❌ エラー発生: {e}")
        return False


def extract_and_save_model_data(driver, prefecture, hall_name, date, csv_path):
    """
    指定した機種のデータを抽出し、CSVに保存する
    :param driver: Selenium WebDriver
    :param section_num: セクション番号 (該当機種のインデックス)
    :param hall_name: ホール名
    :param date: 日付 (yyyy-mm-dd)
    :return: 成功時 True, 失敗時 False
    """
    try:
        # すべての機種を表示
        button = driver.find_element(By.ID, "all_data_btn")
        button.click()

        # データ行を取得
        rows = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, f"#all_data_table_wrapper > div.dataTables_scroll tr")
            ))

        if not rows:
            logger.error(f"⚠️ データが見つかりません")
            # return False

        logger.info(f"⏬ Starting to fetch {len(rows)} rows...")
        logger.info("📥 Downloading data...")

        # ヘッダー取得
        header_cells = rows[0].find_elements(By.TAG_NAME, "th")
        columns = [cell.text for cell in header_cells]

        # データ取得
        data = []
        for row in rows[2:]:  # 最終行を除外
            cells = row.find_elements(By.TAG_NAME, "td")
            data.append([cell.text for cell in cells])
        logger.info(f"✅ Row {len(data)}: Data fetch completed")

        # DataFrameを作成しCSVに保存
        df = pd.DataFrame(data, columns=columns)
        rename_columns ={"機種名": "model_name", "台番号": "unit_no", "G数": "game", "差枚": "medals"}
        df.rename(columns=rename_columns, inplace=True)
        cols_to_convert = ["unit_no", "game", "medals", "BB", "RB"]
        for col in cols_to_convert:
            if col in df.columns:
                df[col] = df[col].replace(",", "", regex=True).astype(int)
            else:
                df[col] = 0
                
        # 同じ機種を指しているが表記ゆれ対策
        alias_map = {
            "SミスタージャグラーKK": "ミスタージャグラー",
            "S ミスタージャグラー KK": "ミスタージャグラー",
        }
        df["model_name"] = df["model_name"].replace(alias_map)
        
        csv_name = f"{csv_path}{prefecture}_{hall_name}_{date}.csv"
        df.to_csv(csv_name, index=False, encoding="utf-8-sig")

        logger.info(rows[1].text)

        logger.info(f"💾 Data saved: {csv_name}")
        return True

    except Exception as e:
        logger.error(f"エラー発生: {e}")
        return False
    
    
def scraper_for_data(driver, days_ago, REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL, MAX_RETRIES=5, SLEEP_TIME=10):
    
    driver.get(URL)
    driver.execute_script(REMOVE_ADS_SCRIPT)
    driver.execute_script("document.body.style.zoom='80%'")
        
    
    click_date_link(driver, days_ago, MAX_RETRIES=MAX_RETRIES)
    driver.execute_script(REMOVE_ADS_SCRIPT)
    driver.execute_script("document.body.style.zoom='80%'")
    
    waiting_time = 10
    logger.info(f"⏳ Please complete the manual verification within {waiting_time} seconds")
    time.sleep(waiting_time)

    date, hall_name = click_machine_by_name(driver)
    extract_and_save_model_data(driver, PREF, hall_name, date, CSV_PATH)
        
    time.sleep(SLEEP_TIME)
    


if __name__ == "__main__":
    
    CSV_PATH = "anaslo_02/csv/"

    PREF = "東京都"
    HALL_NAME = "exa-first"
    
    PREF = "埼玉県"
    HALL_NAME = "第一プラザ狭山店"
    
    URL = f"https://ana-slo.com/ホールデータ/{PREF}/{HALL_NAME}-データ一覧/"

    DAYS_AGO = 1
    RERIOD = 15

    driver = start_google_chrome("https://www.google.com/")

    for days_ago in range(DAYS_AGO, DAYS_AGO+RERIOD):
        scraper_for_data(driver, days_ago, REMOVE_ADS_SCRIPT, CSV_PATH, PREF, URL)

    driver.close()