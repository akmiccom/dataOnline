from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from time import sleep
import pandas as pd
import time
import re
from logger_setup import setup_logger


logger = setup_logger("scraper", log_file="py/scraper.log")

def start_google_chrome(url, screen_width=2200, screen_height=1900):
    """
    Undetected Chrome を起動し、指定URLにアクセス
    - ウィンドウを左半分に配置
    - 広告対策オプションを追加
    :param url: 開くURL
    :param screen_width: 画面の横幅 (デフォルト: 2200)
    :param screen_height: 画面の高さ (デフォルト: 1900)
    :return: WebDriver インスタンス
    """

    # Chromeオプションを設定
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")  # セキュリティサンドボックス無効化
    options.add_argument("--disable-blink-features=AutomationControlled")  # ボット検出回避
    options.add_argument("--disable-extensions")  # 拡張機能無効化
    options.add_argument("--disable-popup-blocking")  # ポップアップブロック無効化
    options.add_argument("--disable-gpu")  # GPUレンダリング無効化
    options.add_argument("--start-maximized")  # 最大化
    options.add_argument("--disable-infobars")  # "Chromeは自動テストソフトウェアによって制御されています"を非表示
    options.add_argument("--disable-notifications")  # 通知をブロック
    options.add_argument("--remote-debugging-port=9222")  # デバッグ用ポート

    # Chromeドライバーの起動
    driver = uc.Chrome(options=options)

    # 画面の左半分にウィンドウを配置
    driver.set_window_position(0, 0)  # 左上 (0,0) に配置
    driver.set_window_size(screen_width // 2, screen_height)  # 画面の半分の幅に設定

    # 指定URLを開く
    driver.get(url)
    logger.info("-" * 50)
    logger.info(f"実行成功: {url}")
    driver.implicitly_wait(5)  # 最大30秒待機
    time.sleep(1)  # 読み込み待機

    return driver  # WebDriver インスタンスを返す


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
            logger.info(f"エラー: DAYS_AGO={DAYS_AGO} に対応するリンクがありません。")
            return False

        target_link = date_links[DAYS_AGO - 1]
        logger.info(f"クリック対象リンク: {target_link.text}")

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
                logger.info(
                    f"⚠ Google広告が表示されました。リトライします ({retries}/{MAX_RETRIES})"
                )
                driver.back()  # 広告ページから戻る
                time.sleep(2)
                continue  # リトライ

            logger.info(f"遷移成功: {driver.current_url}")
            date, hall_name = extract_date_hall_from_url(driver.current_url)
            return date, hall_name

    except Exception as e:
        logger.info(f"エラー発生: {e}")

    logger.info("クリック失敗: リトライ回数超過")
    return False  # 失敗


def extract_date_hall_from_url(url):
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
        logger.info(f"日付: {date}")
        logger.info(f"ホール名: {hall_name}")
        return date, hall_name
    else:
        return None, None  # マッチしない場合


def extract_date_and_hallname(driver):
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
        hall_name = hall_match.group(1).replace(" ", "_") if hall_match else None

        logger.info(f"日付: {date}")
        logger.info(f"ホール名: {hall_name}")

        return date, hall_name

    except Exception as e:
        logger.info(f"❌ エラー発生: {e}")
        return False


def extract_and_save_model_data(driver, hall_name, date):
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
            logger.info(f"⚠️ データが見つかりません")
            return False

        logger.info(f"{len(rows)} row 取得開始")
        logger.info("取得中...")

        # ヘッダー取得
        header_cells = rows[0].find_elements(By.TAG_NAME, "th")
        columns = [cell.text for cell in header_cells]

        # データ取得
        data = []
        for row in rows[2:]:  # 最終行を除外
            cells = row.find_elements(By.TAG_NAME, "td")
            data.append([cell.text for cell in cells])
        logger.info(f"{len(data)} data 取得完了")

        # DataFrameを作成しCSVに保存
        df = pd.DataFrame(data, columns=columns)
        file_name = f"csv/{hall_name}_{date}.csv"
        df.to_csv(file_name, index=False, encoding="utf-8-sig")

        logger.info(f"データ保存完了: {file_name}")
        return df

    except Exception as e:
        logger.info(f"エラー発生: {e}")
        return False