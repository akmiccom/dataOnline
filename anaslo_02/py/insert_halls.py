from start_chrome_driver import start_google_chrome, REMOVE_ADS_SCRIPT
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep
import sqlite3


DATA = "ホールデータ"
URL = f"https://ana-slo.com/{DATA}"
PREF = "愛知県"
DB_NAME = "anaslo_02/anaslo_02.db"


driver = start_google_chrome("https://google.com")
driver.get(URL)
driver.execute_script(REMOVE_ADS_SCRIPT)
# pref_ele = driver.find_elements(By.CLASS_NAME, "pref_list a")

wait = WebDriverWait(driver, 10)
pref_ele = wait.until(
    EC.presence_of_all_elements_located((By.CLASS_NAME, "pref_list a"))
)

retries = 0
# 都道府県選択
for pref in pref_ele:
    if pref.text == PREF:

        while retries < 5:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});",
                pref,
            )
            sleep(2)  # スクロール後の待機
            wait.until(EC.element_to_be_clickable(pref))
            sleep(2)

            pref.click()
            # wait.until(EC.url_changes(driver.current_url))
            sleep(2)

            if "google_vignette" in driver.current_url:
                retries += 1
                print(f"⚠ Google広告が表示されました。リトライします ({retries}/{5})")
                driver.back()  # 広告ページから戻る
                sleep(2)
                continue

        prefecture_name = pref.text
        print(f"都道府県: {prefecture_name}")
        pref.click()
        driver.execute_script(REMOVE_ADS_SCRIPT)
        break


# データベース登録
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

cursor.execute(
    "SELECT prefecture_id FROM prefectures WHERE name = ?", (prefecture_name,)
)
result = cursor.fetchone()
if result is None:
    print(f"❌ 都道府県が見つかりません: {prefecture_name}")

prefecture_id = result[0]
print(f"都道府県ID: {prefecture_id}")

hall_ele = driver.find_elements(By.CLASS_NAME, "table-body a")
for i, h in enumerate(hall_ele):
    cursor.execute(
        """
        INSERT OR IGNORE INTO halls (name, prefecture_id)
        VALUES (?, ?)
        """,
        (h.text.strip(), prefecture_id),
    )

print(f"ホール数: {len(hall_ele)}")
print("ホールデータを登録しました。")
conn.commit()
conn.close()
