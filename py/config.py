REMOVE_ADS_SCRIPT = """
var ads = document.querySelectorAll('[id^="google_ads"], [class*="ads"], [class*="sponsored"]');
ads.forEach(ad => ad.remove());
"""

CSV_FOLDER = "csv"  # CSVファイルがあるフォルダ
DB_PATH = "db/anaslo.db"

MAX_RETRIES = 5

DATA = "ホールデータ"
PREF = "東京都"
HALL_NAME = "exa-first"
EXTRA_WORD = "-データ一覧"
MAIN_URL = f"https://ana-slo.com/"

HALL_URL = f"{MAIN_URL}{DATA}/{PREF}/{HALL_NAME}{EXTRA_WORD}/"

