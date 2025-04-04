REMOVE_ADS_SCRIPT = """
var ads = document.querySelectorAll('[id^="google_ads"], [class*="ads"], [class*="sponsored"]');
ads.forEach(ad => ad.remove());
"""

CSV_FOLDER = "csv"  # CSVファイルがあるフォルダ
DB_PATH = "db/anaslo.db"

MAX_RETRIES = 5

MAIN_URL = f"https://ana-slo.com/"
DATA = "ホールデータ"
EXTRA_WORD = "-データ一覧"

