# ============================
# utils.py
# ============================
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import subprocess
import sys
import pkg_resources
import requests
from logger_setup import setup_logger
from config import LOG_PATH, JSONF, SPREADSHEET_IDS

logger = setup_logger("utils", log_file=LOG_PATH)


def upgrade_uc_if_needed():
    try:
        current = pkg_resources.get_distribution("undetected-chromedriver").version
        latest = requests.get(
            "https://pypi.org/pypi/undetected-chromedriver/json"
        ).json()["info"]["version"]
        if current != latest:
            logger.info(f"undetected-chromedriver 更新: {current} → {latest}")
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--upgrade",
                    "undetected-chromedriver",
                ],
                check=True,
            )
        else:
            logger.info(f"undetected-chromedriver は最新版です。 ({current})")
    except Exception as e:
        logger.error(f"バージョン確認失敗: {e}")


# スプレッドシート認証設定
def connect_to_spreadsheet(SPREADSHEET_ID):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    jsonf = JSONF
    creds = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    logger.info(f"スプレッドシートに接続: {spreadsheet.title}")
    
    return spreadsheet


def log_banner(title: str):
    logger.info("")
    logger.info("=" * 40)
    logger.info("         ANA-SLO データ収集         ")
    logger.info("=" * 40)


if __name__ == "__main__":

    upgrade_uc_if_needed()
    connect_to_spreadsheet(SPREADSHEET_IDS["EXA FIRST"])
