# ============================
# utils.py
# ============================
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import subprocess
import sys
import json
import pkg_resources
import requests
from logger_setup import setup_logger
from config import LOG_PATH, JSONF, SPREADSHEET_IDS

logger = setup_logger("utils", log_file=LOG_PATH)


def upgrade_uc_if_needed():
    '''chromedriverバージョン確認'''
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
            logger.info(f"✅ undetected-chromedriver is up-to-date. ({current})")
    except Exception as e:
        logger.error(f"バージョン確認失敗: {e}")


def connect_to_spreadsheet(SPREADSHEET_ID):
    '''スプレッドシート認証設定'''
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    jsonf = JSONF  # あなたの定義済みパス変数

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        logger.info(f"✅ Connected to Google Spreadsheet: {spreadsheet.title}")
        return spreadsheet

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"❌ スプレッドシートが見つかりませんでした。ID: {SPREADSHEET_ID}")
        return None

    except Exception as e:
        logger.exception(f"❌ スプレッドシート接続中に予期せぬエラーが発生しました: {e}")
        return None


def get_or_create_worksheet(spreadsheet, sheet_name, rows, cols):
    '''存在しないシートを自動で作成'''
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        logger.info(f"✅ Sheet '{sheet_name}'  found. Starting update...")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(sheet_name, rows, cols)
        logger.info(f"💾 Sheet '{sheet_name}' updated successfully")
    return worksheet




def get_existing_worksheet(spreadsheet, sheet_name):
    '''シートがなければログを出して中断'''
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        logger.info(f"✅ Sheet '{sheet_name}'  found. Starting update...")
        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"❌ シート '{sheet_name}' が存在しません")
        return None


def log_banner(title: str):
    logger.info("")
    logger.info("=" * 50)
    logger.info("         ANA-SLO Data Collection Started         ")
    logger.info("=" * 50)




jagglar_rate_path = r"C:\python\dataOnline\anaslo_02\json\jagglar_rate.json"
with open(jagglar_rate_path, "r", encoding="utf-8") as f:
    MODEL_DATA_DICT = json.load(f)


if __name__ == "__main__":

    upgrade_uc_if_needed()
    connect_to_spreadsheet(SPREADSHEET_IDS["EXA FIRST"])
