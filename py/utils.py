# ============================
# utils.py
# ============================
import subprocess
import sys
import pkg_resources
import requests
from logger_setup import setup_logger

logger = setup_logger("utils")

def upgrade_uc_if_needed():
    try:
        current = pkg_resources.get_distribution("undetected-chromedriver").version
        latest = requests.get("https://pypi.org/pypi/undetected-chromedriver/json").json()["info"]["version"]
        if current != latest:
            logger.info(f"undetected-chromedriver 更新: {current} → {latest}")
            subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "undetected-chromedriver"], check=True)
        else:
            logger.info(f"undetected-chromedriver は最新版 ({current})")
    except Exception as e:
        logger.error(f"バージョン確認失敗: {e}")


if __name__ == "__main__":
    upgrade_uc_if_needed()