from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time


REMOVE_ADS_SCRIPT = """
var ads = document.querySelectorAll('[id^="google_ads"], [class*="ads"], [class*="sponsored"]');
ads.forEach(ad => ad.remove());
"""


def start_google_chrome(url, screen_width=2200, screen_height=1900, implicitly_wait=10):
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
    options.add_argument("--disable-infobars")  # "Chromeは自動テスト..."を非表示
    options.add_argument("--disable-notifications")  # 通知をブロック
    options.add_argument("--remote-debugging-port=9222")  # デバッグ用ポート
    # options.add_argument("--force-device-scale-factor=0.90") # 画面スケール調整


    # Chromeドライバーの起動
    driver = uc.Chrome(options=options)

    # ウィンドウを配置
    driver.set_window_position(0, 0)  # 左上 (0,0) に配置
    driver.set_window_size(screen_width // 2, screen_height)  # 画面の半分の幅に設定

    # 指定URLを開く
    driver.get(url)
    print("-" * 50)
    print(f"実行成功: {url}")
    driver.implicitly_wait(implicitly_wait)  # 最大n秒待機
    time.sleep(1)

    return driver