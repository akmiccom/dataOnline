from selenium.webdriver import Chrome, ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from subprocess import Popen
from time import sleep


def start_google_chrome(url):

# Chrome start by specifying port
    chrome_path = r'"C:\Program Files\Google\Chrome\Application\chrome.exe" -remote-debugging-port=9222 --user-data-dir="C:\temp"'
    Popen(chrome_path)
    sleep(1)
# Chrome options
    options = ChromeOptions()
    options.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
# Get chrome with specified port
    driver_path = r"C:\python\chromedriver\chromedriver.exe"
    # driver = Chrome(executable_path=driver_path, options=options)

    new_driver = ChromeDriverManager().install()
    service = Service(executable_path=new_driver)
    driver = Chrome(service=service, options=options)

# open url
    driver.get(url)
    driver.implicitly_wait(30)
    sleep(1)
    
    return driver

def start_google_chrome_headless(url):
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--disable-gpu")
    # options.add_argument("--hide-scrollbars")
    # options.add_argument('--window-size=1200,700')
    options.add_argument("--single-process")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--no-sandbox")
    options.add_argument("--homedir=/tmp")
    options.add_argument("--disable-dev-shm-usage")

    new_driver = ChromeDriverManager().install()
    service = Service(executable_path=new_driver)
    driver = Chrome(service=service, options=options)

    # driver = Chrome(r"C:\python\chromedriver\chromedriver.exe", options=options)
    
    driver.get(url)
    
    return driver

    #driver.set_window_position(1400,130)
    #driver.maximize_window()
    # driver.minimize_window()

def driver_quit(driver):
    driver(quit)

def wait(driver, sec,):
    sleep(sec)
    driver.implicitly_wait(10)
    
def click_css(driver, css, sec):
    ele = driver.find_element(By.CSS_SELECTOR, css,)
    ele.click()
    sleep(sec)
    
def click_xpath(driver, xpath, sec):
    ele = driver.find_element(By.XPATH , xpath,)
    ele.click()
    sleep(sec)
    
def input_and_click_css(driver, css, sec, key,):
    ele = driver.find_element(By.CSS_SELECTOR, css,)
    ele.clear()
    ele.send_keys(key)
    ele.send_keys(Keys.ENTER)
    sleep(sec)
    
def input_and_click_xpath(driver, xpath, sec, key,):
    ele = driver.find_element(By.XPATH, xpath,)
    ele.clear()
    ele.send_keys(key)
    ele.send_keys(Keys.ENTER)
    sleep(sec)
    
def javascript_click_css(driver, css, sec,):
    ele = driver.find_element(By.CSS_SELECTOR, css)
    driver.execute_script('arguments[0].click();', ele)
    sleep(sec)
    
def javascript_click_xpath(driver, xpath, sec,):
    ele = driver.find_element(By.XPATH, xpath)
    driver.execute_script('arguments[0].click();', ele)
    sleep(sec)

def new_tab(driver, tabNumber, url, sec,):
    driver.execute_script("window.open()")
    driver.switch_to.window(driver.window_handles[tabNumber])
    driver.get(url)
    sleep(sec)